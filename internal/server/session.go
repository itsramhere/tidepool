package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/auth"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
)

const (
	// helloTimeout is how long the server waits for the first frame.
	helloTimeout = 5 * time.Second
	// maxFrameBytes is the read limit for one WebSocket text frame.
	maxFrameBytes = 1 << 20 // 1 MiB
)

// session is one authenticated WebSocket connection. Frames to the client go
// through a buffered channel drained by the writer goroutine; a full channel
// means a slow consumer and the session is closed (the client will reconnect
// and reconcile).
type session struct {
	id    string
	srv   *Server
	conn  *websocket.Conn
	actor crdt.ActorID
	uid   string
	// auth holds uid plus every JWT claim, evaluated by the authorization
	// rules on the committer goroutine. Written once during the handshake,
	// before the session can subscribe or push, so the committer never
	// races a read.
	auth map[string]any

	writeCh    chan []byte
	writerDone chan struct{}
	closeOnce  sync.Once
	slowOnce   sync.Once
	done       chan struct{}
}

func newSession(srv *Server, conn *websocket.Conn) *session {
	return &session{
		id:         "s" + itoa(sessionSeq.Add(1)),
		srv:        srv,
		conn:       conn,
		writeCh:    make(chan []byte, srv.cfg.SessionSendBuffer),
		writerDone: make(chan struct{}),
		done:       make(chan struct{}),
	}
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// enqueue sends one frame to the writer without ever blocking the committer.
// It reports whether the frame was accepted.
func (s *session) enqueue(frame []byte) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	select {
	case s.writeCh <- frame:
		return true
	default:
		// The buffer is full, so an err frame could not be delivered either;
		// closing the connection is the only signal we can still send.
		s.slowOnce.Do(func() { s.srv.comm.metrics.SlowConsumers.Inc() })
		s.srv.log.Debug("slow consumer", slog.String("session", s.id))
		s.closeConn()
		return false
	}
}

// sendMessage marshals and enqueues one frame.
func (s *session) sendMessage(m any) bool {
	data, err := protocol.Encode(m)
	if err != nil {
		s.srv.log.Error("marshal frame", slog.Any("err", err))
		return false
	}
	return s.enqueue(data)
}

// fail sends a fatal err frame and closes the session, waiting for the
// writer to flush the frame first. Only call from the reader goroutine —
// it must never run on the committer (PLAN.md known pitfall: the committer
// never blocks on a session).
func (s *session) fail(code, msg string) {
	s.sendMessage(protocol.NewError(code, msg))
	s.shutdown(true)
}

// shutdown tears the session down. With flush, it waits for the writer to
// drain the queued frames (so a just-enqueued err frame reaches the client);
// without flush — the path the committer takes for slow consumers — the TCP
// connection is closed immediately. Either way, closing done unblocks the
// writer's in-flight writes, so shutdown cannot deadlock.
func (s *session) shutdown(flush bool) {
	s.closeOnce.Do(func() {
		close(s.done)
		if flush {
			<-s.writerDone
		}
		_ = s.conn.Close(websocket.StatusInternalError, "server closing")
	})
}

// closeConn closes the connection immediately without flushing. Safe to call
// from any goroutine, including the committer.
func (s *session) closeConn() {
	s.shutdown(false)
}

// writeLoop drains the send buffer; each write is bounded so a stuck client
// cannot hold the session open forever.
func (s *session) writeLoop() {
	defer close(s.writerDone)
	for {
		select {
		case frame := <-s.writeCh:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := s.conn.Write(ctx, websocket.MessageText, frame)
			cancel()
			if err != nil {
				s.srv.log.Debug("write failed", slog.String("session", s.id), slog.Any("err", err))
				return
			}
		case <-s.done:
			// Flush whatever is still queued (a fail() frame), then exit.
			for {
				select {
				case frame := <-s.writeCh:
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					_ = s.conn.Write(ctx, websocket.MessageText, frame)
					cancel()
				default:
					return
				}
			}
		}
	}
}

// sendRequest hands a request to the committer unless the server is shutting
// down. The request channel is never closed, so this cannot panic.
func (s *session) sendRequest(req request) bool {
	if s.srv.comm.closed.Load() {
		return false
	}
	select {
	case s.srv.comm.reqs <- req:
		return true
	case <-s.srv.comm.done:
		return false
	}
}

// readLoop authenticates and then serves the connection. It runs on the HTTP
// handler goroutine and returns when the connection dies or the server shuts
// down.
func (s *session) readLoop() {
	defer s.cleanup()
	s.conn.SetReadLimit(maxFrameBytes)

	helloCtx, cancelHello := context.WithTimeout(context.Background(), helloTimeout)
	defer cancelHello()
	authed := false

	for {
		ctx := context.Background()
		if !authed {
			ctx = helloCtx
		}
		typ, data, err := s.conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			s.fail("protocol_error", "frames must be text")
			return
		}
		msg, err := protocol.Decode(data)
		if err != nil {
			s.fail("bad_frame", err.Error())
			return
		}
		if !authed {
			hello, ok := msg.(protocol.Hello)
			if !ok {
				s.fail("protocol_error", "first frame must be hello")
				return
			}
			if hello.V != protocol.ProtocolVersion {
				s.fail("unsupported_version", "protocol version mismatch")
				return
			}
			uid, claims, err := auth.Verify(s.srv.cfg.JWTSecret, hello.Token)
			if err != nil {
				s.fail("unauthorized", "invalid token")
				return
			}
			if err := crdt.ValidateActor(hello.Actor); err != nil {
				s.fail("invalid_actor", "actor id must be 16 lowercase hex chars")
				return
			}
			s.uid = uid
			claims["uid"] = uid // rules see uid alongside every claim
			s.auth = claims
			s.actor = hello.Actor
			authed = true
			s.sendMessage(protocol.NewWelcome(s.srv.cfg.NodeID, s.actor))
			continue
		}

		switch m := msg.(type) {
		case protocol.Hello:
			s.fail("protocol_error", "duplicate hello")
			return
		case protocol.Push:
			if len(m.Ops) > s.srv.cfg.MaxOpsPerPush {
				s.fail("too_many_ops", "push exceeds max_ops_per_push")
				return
			}
			s.sendRequest(&pushReq{sess: s, req: m.Req, uid: s.uid, actor: s.actor, ops: m.Ops})
		case protocol.Sub:
			s.sendRequest(&subscribeReq{sess: s, id: m.ID, coll: m.Coll, filter: m.Filter, known: m.Known})
		case protocol.Unsub:
			s.sendRequest(&unsubscribeReq{sess: s, id: m.ID})
		case protocol.Ping:
			s.sendMessage(protocol.NewPong())
		default:
			s.fail("protocol_error", "unsupported frame type")
			return
		}
	}
}

// cleanup detaches the session from the committer and releases the
// connection. It runs exactly once per session.
func (s *session) cleanup() {
	s.closeConn()
	s.sendRequest(&removeSessionReq{sess: s})
	// Drain anything still buffered so the writer goroutine can exit even
	// though nobody reads s.writeCh again.
	for {
		select {
		case <-s.writeCh:
		default:
			return
		}
	}
}
