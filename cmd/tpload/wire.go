package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
	"strconv"
	"sync"
	"time"
)

func write(ctx context.Context, conn *websocket.Conn, v any) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	data, err := protocol.Encode(v)
	if err != nil {
		return fmt.Errorf("encode frame: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}
func connect(ctx context.Context, o options, actor crdt.ActorID) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, o.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(16 << 20)
	if err := write(ctx, conn, protocol.NewHello(1, o.Token, actor)); err != nil {
		closeSocket(conn)
		return nil, err
	}
	_, data, err := conn.Read(ctx)
	if err != nil {
		closeSocket(conn)
		return nil, fmt.Errorf("welcome: %w", err)
	}
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		closeSocket(conn)
		return nil, fmt.Errorf("decode welcome: %w", err)
	}
	if env.T != "welcome" {
		closeSocket(conn)
		return nil, fmt.Errorf("handshake refused: %s", data)
	}
	return conn, nil
}
func closeSocket(conn *websocket.Conn) {
	if err := conn.CloseNow(); err != nil { /* already closed by reader or context */
	}
}

type loadClient struct {
	conn  *websocket.Conn
	actor crdt.ActorID
	jobs  chan string
	// mu guards pending send timestamps shared by the writer and reader.
	mu      sync.Mutex
	pending map[uint64]time.Time
}

func (c *loadClient) sendLoop(ctx context.Context, stats *statistics) {
	seqs := map[string]uint64{}
	var req, clock uint64
	for {
		select {
		case <-ctx.Done():
			return
		case doc, ok := <-c.jobs:
			if !ok {
				return
			}
			req++
			clock++
			seqs[doc]++
			start := time.Now()
			op := crdt.Op{ID: crdt.OpID{Actor: c.actor, Seq: seqs[doc]}, Key: crdt.DocKey{Coll: "load", Doc: doc}, Lamport: clock, Field: "sentAt", Kind: crdt.KindSet, Value: json.RawMessage(strconv.FormatInt(start.UnixNano(), 10))}
			c.mu.Lock()
			c.pending[req] = start
			c.mu.Unlock()
			stats.mu.Lock()
			stats.sent++
			stats.outstanding++
			stats.mu.Unlock()
			if err := write(ctx, c.conn, protocol.NewPush(req, []crdt.Op{op})); err != nil {
				stats.error()
				return
			}
		}
	}
}

func (c *loadClient) readLoop(ctx context.Context, stats *statistics) {
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			if ctx.Err() == nil {
				stats.error()
			}
			return
		}
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			stats.error()
			return
		}
		switch env.T {
		case protocol.TypeAck:
			var ack protocol.Ack
			if err := json.Unmarshal(data, &ack); err != nil {
				stats.error()
				return
			}
			c.mu.Lock()
			start, ok := c.pending[ack.Req]
			delete(c.pending, ack.Req)
			c.mu.Unlock()
			if !ok {
				stats.error()
				continue
			}
			stats.mu.Lock()
			stats.outstanding--
			if len(ack.Results) == 1 && ack.Results[0].Status == protocol.StatusOK {
				stats.acked++
				stats.ack.add(time.Since(start))
			} else {
				stats.errors++
			}
			stats.mu.Unlock()
		case protocol.TypeOp:
			var frame protocol.OpFrame
			if err := json.Unmarshal(data, &frame); err != nil {
				stats.error()
				return
			}
			if frame.Op.Field != "sentAt" || frame.Op.Kind != crdt.KindSet || frame.Op.ID.Actor == c.actor {
				continue
			}
			stamp, err := strconv.ParseInt(string(frame.Op.Value), 10, 64)
			if err != nil {
				stats.error()
				continue
			}
			d := time.Duration(time.Now().UnixNano() - stamp)
			stats.mu.Lock()
			if d < 0 {
				stats.errors++
			} else {
				stats.fanout.add(d)
			}
			stats.mu.Unlock()
		case protocol.TypeErr:
			stats.error()
			return
		}
	}
}
