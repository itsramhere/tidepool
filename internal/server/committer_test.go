package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/auth"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
	"github.com/itsramhere/tidepool/internal/storage"
)

func mustAck(t *testing.T, conn *websocket.Conn, req uint64) []protocol.AckResult {
	t.Helper()
	for {
		data := recv(t, conn)
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			t.Fatalf("unmarshal envelope: %v", err)
		}
		if env.T == protocol.TypeOp || env.T == protocol.TypeSnapshot || env.T == protocol.TypeEvict {
			continue // broadcast frames (echoes, move-in snaps, move-out evicts)
		}
		if env.T != protocol.TypeAck {
			t.Fatalf("frame type = %q, want ack", env.T)
		}
		var ack protocol.Ack
		if err := json.Unmarshal(data, &ack); err != nil {
			t.Fatalf("unmarshal ack: %v", err)
		}
		if ack.Req != req {
			t.Errorf("ack req = %d, want %d", ack.Req, req)
		}
		return ack.Results
	}
}

// mustEcho consumes the blank-sub echo of a committed op (G2) for an author
// without subscriptions.
func mustEcho(t *testing.T, conn *websocket.Conn) protocol.OpFrame {
	t.Helper()
	data := recvType(t, conn, "op")
	var opf protocol.OpFrame
	if err := json.Unmarshal(data, &opf); err != nil {
		t.Fatal(err)
	}
	if opf.Sub != "" {
		t.Errorf("echo sub = %q, want \"\"", opf.Sub)
	}
	return opf
}

func TestPushAckOk(t *testing.T) {
	ts, srv := newTestServer(t, nil)
	conn := dial(t, ts)
	hello(t, conn, aliceActor)

	send(t, conn, protocol.NewPush(1, []crdt.Op{validSetOp(t, aliceActor, 1, "c1", "title", "hello")}))
	results := mustAck(t, conn, 1)
	if len(results) != 1 || results[0].Status != protocol.StatusOK {
		t.Fatalf("ack = %+v, want one ok result", results)
	}
	// The author (not subscribed) gets a blank-sub echo of its own op.
	mustEcho(t, conn)

	// The op is durable in the store and the feed advanced.
	last, err := srv.store.LastFeedSeq()
	if err != nil {
		t.Fatalf("LastFeedSeq: %v", err)
	}
	if last != 1 {
		t.Errorf("LastFeedSeq = %d, want 1", last)
	}

	// A second op gets the next seq (per-actor contiguity survives).
	send(t, conn, protocol.NewPush(2, []crdt.Op{validIncOp(aliceActor, 2, "c1", 3)}))
	results = mustAck(t, conn, 2)
	if len(results) != 1 || results[0].Status != protocol.StatusOK {
		t.Fatalf("ack = %+v, want one ok result", results)
	}
	mustEcho(t, conn)
}

func TestPushDuplicateAndGap(t *testing.T) {
	ts, _ := newTestServer(t, nil)
	conn := dial(t, ts)
	hello(t, conn, aliceActor)

	send(t, conn, protocol.NewPush(1, []crdt.Op{validSetOp(t, aliceActor, 1, "c1", "title", "hello")}))
	if results := mustAck(t, conn, 1); results[0].Status != protocol.StatusOK {
		t.Fatalf("first push status = %q, want ok", results[0].Status)
	}
	mustEcho(t, conn)

	// Same op again: dup, no state change, no echo.
	send(t, conn, protocol.NewPush(2, []crdt.Op{validSetOp(t, aliceActor, 1, "c1", "title", "hello")}))
	results := mustAck(t, conn, 2)
	if results[0].Status != protocol.StatusDup {
		t.Errorf("duplicate push status = %q, want dup", results[0].Status)
	}

	// A gap (seq 3 without 2): retry, no echo.
	send(t, conn, protocol.NewPush(3, []crdt.Op{validSetOp(t, aliceActor, 3, "c1", "title", "next")}))
	results = mustAck(t, conn, 3)
	if results[0].Status != protocol.StatusRetry {
		t.Errorf("gap push status = %q, want retry", results[0].Status)
	}

	// The missing seq 2 commits fine afterwards.
	send(t, conn, protocol.NewPush(4, []crdt.Op{validSetOp(t, aliceActor, 2, "c1", "title", "again")}))
	results = mustAck(t, conn, 4)
	if results[0].Status != protocol.StatusOK {
		t.Errorf("fill-in push status = %q, want ok", results[0].Status)
	}
	mustEcho(t, conn)
}

func TestPushRejectedBecomesNoop(t *testing.T) {
	ts, _ := newTestServer(t, nil)
	conn := dial(t, ts)
	hello(t, conn, aliceActor)

	// An object value is not a register scalar: rejected, stored as noop.
	bad := crdt.Op{
		ID:      crdt.OpID{Actor: crdt.ActorID(aliceActor), Seq: 1},
		Key:     crdt.DocKey{Coll: "cards", Doc: "c1"},
		Lamport: 1,
		Field:   "title",
		Kind:    crdt.KindSet,
		Value:   jsonValue(t, map[string]any{"nope": 1}),
	}
	send(t, conn, protocol.NewPush(1, []crdt.Op{bad}))
	results := mustAck(t, conn, 1)
	if results[0].Status != protocol.StatusRejected {
		t.Fatalf("status = %q, want rejected", results[0].Status)
	}

	// The echo of the rejected op is its noop version (G2), with a blank sub
	// because the author has no subscription.
	echo := mustEcho(t, conn)
	if echo.Op.ID != bad.ID || echo.Op.Kind != crdt.KindNoop {
		t.Errorf("echo = %+v kind %s, want noop with id %+v", echo.Op, echo.Op.Kind, bad.ID)
	}

	// The noop advanced the VV, so seq 2 commits (the rejected op is not
	// retried forever).
	send(t, conn, protocol.NewPush(2, []crdt.Op{validSetOp(t, aliceActor, 2, "c1", "title", "ok")}))
	if results := mustAck(t, conn, 2); results[0].Status != protocol.StatusOK {
		t.Fatalf("post-rejection push status = %q, want ok", results[0].Status)
	}
	mustEcho(t, conn)
}

func TestPushInvalidNotStored(t *testing.T) {
	ts, srv := newTestServer(t, nil)
	conn := dial(t, ts)
	hello(t, conn, aliceActor)

	// Bad doc key (uppercase collection): not stored at all.
	bad := validSetOp(t, aliceActor, 1, "c1", "title", "x")
	bad.Key = crdt.DocKey{Coll: "Cards", Doc: "c1"}
	send(t, conn, protocol.NewPush(1, []crdt.Op{bad}))
	results := mustAck(t, conn, 1)
	if results[0].Status != protocol.StatusInvalid {
		t.Fatalf("status = %q, want invalid", results[0].Status)
	}

	// G2: invalid ops are not echoed. The seq was not consumed either, so
	// re-pushing with the valid key commits as the actor's seq 1.
	send(t, conn, protocol.NewPush(2, []crdt.Op{validSetOp(t, aliceActor, 1, "c1", "title", "x")}))
	if results := mustAck(t, conn, 2); results[0].Status != protocol.StatusOK {
		t.Fatalf("seq 1 after invalid = %q, want ok", results[0].Status)
	}
	last, err := srv.store.LastFeedSeq()
	if err != nil {
		t.Fatal(err)
	}
	if last != 1 {
		t.Errorf("LastFeedSeq = %d, want 1 (invalid op never stored)", last)
	}
}

func TestActorOwnership(t *testing.T) {
	ts, _ := newTestServer(t, nil)
	alice := dial(t, ts)
	hello(t, alice, aliceActor)
	bob := dial(t, ts)
	helloAs(t, bob, "bob", bobActor)

	// Alice claims her actor and pushes fine.
	send(t, alice, protocol.NewPush(1, []crdt.Op{validSetOp(t, aliceActor, 1, "c1", "title", "by-alice")}))
	if results := mustAck(t, alice, 1); results[0].Status != protocol.StatusOK {
		t.Fatalf("alice push = %q, want ok", results[0].Status)
	}

	// Bob cannot push ops claiming Alice's actor.
	send(t, bob, protocol.NewPush(1, []crdt.Op{validSetOp(t, aliceActor, 1, "c2", "title", "by-bob")}))
	results := mustAck(t, bob, 1)
	if results[0].Status != protocol.StatusInvalid {
		t.Fatalf("bob pushing alice's actor = %q, want invalid", results[0].Status)
	}

	// Bob's own actor works.
	send(t, bob, protocol.NewPush(2, []crdt.Op{validSetOp(t, bobActor, 1, "c2", "title", "by-bob")}))
	if results := mustAck(t, bob, 2); results[0].Status != protocol.StatusOK {
		t.Fatalf("bob push own actor = %q, want ok", results[0].Status)
	}
}

func TestTwoSessionsFanout(t *testing.T) {
	ts, _ := newTestServer(t, nil)
	alice := dial(t, ts)
	hello(t, alice, aliceActor)
	bob := dial(t, ts)
	hello(t, bob, bobActor)

	// Bob subscribes to the whole collection.
	send(t, bob, protocol.NewSub("b-sub", "cards", "", nil))
	ready := recvType(t, bob, "ready")
	var r protocol.Ready
	if err := json.Unmarshal(ready, &r); err != nil {
		t.Fatal(err)
	}
	if r.Sub != "b-sub" {
		t.Errorf("ready sub = %q, want b-sub", r.Sub)
	}

	// Alice (not subscribed) pushes; Bob's subscription has no copy of c1,
	// so the first op moves the doc into his shape as a snapshot, and Alice
	// receives the op as a sub:"" echo (G2).
	send(t, alice, protocol.NewPush(1, []crdt.Op{validSetOp(t, aliceActor, 1, "c1", "title", "from-alice")}))
	if results := mustAck(t, alice, 1); results[0].Status != protocol.StatusOK {
		t.Fatalf("push = %q, want ok", results[0].Status)
	}

	snapFrame := recvType(t, bob, "snap")
	var snapF protocol.SnapshotFrame
	if err := json.Unmarshal(snapFrame, &snapF); err != nil {
		t.Fatal(err)
	}
	if snapF.Sub != "b-sub" {
		t.Errorf("move-in snap sub = %q, want b-sub", snapF.Sub)
	}

	// A second op on the now-known doc streams as an op frame.
	send(t, alice, protocol.NewPush(2, []crdt.Op{validSetOp(t, aliceActor, 2, "c1", "title", "again")}))
	if results := mustAck(t, alice, 2); results[0].Status != protocol.StatusOK {
		t.Fatalf("push 2 = %q, want ok", results[0].Status)
	}
	frame := recvType(t, bob, "op")
	var bobFrame protocol.OpFrame
	if err := json.Unmarshal(frame, &bobFrame); err != nil {
		t.Fatal(err)
	}
	if bobFrame.Sub != "b-sub" || bobFrame.Op.Field != "title" {
		t.Errorf("bob frame = sub %q field %q, want b-sub/title", bobFrame.Sub, bobFrame.Op.Field)
	}

	echo := recvType(t, alice, "op")
	var aliceFrame protocol.OpFrame
	if err := json.Unmarshal(echo, &aliceFrame); err != nil {
		t.Fatal(err)
	}
	if aliceFrame.Sub != "" {
		t.Errorf("author echo sub = %q, want \"\"", aliceFrame.Sub)
	}
}

func TestSubscribedAuthorGetsSubEchoNotBlank(t *testing.T) {
	ts, _ := newTestServer(t, nil)
	alice := dial(t, ts)
	hello(t, alice, aliceActor)
	send(t, alice, protocol.NewSub("a-sub", "cards", "", nil))
	recvType(t, alice, "ready")

	send(t, alice, protocol.NewPush(1, []crdt.Op{validSetOp(t, aliceActor, 1, "c1", "title", "x")}))
	if results := mustAck(t, alice, 1); results[0].Status != protocol.StatusOK {
		t.Fatalf("push = %q, want ok", results[0].Status)
	}
	// The doc is new to the author's subscription, so it moves in as a
	// snapshot; the op itself is still echoed exactly once with sub:"" (G2).
	snapFrame := recvType(t, alice, "snap")
	var snapF protocol.SnapshotFrame
	if err := json.Unmarshal(snapFrame, &snapF); err != nil {
		t.Fatal(err)
	}
	if snapF.Sub != "a-sub" {
		t.Errorf("move-in snap sub = %q, want a-sub", snapF.Sub)
	}
	frame := recvType(t, alice, "op")
	var opf protocol.OpFrame
	if err := json.Unmarshal(frame, &opf); err != nil {
		t.Fatal(err)
	}
	if opf.Sub != "" {
		t.Errorf("echo sub = %q, want blank (snapshot already carried the state)", opf.Sub)
	}

	// A second op on the known doc echoes through the subscription.
	send(t, alice, protocol.NewPush(2, []crdt.Op{validSetOp(t, aliceActor, 2, "c1", "title", "y")}))
	if results := mustAck(t, alice, 2); results[0].Status != protocol.StatusOK {
		t.Fatalf("push 2 = %q, want ok", results[0].Status)
	}
	frame2 := recvType(t, alice, "op")
	var opf2 protocol.OpFrame
	if err := json.Unmarshal(frame2, &opf2); err != nil {
		t.Fatal(err)
	}
	if opf2.Sub != "a-sub" {
		t.Errorf("echo sub = %q, want a-sub", opf2.Sub)
	}
}

// seedDoc commits ops straight to the store, before any session connects.
func seedDoc(t *testing.T, s storage.Store, docID string, actor string, n int) []crdt.Op {
	t.Helper()
	key := crdt.DocKey{Coll: "cards", Doc: docID}
	sch, ok := testSchemas.Schema("cards")
	if !ok {
		t.Fatal("cards schema missing")
	}
	doc := crdt.NewDocument(key)
	ed := &crdt.Editor{Actor: crdt.ActorID(actor), Doc: doc, Schema: sch}
	var ops []crdt.Op
	for i := 0; i < n; i++ {
		created, err := ed.Set("title", string(rune('a'+i)))
		if err != nil {
			t.Fatalf("seed edit: %v", err)
		}
		ops = append(ops, created...)
	}
	stored := make([]storage.StoredOp, len(ops))
	for i, op := range ops {
		stored[i] = storage.StoredOp{Op: op, DocSeq: uint64(i + 1), FeedSeq: uint64(i + 1)}
	}
	if err := s.Commit(&storage.Batch{
		Ops:   stored,
		Metas: map[crdt.DocKey]storage.DocMeta{key: {LastDocSeq: uint64(n)}},
		Sync:  true,
	}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	return ops
}

func applySeed(t *testing.T, doc *crdt.Document, ops []crdt.Op) {
	t.Helper()
	sch, ok := testSchemas.Schema("cards")
	if !ok {
		t.Fatal("cards schema missing")
	}
	for i, op := range ops {
		if err := doc.Apply(op, sch); err != nil {
			t.Fatalf("apply seed op %d: %v", i, err)
		}
	}
}

func TestSubscribeReconcile(t *testing.T) {
	ts, srv := newTestServer(t, nil)
	ops := seedDoc(t, srv.store, "c1", aliceActor, 3)

	t.Run("empty known gets snapshot", func(t *testing.T) {
		conn := dial(t, ts)
		hello(t, conn, bobActor)
		send(t, conn, protocol.NewSub("s", "cards", "", nil))
		snap := recvType(t, conn, "snap")
		var sf protocol.SnapshotFrame
		if err := json.Unmarshal(snap, &sf); err != nil {
			t.Fatal(err)
		}
		if sf.Sub != "s" {
			t.Errorf("snap sub = %q, want s", sf.Sub)
		}
		want := crdt.NewDocument(crdt.DocKey{Coll: "cards", Doc: "c1"})
		applySeed(t, want, ops)
		raw, err := want.MarshalSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		var got, wantMap map[string]any
		if err := json.Unmarshal(sf.Snapshot, &got); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &wantMap); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, wantMap) {
			t.Error("snapshot does not match the seeded document")
		}
		recvType(t, conn, "ready")
	})

	t.Run("partial known gets only missing ops", func(t *testing.T) {
		conn := dial(t, ts)
		hello(t, conn, bobActor)
		known := map[string]protocol.VersionVector{
			"c1": {crdt.ActorID(aliceActor): 2},
		}
		send(t, conn, protocol.NewSub("s", "cards", "", known))
		// Exactly one op (seq 3) must arrive, then ready — no snap.
		frame := recvType(t, conn, "op")
		var opf protocol.OpFrame
		if err := json.Unmarshal(frame, &opf); err != nil {
			t.Fatal(err)
		}
		if opf.Op.ID.Seq != 3 || opf.Sub != "s" {
			t.Errorf("reconciled op = seq %d sub %q, want seq 3 sub s", opf.Op.ID.Seq, opf.Sub)
		}
		recvType(t, conn, "ready")
	})

	t.Run("unknown doc is evicted", func(t *testing.T) {
		conn := dial(t, ts)
		hello(t, conn, bobActor)
		known := map[string]protocol.VersionVector{
			"ghost": {crdt.ActorID(aliceActor): 1},
		}
		send(t, conn, protocol.NewSub("s", "cards", "", known))
		// c1 exists and is unknown to the client: snapshot first.
		recvType(t, conn, "snap")
		ev := recvType(t, conn, "evict")
		var e protocol.Evict
		if err := json.Unmarshal(ev, &e); err != nil {
			t.Fatal(err)
		}
		if e.Sub != "s" || e.Key.Doc != "ghost" {
			t.Errorf("evict = %+v, want ghost", e)
		}
		recvType(t, conn, "ready")
	})

	t.Run("fully known doc gets only ready", func(t *testing.T) {
		conn := dial(t, ts)
		hello(t, conn, bobActor)
		known := map[string]protocol.VersionVector{
			"c1": {crdt.ActorID(aliceActor): 3},
		}
		send(t, conn, protocol.NewSub("s", "cards", "", known))
		recvType(t, conn, "ready")
	})
}

// dialTinyBuffer dials with a 4 KiB TCP receive buffer, so a client that
// stops reading backs up the server's writer quickly and deterministically
// (kernel loopback autotuning would otherwise absorb megabytes).
func dialTinyBuffer(t *testing.T, ts *httptest.Server) *websocket.Conn {
	t.Helper()
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err == nil {
					if tcp, ok := c.(*net.TCPConn); ok {
						_ = tcp.SetReadBuffer(4096)
					}
				}
				return c, err
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL(ts), &websocket.DialOptions{HTTPClient: client})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.SetReadLimit(maxFrameBytes)
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

// tinySendListener pins the server side of every accepted connection to a
// 4 KiB send buffer. Linux autotunes the send buffer past the whole ~1.8 MiB
// slow-consumer flood (tcp_wmem defaults allow ~4 MiB), so the server's
// writer would never block and the bounded send queue would never fill;
// with the buffer pinned, a stalled peer blocks the writer within a few KiB
// on every platform.
type tinySendListener struct {
	net.Listener
}

func (l tinySendListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		if tcp, ok := c.(*net.TCPConn); ok {
			_ = tcp.SetWriteBuffer(4096)
		}
	}
	return c, err
}

func TestSlowConsumerDisconnected(t *testing.T) {
	ts, srv := newTestServerOnListener(t, testSchemas, func(cfg *Config) { cfg.SessionSendBuffer = 2 },
		func(l net.Listener) net.Listener { return tinySendListener{l} })
	reader := dialTinyBuffer(t, ts)
	hello(t, reader, bobActor)
	send(t, reader, protocol.NewSub("s", "cards", "", nil))
	recvType(t, reader, "ready")

	pusher := dial(t, ts)
	hello(t, pusher, aliceActor)

	// A small op first: its ack plus author echo exactly fill the 2-slot
	// send queue and drain through the pinned 4 KiB server send buffer, so
	// ordinary traffic still works end to end. It seeds doc "big" so the
	// flood's seqs stay contiguous for that document's version vector.
	send(t, pusher, protocol.NewPush(1, []crdt.Op{validSetOp(t, aliceActor, 1, "big", "title", "hi")}))
	if results := mustAck(t, pusher, 1); results[0].Status != protocol.StatusOK {
		t.Fatalf("small push = %q, want ok", results[0].Status)
	}

	// One push of 15 ops with ~60 KiB values each toward a client that
	// stopped reading with a 4 KiB TCP buffer: the reader's writer blocks
	// inside the first 60 KiB frame (the pinned socket buffers hold only a
	// few KiB), the 2-slot send queue fills, and the next enqueue closes the
	// session.
	//
	// The pusher must not mustAck this push: its own 60 KiB G2 echoes fill
	// the same 2-slot queue and closeConn discards the queued ack. Wait for
	// the commit instead — the broadcast, and with it the reader's
	// disconnect, follows immediately.
	big := strings.Repeat("x", 60*1024)
	ops := make([]crdt.Op, 0, 15)
	for i := 0; i < 15; i++ {
		ops = append(ops, validSetOp(t, aliceActor, uint64(i+2), "big", "title", big))
	}
	send(t, pusher, protocol.NewPush(2, ops))
	for deadline := time.Now().Add(5 * time.Second); ; {
		last, err := srv.store.LastFeedSeq()
		if err != nil {
			t.Fatalf("LastFeedSeq: %v", err)
		}
		if last == 16 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("LastFeedSeq = %d after flood push, want 16", last)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// The reader session gets closed. Data already flushed into kernel
	// buffers before the close is still deliverable, so read until an error
	// (the close) rather than expecting the very next read to fail.
	readErr := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _, err := reader.Read(ctx)
			cancel()
			if err != nil {
				readErr <- nil // closed as expected
				return
			}
		}
		readErr <- errors.New("reader never saw the disconnect")
	}()
	select {
	case err := <-readErr:
		if err != nil {
			t.Fatalf("slow consumer not disconnected: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for slow consumer disconnect")
	}
}

// rawDial/rawSend/rawReadAck are goroutine-safe variants without *testing.T,
// used by the concurrent group-commit test.
func rawDial(ts *httptest.Server) (*websocket.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL(ts), nil)
	return conn, err
}

func rawSend(conn *websocket.Conn, m any) error {
	data, err := protocol.Encode(m)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, data)
}

// rawReadAck waits for the ack of one push, skipping broadcast frames
// (op echoes, move-in snaps, move-out evicts) that the server interleaves
// with acks (G1/G2).
func rawReadAck(conn *websocket.Conn, req uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if typ != websocket.MessageText {
			return fmt.Errorf("non-text frame")
		}
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			return err
		}
		if env.T == protocol.TypeOp || env.T == protocol.TypeSnapshot || env.T == protocol.TypeEvict {
			continue // broadcast frames
		}
		var ack protocol.Ack
		if err := json.Unmarshal(data, &ack); err != nil {
			return err
		}
		if ack.Req != req {
			return fmt.Errorf("ack req = %d, want %d", ack.Req, req)
		}
		for _, r := range ack.Results {
			if r.Status != protocol.StatusOK {
				return fmt.Errorf("op %v status %q reason %q", r.ID, r.Status, r.Reason)
			}
		}
		return nil
	}
}

func TestGroupCommit(t *testing.T) {
	ts, srv := newTestServer(t, nil)
	const workers = 50
	const pushesPerWorker = 10

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			actor := workerActor(w)
			conn, err := rawDial(ts)
			if err != nil {
				errs <- fmt.Errorf("worker %d dial: %w", w, err)
				return
			}
			defer conn.Close(websocket.StatusNormalClosure, "")
			if err := rawSend(conn, protocol.NewHello(1, workerToken(), actor)); err != nil {
				errs <- fmt.Errorf("worker %d hello: %w", w, err)
				return
			}
			if err := rawReadWelcome(conn); err != nil {
				errs <- fmt.Errorf("worker %d welcome: %w", w, err)
				return
			}
			<-start
			for i := 0; i < pushesPerWorker; i++ {
				op := crdt.Op{
					ID:      crdt.OpID{Actor: actor, Seq: uint64(i + 1)},
					Key:     crdt.DocKey{Coll: "cards", Doc: workerDoc(w)},
					Lamport: uint64(i + 1),
					Field:   "title",
					Kind:    crdt.KindSet,
					Value:   jsonValue(t, fmt.Sprintf("v%d", i)),
				}
				if err := rawSend(conn, protocol.NewPush(uint64(i+1), []crdt.Op{op})); err != nil {
					errs <- fmt.Errorf("worker %d push %d: %w", w, i, err)
					return
				}
				if err := rawReadAck(conn, uint64(i+1)); err != nil {
					errs <- fmt.Errorf("worker %d ack %d: %w", w, i, err)
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	const total = workers * pushesPerWorker
	if got := srv.comm.batches.Load(); got >= total {
		t.Errorf("batches = %d, want < %d (group commit did not batch)", got, total)
	}
	if got := srv.comm.batches.Load(); got == 0 {
		t.Error("batches = 0, want at least one commit")
	}
	last, err := srv.store.LastFeedSeq()
	if err != nil {
		t.Fatalf("LastFeedSeq: %v", err)
	}
	if last != total {
		t.Errorf("LastFeedSeq = %d, want %d", last, total)
	}
}

// workerToken issues a token without touching *testing.T (goroutine-safe).
func workerToken() string {
	token, err := auth.Issue(testJWTSecret, "alice", time.Minute)
	if err != nil {
		panic(err) // cannot happen with a non-empty uid and secret
	}
	return token
}

func workerActor(w int) crdt.ActorID {
	return crdt.ActorID(fmt.Sprintf("%016x", w+1))
}

func workerDoc(w int) string {
	return fmt.Sprintf("w%d", w)
}

func rawReadWelcome(conn *websocket.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	if typ != websocket.MessageText {
		return fmt.Errorf("non-text frame")
	}
	var w protocol.Welcome
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	if w.T != protocol.TypeWelcome {
		return fmt.Errorf("first frame = %q, want welcome", w.T)
	}
	return nil
}
