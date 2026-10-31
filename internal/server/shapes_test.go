package server

import (
	"encoding/json"
	"testing"

	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
	"github.com/itsramhere/tidepool/internal/schema"
)

// shapesSchemas has no rules at all: shape membership comes purely from the
// subscription filter, keeping the M8 test focused on move-in/move-out.
var shapesSchemas = func() *schema.Config {
	cfg, err := schema.Parse([]byte(`collections:
  cards:
    fields:
      title: register
      status: register
`))
	if err != nil {
		panic(err)
	}
	return cfg
}()

// wireFrame is one received frame reduced to what shape assertions need.
type wireFrame struct {
	typ string // ack / op / snap / evict / err / ready / welcome
	sub string
	doc string
}

func readWireFrame(t *testing.T, conn *websocket.Conn) wireFrame {
	t.Helper()
	data := recv(t, conn)
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal frame %s: %v", data, err)
	}
	f := wireFrame{typ: env.T}
	switch env.T {
	case protocol.TypeOp:
		var of protocol.OpFrame
		if err := json.Unmarshal(data, &of); err != nil {
			t.Fatal(err)
		}
		f.sub, f.doc = of.Sub, of.Op.Key.Doc
	case protocol.TypeSnapshot:
		var sf protocol.SnapshotFrame
		if err := json.Unmarshal(data, &sf); err != nil {
			t.Fatal(err)
		}
		f.sub = sf.Sub
		var raw struct {
			Key struct {
				Doc string `json:"doc"`
			} `json:"key"`
		}
		if err := json.Unmarshal(sf.Snapshot, &raw); err != nil {
			t.Fatal(err)
		}
		f.doc = raw.Key.Doc
	case protocol.TypeEvict:
		var ef protocol.Evict
		if err := json.Unmarshal(data, &ef); err != nil {
			t.Fatal(err)
		}
		f.sub, f.doc = ef.Sub, ef.Key.Doc
	case protocol.TypeErr:
		var ef protocol.Error
		if err := json.Unmarshal(data, &ef); err != nil {
			t.Fatal(err)
		}
		f.sub, f.doc = ef.Sub, ef.Code
	case protocol.TypeReady:
		var rf protocol.Ready
		if err := json.Unmarshal(data, &rf); err != nil {
			t.Fatal(err)
		}
		f.sub = rf.Sub
	}
	return f
}

// expect consumes one frame and requires the given shape.
func expect(t *testing.T, conn *websocket.Conn, typ, sub, doc string) wireFrame {
	t.Helper()
	f := readWireFrame(t, conn)
	if f.typ != typ || f.sub != sub || f.doc != doc {
		t.Fatalf("frame = %+v, want %s/%s/%s", f, typ, sub, doc)
	}
	return f
}

// TestShapeLifecycle covers PLAN.md M8 task 5: a filtered subscriber gets a
// snapshot when a doc moves in, ops while it stays, an op then an evict when
// it moves out (the author's G2 echo riding the op), and a snapshot again on
// reopen; the unfiltered subscriber sees only ops throughout.
func TestShapeLifecycle(t *testing.T) {
	ts, _ := newTestServerWithSchemas(t, shapesSchemas, nil)

	// The author holds two subscriptions: unfiltered and open-filtered —
	// the latter is the one that must receive the closing op (its G2 echo)
	// before the evict. Carol is the unfiltered bystander, Bob the filtered
	// one.
	owner := dial(t, ts)
	hello(t, owner, aliceActor)
	send(t, owner, protocol.NewSub("a-all", "cards", "", nil))
	send(t, owner, protocol.NewSub("a-open", "cards", `doc.status == "open"`, nil))
	expect(t, owner, "ready", "a-all", "")
	expect(t, owner, "ready", "a-open", "")

	bob := dial(t, ts)
	hello(t, bob, bobActor)
	send(t, bob, protocol.NewSub("b-open", "cards", `doc.status == "open"`, nil))
	expect(t, bob, "ready", "b-open", "")

	carol := dial(t, ts)
	hello(t, carol, "cccccccccccccccc")
	send(t, carol, protocol.NewSub("c-all", "cards", "", nil))
	expect(t, carol, "ready", "c-all", "")

	// Create card1 as open, then edit it: move-in snap first, then ops.
	send(t, owner, protocol.NewPush(1, []crdt.Op{
		validSetOp(t, aliceActor, 1, "card1", "status", "open"),
		validSetOp(t, aliceActor, 2, "card1", "title", "hello"),
	}))
	mustAck(t, owner, 1)

	// Owner's own subs moved the doc in too, plus the blank-sub G2 echo for
	// the first op (snapshots are not op frames).
	expect(t, owner, "snap", "a-all", "card1")
	expect(t, owner, "snap", "a-open", "card1")
	expect(t, owner, "op", "", "card1")
	expect(t, owner, "op", "a-all", "card1")
	expect(t, owner, "op", "a-open", "card1")
	expect(t, bob, "snap", "b-open", "card1")
	expect(t, bob, "op", "b-open", "card1")
	expect(t, carol, "snap", "c-all", "card1")
	expect(t, carol, "op", "c-all", "card1")

	// Closing the card moves it out of the open shapes: op THEN evict. The
	// unfiltered subscribers just see the op.
	send(t, owner, protocol.NewPush(3, []crdt.Op{
		validSetOp(t, aliceActor, 3, "card1", "status", "closed"),
	}))
	mustAck(t, owner, 3)
	expect(t, owner, "op", "a-all", "card1")
	expect(t, owner, "op", "a-open", "card1")    // G2 echo via the sub...
	expect(t, owner, "evict", "a-open", "card1") // ...before the evict
	expect(t, bob, "op", "b-open", "card1")
	expect(t, bob, "evict", "b-open", "card1")
	expect(t, carol, "op", "c-all", "card1")

	// Reopening moves it back in as a snapshot.
	send(t, owner, protocol.NewPush(4, []crdt.Op{
		validSetOp(t, aliceActor, 4, "card1", "status", "open"),
	}))
	mustAck(t, owner, 4)
	expect(t, owner, "op", "a-all", "card1")
	expect(t, owner, "snap", "a-open", "card1")
	expect(t, bob, "snap", "b-open", "card1")
	expect(t, carol, "op", "c-all", "card1")
}

// TestBadFilterKeepsSessionAlive: an invalid filter fails only that
// subscription via err{sub, bad_filter}; the connection and later
// subscriptions keep working.
func TestBadFilterKeepsSessionAlive(t *testing.T) {
	ts, _ := newTestServerWithSchemas(t, shapesSchemas, nil)
	conn := dial(t, ts)
	hello(t, conn, aliceActor)

	send(t, conn, protocol.NewSub("bad", "cards", `doc.status ==`, nil))
	f := expect(t, conn, "err", "bad", "bad_filter")

	// The session is still alive: a valid subscription readies, and a push
	// is acked.
	send(t, conn, protocol.NewSub("good", "cards", "", nil))
	expect(t, conn, "ready", "good", "")
	send(t, conn, protocol.NewPush(1, []crdt.Op{validSetOp(t, aliceActor, 1, "c1", "title", "x")}))
	if results := mustAck(t, conn, 1); results[0].Status != protocol.StatusOK {
		t.Fatalf("push after bad filter = %q, want ok", results[0].Status)
	}
	_ = f
}
