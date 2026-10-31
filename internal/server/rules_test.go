package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
	"github.com/itsramhere/tidepool/internal/schema"
)

// drainUntilReady reads frames until the subscription's ready frame,
// collecting the docs snapshots were sent for.
func drainUntilReady(t *testing.T, conn *websocket.Conn) []string {
	t.Helper()
	var docs []string
	for {
		data := recv(t, conn)
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			t.Fatal(err)
		}
		if env.T == "ready" {
			return docs
		}
		if env.T == "snap" {
			var f protocol.SnapshotFrame
			if err := json.Unmarshal(data, &f); err != nil {
				t.Fatal(err)
			}
			var snap struct {
				Key struct {
					Doc string `json:"doc"`
				} `json:"key"`
			}
			if err := json.Unmarshal(f.Snapshot, &snap); err != nil {
				t.Fatal(err)
			}
			docs = append(docs, snap.Key.Doc)
		}
	}
}

// rulesSchemas is the PLAN.md M7 example: cards readable only for boards in
// the caller's claim; writes by the owner, plus shared fields for everyone;
// field creation requires claiming ownership.
var rulesSchemas = func() *schema.Config {
	cfg, err := schema.Parse([]byte(`collections:
  cards:
    fields:
      title: register
      owner: register
      board: register
      votes: counter
      labels: set
    rules:
      read: "doc.board in auth.boards"
      write: "isNew ? newDoc.owner == auth.uid : (doc.owner == auth.uid || op.field in ['votes', 'labels'])"
`))
	if err != nil {
		panic(err)
	}
	return cfg
}()

// helloWithClaims authenticates a session for uid with extra JWT claims.
func helloWithClaims(t *testing.T, conn *websocket.Conn, uid string, claims map[string]any, actor string) {
	t.Helper()
	token := mustTokenWithClaims(t, uid, claims)
	send(t, conn, protocol.NewHello(1, token, crdt.ActorID(actor)))
	welcome := recvType(t, conn, "welcome")
	var w protocol.Welcome
	if err := json.Unmarshal(welcome, &w); err != nil {
		t.Fatal(err)
	}
}

// mustAckResult finds the result for one op in an ack frame.
func mustAckResult(t *testing.T, conn *websocket.Conn, req uint64, op crdt.Op) protocol.AckResult {
	t.Helper()
	for _, r := range mustAck(t, conn, req) {
		if r.ID == op.ID {
			return r
		}
	}
	t.Fatalf("ack %d has no result for op %v", req, op.ID)
	return protocol.AckResult{}
}

// TestWriteRuleDeniedBecomesNoop: Bob's title write to Alice's card is
// rejected with reason rule_denied, stored as a noop, and echoed back as
// that noop (G2) — every replica converges on "this op did nothing".
func TestWriteRuleDeniedBecomesNoop(t *testing.T) {
	ts, srv := newTestServerWithSchemas(t, rulesSchemas, nil)

	alice := dial(t, ts)
	helloWithClaims(t, alice, "alice", map[string]any{"boards": []string{"b1"}}, aliceActor)
	send(t, alice, protocol.NewSub("s", "cards", "", nil))
	recvType(t, alice, "ready")

	// Alice creates the card: owner first (field creation must claim
	// ownership), then board, title, and the shared counter.
	ops := []crdt.Op{
		validSetOp(t, aliceActor, 1, "card1", "owner", "alice"),
		validSetOp(t, aliceActor, 2, "card1", "board", "b1"),
		validSetOp(t, aliceActor, 3, "card1", "title", "alice's"),
		validIncOp(aliceActor, 4, "card1", 1),
	}
	send(t, alice, protocol.NewPush(1, ops))
	for _, r := range mustAck(t, alice, 1) {
		if r.Status != protocol.StatusOK {
			t.Fatalf("alice op %v = %q (%s), want ok", r.ID, r.Status, r.Reason)
		}
	}

	bob := dial(t, ts)
	helloWithClaims(t, bob, "bob", map[string]any{"boards": []string{"b1"}}, bobActor)
	send(t, bob, protocol.NewSub("s", "cards", "", nil))
	if docs := drainUntilReady(t, bob); len(docs) != 1 || docs[0] != "card1" {
		t.Fatalf("bob reconcile snapshots = %v, want only card1", docs)
	}

	// Bob's title write is denied by the write rule.
	stolen := validSetOp(t, bobActor, 1, "card1", "title", "stolen")
	send(t, bob, protocol.NewPush(1, []crdt.Op{stolen}))
	result := mustAckResult(t, bob, 1, stolen)
	if result.Status != protocol.StatusRejected || result.Reason != "rule_denied" {
		t.Fatalf("bob title write = %+v, want rejected rule_denied", result)
	}
	// G2: the noop version of the op is echoed back.
	echo := recvType(t, bob, "op")
	var f protocol.OpFrame
	if err := json.Unmarshal(echo, &f); err != nil {
		t.Fatal(err)
	}
	if f.Op.ID != stolen.ID || f.Op.Kind != crdt.KindNoop {
		t.Fatalf("echo = %v kind %s, want noop of %v", f.Op.ID, f.Op.Kind, stolen.ID)
	}

	// The denial is durable: a noop was stored, so the feed advanced.
	last, err := srv.store.LastFeedSeq()
	if err != nil {
		t.Fatalf("LastFeedSeq: %v", err)
	}
	if last != 5 { // four alice ops + bob's noop
		t.Fatalf("LastFeedSeq = %d, want 5", last)
	}

	// Bob may still write the shared counter field.
	inc := validIncOp(bobActor, 2, "card1", 5)
	send(t, bob, protocol.NewPush(2, []crdt.Op{inc}))
	if r := mustAckResult(t, bob, 2, inc); r.Status != protocol.StatusOK {
		t.Fatalf("bob votes inc = %+v, want ok", r)
	}
}

// TestReadRuleFiltersSubscribeAndBroadcast: a session never receives docs
// whose board is outside its claim — neither at subscribe (no snapshot) nor
// via broadcast (no op frames) — while its own ops still echo (G2).
func TestReadRuleFiltersSubscribeAndBroadcast(t *testing.T) {
	ts, _ := newTestServerWithSchemas(t, rulesSchemas, nil)

	alice := dial(t, ts)
	helloWithClaims(t, alice, "alice", map[string]any{"boards": []string{"b1", "b2"}}, aliceActor)
	send(t, alice, protocol.NewSub("s", "cards", "", nil))
	recvType(t, alice, "ready")

	// Owner must come before board on each card (field creation claims
	// ownership per the write rule).
	card1Owner := validSetOp(t, aliceActor, 1, "card1", "owner", "alice")
	card1 := validSetOp(t, aliceActor, 2, "card1", "board", "b1")
	card2Owner := validSetOp(t, aliceActor, 1, "card2", "owner", "alice")
	card2 := validSetOp(t, aliceActor, 2, "card2", "board", "b2")
	send(t, alice, protocol.NewPush(1, []crdt.Op{card1Owner, card1, card2Owner, card2}))
	if results := mustAck(t, alice, 1); results[0].Status != protocol.StatusOK || results[3].Status != protocol.StatusOK {
		t.Fatalf("card ops = %+v, want ok", results)
	}

	// Alice creates a title on card2; Bob must not receive it.
	title2 := validSetOp(t, aliceActor, 3, "card2", "title", "hidden")
	send(t, alice, protocol.NewPush(2, []crdt.Op{title2}))
	mustAck(t, alice, 2)

	bob := dial(t, ts)
	helloWithClaims(t, bob, "bob", map[string]any{"boards": []string{"b1"}}, bobActor)
	send(t, bob, protocol.NewSub("s", "cards", "", nil))

	// Reconcile: only card1's snapshot arrives before ready.
	if docs := drainUntilReady(t, bob); len(docs) != 1 || docs[0] != "card1" {
		t.Fatalf("bob reconcile snapshots = %v, want only card1", docs)
	}

	// Broadcast: the card1 op arrives...
	visible := validSetOp(t, aliceActor, 3, "card1", "title", "shown")
	send(t, alice, protocol.NewPush(3, []crdt.Op{visible}))
	mustAck(t, alice, 3)

	data := recvType(t, bob, "op")
	var f protocol.OpFrame
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	if f.Sub != "s" || f.Op.ID != visible.ID {
		t.Fatalf("bob op frame = sub %q op %v, want s/%v", f.Sub, f.Op.ID, visible.ID)
	}
	// ...and nothing else follows within a short window: card2's op must
	// not leak to Bob.
	if extra, ok := recvTimeout(t, bob, 500*time.Millisecond); ok {
		t.Fatalf("bob received unexpected frame %s after the card1 op", extra)
	}

	// Alice, subscribed and allowed, receives the hidden op with sub "s".
	aliceFrame := recvType(t, alice, "op")
	var af protocol.OpFrame
	if err := json.Unmarshal(aliceFrame, &af); err != nil {
		t.Fatal(err)
	}
	if af.Op.ID != title2.ID {
		t.Fatalf("alice echo = %v, want %v", af.Op.ID, title2.ID)
	}
}
