package client

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
)

// docWithPending builds a client pointed at url with one doc and n pending
// inc ops (unsent until Run connects).
func docWithPending(t *testing.T, url string, n int) *Client {
	t.Helper()
	c, err := New(Options{
		URL:     url,
		Token:   issueToken(t, "user-1"),
		Schemas: map[string]crdt.Schema{"cards": allTypesSchema},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	board := c.Doc("cards", "d1")
	for i := 0; i < n; i++ {
		if err := board.Inc("votes", 1); err != nil {
			t.Fatalf("Inc: %v", err)
		}
	}
	return c
}

// TestEvictDropsDocWithoutPending: a doc the client knows but never edited,
// and the server does not have, is evicted on reconcile.
func TestEvictDropsDocWithoutPending(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	c := docWithPending(t, wsURL(ts), 0) // Doc("cards","d1") with no ops
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	c.Subscribe("cards", "")
	waitReady(t, c)

	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		_, exists := c.docs[crdt.DocKey{Coll: "cards", Doc: "d1"}]
		c.mu.Unlock()
		if !exists {
			return // evicted
		}
		if time.Now().After(deadline) {
			t.Fatal("doc without pending ops was not evicted")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestEvictKeepsDocWithPending: pending local edits win over eviction; after
// the reconnect pushes them, the doc exists server-side and matches.
func TestEvictKeepsDocWithPending(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	c := docWithPending(t, wsURL(ts), 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	c.Subscribe("cards", "")
	waitReady(t, c)
	waitSynced(t, c)

	board := c.Doc("cards", "d1")
	if got := board.Values(); !valuesEqual(got, map[string]any{"votes": int64(3)}) {
		t.Fatalf("values = %v, want votes 3", got)
	}
}

// TestRetryAckReschedulesResend: the client's pending ops start past a seq
// the server never saw, so every push is retried. The retry ack must keep
// the ops pending, clear their in-flight marks, and re-send them after the
// backoff.
func TestRetryAckReschedulesResend(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	c := docWithPending(t, wsURL(ts), 3)
	key := crdt.DocKey{Coll: "cards", Doc: "d1"}

	// Simulate a confirmed first op the server never received: confirmed
	// VV advances to 1, pending becomes seqs 2..3, which the fresh server
	// must reject as out-of-order forever.
	c.mu.Lock()
	ds := c.docs[key]
	if op := ds.pending[0]; ds.confirmed.Apply(op, allTypesSchema) != nil {
		c.mu.Unlock()
		t.Fatal("seed apply failed")
	}
	ds.pending = ds.pending[1:]
	ds.viewValid = false
	if got := len(ds.pending); got != 2 {
		c.mu.Unlock()
		t.Fatalf("pending = %d ops, want 2", got)
	}
	c.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	c.Subscribe("cards", "")
	// WaitSynced can never return here — the pending ops stay pending by
	// design — so poll the subscription's ready flag directly.
	waitSubReady(t, c)

	// The server rejects seqs 2..3 forever, so the client must keep
	// cycling: push → retry ack (ops stay pending, in-flight marks clear)
	// → backoff resend → retry ack … Each push increments nextReq, so the
	// resend loop is observable as a growing request counter (the in-flight
	// window itself is far too short to poll).
	c.mu.Lock()
	reqs0 := c.nextReq
	c.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		reqs := c.nextReq
		pending := len(c.docs[key].pending)
		c.mu.Unlock()
		if reqs >= reqs0+3 && pending == 2 {
			return // initial push plus at least two backoff resends
		}
		if time.Now().After(deadline) {
			t.Fatalf("pushes = %d (want >= %d), pending = %d (want 2)", reqs, reqs0+3, pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitSubReady waits until every subscription is ready without requiring
// the pending queues to drain.
func waitSubReady(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		ready := true
		for _, id := range c.subOrder {
			if s := c.subs[id]; s == nil || !s.ready {
				ready = false
			}
		}
		c.mu.Unlock()
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("subscription never became ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestInvalidAckDropsLaterPending: PLAN.md §4.7 — an invalid result drops
// that op and all later pending ops of the same doc and surfaces an error.
func TestInvalidAckDropsLaterPending(t *testing.T) {
	c := docWithPending(t, "ws://unused.invalid/v1/sync", 3)
	key := crdt.DocKey{Coll: "cards", Doc: "d1"}
	c.mu.Lock()
	ds := c.docs[key]
	ops := append([]crdt.Op(nil), ds.pending...)
	for _, op := range ops {
		ds.sent[op.ID] = struct{}{}
	}
	c.nextReq = 7
	c.ackDoc[7] = key
	c.mu.Unlock()

	ack, err := protocol.Encode(protocol.NewAck(7, []protocol.AckResult{
		{ID: ops[1].ID, Status: protocol.StatusInvalid, Reason: "actor_not_owned"},
	}))
	if err != nil {
		t.Fatalf("encode ack: %v", err)
	}
	c.handleAck(ack)

	c.mu.Lock()
	remaining := append([]crdt.Op(nil), ds.pending...)
	sent := len(ds.sent)
	c.mu.Unlock()
	if len(remaining) != 1 || remaining[0].ID != ops[0].ID {
		t.Fatalf("pending after invalid = %v, want only the first op", remaining)
	}
	if sent != 1 {
		t.Fatalf("sent after invalid = %d entries, want 1", sent)
	}
	select {
	case err := <-c.Errors():
		var opErr *OpError
		if !errorsAs(err, &opErr) || opErr.Status != protocol.StatusInvalid {
			t.Fatalf("error = %v, want invalid OpError", err)
		}
		if opErr.Reason != "actor_not_owned" {
			t.Fatalf("reason = %q, want actor_not_owned", opErr.Reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no error surfaced for invalid ack")
	}
}

func errorsAs(err error, target **OpError) bool {
	type unwrapper interface{ Unwrap() error }
	for err != nil {
		if e, ok := err.(*OpError); ok {
			*target = e
			return true
		}
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// TestReconnectResubscribes forces a connection loss and verifies the client
// reconnects, re-subscribes (ready again) and keeps syncing.
func TestReconnectResubscribes(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	c := newTestClient(t, ts, nil)
	c.Subscribe("cards", "")
	waitReady(t, c)

	board := c.Doc("cards", "b1")
	if err := board.Set("title", "v1"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	waitSynced(t, c)

	// Kill the connection underneath the client.
	c.mu.Lock()
	conn := c.cw.conn
	c.mu.Unlock()
	_ = conn.CloseNow()

	// The client must reconnect and re-subscribe on its own.
	waitReady(t, c)
	if err := board.Set("title", "v2"); err != nil {
		t.Fatalf("Set after reconnect: %v", err)
	}
	waitSynced(t, c)
	if got := board.Values(); !valuesEqual(got, map[string]any{"title": "v2"}) {
		t.Fatalf("values after reconnect = %v, want title v2", got)
	}
}

// TestOfflineEditsConvergeAfterReconnect: cancel Run, edit offline while
// another client edits, then run again — both converge.
func TestOfflineEditsConvergeAfterReconnect(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))

	mk := func() (*Client, context.CancelFunc) {
		opts := Options{
			URL:     wsURL(ts),
			Token:   issueToken(t, "user-1"),
			Schemas: map[string]crdt.Schema{"cards": allTypesSchema},
		}
		c, err := New(opts)
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = c.Run(ctx) }()
		return c, cancel
	}

	c1, cancel1 := mk()
	c1.Subscribe("cards", "")
	waitReady(t, c1)

	b1 := c1.Doc("cards", "shared")
	if err := b1.Set("title", "start"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	waitSynced(t, c1)

	// c1 goes offline and edits; c2 edits meanwhile.
	cancel1()
	if err := b1.Inc("votes", 5); err != nil {
		t.Fatalf("offline Inc: %v", err)
	}
	if err := b1.Add("labels", "offline"); err != nil {
		t.Fatalf("offline Add: %v", err)
	}

	c2, cancel2 := mk()
	defer cancel2()
	c2.Subscribe("cards", "")
	waitReady(t, c2)
	b2 := c2.Doc("cards", "shared")
	if err := b2.Inc("votes", 7); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	if err := b2.InsertText("description", 0, "hi"); err != nil {
		t.Fatalf("InsertText: %v", err)
	}
	waitSynced(t, c2)

	// c1 comes back: reconcile pulls c2's ops, pending ops are pushed.
	ctx2, cancelCtx2 := context.WithCancel(context.Background())
	defer cancelCtx2()
	go func() { _ = c1.Run(ctx2) }()
	waitReady(t, c1)
	waitSynced(t, c1)
	waitSynced(t, c2)

	want := map[string]any{
		"title":       "start",
		"votes":       int64(12),
		"labels":      []string{"offline"},
		"description": "hi",
	}
	if got := b1.Values(); !valuesEqual(got, want) {
		t.Fatalf("c1 values = %v, want %v", got, want)
	}
	if got := b2.Values(); !valuesEqual(got, want) {
		t.Fatalf("c2 values = %v, want %v", got, want)
	}
}

// TestFileStorePersistence: a FileStore-backed client restarts with the same
// actor, pending ops and confirmed values, then syncs.
func TestFileStorePersistence(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	path := filepath.Join(t.TempDir(), "state.json")

	mk := func() (*Client, context.CancelFunc) {
		opts := Options{
			URL:     wsURL(ts),
			Token:   issueToken(t, "user-1"),
			Schemas: map[string]crdt.Schema{"cards": allTypesSchema},
			Store:   NewFileStore(path),
		}
		c, err := New(opts)
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = c.Run(ctx) }()
		return c, cancel
	}

	c1, cancel1 := mk()
	actor := c1.actor
	c1.Subscribe("cards", "")
	waitReady(t, c1)
	b1 := c1.Doc("cards", "persisted")
	if err := b1.Set("title", "kept"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	waitSynced(t, c1)
	// One op stays pending forever (its Run is canceled before it can flush).
	if err := b1.Inc("votes", 2); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	cancel1()

	c2, cancel2 := mk()
	defer cancel2()
	if c2.actor != actor {
		t.Fatalf("restored actor = %s, want %s", c2.actor, actor)
	}
	b2 := c2.Doc("cards", "persisted")
	if got := b2.Values(); !valuesEqual(got, map[string]any{"title": "kept", "votes": int64(2)}) {
		t.Fatalf("restored values = %v, want pending inc included", got)
	}
	waitReady(t, c2)
	waitSynced(t, c2)
	if got := b2.Values(); !valuesEqual(got, map[string]any{"title": "kept", "votes": int64(2)}) {
		t.Fatalf("synced values = %v, want votes 2", got)
	}
}
