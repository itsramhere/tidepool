package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itsramhere/tidepool/internal/auth"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/schema"
	"github.com/itsramhere/tidepool/internal/server"
	"github.com/itsramhere/tidepool/internal/storage"
)

const testJWTSecret = "test-secret"

// allTypesSchema covers every field type; client and server schemas agree
// except where a test deliberately diverges them.
var allTypesSchema = crdt.Schema{
	"title":       crdt.FieldRegister,
	"votes":       crdt.FieldCounter,
	"labels":      crdt.FieldSet,
	"checklist":   crdt.FieldList,
	"description": crdt.FieldText,
}

func testSchemasConfig(t *testing.T) *schema.Config {
	t.Helper()
	cfg, err := schema.Parse([]byte(`collections:
  cards:
    fields:
      title: register
      votes: counter
      labels: set
      checklist: list
      description: text
`))
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	return cfg
}

// startServer runs an in-process server over a memory store.
func startServer(t *testing.T, schemas *schema.Config) *httptest.Server {
	t.Helper()
	cfg := &server.Config{
		NodeID:            "test-node",
		Listen:            "unused-by-tests",
		DataDir:           "unused-by-tests",
		Fsync:             "never",
		JWTSecret:         testJWTSecret,
		MaxOpsPerPush:     1000,
		SessionSendBuffer: 1024,
	}
	srv, err := server.New(cfg, schemas, storage.NewMemoryStore(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.Stop()
	})
	return ts
}

func wsURL(ts *httptest.Server) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/sync"
}

func issueToken(t *testing.T, uid string) string {
	t.Helper()
	token, err := auth.Issue(testJWTSecret, uid, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return token
}

// newTestClient builds a client for ts and runs it until t finishes.
func newTestClient(t *testing.T, ts *httptest.Server, mutate func(*Options)) *Client {
	t.Helper()
	opts := Options{
		URL:     wsURL(ts),
		Token:   issueToken(t, "user-1"),
		Schemas: map[string]crdt.Schema{"cards": allTypesSchema},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if mutate != nil {
		mutate(&opts)
	}
	c, err := New(opts)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()
	return c
}

func waitReady(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.WaitSynced(ctx); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("New without URL = nil error, want error")
	}
	_, err := New(Options{URL: "ws://127.0.0.1:1/v1/sync", Actor: "NOT-VALID"})
	if err == nil || !strings.Contains(err.Error(), "actor") {
		t.Fatalf("New with bad actor = %v, want actor validation error", err)
	}
}

func TestRunRejectsReentry(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
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
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	// A subscription only readies after the handshake, so once WaitSynced
	// returns, the first Run is provably inside serve and owns the
	// running flag.
	c.Subscribe("cards", "")
	waitReady(t, c)

	if err := c.Run(ctx); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run = %v, want ErrAlreadyRunning", err)
	}
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancel", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestConnectSubscribeWaitSynced(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	c := newTestClient(t, ts, nil)

	if c.actor == "" {
		t.Fatal("client actor is empty")
	}
	sub := c.Subscribe("cards", "")
	if sub.ID() == "" {
		t.Fatal("subscription id is empty")
	}
	waitReady(t, c)

	// A second subscription with a valid filter becomes ready too.
	c.Subscribe("cards", "doc.title != ''")
	waitReady(t, c)

	// Subscribing to an unknown collection surfaces an error on Errors()
	// and marks the subscription failed (it can never become ready).
	bad := c.Subscribe("missing", "")
	if bad.failed == nil {
		t.Fatal("subscription for unknown collection not marked failed")
	}
	select {
	case err := <-c.Errors():
		if !strings.Contains(err.Error(), "missing") {
			t.Fatalf("error = %v, want mention of collection", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no error for unknown collection subscription")
	}
}

func TestLocalEditsPushAndEcho(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	c := newTestClient(t, ts, nil)

	// atomic: the callback runs on the client's reader goroutine.
	var changes atomic.Int64
	c.OnChange(func(crdt.DocKey) { changes.Add(1) })

	// Edit while connected: every field type.
	board := c.Doc("cards", "b1")
	if err := board.Set("title", "Hello"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := board.Inc("votes", 3); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	if err := board.Add("labels", "urgent"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := board.Add("labels", "bug"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := board.Remove("labels", "bug"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := board.Insert("checklist", 0, "first"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := board.Insert("checklist", 1, "second"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := board.Delete("checklist", 0); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := board.InsertText("description", 0, "héllo"); err != nil {
		t.Fatalf("InsertText: %v", err)
	}
	waitReady(t, c)
	waitSynced(t, c)

	want := map[string]any{
		"title":       "Hello",
		"votes":       int64(3),
		"labels":      []string{"urgent"},
		"checklist":   []any{"second"},
		"description": "héllo",
	}
	got := board.Values()
	if !valuesEqual(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
	if changes.Load() == 0 {
		t.Fatal("OnChange never fired for local edits")
	}
}

// waitSynced is waitReady without a subscription: pending ops only.
func waitSynced(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.WaitSynced(ctx); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
}

// valuesEqual compares materialized maps through canonical JSON.
func valuesEqual(a, b map[string]any) bool {
	aj, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bj, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(aj) == string(bj)
}

func TestValuesShowPendingWhileOffline(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	opts := Options{
		URL:     wsURL(ts),
		Token:   issueToken(t, "user-1"),
		Schemas: map[string]crdt.Schema{"cards": allTypesSchema},
	}
	c, err := New(opts)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	// No Run: everything stays local.
	board := c.Doc("cards", "offline")
	if err := board.Set("title", "draft"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := board.Inc("votes", 1); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	if err := board.Inc("votes", 2); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	want := map[string]any{"title": "draft", "votes": int64(3)}
	if got := board.Values(); !valuesEqual(got, want) {
		t.Fatalf("values = %v, want %v (pending ops must be visible)", got, want)
	}
}

// TestRejectedOpSurfacesError: the client's schema knows a field the server's
// does not, so the server stores the op as a noop. The client must surface
// the rejection on Errors(), drop the pending op, and keep its seq bookkeeping
// intact (a later op still syncs).
func TestRejectedOpSurfacesError(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	wider := crdt.Schema{
		"title":       crdt.FieldRegister,
		"votes":       crdt.FieldCounter,
		"labels":      crdt.FieldSet,
		"checklist":   crdt.FieldList,
		"description": crdt.FieldText,
		"ghost":       crdt.FieldRegister, // unknown to the server
	}
	c := newTestClient(t, ts, func(o *Options) {
		o.Schemas = map[string]crdt.Schema{"cards": wider}
		o.Token = issueToken(t, "user-1")
	})
	c.Subscribe("cards", "")
	waitReady(t, c)

	board := c.Doc("cards", "b1")
	if err := board.Set("ghost", "boo"); err != nil {
		t.Fatalf("Set ghost: %v", err)
	}
	if err := board.Set("title", "real"); err != nil {
		t.Fatalf("Set title: %v", err)
	}
	waitSynced(t, c)

	select {
	case err := <-c.Errors():
		var opErr *OpError
		if !errors.As(err, &opErr) || opErr.Status != "rejected" {
			t.Fatalf("error = %v, want rejected OpError", err)
		}
		if opErr.Key.Doc != "b1" {
			t.Fatalf("error key = %v, want b1", opErr.Key)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no rejection surfaced on Errors()")
	}
	// The rejected field must be absent (the noop changed nothing locally on
	// rebuild) and the follow-up op must have synced.
	want := map[string]any{"title": "real"}
	if got := board.Values(); !valuesEqual(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

// TestClientReadLimit: server frames (op echoes) larger than coder/websocket's
// default 32 KiB read limit must still arrive.
func TestClientReadLimit(t *testing.T) {
	ts := startServer(t, testSchemasConfig(t))
	c := newTestClient(t, ts, nil)
	c.Subscribe("cards", "")
	waitReady(t, c)

	big := strings.Repeat("x", 60*1024)
	board := c.Doc("cards", "big")
	if err := board.Set("title", big); err != nil {
		t.Fatalf("Set: %v", err)
	}
	waitSynced(t, c) // committed and echoed before the second client connects

	// A second client sees the big value via snapshot replay.
	c2 := newTestClient(t, ts, func(o *Options) { o.Token = issueToken(t, "user-2") })
	c2.Subscribe("cards", "")
	waitReady(t, c2)

	want := map[string]any{"title": big}
	if got := c2.Doc("cards", "big").Values(); !valuesEqual(got, want) {
		t.Fatalf("second client values = %v, want the %d-byte title", got, len(big))
	}
}
