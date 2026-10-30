package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/auth"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
	"github.com/itsramhere/tidepool/internal/schema"
	"github.com/itsramhere/tidepool/internal/storage"
)

const testJWTSecret = "test-secret"

// Test actors: valid 16-char lowercase hex.
const (
	aliceActor = "aaaaaaaaaaaaaaaa"
	bobActor   = "bbbbbbbbbbbbbbbb"
)

var testSchemas = func() *schema.Config {
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
		panic(err)
	}
	return cfg
}()

// newTestServer starts a Server over a memory store on an httptest server.
func newTestServer(t *testing.T, mutate func(*Config)) (*httptest.Server, *Server) {
	t.Helper()
	cfg := &Config{
		NodeID:            "test-node",
		Listen:            "unused-by-tests",
		DataDir:           "unused-by-tests",
		Fsync:             "never",
		JWTSecret:         testJWTSecret,
		MaxOpsPerPush:     1000,
		SessionSendBuffer: 1024,
	}
	if mutate != nil {
		mutate(cfg)
	}
	srv, err := New(cfg, testSchemas, storage.NewMemoryStore(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.Stop()
	})
	return ts, srv
}

func wsURL(ts *httptest.Server) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/sync"
}

func dial(t *testing.T, ts *httptest.Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL(ts), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// Test clients must accept the same frame sizes the server allows.
	conn.SetReadLimit(maxFrameBytes)
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

func send(t *testing.T, conn *websocket.Conn, m any) {
	t.Helper()
	data, err := protocol.Encode(m)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func sendRaw(t *testing.T, conn *websocket.Conn, data string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(data)); err != nil {
		t.Fatalf("write raw: %v", err)
	}
}

// recv reads one text frame.
func recv(t *testing.T, conn *websocket.Conn) json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatal("read: expected a text frame")
	}
	return json.RawMessage(data)
}

// recvType reads one frame and requires its type.
func recvType(t *testing.T, conn *websocket.Conn, want string) json.RawMessage {
	t.Helper()
	data := recv(t, conn)
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	if env.T != want {
		t.Fatalf("frame type = %q, want %q (frame: %s)", env.T, want, data)
	}
	return data
}

// recvClosed expects the connection to be closed by the server.
func recvClosed(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("read after server close = nil error, want connection closed")
	}
}

func mustToken(t *testing.T, uid string) string {
	t.Helper()
	token, err := auth.Issue(testJWTSecret, uid, time.Minute)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return token
}

// helloAs authenticates a session and waits for the welcome frame.
func helloAs(t *testing.T, conn *websocket.Conn, uid, actor string) {
	t.Helper()
	send(t, conn, protocol.NewHello(1, mustToken(t, uid), crdt.ActorID(actor)))
	welcome := recvType(t, conn, "welcome")
	var w protocol.Welcome
	if err := json.Unmarshal(welcome, &w); err != nil {
		t.Fatalf("unmarshal welcome: %v", err)
	}
	if w.Node != "test-node" || w.Actor != crdt.ActorID(actor) {
		t.Errorf("welcome = %+v, want node test-node actor %s", w, actor)
	}
}

func hello(t *testing.T, conn *websocket.Conn, actor string) {
	t.Helper()
	helloAs(t, conn, "alice", actor)
}

// jsonValue marshals v as a raw JSON value for op payloads.
func jsonValue(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %v: %v", v, err)
	}
	return raw
}

// validSetOp builds a register set op with seq == lamport (valid per actor).
func validSetOp(t *testing.T, actor string, seq uint64, docID, field, value string) crdt.Op {
	t.Helper()
	return crdt.Op{
		ID:      crdt.OpID{Actor: crdt.ActorID(actor), Seq: seq},
		Key:     crdt.DocKey{Coll: "cards", Doc: docID},
		Lamport: seq,
		Field:   field,
		Kind:    crdt.KindSet,
		Value:   jsonValue(t, value),
	}
}

func validIncOp(actor string, seq uint64, docID string, delta int64) crdt.Op {
	return crdt.Op{
		ID:      crdt.OpID{Actor: crdt.ActorID(actor), Seq: seq},
		Key:     crdt.DocKey{Coll: "cards", Doc: docID},
		Lamport: seq,
		Field:   "votes",
		Kind:    crdt.KindInc,
		Delta:   delta,
	}
}
