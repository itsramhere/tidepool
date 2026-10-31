package server

import (
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsAndAdminIsolation(t *testing.T) {
	ts, srv := newTestServer(t, func(c *Config) { c.AdminToken = "admin-secret" })
	conn := dial(t, ts)
	hello(t, conn, aliceActor)
	send(t, conn, protocol.NewSub("s", "cards", "", nil))
	recvType(t, conn, "ready")
	op := validSetOp(t, aliceActor, 1, "c1", "title", "hello")
	send(t, conn, protocol.NewPush(1, []crdt.Op{op}))
	mustAck(t, conn, 1)
	send(t, conn, protocol.NewPush(2, []crdt.Op{op}))
	mustAck(t, conn, 2)
	// A duplicate needs no storage write. A rejected op must commit a noop,
	// giving two actual storage commits while exercising all three outcomes.
	bad := op
	bad.ID.Seq, bad.Lamport, bad.Kind = 2, 2, crdt.KindInc
	bad.Value, bad.Delta = nil, 1
	send(t, conn, protocol.NewPush(3, []crdt.Op{bad}))
	results := mustAck(t, conn, 3)
	if results[0].Status != protocol.StatusRejected {
		t.Fatal("expected noop rejection")
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{`tidepool_ops_total{status="ok"} 1`, `tidepool_ops_total{status="dup"} 1`, `tidepool_ops_total{status="rejected"} 1`, "tidepool_sessions 1", "tidepool_subscriptions 1", "tidepool_commit_seconds_count 2", "tidepool_commit_batch_requests_count 2"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("missing %s in metrics", want)
		}
	}
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/goroutine?debug=1"} {
		for _, token := range []string{"", "Bearer wrong", "Bearer admin-secret"} {
			req := httptest.NewRequest("GET", path, nil)
			req.Header.Set("Authorization", token)
			rec = httptest.NewRecorder()
			srv.AdminHandler().ServeHTTP(rec, req)
			want := http.StatusUnauthorized
			if token == "Bearer admin-secret" {
				want = http.StatusOK
			}
			if rec.Code != want {
				t.Fatalf("%s token %q: %d", path, token, rec.Code)
			}
		}
		rec = httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != 404 {
			t.Fatal("public listener exposed pprof")
		}
	}
	send(t, conn, protocol.NewUnsub("s"))
	deadline := time.Now().Add(time.Second)
	for {
		rec = httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		if strings.Contains(rec.Body.String(), "tidepool_subscriptions 0") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("unsub gauge did not decrease")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAdminRequiresToken(t *testing.T) {
	_, srv := newTestServer(t, nil)
	rec := httptest.NewRecorder()
	srv.AdminHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/debug/pprof/", nil))
	if rec.Code != 401 {
		t.Fatal("empty admin token must disable access")
	}
	if err := (&Config{AdminListen: "127.0.0.1:0"}).validate(); err == nil {
		t.Fatal("admin listener needs a token")
	}
}
