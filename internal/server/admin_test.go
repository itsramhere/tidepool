package server

import (
	"encoding/json"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
	"net/http/httptest"
	"testing"
)

func TestAdminDocumentInspection(t *testing.T) {
	ts, srv := newTestServer(t, func(c *Config) { c.AdminToken = "secret" })
	conn := dial(t, ts)
	hello(t, conn, aliceActor)
	send(t, conn, protocol.NewPush(1, []crdt.Op{validSetOp(t, aliceActor, 1, "c1", "title", "hello")}))
	mustAck(t, conn, 1)
	get := func(path string, status int) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer secret")
		rec := httptest.NewRecorder()
		srv.AdminHandler().ServeHTTP(rec, req)
		if rec.Code != status {
			t.Fatalf("%s: %d %s", path, rec.Code, rec.Body.String())
		}
		return rec
	}
	rec := get("/admin/docs/cards/c1", 200)
	var body struct {
		Values   map[string]any
		VV       map[crdt.ActorID]uint64
		Snapshot json.RawMessage
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.UnmarshalSnapshot(body.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if body.Values["title"] != "hello" || doc.Values()["title"] != "hello" || body.VV[aliceActor] != 1 {
		t.Fatal("inspection mismatch")
	}
	rec = get("/admin/docs/cards", 200)
	var list struct{ Docs []string }
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Docs) != 1 || list.Docs[0] != "c1" {
		t.Fatal(list)
	}
	get("/admin/docs/cards/missing", 404)
	get("/admin/docs/missing", 404)
	get("/admin/docs/BAD/c1", 400)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/admin/docs/cards/c1", nil))
	if rec.Code != 404 {
		t.Fatal("public admin route")
	}
}
