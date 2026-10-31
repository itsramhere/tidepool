package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itsramhere/tidepool/internal/auth"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/schema"
	"github.com/itsramhere/tidepool/internal/server"
	"github.com/itsramhere/tidepool/internal/storage"
	"github.com/itsramhere/tidepool/pkg/client"
)

const testJWTSecret = "test-secret"

const rulesYAML = `collections:
  cards:
    fields:
      title: register
      votes: counter
      labels: set
      checklist: list
      description: text
`

var cardsSchema = crdt.Schema{
	"title":       crdt.FieldRegister,
	"votes":       crdt.FieldCounter,
	"labels":      crdt.FieldSet,
	"checklist":   crdt.FieldList,
	"description": crdt.FieldText,
}

// node is one running server. The listener address is kept so a restarted
// node can reclaim the same endpoint and clients can reconnect transparently.
type node struct {
	ts   *httptest.Server
	srv  *server.Server
	addr string // host:port
}

// startNode runs a server backed by a pebble store in dir (memory when dir
// is empty) listening on addr (a fresh port when empty), stopped at test end.
func startNode(t *testing.T, dir, addr string) *node {
	t.Helper()
	n := startNodeRaw(t, dir, addr)
	t.Cleanup(func() {
		n.ts.Close()
		n.srv.Stop()
	})
	return n
}

// startNodeRaw is startNode without cleanup registration, for tests that
// stop and restart the node themselves.
func startNodeRaw(t *testing.T, dir, addr string) *node {
	t.Helper()
	schemas, err := schema.Parse([]byte(rulesYAML))
	if err != nil {
		t.Fatalf("parse rules: %v", err)
	}
	cfg := &server.Config{
		NodeID:            "test-node",
		Listen:            "unused-by-tests",
		DataDir:           "unused-by-tests",
		Fsync:             "always",
		JWTSecret:         testJWTSecret,
		MaxOpsPerPush:     1000,
		SessionSendBuffer: 1024,
	}
	var store storage.Store
	if dir == "" {
		store = storage.NewMemoryStore()
	} else {
		store, err = storage.Open(dir)
		if err != nil {
			t.Fatalf("open pebble store: %v", err)
		}
	}
	srv, err := server.New(cfg, schemas, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ts := httptest.NewUnstartedServer(srv.Handler())
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		srv.Stop()
		t.Fatalf("listen on %q: %v", addr, err)
	}
	ts.Listener.Close()
	ts.Listener = ln
	ts.Start()
	return &node{ts: ts, srv: srv, addr: ln.Addr().String()}
}

func (n *node) wsURL() string { return "ws://" + n.addr + "/v1/sync" }

// newClient starts a client against n and runs it until ctx is done.
func newClient(t *testing.T, n *node, mutate func(*client.Options)) *client.Client {
	t.Helper()
	opts := client.Options{
		URL:     n.wsURL(),
		Token:   mustToken(t),
		Schemas: map[string]crdt.Schema{"cards": cardsSchema},
	}
	if mutate != nil {
		mutate(&opts)
	}
	c, err := client.New(opts)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()
	return c
}

func mustToken(t *testing.T) string {
	t.Helper()
	token, err := auth.Issue(testJWTSecret, "user-1", time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return token
}

func waitSynced(t *testing.T, c *client.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.WaitSynced(ctx); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
}

// waitDocValues polls a handle until its view equals want — used to wait
// for inbound frames, which WaitSynced does not cover.
func waitDocValues(t *testing.T, h *client.DocHandle, want map[string]any) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := h.Values()
		if valuesEqual(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("doc values = %v, want %v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

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

// TestTwoClientsConvergeOnEveryFieldType: both clients edit all five field
// types of one doc; after both sync, their materialized values match.
func TestTwoClientsConvergeOnEveryFieldType(t *testing.T) {
	n := startNode(t, "", "")
	a := newClient(t, n, nil)
	b := newClient(t, n, func(o *client.Options) { o.Actor = crdt.ActorID("bbbbbbbbbbbbbbbb") })

	a.Subscribe("cards", "")
	b.Subscribe("cards", "")
	waitSynced(t, a)
	waitSynced(t, b)

	da := a.Doc("cards", "board")
	db := b.Doc("cards", "board")
	if err := da.Set("title", "from A"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := da.Inc("votes", 3); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	if err := da.Add("labels", "red"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := da.Insert("checklist", 0, "a0"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := da.InsertText("description", 0, "abc"); err != nil {
		t.Fatalf("InsertText: %v", err)
	}

	// B must have received all of A's ops before editing relative to them
	// (WaitSynced only covers pending ops, not inbound frames). Frames
	// arrive in commit order, so the last-edited field being complete means
	// everything before it arrived too.
	waitDocValues(t, db, map[string]any{
		"title":       "from A",
		"votes":       int64(3),
		"labels":      []string{"red"},
		"checklist":   []any{"a0"},
		"description": "abc",
	})

	if err := db.Set("title", "from B"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Inc("votes", 4); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	if err := db.Add("labels", "blue"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// B's b0 has the higher lamport, and RGA sorts higher-lamport head
	// inserts first, so the list is [b0, a0] and index 1 is A's a0.
	if err := db.Insert("checklist", 0, "b0"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := db.Delete("checklist", 1); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := db.InsertText("description", 3, "de"); err != nil {
		t.Fatalf("InsertText: %v", err)
	}

	waitSynced(t, a)
	waitSynced(t, b)

	// WaitSynced covers only pending ops — inbound broadcast frames can
	// still be in flight — so converge on observed values.
	want := map[string]any{
		"title":       "from B",
		"votes":       int64(7),
		"labels":      []string{"blue", "red"},
		"checklist":   []any{"b0"},
		"description": "abcde",
	}
	waitDocValues(t, da, want)
	waitDocValues(t, db, want)
}

// TestOfflineEditsConvergeAfterReconnect: cancel Run, edit offline while
// another client edits, then run again — both converge.
func TestOfflineEditsConvergeAfterReconnect(t *testing.T) {
	n := startNode(t, "", "")

	// A manually managed client so the test can take it offline mid-way.
	opts := client.Options{
		URL:     n.wsURL(),
		Token:   mustToken(t),
		Schemas: map[string]crdt.Schema{"cards": cardsSchema},
	}
	offline, err := client.New(opts)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = offline.Run(ctx1); close(runDone) }()
	offline.Subscribe("cards", "")
	waitSynced(t, offline)
	cancel1()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	// Offline now: edits land in the pending queue only.
	do := offline.Doc("cards", "board")
	if err := do.Set("title", "start"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := do.Inc("votes", 5); err != nil {
		t.Fatalf("offline Inc: %v", err)
	}
	if err := do.Add("labels", "offline"); err != nil {
		t.Fatalf("offline Add: %v", err)
	}

	b := newClient(t, n, func(o *client.Options) { o.Actor = crdt.ActorID("bbbbbbbbbbbbbbbb") })
	b.Subscribe("cards", "")
	waitSynced(t, b)
	db := b.Doc("cards", "board")
	if err := db.Inc("votes", 7); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	if err := db.InsertText("description", 0, "hi"); err != nil {
		t.Fatalf("InsertText: %v", err)
	}
	waitSynced(t, b)

	// A comes back online: reconcile pulls B's ops, pending ops are pushed.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = offline.Run(ctx2) }()
	waitSynced(t, offline)
	waitSynced(t, b)

	// WaitSynced covers only pending ops — inbound broadcast frames can
	// still be in flight — so converge on observed values.
	want := map[string]any{
		"title":       "start",
		"votes":       int64(12),
		"labels":      []string{"offline"},
		"description": "hi",
	}
	waitDocValues(t, do, want)
	waitDocValues(t, db, want)
}

// TestServerRestartLosesNothing: a pebble-backed node restarts on the same
// address; the offline client's pending ops and the server's committed state
// all survive.
func TestServerRestartLosesNothing(t *testing.T) {
	dir := t.TempDir()
	n1 := startNodeRaw(t, dir, "")
	c := newClient(t, n1, nil)
	c.Subscribe("cards", "")
	waitSynced(t, c)

	da := c.Doc("cards", "board")
	if err := da.Set("title", "durable"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := da.Inc("votes", 1); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	waitSynced(t, c)

	// A second, manually managed client goes offline after syncing and adds
	// ops that only exist locally.
	opts := client.Options{
		URL:     n1.wsURL(),
		Token:   mustToken(t),
		Schemas: map[string]crdt.Schema{"cards": cardsSchema},
	}
	offline, err := client.New(opts)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	go func() { _ = offline.Run(ctx1) }()
	offline.Subscribe("cards", "")
	waitSynced(t, offline)
	cancel1()
	do := offline.Doc("cards", "board")
	if err := do.Inc("votes", 2); err != nil {
		t.Fatalf("offline Inc: %v", err)
	}

	// Restart the node on the same address. The first node's listener and
	// store are closed exactly once here; the replacement node owns the
	// test-end cleanup.
	n1.ts.Close()
	n1.srv.Stop()
	n2 := startNode(t, dir, n1.addr)

	// The offline client reconnects; a fresh client reads the state.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = offline.Run(ctx2) }()
	waitSynced(t, offline)

	b := newClient(t, n2, func(o *client.Options) { o.Actor = crdt.ActorID("bbbbbbbbbbbbbbbb") })
	b.Subscribe("cards", "")
	waitSynced(t, b)

	want := map[string]any{"title": "durable", "votes": int64(3)}
	waitDocValues(t, do, want)
	waitDocValues(t, b.Doc("cards", "board"), want)
}

// testSeed honors TIDEPOOL_SEED (PLAN.md golden rule 8).
func testSeed(t *testing.T) uint64 {
	t.Helper()
	if s := os.Getenv("TIDEPOOL_SEED"); s != "" {
		if n, err := strconv.ParseUint(s, 10, 64); err == nil {
			t.Logf("seed=%d", n)
			return n
		}
	}
	n := uint64(time.Now().UnixNano())
	t.Logf("seed=%d", n)
	return n
}

// flakyProxy forwards TCP to a target and tears each connection down after a
// random byte budget, so clients must reconnect and re-reconcile mid-flight.
type flakyProxy struct {
	ln     net.Listener
	target string

	mu  sync.Mutex
	rng *rand.Rand
}

func startFlakyProxy(t *testing.T, target string) *flakyProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &flakyProxy{ln: ln, target: target, rng: rand.New(rand.NewPCG(testSeed(t), testSeed(t)))}
	go p.acceptLoop()
	t.Cleanup(func() { ln.Close() })
	return p
}

func (p *flakyProxy) addr() string { return p.ln.Addr().String() }

func (p *flakyProxy) acceptLoop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

// draw returns this connection's byte budget: small budgets cut connections
// mid-push, large ones let sessions run a while before dying.
func (p *flakyProxy) draw() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rng.Float64() < 0.35 {
		return int64(16<<10) + p.rng.Int64N(32<<10)
	}
	return int64(256<<10) + p.rng.Int64N(768<<10)
}

func (p *flakyProxy) handle(down net.Conn) {
	budget := p.draw()
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		down.Close()
		return
	}
	var total atomic.Int64
	kill := sync.OnceFunc(func() {
		down.Close()
		up.Close()
	})
	go p.pump(down, up, &total, budget, kill)
	go p.pump(up, down, &total, budget, kill)
}

// pump copies one direction, tearing the pair down once the shared byte
// budget is exhausted (or either side ends).
func (p *flakyProxy) pump(src, dst net.Conn, total *atomic.Int64, budget int64, kill func()) {
	defer kill()
	buf := make([]byte, 4096)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if total.Add(int64(n)) > budget {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if rerr != nil {
			return
		}
	}
}

// TestFlakyConnectionConverges: 1000 ops pushed through a proxy that randomly
// kills connections must all eventually commit, and a second client must see
// the same state.
func TestFlakyConnectionConverges(t *testing.T) {
	n := startNode(t, "", "")
	target := strings.TrimPrefix(n.ts.URL, "http://")
	proxy := startFlakyProxy(t, target)

	flaky := *n
	flaky.addr = proxy.addr()
	c1 := newClient(t, &flaky, nil)
	c1.Subscribe("cards", "")
	waitSynced(t, c1)

	d1 := c1.Doc("cards", "stress")
	for i := 0; i < 1000; i++ {
		var err error
		if i%10 == 0 {
			err = d1.Set("title", fmt.Sprintf("v%d", i))
		} else {
			err = d1.Inc("votes", 1)
		}
		if err != nil {
			t.Fatalf("edit %d: %v", i, err)
		}
	}

	b := newClient(t, n, func(o *client.Options) { o.Actor = crdt.ActorID("bbbbbbbbbbbbbbbb") })
	b.Subscribe("cards", "")

	waitSynced(t, c1)
	waitSynced(t, b)

	want := map[string]any{
		"title": "v990",
		"votes": int64(900),
	}
	waitDocValues(t, d1, want)
	waitDocValues(t, b.Doc("cards", "stress"), want)
}
