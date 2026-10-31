package chaos

import (
	"context"
	"encoding/binary"
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
	"testing"
	"time"

	"github.com/itsramhere/tidepool/internal/auth"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/schema"
	"github.com/itsramhere/tidepool/internal/server"
	"github.com/itsramhere/tidepool/internal/storage"
	"github.com/itsramhere/tidepool/pkg/client"
)

const testJWTSecret = "chaos-secret"
const testUID = "user-1"

const chaosRulesYAML = `collections:
  cards:
    fields:
      title: register
      status: register
      votes: counter
      labels: set
      checklist: list
      description: text
`

var chaosSchema = crdt.Schema{
	"title":       crdt.FieldRegister,
	"status":      crdt.FieldRegister,
	"votes":       crdt.FieldCounter,
	"labels":      crdt.FieldSet,
	"checklist":   crdt.FieldList,
	"description": crdt.FieldText,
}

// node is one Tidepool server with its own pebble store and HTTP server.
// The feed endpoint and pullers are live when peers are configured. Server
// .Stop is idempotent, so a node may be restarted on the same store.
type node struct {
	name  string
	ts    *httptest.Server
	srv   *server.Server
	store storage.Store
	addr  string
}

// chaosLog is the node logger; TIDEPOOL_CHAOS_LOG=stderr turns it on.
func chaosLog() *slog.Logger {
	if os.Getenv("TIDEPOOL_CHAOS_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startNode brings the node up on addr (a fresh port when empty). Restarts
// pass the same addr so peer URLs survive.
func startNode(t *testing.T, name, dir, addr string, peers []server.PeerConfig) *node {
	t.Helper()
	schemas, err := schema.Parse([]byte(chaosRulesYAML))
	if err != nil {
		t.Fatalf("parse rules: %v", err)
	}
	cfg := &server.Config{
		NodeID:            name,
		Listen:            "unused-by-tests",
		DataDir:           "unused-by-tests",
		Fsync:             "always",
		JWTSecret:         testJWTSecret,
		MaxOpsPerPush:     1000,
		SessionSendBuffer: 1024,
		Peers:             peers,
	}
	store, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("open pebble store %s: %v", name, err)
	}
	srv, err := server.New(cfg, schemas, store, chaosLog())
	if err != nil {
		t.Fatalf("new server %s: %v", name, err)
	}
	ts := httptest.NewUnstartedServer(srv.Handler())
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		srv.Stop()
		t.Fatalf("listen %s on %q: %v", name, addr, err)
	}
	ts.Listener.Close()
	ts.Listener = ln
	ts.Start()
	n := &node{name: name, ts: ts, srv: srv, store: store, addr: strings.TrimPrefix(ts.URL, "http://")}
	t.Cleanup(func() {
		ts.Close()
		srv.Stop()
	})
	return n
}

// linkProxies builds one blockable proxy per ordered node pair and returns
// each node's peer list plus the proxies keyed "from->to".
func linkProxies(t *testing.T, names []string, addrs map[string]string) (map[string][]server.PeerConfig, map[string]*Proxy) {
	t.Helper()
	peers := make(map[string][]server.PeerConfig, len(names))
	proxies := make(map[string]*Proxy)
	for _, from := range names {
		for _, to := range names {
			if from == to {
				continue
			}
			px, err := StartProxy(addrs[to])
			if err != nil {
				t.Fatalf("start proxy %s->%s: %v", from, to, err)
			}
			t.Cleanup(px.Close)
			proxies[from+"->"+to] = px
			peers[from] = append(peers[from], server.PeerConfig{
				ID:     to,
				URL:    "http://" + px.Addr(),
				Secret: testJWTSecret,
			})
		}
	}
	return peers, proxies
}

func clientProxy(t *testing.T, target string) *Proxy {
	t.Helper()
	px, err := StartProxy(target)
	if err != nil {
		t.Fatalf("start client proxy: %v", err)
	}
	t.Cleanup(px.Close)
	return px
}

func mustToken(t *testing.T) string {
	t.Helper()
	token, err := auth.Issue(testJWTSecret, testUID, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return token
}

// chaosClient is one SDK client whose lifecycle the test controls: it runs
// through its own blockable proxy, and moving it to another node cancels the
// old Run before a new client (same actor, same store) takes over.
type chaosClient struct {
	actor crdt.ActorID
	store client.LocalStore

	cancel  context.CancelFunc
	runDone chan struct{}
	proxy   *Proxy
	client  *client.Client
}

// startOn connects the client (or its replacement) to a node via a fresh
// proxy, stopping any previous Run first.
func (cc *chaosClient) startOn(t *testing.T, target string) {
	t.Helper()
	// The previous Run (if any) must be FULLY stopped before the
	// replacement loads the store: a still-running old client keeps saving
	// its own state and would race the new client's view.
	if cc.cancel != nil {
		cc.cancel()
		select {
		case <-cc.runDone:
		case <-time.After(10 * time.Second):
			t.Fatal("old client Run did not stop")
		}
	}
	cc.proxy = clientProxy(t, target)
	c, err := client.New(client.Options{
		URL:     "ws://" + cc.proxy.Addr() + "/v1/sync",
		Token:   mustToken(t),
		Actor:   cc.actor,
		Schemas: map[string]crdt.Schema{"cards": chaosSchema},
		Store:   cc.store,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cc.cancel = cancel
	cc.runDone = make(chan struct{})
	cc.client = c
	go func() {
		defer close(cc.runDone)
		_ = c.Run(ctx)
	}()
	c.Subscribe("cards", "")
}

func waitSynced(t *testing.T, c *client.Client, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.WaitSynced(ctx); err != nil {
		t.Fatalf("WaitSynced: %v", err)
	}
}

// waitFeedSettled blocks until every node's LastFeedSeq has stopped changing
// and every peer cursor equals its target's LastFeedSeq.
func waitFeedSettled(t *testing.T, nodes []*node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		lasts := make([]uint64, len(nodes))
		for i, n := range nodes {
			last, err := n.store.LastFeedSeq()
			if err != nil {
				t.Fatalf("LastFeedSeq(%s): %v", n.name, err)
			}
			lasts[i] = last
		}
		time.Sleep(400 * time.Millisecond)
		stable, caughtUp := true, true
		for i, n := range nodes {
			last, err := n.store.LastFeedSeq()
			if err != nil {
				t.Fatalf("LastFeedSeq(%s): %v", n.name, err)
			}
			if last != lasts[i] {
				stable = false
			}
			for j, to := range nodes {
				if i == j {
					continue
				}
				raw, ok, err := n.store.GetMeta("p/" + to.name)
				if err != nil {
					t.Fatalf("cursor %s->%s: %v", n.name, to.name, err)
				}
				var cursor uint64
				if ok && len(raw) == 8 {
					cursor = binary.BigEndian.Uint64(raw)
				}
				if cursor != lasts[j] {
					caughtUp = false
				}
			}
		}
		if stable && caughtUp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("feeds not settled: lasts=%v stable=%v caughtUp=%v", lasts, stable, caughtUp)
		}
	}
}

func nodeValues(t *testing.T, n *node, key crdt.DocKey) map[string]any {
	t.Helper()
	doc, _, err := storage.LoadDocument(n.store, key, chaosSchema)
	if err != nil {
		t.Fatalf("load doc %v from %s: %v", key, n.name, err)
	}
	return doc.Values()
}

func valuesJSON(t *testing.T, m map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal values: %v", err)
	}
	return string(raw)
}

// TestTwoNodesConverge: the minimal replication pipeline — two nodes pulling
// from each other, one client on each, edits made on both nodes converge
// everywhere.
func TestTwoNodesConverge(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in -short")
	}
	dir1, dir2 := t.TempDir(), t.TempDir()
	// First pass learns the addresses; both nodes restart ON THE SAME
	// addresses with peers wired.
	n1 := startNode(t, "n1", dir1, "", nil)
	n2 := startNode(t, "n2", dir2, "", nil)
	addr1, addr2 := n1.addr, n2.addr
	for _, n := range []*node{n1, n2} {
		n.ts.Close()
		n.srv.Stop()
	}
	n1 = startNode(t, "n1", dir1, addr1, []server.PeerConfig{
		{ID: "n2", URL: "http://" + addr2, Secret: testJWTSecret},
	})
	n2 = startNode(t, "n2", dir2, addr2, []server.PeerConfig{
		{ID: "n1", URL: "http://" + addr1, Secret: testJWTSecret},
	})

	clients := make([]*chaosClient, 2)
	for i, n := range []*node{n1, n2} {
		cc := &chaosClient{
			actor: crdt.ActorID(fmt.Sprintf("%016x", i+1)),
			store: client.NewMemoryStore(),
		}
		cc.startOn(t, n.addr)
		clients[i] = cc
	}

	d1 := clients[0].client.Doc("cards", "d1")
	if err := d1.Set("title", "from n1"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	d2 := clients[1].client.Doc("cards", "d2")
	if err := d2.Inc("votes", 3); err != nil {
		t.Fatalf("Inc: %v", err)
	}
	if err := d1.Set("status", "open"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	for _, cc := range clients {
		waitSynced(t, cc.client, 15*time.Second)
	}
	waitFeedSettled(t, []*node{n1, n2}, 20*time.Second)

	key1 := crdt.DocKey{Coll: "cards", Doc: "d1"}
	want := valuesJSON(t, map[string]any{"title": "from n1", "status": "open"})
	for _, n := range []*node{n1, n2} {
		if got := valuesJSON(t, nodeValues(t, n, key1)); got != want {
			t.Fatalf("%s d1 = %s, want %s", n.name, got, want)
		}
	}
	if got := valuesJSON(t, clients[1].client.Doc("cards", "d1").Values()); got != want {
		t.Fatalf("client on n2 sees %s, want %s", got, want)
	}
}

// TestChaosThreeNodes is the PLAN.md M9.5 scenario: 3 pebble nodes fully
// meshed through blockable proxies, 6 clients through their own proxies,
// random edits, partition block/unblock and node moves for TIDEPOOL_ITER
// steps; then everything unblocked, feeds settled, and values asserted equal
// across nodes and clients.
func TestChaosThreeNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in -short")
	}
	seed := testSeed(t)
	iter := testIter(t)
	rng := rand.New(rand.NewPCG(seed, seed))

	// Three nodes; learn addresses first, then restart each with its six
	// peer links (all directed pairs) through proxies.
	names := []string{"n1", "n2", "n3"}
	dirs := make([]string, len(names))
	nodes := make([]*node, len(names))
	for i, name := range names {
		dirs[i] = t.TempDir()
		nodes[i] = startNode(t, name, dirs[i], "", nil)
	}
	addrs := make(map[string]string, len(names))
	for i, n := range nodes {
		addrs[names[i]] = n.addr
	}
	peerLinks, peerProxies := linkProxies(t, names, addrs)
	for i, name := range names {
		nodes[i].ts.Close()
		nodes[i].srv.Stop()
		nodes[i] = startNode(t, name, dirs[i], addrs[names[i]], peerLinks[name])
	}

	// Six clients with their own actors and stores, on random nodes.
	clients := make([]*chaosClient, 6)
	for i := range clients {
		clients[i] = &chaosClient{
			actor: crdt.ActorID(fmt.Sprintf("%016x", i+1)),
			store: client.NewMemoryStore(),
		}
		clients[i].startOn(t, nodes[rng.IntN(len(nodes))].addr)
	}

	docs := []string{"board1", "board2", "board3", "board4", "board5"}

	edit := func(cc *chaosClient, doc string) {
		h := cc.client.Doc("cards", doc)
		switch rng.IntN(6) {
		case 0:
			_ = h.Set("title", fmt.Sprintf("t%d", rng.IntN(1000)))
		case 1:
			_ = h.Set("status", []string{"open", "closed"}[rng.IntN(2)])
		case 2:
			_ = h.Inc("votes", int64(rng.IntN(5))-2)
		case 3:
			_ = h.Add("labels", fmt.Sprintf("l%d", rng.IntN(5)))
		case 4:
			_ = h.InsertText("description", 0, "x")
		case 5:
			_ = h.Insert("checklist", 0, fmt.Sprintf("c%d", rng.IntN(100)))
		}
	}

	for step := 0; step < iter; step++ {
		switch roll := rng.IntN(10); {
		case roll < 6: // a random client edits a random doc
			cc := clients[rng.IntN(len(clients))]
			edit(cc, docs[rng.IntN(len(docs))])
		case roll < 8: // block or unblock a random directed peer link
			from, to := names[rng.IntN(3)], names[rng.IntN(3)]
			if px := peerProxies[from+"->"+to]; px != nil {
				px.Block()
			}
		case roll < 9: // block a random client link
			clients[rng.IntN(len(clients))].proxy.Block()
		default: // move a client to another node
			clients[rng.IntN(len(clients))].startOn(t, nodes[rng.IntN(len(nodes))].addr)
		}
	}

	// Unblock everything and let the system settle.
	for _, px := range peerProxies {
		px.Unblock()
	}
	for _, cc := range clients {
		cc.proxy.Unblock()
		waitSynced(t, cc.client, 30*time.Second)
	}
	waitFeedSettled(t, nodes, 60*time.Second)

	// Every node's doc values are identical, and every client's view
	// matches them.
	for _, doc := range docs {
		key := crdt.DocKey{Coll: "cards", Doc: doc}
		want := valuesJSON(t, nodeValues(t, nodes[0], key))
		for _, n := range nodes[1:] {
			if got := valuesJSON(t, nodeValues(t, n, key)); got != want {
				dumpDocOps(t, nodes, key)
				t.Fatalf("%s: %s = %s, n1 = %s", n.name, doc, got, want)
			}
		}
		for ci, cc := range clients {
			if got := valuesJSON(t, cc.client.Doc("cards", doc).Values()); got != want {
				t.Fatalf("client %d view of %s = %s, nodes say %s", ci, doc, got, want)
			}
		}
	}
}

// dumpDocOps logs every node's stored op list for one doc, for divergence
// diagnostics.
func dumpDocOps(t *testing.T, nodes []*node, key crdt.DocKey) {
	t.Helper()
	for _, n := range nodes {
		var ops []string
		err := n.store.DocOps(key, 0, func(so storage.StoredOp) error {
			ops = append(ops, fmt.Sprintf("%v/%s@%d", so.Op.ID, so.Op.Kind, so.Op.Lamport))
			return nil
		})
		if err != nil {
			t.Logf("%s: DocOps: %v", n.name, err)
			continue
		}
		t.Logf("%s %s ops: %s", n.name, key.Doc, strings.Join(ops, " "))
	}
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

// testIter honors TIDEPOOL_ITER (default 300).
func testIter(t *testing.T) int {
	t.Helper()
	if s := os.Getenv("TIDEPOOL_ITER"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 300
}
