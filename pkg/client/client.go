// Package client provides an offline-capable Go SDK for Tidepool: local CRDT
// documents with pending-op queues, shape subscriptions, and automatic
// reconnection and reconciliation.
//
// The zero-configuration flow: New with a URL and token, Subscribe to the
// collections of interest, get doc handles with Doc and edit them; Run drives
// the connect/reconnect loop until its context is canceled. Edits work while
// disconnected — they land in the pending queue and are pushed on the next
// connection.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
)

const (
	baseBackoff        = 200 * time.Millisecond // first reconnect delay
	maxBackoff         = 10 * time.Second       // reconnect delay cap
	maxRetryBackoff    = 2 * time.Second        // per-doc resend cap
	dialTimeout        = 10 * time.Second
	handshakeTimeout   = 10 * time.Second
	writeTimeout       = 5 * time.Second
	sendBuffer         = 64 // frames queued to the connection writer
	errChanBuffer      = 16
	maxOpsPerPush      = 1000      // server default max_ops_per_push
	maxPushBytes       = 512 << 10 // stay well under the server's 1 MiB frame limit
	perOpFrameOverhead = 128       // JSON envelope cost per op in a push
	// Server frames can exceed the server's own 1 MiB client-frame limit —
	// a snapshot mirrors a whole document — so the client read limit is
	// generous.
	maxReadLimit = 16 << 20
)

// ErrAlreadyRunning is returned by Run when another Run is still active.
var ErrAlreadyRunning = errors.New("client: Run is already running")

// OpError reports one pushed op that the server refused: rejected ops were
// stored as noops (every replica agrees the op did nothing), invalid ops were
// dropped entirely along with all later pending ops of the same document.
type OpError struct {
	OpID   crdt.OpID
	Key    crdt.DocKey
	Status string // protocol.StatusRejected or protocol.StatusInvalid
	Reason string
}

func (e *OpError) Error() string {
	return fmt.Sprintf("client: op %v on %v %s: %s", e.OpID, e.Key, e.Status, e.Reason)
}

// Options configures a Client.
type Options struct {
	// URL is the server's sync endpoint, e.g. ws://127.0.0.1:7100/v1/sync.
	URL string
	// Token is the JWT presented in the hello frame.
	Token string
	// Actor identifies this device; generated when empty. A stored actor
	// from the LocalStore wins over a generated one.
	Actor crdt.ActorID
	// Schemas maps collection name to field schema. Docs in collections
	// without a schema can be read but not edited.
	Schemas map[string]crdt.Schema
	// Store persists the local model across restarts; MemoryStore when nil.
	Store LocalStore
	// Logger receives diagnostics; silent when nil.
	Logger *slog.Logger
}

// Subscription represents one shape subscription (collection + filter). The
// client re-subscribes automatically on every reconnect.
type Subscription struct {
	id     string
	coll   string
	filter string
	ready  bool
	failed error // why the subscription can never become ready
}

// ID returns the client-chosen subscription id used in wire frames.
func (s *Subscription) ID() string { return s.id }

// Client is one local replica. All methods are safe for concurrent use; the
// Client.mu mutex guards the whole local model, and connection writes are
// serialized through a per-connection writer goroutine so no method ever
// blocks on the network (see connWriter).
type Client struct {
	opts    Options
	actor   crdt.ActorID
	schemas map[string]crdt.Schema
	store   LocalStore
	log     *slog.Logger

	running atomic.Bool

	// mu guards everything below. Frame sends go through sendLocked (the
	// caller must hold mu); notification and error sends are non-blocking.
	mu       sync.Mutex
	cw       *connWriter // nil when disconnected
	docs     map[crdt.DocKey]*docState
	subs     map[string]*Subscription
	subOrder []string // subscription creation order
	nextSub  int
	nextReq  uint64
	ackDoc   map[uint64]crdt.DocKey // in-flight push req → doc
	changed  chan struct{}          // closed (and replaced) on every state change
	errCh    chan error
	onChange []func(crdt.DocKey)
}

// New builds a Client, restoring the local model from o.Store. A missing
// actor identity is generated once and persisted immediately.
func New(o Options) (*Client, error) {
	if o.URL == "" {
		return nil, errors.New("client: URL is required")
	}
	log := o.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	store := o.Store
	if store == nil {
		store = NewMemoryStore()
	}
	state, err := store.Load()
	if err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	actor := o.Actor
	if actor == "" {
		actor = state.Actor
	}
	generated := false
	if actor == "" {
		actor = crdt.NewActorID()
		generated = true
	}
	if err := crdt.ValidateActor(actor); err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	c := &Client{
		opts:    o,
		actor:   actor,
		schemas: o.Schemas,
		store:   store,
		log:     log,
		docs:    make(map[crdt.DocKey]*docState),
		subs:    make(map[string]*Subscription),
		ackDoc:  make(map[uint64]crdt.DocKey),
		changed: make(chan struct{}),
		errCh:   make(chan error, errChanBuffer),
	}
	for _, pd := range state.Docs {
		key := crdt.DocKey{Coll: pd.Coll, Doc: pd.Doc}
		if err := crdt.ValidateKey(key); err != nil {
			return nil, fmt.Errorf("client: stored doc: %w", err)
		}
		doc := crdt.NewDocument(key)
		if len(pd.Snapshot) > 0 {
			doc, err = crdt.UnmarshalSnapshot(pd.Snapshot)
			if err != nil {
				return nil, fmt.Errorf("client: stored doc %v: %w", key, err)
			}
			if doc.Key != key {
				return nil, fmt.Errorf("client: stored doc %v has snapshot for %v", key, doc.Key)
			}
		}
		c.docs[key] = &docState{
			key:       key,
			confirmed: doc,
			pending:   pd.Pending,
			sent:      make(map[crdt.OpID]struct{}),
		}
	}
	if generated {
		// Persist the fresh identity right away so a crash cannot silently
		// fork the device identity.
		if err := c.store.Save(c.stateLocked()); err != nil {
			log.Warn("client: persist generated actor", slog.Any("err", err))
		}
	}
	return c, nil
}

// Run connects, reconciles and serves frames until ctx is done. Connection
// losses are retried with exponential backoff (200 ms → 10 s, jittered).
// Run returns nil when ctx is done and ErrAlreadyRunning if a previous Run
// has not returned yet.
func (c *Client) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer c.running.Store(false)
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		err := c.connectAndServe(ctx, &attempt)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			c.log.Warn("client: connection lost", slog.Any("err", err), slog.Int("attempt", attempt))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoffDelay(attempt)):
		}
	}
}

// connectAndServe dials once and serves until the connection dies.
func (c *Client) connectAndServe(ctx context.Context, attempt *int) error {
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, _, err := websocket.Dial(dctx, c.opts.URL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.opts.URL, err)
	}
	return c.serve(ctx, conn, attempt)
}

// connWriter serializes all frame writes for one connection. Frame producers
// enqueue without ever blocking on the network: a full queue or a failed
// write tears the connection down and Run reconnects (PLAN.md pitfall: never
// block a state owner on a socket).
type connWriter struct {
	conn *websocket.Conn
	ch   chan []byte
	quit chan struct{}
}

// run writes queued frames until the writer is torn down. It is started once
// per connection by serve.
func (w *connWriter) run(log *slog.Logger) {
	for {
		select {
		case frame := <-w.ch:
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			err := w.conn.Write(ctx, websocket.MessageText, frame)
			cancel()
			if err != nil {
				log.Debug("client: write failed", slog.Any("err", err))
				_ = w.conn.CloseNow() // unblock the reader; Run reconnects
				return
			}
		case <-w.quit:
			return
		}
	}
}

// serve runs one connection: handshake, resubscribe, re-push pending, then
// the read loop. It resets *attempt after a successful handshake.
func (c *Client) serve(ctx context.Context, conn *websocket.Conn, attempt *int) error {
	// Server frames mirror whole documents (snapshots, op echoes), so the
	// read limit must exceed the coder/websocket default of 32 KiB.
	conn.SetReadLimit(maxReadLimit)
	cw := &connWriter{conn: conn, ch: make(chan []byte, sendBuffer), quit: make(chan struct{})}
	c.mu.Lock()
	c.cw = cw
	c.mu.Unlock()
	go cw.run(c.log)

	defer func() {
		c.mu.Lock()
		if c.cw == cw {
			c.cw = nil
		}
		// The next connection re-subscribes and re-pushes everything: ops
		// marked sent may have committed without their ack reaching us.
		for _, s := range c.subs {
			s.ready = false
		}
		clear(c.ackDoc)
		for _, ds := range c.docs {
			clear(ds.sent)
		}
		c.notifyLocked()
		c.mu.Unlock()
		close(cw.quit)
		_ = conn.CloseNow()
	}()

	// Handshake: hello must be the first frame; the server answers with a
	// welcome or an err and closes.
	if !c.sendLocked(protocol.NewHello(protocol.ProtocolVersion, c.opts.Token, c.actor)) {
		return errors.New("client: send hello failed")
	}
	wctx, cancelWelcome := context.WithTimeout(ctx, handshakeTimeout)
	typ, data, err := conn.Read(wctx)
	cancelWelcome()
	if err != nil {
		return fmt.Errorf("client: await welcome: %w", err)
	}
	if typ != websocket.MessageText {
		return errors.New("client: welcome frame is not text")
	}
	var welcome protocol.Welcome
	if err := json.Unmarshal(data, &welcome); err != nil || welcome.T != protocol.TypeWelcome {
		return fmt.Errorf("client: expected welcome, got %s", data)
	}
	if welcome.Actor != c.actor {
		return fmt.Errorf("client: welcome actor %q, want %q", welcome.Actor, c.actor)
	}

	c.mu.Lock()
	*attempt = 0
	// Re-subscribe every subscription with the confirmed VVs of its
	// collection, then re-push all pending ops in deterministic doc order.
	for _, id := range c.subOrder {
		if sub := c.subs[id]; sub != nil && sub.failed == nil {
			c.sendSubLocked(sub)
		}
	}
	for _, key := range c.sortedDocKeys() {
		if ds := c.docs[key]; len(ds.pending) > 0 {
			c.pushPendingLocked(ds)
		}
	}
	c.notifyLocked()
	c.mu.Unlock()

	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("client: read: %w", err)
		}
		if typ != websocket.MessageText {
			continue
		}
		if ferr := c.dispatch(data); ferr != nil {
			return ferr
		}
	}
}

// dispatch handles one server frame. A returned error ends the connection
// (Run reconnects); everything else is absorbed.
func (c *Client) dispatch(data []byte) error {
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		c.log.Warn("client: undecodable frame", slog.Any("err", err))
		return nil
	}
	switch env.T {
	case protocol.TypeSnapshot:
		c.handleSnapshot(data)
	case protocol.TypeOp:
		c.handleOpFrame(data)
	case protocol.TypeEvict:
		c.handleEvict(data)
	case protocol.TypeReady:
		c.handleReady(data)
	case protocol.TypeAck:
		c.handleAck(data)
	case protocol.TypeErr:
		var f protocol.Error
		if err := json.Unmarshal(data, &f); err != nil {
			return fmt.Errorf("client: undecodable err frame: %w", err)
		}
		c.mu.Lock()
		c.emitErrorLocked(fmt.Errorf("client: server error %s: %s", f.Code, f.Msg))
		c.mu.Unlock()
		return fmt.Errorf("client: server error %s: %s", f.Code, f.Msg)
	case protocol.TypeWelcome, protocol.TypePong:
		// welcome only precedes the read loop; pong answers nothing we send.
	default:
		c.log.Warn("client: unknown frame type", slog.String("t", env.T))
	}
	return nil
}

// handleSnapshot replaces the confirmed state of one document (PLAN.md §4.7:
// "Replace local confirmed state"). Pending ops survive: the view rebuild
// re-applies them, dropping any the snapshot already covers.
func (c *Client) handleSnapshot(data []byte) {
	var f protocol.SnapshotFrame
	if err := json.Unmarshal(data, &f); err != nil {
		c.log.Warn("client: bad snap frame", slog.Any("err", err))
		return
	}
	doc, err := crdt.UnmarshalSnapshot(f.Snapshot)
	if err != nil {
		c.log.Warn("client: bad snapshot in snap frame", slog.Any("err", err))
		return
	}
	c.mu.Lock()
	ds := c.ensureDocLocked(doc.Key)
	ds.confirmed = doc
	ds.viewValid = false
	c.notifyLocked()
	c.mu.Unlock()
	c.fireChange(doc.Key)
}

// handleOpFrame applies one committed op to the confirmed state (duplicates
// are ignored — the op is already known), removes it from pending when it is
// one of ours, and surfaces rejected ops (delivered as noops) as errors.
func (c *Client) handleOpFrame(data []byte) {
	var f protocol.OpFrame
	if err := json.Unmarshal(data, &f); err != nil {
		c.log.Warn("client: bad op frame", slog.Any("err", err))
		return
	}
	c.mu.Lock()
	ds := c.ensureDocLocked(f.Op.Key)
	if err := ds.confirmed.Apply(f.Op, c.schemas[f.Op.Key.Coll]); err != nil {
		if !errors.Is(err, crdt.ErrDuplicate) {
			c.log.Warn("client: apply remote op", slog.Any("op", f.Op.ID), slog.Any("err", err))
			c.mu.Unlock()
			return
		}
	} else {
		ds.viewValid = false
	}
	if i := pendingIndexOf(ds, f.Op.ID); i >= 0 {
		ds.pending = append(ds.pending[:i], ds.pending[i+1:]...)
		delete(ds.sent, f.Op.ID)
		if f.Op.Kind == crdt.KindNoop {
			// The server stored our op as a noop: it was rejected.
			c.emitErrorLocked(&OpError{OpID: f.Op.ID, Key: f.Op.Key, Status: protocol.StatusRejected, Reason: "rejected"})
		}
	}
	c.notifyLocked()
	c.mu.Unlock()
	c.fireChange(f.Op.Key)
}

// handleEvict drops a document that left the subscription's shape — unless
// local pending ops exist, which win over eviction (PLAN.md §4.7).
func (c *Client) handleEvict(data []byte) {
	var f protocol.Evict
	if err := json.Unmarshal(data, &f); err != nil {
		c.log.Warn("client: bad evict frame", slog.Any("err", err))
		return
	}
	c.mu.Lock()
	ds := c.docs[f.Key]
	if ds == nil {
		c.mu.Unlock()
		return
	}
	if len(ds.pending) > 0 {
		c.log.Debug("client: keep evicted doc with pending ops", slog.Any("key", f.Key))
		c.mu.Unlock()
		return
	}
	delete(c.docs, f.Key)
	c.notifyLocked()
	c.mu.Unlock()
	c.fireChange(f.Key)
}

// handleReady marks one subscription's initial reconcile as finished.
func (c *Client) handleReady(data []byte) {
	var f protocol.Ready
	if err := json.Unmarshal(data, &f); err != nil {
		c.log.Warn("client: bad ready frame", slog.Any("err", err))
		return
	}
	c.mu.Lock()
	if sub := c.subs[f.Sub]; sub != nil {
		sub.ready = true
	}
	c.notifyLocked()
	c.mu.Unlock()
}

// Subscribe adds a shape subscription for a collection. It is sent at once
// when connected, and re-sent on every reconnect. The collection must be
// listed in Options.Schemas; a failed subscription never becomes ready and
// surfaces its error on Errors().
func (c *Client) Subscribe(coll, filter string) *Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextSub++
	sub := &Subscription{id: fmt.Sprintf("s%d", c.nextSub), coll: coll, filter: filter}
	if _, ok := c.schemas[coll]; !ok {
		sub.failed = fmt.Errorf("client: no schema for collection %q", coll)
		c.emitErrorLocked(sub.failed)
	}
	c.subs[sub.id] = sub
	c.subOrder = append(c.subOrder, sub.id)
	if sub.failed == nil && c.cw != nil {
		c.sendSubLocked(sub)
	}
	return sub
}

// sendSubLocked sends one subscription frame with known = confirmed VVs of
// the local docs in its collection, so the server replays only what the
// client is missing (PLAN.md §4.7 reconcile).
func (c *Client) sendSubLocked(sub *Subscription) {
	known := make(map[string]protocol.VersionVector)
	for key, ds := range c.docs {
		if key.Coll != sub.coll {
			continue
		}
		known[key.Doc] = protocol.VersionVector(ds.confirmed.VV)
	}
	c.sendLocked(protocol.NewSub(sub.id, sub.coll, sub.filter, known))
}

// handleAck processes the results of one pushed chunk. ok waits for the G2
// echo, dup resolves the op locally (the server sends no echo for dups),
// retry and invalid follow PLAN.md §4.7's client rules.
func (c *Client) handleAck(data []byte) {
	var f protocol.Ack
	if err := json.Unmarshal(data, &f); err != nil {
		c.log.Warn("client: bad ack frame", slog.Any("err", err))
		return
	}
	c.mu.Lock()
	key, ok := c.ackDoc[f.Req]
	if !ok {
		c.mu.Unlock()
		c.log.Warn("client: ack for unknown push", slog.Uint64("req", f.Req))
		return
	}
	delete(c.ackDoc, f.Req)
	ds := c.docs[key]
	if ds == nil {
		c.mu.Unlock()
		return
	}
	var touched bool
	for _, r := range f.Results {
		switch r.Status {
		case protocol.StatusOK:
			// The G2 echo removes the op from pending.
			delete(ds.sent, r.ID)
			ds.retryAttempt = 0
		case protocol.StatusDup:
			// No echo follows a dup: resolve the op locally instead.
			delete(ds.sent, r.ID)
			if i := pendingIndexOf(ds, r.ID); i >= 0 {
				op := ds.pending[i]
				if err := ds.confirmed.Apply(op, c.schemas[key.Coll]); err != nil && !errors.Is(err, crdt.ErrDuplicate) {
					c.log.Warn("client: apply dup op", slog.Any("op", r.ID), slog.Any("err", err))
				} else {
					ds.viewValid = false
				}
				ds.pending = append(ds.pending[:i], ds.pending[i+1:]...)
				touched = true
			}
			ds.retryAttempt = 0
		case protocol.StatusRejected:
			// The noop echo carries the error report.
			delete(ds.sent, r.ID)
		case protocol.StatusRetry:
			// Keep the op pending; resend it and everything after it once
			// the backoff fires (PLAN.md: "resend from that op").
			for _, op := range ds.pending {
				if op.ID.Seq >= r.ID.Seq && op.ID.Actor == r.ID.Actor {
					delete(ds.sent, op.ID)
				}
			}
			c.scheduleResendLocked(ds)
		case protocol.StatusInvalid:
			// PLAN.md §4.7: drop that op and all later pending ops of the
			// same doc, and surface an error. Later ops of the same actor
			// can never commit once this one is gone.
			if i := pendingIndexOf(ds, r.ID); i >= 0 {
				for _, op := range ds.pending[i:] {
					delete(ds.sent, op.ID)
				}
				ds.pending = ds.pending[:i]
				ds.viewValid = false
				touched = true
			}
			c.emitErrorLocked(&OpError{OpID: r.ID, Key: key, Status: protocol.StatusInvalid, Reason: r.Reason})
		}
	}
	if touched {
		c.notifyLocked()
	}
	c.mu.Unlock()
	if touched {
		c.fireChange(key)
	}
}

// scheduleResendLocked arms the doc's resend timer with backoff. The timer
// fires pushPendingLocked, which sends every pending op not yet in flight.
func (c *Client) scheduleResendLocked(ds *docState) {
	ds.retryAttempt++
	delay := baseBackoff << min(ds.retryAttempt-1, 4)
	if delay > maxRetryBackoff {
		delay = maxRetryBackoff
	}
	key := ds.key
	time.AfterFunc(jitter(delay), func() { c.resendDoc(key) })
}

// resendDoc re-pushes a doc's unsent pending ops if still connected.
func (c *Client) resendDoc(key crdt.DocKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ds := c.docs[key]; ds != nil && c.cw != nil {
		c.pushPendingLocked(ds)
	}
}

// pushPendingLocked sends ds's unsent pending ops as one or more push frames
// (chunked under the server's op-count and frame-size limits) and marks them
// in flight. Pending ops form a contiguous seq suffix, so every chunk keeps
// per-actor order.
func (c *Client) pushPendingLocked(ds *docState) {
	var chunk []crdt.Op
	bytes := 0
	flush := func() {
		if len(chunk) == 0 {
			return
		}
		c.nextReq++
		req := c.nextReq
		if c.sendLocked(protocol.NewPush(req, chunk)) {
			c.ackDoc[req] = ds.key
			for _, op := range chunk {
				ds.sent[op.ID] = struct{}{}
			}
		}
		chunk = nil
		bytes = 0
	}
	for _, op := range ds.pending {
		if _, inFlight := ds.sent[op.ID]; inFlight {
			continue
		}
		size := perOpFrameOverhead + len(op.Value) + 24*len(op.Observed)
		if len(chunk) > 0 && (len(chunk) >= maxOpsPerPush || bytes+size > maxPushBytes) {
			flush()
		}
		chunk = append(chunk, op)
		bytes += size
	}
	flush()
}

// sendLocked queues one frame to the current connection writer. It reports
// false when no connection is up or the queue is full; in the latter case
// the connection is torn down and Run reconnects. Caller holds mu.
func (c *Client) sendLocked(frame any) bool {
	if c.cw == nil {
		return false
	}
	data, err := protocol.Encode(frame)
	if err != nil {
		c.log.Error("client: encode frame", slog.Any("err", err))
		return false
	}
	select {
	case c.cw.ch <- data:
		return true
	default:
		c.log.Warn("client: send queue full; dropping connection")
		_ = c.cw.conn.CloseNow()
		return false
	}
}

// ensureDocLocked returns the doc state, creating an empty confirmed
// document when the doc is unknown (a snapshot or op may arrive for a doc
// the client never opened). Caller holds mu.
func (c *Client) ensureDocLocked(key crdt.DocKey) *docState {
	if ds, ok := c.docs[key]; ok {
		return ds
	}
	ds := &docState{key: key, confirmed: crdt.NewDocument(key), sent: make(map[crdt.OpID]struct{})}
	c.docs[key] = ds
	return ds
}

// sortedDocKeys returns all doc keys in (coll, doc) order, so reconnect
// pushes and persisted files are deterministic. Caller holds mu.
func (c *Client) sortedDocKeys() []crdt.DocKey {
	keys := make([]crdt.DocKey, 0, len(c.docs))
	for key := range c.docs {
		keys = append(keys, key)
	}
	sortKeys(keys)
	return keys
}

// stateLocked builds the PersistedState for the local model. Caller holds mu.
func (c *Client) stateLocked() PersistedState {
	state := PersistedState{Actor: c.actor}
	for _, key := range c.sortedDocKeys() {
		ds := c.docs[key]
		snap, err := ds.confirmed.MarshalSnapshot()
		if err != nil {
			c.log.Error("client: marshal snapshot for store", slog.Any("key", key), slog.Any("err", err))
			continue
		}
		state.Docs = append(state.Docs, PersistedDoc{
			Coll:     key.Coll,
			Doc:      key.Doc,
			Snapshot: snap,
			Pending:  ds.pending,
		})
	}
	return state
}

// persistLocked saves the local model through the LocalStore. Failures are
// logged, never fatal: the in-memory model stays authoritative. Caller
// holds mu.
func (c *Client) persistLocked() {
	if err := c.store.Save(c.stateLocked()); err != nil {
		c.log.Warn("client: persist state", slog.Any("err", err))
	}
}

// emitErrorLocked delivers one error to Errors() without ever blocking; a
// full channel drops the error with a log line. Caller holds mu.
func (c *Client) emitErrorLocked(err error) {
	select {
	case c.errCh <- err:
	default:
		c.log.Warn("client: error channel full; dropping", slog.Any("err", err))
	}
}

// notifyLocked wakes every WaitSynced waiter. Caller holds mu.
func (c *Client) notifyLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// OnChange registers a callback invoked after any visible change to a
// document (incoming ops and snapshots, evictions, local edits). Callbacks
// run on the goroutine that produced the change; panics are recovered and
// logged.
func (c *Client) OnChange(fn func(crdt.DocKey)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onChange = append(c.onChange, fn)
}

// fireChange invokes the change callbacks for the given keys. Called without
// the mutex held so callbacks may call back into the client.
func (c *Client) fireChange(keys ...crdt.DocKey) {
	c.mu.Lock()
	cbs := make([]func(crdt.DocKey), len(c.onChange))
	copy(cbs, c.onChange)
	c.mu.Unlock()
	for _, fn := range cbs {
		for _, key := range keys {
			func() {
				defer func() {
					if r := recover(); r != nil {
						c.log.Error("client: OnChange callback panicked", slog.Any("panic", r))
					}
				}()
				fn(key)
			}()
		}
	}
}

// Errors exposes a channel receiving rejected and invalid ops (as *OpError)
// and fatal server errors. Receivers must keep draining it; when the channel
// is full errors are dropped and logged.
func (c *Client) Errors() <-chan error { return c.errCh }

// WaitSynced blocks until no pending ops remain and every subscription has
// finished its initial reconcile, or ctx is done. It is safe to call before
// Run: a client with nothing to wait for returns immediately.
func (c *Client) WaitSynced(ctx context.Context) error {
	for {
		c.mu.Lock()
		synced := true
		for _, id := range c.subOrder {
			sub := c.subs[id]
			if sub == nil || sub.failed != nil || !sub.ready {
				synced = false
				break
			}
		}
		if synced {
			for _, ds := range c.docs {
				if len(ds.pending) > 0 {
					synced = false
					break
				}
			}
		}
		changed := c.changed
		c.mu.Unlock()
		if synced {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("client: WaitSynced: %w", ctx.Err())
		}
	}
}

// backoffDelay returns the reconnect delay for the n-th consecutive failure:
// 200 ms doubling to the 10 s cap, jittered over the upper half so many
// clients never retry in lockstep.
func backoffDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := baseBackoff << min(attempt, 6)
	if d > maxBackoff {
		d = maxBackoff
	}
	return jitter(d)
}

// jitter returns d/2 + rand[0, d/2).
func jitter(d time.Duration) time.Duration {
	half := d / 2
	return half + time.Duration(rand.Float64()*float64(half))
}
