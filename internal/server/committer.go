package server

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"

	"github.com/google/cel-go/cel"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
	"github.com/itsramhere/tidepool/internal/schema"
	"github.com/itsramhere/tidepool/internal/storage"
)

const (
	// docCacheSize bounds the committer's document cache.
	docCacheSize = 10000
	// snapshotEvery is how many committed ops may accumulate in a document
	// before the committer snapshots it into the batch.
	snapshotEvery = 100
	// maxGroupSize is how many requests one group commit may cover.
	maxGroupSize = 256
)

// docEntry is the committer's cached view of one document. It is only ever
// mutated on the committer goroutine, in lockstep with the store.
type docEntry struct {
	doc          *crdt.Document
	meta         storage.DocMeta
	opsSinceSnap int
}

// Requests are handed to the committer, the only goroutine that touches
// documents and the store. Sessions block on the send, which is fine: the
// committer never blocks on a session (session.enqueue is non-blocking).
type request interface{}

type pushReq struct {
	sess  *session
	req   uint64
	uid   string
	actor crdt.ActorID
	ops   []crdt.Op

	// results is filled during staging and read after the commit.
	results []protocol.AckResult
}

type subscribeReq struct {
	sess   *session
	id     string
	coll   string
	filter string
	known  map[string]protocol.VersionVector
}

type unsubscribeReq struct {
	sess *session
	id   string
}

// removeSessionReq detaches a dying session's subscriptions.
type removeSessionReq struct {
	sess *session
}

// broadcastEntry is one committed op awaiting fan-out after the commit.
// doc is the post-apply document state, read by the read rule (committer
// goroutine only).
type broadcastEntry struct {
	op     crdt.Op
	coll   string
	author *session
	doc    *crdt.Document
}

// committer serializes all document and store access (PLAN.md golden rule
// 10: the single-writer design needs no locks on docs or the doc cache).
type committer struct {
	reqs chan request
	// reqs is never closed; shutdown is signaled via stopped so that dying
	// sessions can always hand over their cleanup request without the risk
	// of a send on a closed channel.
	stopped chan struct{}
	closed  atomic.Bool
	done    chan struct{}

	cfg     *Config
	schemas *schema.Config
	store   storage.Store
	log     *slog.Logger
	subs    *subscriptionSet

	lru         *lru.Cache[crdt.DocKey, *docEntry]
	nextFeedSeq uint64
	// filterCache memoizes compiled subscription filters by their text.
	// Committer-goroutine only, like everything else here; the set of
	// distinct filter texts a deployment uses is small.
	filterCache map[string]cel.Program

	// batches counts committed groups; a test-only counter for the group
	// commit test.
	batches atomic.Int64
}

func newCommitter(cfg *Config, schemas *schema.Config, store storage.Store, log *slog.Logger) (*committer, error) {
	last, err := store.LastFeedSeq()
	if err != nil {
		return nil, fmt.Errorf("read last feed seq: %w", err)
	}
	cache, err := lru.New[crdt.DocKey, *docEntry](docCacheSize)
	if err != nil {
		return nil, fmt.Errorf("create doc cache: %w", err)
	}
	return &committer{
		reqs:        make(chan request),
		stopped:     make(chan struct{}),
		done:        make(chan struct{}),
		cfg:         cfg,
		schemas:     schemas,
		store:       store,
		log:         log,
		subs:        newSubscriptionSet(),
		lru:         cache,
		nextFeedSeq: last,
		filterCache: make(map[string]cel.Program),
	}, nil
}

// run processes request groups until Stop is called, then drains whatever is
// still queued.
func (c *committer) run() {
	defer close(c.done)
	for {
		select {
		case req := <-c.reqs:
			c.handleGroup(req)
		case <-c.stopped:
			for {
				select {
				case req := <-c.reqs:
					c.handleGroup(req)
				default:
					return
				}
			}
		}
	}
}

// handleGroup takes one request plus up to maxGroupSize-1 more that are
// already queued (group commit), stages every op, commits once, then acks
// and broadcasts.
func (c *committer) handleGroup(first request) {
	group := make([]request, 0, maxGroupSize)
	group = append(group, first)
drain:
	for len(group) < maxGroupSize {
		select {
		case r := <-c.reqs:
			group = append(group, r)
		default:
			break drain
		}
	}

	batch := &storage.Batch{Sync: c.cfg.FsyncAlways()}
	var (
		pushes     []*pushReq
		broadcasts []broadcastEntry
		touched    = make(map[crdt.DocKey]struct{})
		sessions   = make(map[*session]struct{})
	)
	for _, req := range group {
		switch r := req.(type) {
		case *pushReq:
			pushes = append(pushes, r)
			sessions[r.sess] = struct{}{}
			for _, op := range r.ops {
				r.results = append(r.results, c.stagePush(r, op, batch, touched, &broadcasts))
			}
		case *subscribeReq:
			sessions[r.sess] = struct{}{}
			c.stageSubscribe(r)
		case *unsubscribeReq:
			c.subs.remove(r.sess, r.id)
		case *removeSessionReq:
			c.subs.removeAll(r.sess)
		}
	}

	if len(batch.Ops) > 0 || len(batch.Metas) > 0 || len(batch.Snapshots) > 0 || len(batch.Meta) > 0 {
		if err := c.store.Commit(batch); err != nil {
			c.log.Error("commit failed, dropping cache entries",
				slog.Any("err", err), slog.Int("group_size", len(group)), slog.Int("ops", len(batch.Ops)))
			for key := range touched {
				c.lru.Remove(key)
			}
			for sess := range sessions {
				sess.fail("internal", "commit failed; reconnect and reconcile")
			}
			return
		}
		c.batches.Add(1)
	}

	// Acks first, then the broadcast — a subscribed author sees its ack
	// before the echo of its own op.
	for _, r := range pushes {
		r.sess.sendMessage(protocol.NewAck(r.req, r.results))
		r.results = nil
	}
	for _, b := range broadcasts {
		c.broadcast(b)
	}
}

// stagePush validates one pushed op, applies it (or a noop for rejections)
// and stages the write. The returned AckResult is sent to the author after
// the commit.
func (c *committer) stagePush(r *pushReq, op crdt.Op, batch *storage.Batch, touched map[crdt.DocKey]struct{}, broadcasts *[]broadcastEntry) protocol.AckResult {
	invalid := func(reason string) protocol.AckResult {
		return protocol.AckResult{ID: op.ID, Status: protocol.StatusInvalid, Reason: reason}
	}
	// PLAN.md §4.7: only bad keys/actors/fields and ownership violations are
	// "invalid" (not stored at all); everything else that fails lands as a
	// noop so per-actor seqs stay contiguous.
	if err := crdt.ValidateKey(op.Key); err != nil {
		return invalid("bad_key")
	}
	if err := crdt.ValidateActor(op.ID.Actor); err != nil {
		return invalid("bad_actor")
	}
	if err := crdt.ValidateField(op.Field); err != nil {
		return invalid("bad_field")
	}

	// Actor ownership: a/{actor} must be absent (claim it for this session's
	// uid) or already owned by the same uid.
	ownerKey := "a/" + string(op.ID.Actor)
	owner, ok, err := c.store.GetMeta(ownerKey)
	if err != nil {
		c.log.Error("read actor owner", slog.String("actor", string(op.ID.Actor)), slog.Any("err", err))
		return invalid("internal_error")
	}
	if ok {
		if string(owner) != r.uid {
			return invalid("actor_not_owned")
		}
	} else {
		if batch.Meta == nil {
			batch.Meta = make(map[string][]byte)
		}
		batch.Meta[ownerKey] = []byte(r.uid)
	}

	sch, ok := c.schemas.Schema(op.Key.Coll)
	if !ok {
		return invalid("unknown_collection")
	}

	entry, err := c.loadDoc(op.Key, sch, touched)
	if err != nil {
		c.log.Error("load document", slog.Any("key", op.Key), slog.Any("err", err))
		return invalid("internal_error")
	}

	// When a write rule exists, trial-apply on a clone so the rule sees the
	// would-be state without mutating the document (a denial must leave the
	// document untouched).
	coll, _ := c.schemas.Collection(op.Key.Coll)
	_, hasWriteRule := coll.WriteRule()
	target := entry.doc
	if hasWriteRule {
		target = entry.doc.Clone()
	}

	err = target.Apply(op, sch)
	switch {
	case err == nil:
		if hasWriteRule && !c.writeAllowed(r.sess.auth, entry.doc, target, op) {
			return c.rejectWithNoop(op, entry, sch, batch, broadcasts, r.sess, "rule_denied")
		}
		if target != entry.doc {
			// Commit the trial state by applying for real.
			if err := entry.doc.Apply(op, sch); err != nil {
				// Unreachable: the same op just applied cleanly to a clone
				// of this document.
				c.log.Error("re-apply after rule allow", slog.Any("op", op.ID), slog.Any("err", err))
				return invalid("internal_error")
			}
		}
		c.stageCommitted(op, entry, batch, broadcasts, r.sess)
		return protocol.AckResult{ID: op.ID, Status: protocol.StatusOK}
	case errors.Is(err, crdt.ErrDuplicate):
		return protocol.AckResult{ID: op.ID, Status: protocol.StatusDup}
	case errors.Is(err, crdt.ErrOutOfOrder), errors.Is(err, crdt.ErrMissingDeps):
		return protocol.AckResult{ID: op.ID, Status: protocol.StatusRetry, Reason: err.Error()}
	case errors.Is(err, crdt.ErrInvalid):
		// Store a noop in place of the rejected op: the author and every
		// subscriber converge on "this op did nothing".
		return c.rejectWithNoop(op, entry, sch, batch, broadcasts, r.sess, "invalid_op")
	default:
		c.log.Error("apply", slog.Any("op", op.ID), slog.Any("err", err))
		return invalid("internal_error")
	}
}

// writeAllowed evaluates the collection's write rule: doc holds the values
// before the op, trial the values after. Evaluation errors deny (PLAN.md
// M7: errors count as deny) and are logged at debug level.
func (c *committer) writeAllowed(auth map[string]any, before, trial *crdt.Document, op crdt.Op) bool {
	coll, _ := c.schemas.Collection(op.Key.Coll)
	prog, ok := coll.WriteRule()
	if !ok {
		return true
	}
	allowed, err := schema.Eval(prog, schema.RuleInputs{
		Auth:   auth,
		Doc:    before.Values(),
		NewDoc: trial.Values(),
		IsNew:  before.Fields[op.Field] == nil,
		Field:  op.Field,
		Kind:   string(op.Kind),
	})
	if err != nil {
		c.log.Debug("write rule eval failed; denying", slog.Any("op", op.ID), slog.Any("err", err))
		return false
	}
	return allowed
}

// rejectWithNoop stores a noop in place of a denied op and reports it as
// rejected: the author and every subscriber converge on "this op did
// nothing".
func (c *committer) rejectWithNoop(op crdt.Op, entry *docEntry, sch crdt.Schema, batch *storage.Batch, broadcasts *[]broadcastEntry, sess *session, reason string) protocol.AckResult {
	noop := op
	noop.Kind = crdt.KindNoop
	noop.Value, noop.Delta, noop.Ref, noop.Observed = nil, 0, nil, nil
	if err := entry.doc.Apply(noop, sch); err != nil {
		// Unreachable: the original op passed the seq gates, and a noop
		// only advances the VV.
		c.log.Error("apply noop", slog.Any("op", op.ID), slog.Any("err", err))
		return protocol.AckResult{ID: op.ID, Status: protocol.StatusInvalid, Reason: "internal_error"}
	}
	c.stageCommitted(noop, entry, batch, broadcasts, sess)
	return protocol.AckResult{ID: op.ID, Status: protocol.StatusRejected, Reason: reason}
}

// stageCommitted records one applied op in the batch with fresh DocSeq and
// FeedSeq values and queues it for broadcast.
func (c *committer) stageCommitted(op crdt.Op, entry *docEntry, batch *storage.Batch, broadcasts *[]broadcastEntry, author *session) {
	entry.meta.LastDocSeq++
	entry.opsSinceSnap++
	if entry.opsSinceSnap >= snapshotEvery {
		snap, err := entry.doc.MarshalSnapshot()
		if err != nil {
			c.log.Error("marshal snapshot", slog.Any("err", err))
		} else {
			if batch.Snapshots == nil {
				batch.Snapshots = make(map[crdt.DocKey][]byte)
			}
			batch.Snapshots[op.Key] = snap
			entry.meta.SnapDocSeq = entry.meta.LastDocSeq
			entry.opsSinceSnap = 0
		}
	}
	c.nextFeedSeq++
	if batch.Metas == nil {
		batch.Metas = make(map[crdt.DocKey]storage.DocMeta)
	}
	batch.Metas[op.Key] = entry.meta
	batch.Ops = append(batch.Ops, storage.StoredOp{Op: op, DocSeq: entry.meta.LastDocSeq, FeedSeq: c.nextFeedSeq})
	*broadcasts = append(*broadcasts, broadcastEntry{op: op, coll: op.Key.Coll, author: author, doc: entry.doc})
}

// loadDoc returns the cached entry or loads it from the store.
func (c *committer) loadDoc(key crdt.DocKey, sch crdt.Schema, touched map[crdt.DocKey]struct{}) (*docEntry, error) {
	if entry, ok := c.lru.Get(key); ok {
		touched[key] = struct{}{}
		return entry, nil
	}
	doc, meta, err := storage.LoadDocument(c.store, key, sch)
	if err != nil {
		return nil, err
	}
	entry := &docEntry{doc: doc, meta: meta}
	c.lru.Add(key, entry)
	touched[key] = struct{}{}
	return entry, nil
}

// stageSubscribe reconciles one subscription and registers it. Everything
// happens on the committer goroutine, so no committed op can slip in between
// the reconciliation reads and the registration (see ADR 0002). The filter
// compiles here (cached by text); a bad filter fails only this subscription
// via an err frame carrying its sub id (PLAN.md M8) — the session stays up.
// Reconcile walks every doc of the collection and sends only what matches
// (see ADR 0004 for the O(collection) cost).
func (c *committer) stageSubscribe(r *subscribeReq) {
	sch, ok := c.schemas.Schema(r.coll)
	if !ok {
		r.sess.fail("unknown_collection", "collection is not configured")
		return
	}
	var filterProg cel.Program
	if r.filter != "" {
		prog, cached := c.filterCache[r.filter]
		if !cached {
			p, err := schema.CompileRule(r.coll+"/filter", r.filter)
			if err != nil {
				r.sess.sendMessage(protocol.NewSubError(r.id, "bad_filter", err.Error()))
				return
			}
			c.filterCache[r.filter] = p
			prog = p
		}
		filterProg = prog
	}
	sub := &subscription{
		id:         r.id,
		coll:       r.coll,
		filter:     r.filter,
		filterProg: filterProg,
		inShape:    make(map[string]struct{}),
		sess:       r.sess,
	}
	coll, _ := c.schemas.Collection(r.coll)
	readProg, hasRead := coll.ReadRule()

	var existing []string
	if err := c.store.ListDocs(r.coll, func(docID string) error {
		existing = append(existing, docID)
		return nil
	}); err != nil {
		c.log.Error("list docs", slog.String("coll", r.coll), slog.Any("err", err))
		r.sess.fail("internal", "subscribe failed")
		return
	}
	existingSet := make(map[string]struct{}, len(existing))
	for _, docID := range existing {
		existingSet[docID] = struct{}{}
	}
	for _, docID := range existing {
		key := crdt.DocKey{Coll: r.coll, Doc: docID}
		doc, _, err := storage.LoadDocument(c.store, key, sch)
		if err != nil {
			c.log.Error("load doc for subscribe", slog.Any("key", key), slog.Any("err", err))
			r.sess.fail("internal", "subscribe failed")
			return
		}
		if !c.matches(sub, readProg, hasRead, doc.Values()) {
			continue // outside the shape: no snapshot, no ops, not in inShape
		}
		if vv, known := r.known[docID]; known {
			// Send only the ops the client is missing, in docSeq order.
			err := c.store.DocOps(key, 0, func(so storage.StoredOp) error {
				if so.Op.ID.Seq <= vv[so.Op.ID.Actor] {
					return nil
				}
				r.sess.sendMessage(protocol.NewOp(r.id, so.Op))
				return nil
			})
			if err != nil {
				c.log.Error("replay ops for subscribe", slog.Any("key", key), slog.Any("err", err))
				r.sess.fail("internal", "subscribe failed")
				return
			}
		} else {
			snap, err := doc.MarshalSnapshot()
			if err != nil {
				c.log.Error("marshal snapshot for subscribe", slog.Any("key", key), slog.Any("err", err))
				r.sess.fail("internal", "subscribe failed")
				return
			}
			r.sess.sendMessage(protocol.NewSnapshot(r.id, snap))
		}
		sub.inShape[docID] = struct{}{}
	}
	// Docs the client knows that are outside the shape now — gone here, or
	// no longer matching — are evicted so the client's copy follows the
	// shape (move-out at reconcile time).
	knownIDs := make([]string, 0, len(r.known))
	for docID := range r.known {
		knownIDs = append(knownIDs, docID)
	}
	sort.Strings(knownIDs)
	for _, docID := range knownIDs {
		if _, ok := existingSet[docID]; !ok {
			r.sess.sendMessage(protocol.NewEvict(r.id, crdt.DocKey{Coll: r.coll, Doc: docID}))
			continue
		}
		if _, inShape := sub.inShape[docID]; !inShape {
			r.sess.sendMessage(protocol.NewEvict(r.id, crdt.DocKey{Coll: r.coll, Doc: docID}))
		}
	}
	r.sess.sendMessage(protocol.NewReady(r.id))
	c.subs.add(sub)
	return
}

// broadcast fans one committed op out to every subscription on its
// collection, maintaining each subscription's shape (PLAN.md M8):
//
//   - move-in (!was && is): the sub had no copy — send a full snapshot;
//   - inside (was && is): send the op;
//   - move-out (was && !is): send the op then evict, so the client sees the
//     last transition (and the author's G2 echo) before dropping the doc;
//   - outside (!was && !is): nothing.
//
// The author echo falls back to sub:"" when none of the author's own
// subscriptions delivered an op frame (e.g. the op moved a doc into the
// author's shape as a snapshot), keeping the G2 exactly-once guarantee.
func (c *committer) broadcast(b broadcastEntry) {
	coll, _ := c.schemas.Collection(b.coll)
	readProg, hasRead := coll.ReadRule()
	subs := c.subs.forColl(b.coll)
	var values map[string]any
	if hasRead || len(subs) > 0 {
		values = b.doc.Values()
	}
	var snap []byte
	docID := b.op.Key.Doc
	authorMatched := false
	for _, sub := range subs {
		was := false
		if sub.inShape != nil {
			_, was = sub.inShape[docID]
		}
		is := c.matches(sub, readProg, hasRead, values)
		switch {
		case !was && is:
			// Move-in: the sub has no copy, so send the full state.
			if snap == nil {
				s, err := b.doc.MarshalSnapshot()
				if err != nil {
					c.log.Error("marshal snapshot for move-in", slog.Any("key", b.op.Key), slog.Any("err", err))
					continue
				}
				snap = s
			}
			sub.sess.sendMessage(protocol.NewSnapshot(sub.id, snap))
			sub.inShape[docID] = struct{}{}
		case was && is:
			sub.sess.sendMessage(protocol.NewOp(sub.id, b.op))
			if sub.sess == b.author {
				authorMatched = true
			}
		case was && !is:
			// Move-out: the op first (the author's echo rides it), then the
			// evict that drops the doc.
			sub.sess.sendMessage(protocol.NewOp(sub.id, b.op))
			sub.sess.sendMessage(protocol.NewEvict(sub.id, b.op.Key))
			delete(sub.inShape, docID)
			if sub.sess == b.author {
				authorMatched = true
			}
		}
	}
	if b.author != nil && !authorMatched {
		b.author.sendMessage(protocol.NewOp("", b.op))
	}
}

// matches reports whether one document (by its current values) belongs to
// the subscription's shape: the collection's read rule AND the
// subscription's filter must both pass (PLAN.md M8). Evaluation errors deny
// and are logged at debug level.
func (c *committer) matches(sub *subscription, readProg cel.Program, hasRead bool, values map[string]any) bool {
	if hasRead {
		allowed, err := schema.Eval(readProg, schema.RuleInputs{Auth: sub.sess.auth, Doc: values, NewDoc: values})
		if err != nil {
			c.log.Debug("read rule eval failed; denying", slog.Any("err", err))
			return false
		}
		if !allowed {
			return false
		}
	}
	if sub.filterProg != nil {
		allowed, err := schema.Eval(sub.filterProg, schema.RuleInputs{Auth: sub.sess.auth, Doc: values, NewDoc: values})
		if err != nil {
			c.log.Debug("filter eval failed; denying", slog.Any("err", err))
			return false
		}
		if !allowed {
			return false
		}
	}
	return true
}
