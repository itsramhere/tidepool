package client

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/itsramhere/tidepool/internal/crdt"
)

// slogOp is the log attribute for one op's identity.
func slogOp(op crdt.Op) slog.Attr { return slog.Any("op", op.ID) }

// docState is one document's local model, guarded by Client.mu:
//   - confirmed: every op known to be on the server (echoes, reconcile,
//     snapshots applied),
//   - pending: local ops not yet confirmed by the server, in creation order
//     (per-actor seqs stay contiguous), with sent marking the contiguous
//     prefix currently in flight,
//   - view: confirmed.Clone() with pending applied — the editable state.
type docState struct {
	key       crdt.DocKey
	confirmed *crdt.Document
	pending   []crdt.Op
	sent      map[crdt.OpID]struct{}
	view      *crdt.Document
	viewValid bool
	// retryAttempt counts consecutive retry acks for this doc's pending
	// tail; reset by any ok/dup ack.
	retryAttempt int
}

// pendingIndexOf returns the index of id in ds.pending, or -1.
func pendingIndexOf(ds *docState, id crdt.OpID) int {
	for i, op := range ds.pending {
		if op.ID == id {
			return i
		}
	}
	return -1
}

// sortKeys orders doc keys by (coll, doc).
func sortKeys(keys []crdt.DocKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Coll != keys[j].Coll {
			return keys[i].Coll < keys[j].Coll
		}
		return keys[i].Doc < keys[j].Doc
	})
}

// viewLocked returns the document's editable view: confirmed state with all
// pending ops applied. The view is cached and rebuilt whenever confirmed
// state changed. Pending ops that the confirmed state already covers (their
// echo or a reconcile delivered them first) are dropped as duplicates.
// Caller holds mu.
func (c *Client) viewLocked(ds *docState) *crdt.Document {
	if ds.viewValid {
		return ds.view
	}
	sch := c.schemas[ds.key.Coll]
	view := ds.confirmed.Clone()
	kept := make([]crdt.Op, 0, len(ds.pending))
	for _, op := range ds.pending {
		err := view.Apply(op, sch)
		switch {
		case err == nil:
			kept = append(kept, op)
		case errors.Is(err, crdt.ErrDuplicate):
			// Already confirmed server-side; the echo will remove it too.
		default:
			// Unreachable for well-formed state: pending seqs are contiguous
			// behind confirmed, and deps visible at edit time only grow.
			// Keep the op (it may apply after more confirmed state arrives)
			// and make the anomaly visible.
			c.log.Warn("client: pending op failed view apply", slogOp(op), slog.Any("err", err))
			kept = append(kept, op)
		}
	}
	ds.pending = kept
	ds.view = view
	ds.viewValid = true
	return view
}

// Doc returns a handle for one document, creating it (empty, fully local)
// when unknown. Editing an unknown doc works while offline; it syncs on the
// next connection.
func (c *Client) Doc(coll, id string) *DocHandle {
	key := crdt.DocKey{Coll: coll, Doc: id}
	c.mu.Lock()
	c.ensureDocLocked(key)
	c.mu.Unlock()
	return &DocHandle{c: c, key: key}
}

// DocHandle edits and reads one document. Handles are cheap value objects;
// all methods are safe for concurrent use.
type DocHandle struct {
	c   *Client
	key crdt.DocKey
}

// Key returns the document's collection and id.
func (h *DocHandle) Key() crdt.DocKey { return h.key }

// Values returns the materialized field map of the document's current view
// (confirmed state plus pending local ops). Fields that never received an op
// are absent.
func (h *DocHandle) Values() map[string]any {
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	ds := h.c.docs[h.key]
	if ds == nil {
		return map[string]any{}
	}
	return h.c.viewLocked(ds).Values()
}

// Set builds, applies locally and queues a register set op.
func (h *DocHandle) Set(field string, v any) error {
	return h.c.edit(h.key, func(ed *crdt.Editor) ([]crdt.Op, error) { return ed.Set(field, v) })
}

// Inc builds, applies locally and queues a counter inc op.
func (h *DocHandle) Inc(field string, delta int64) error {
	return h.c.edit(h.key, func(ed *crdt.Editor) ([]crdt.Op, error) { return ed.Inc(field, delta) })
}

// Add builds, applies locally and queues a set add op.
func (h *DocHandle) Add(field string, s string) error {
	return h.c.edit(h.key, func(ed *crdt.Editor) ([]crdt.Op, error) { return ed.Add(field, s) })
}

// Remove builds, applies locally and queues a set rem op observing every
// live tag of s.
func (h *DocHandle) Remove(field string, s string) error {
	return h.c.edit(h.key, func(ed *crdt.Editor) ([]crdt.Op, error) { return ed.Remove(field, s) })
}

// Insert builds, applies locally and queues a list ins op. Index 0 inserts
// at the head; otherwise the element lands after the visible element at
// index-1.
func (h *DocHandle) Insert(field string, index int, v any) error {
	return h.c.edit(h.key, func(ed *crdt.Editor) ([]crdt.Op, error) { return ed.Insert(field, index, v) })
}

// InsertText builds, applies locally and queues one ins op per code point of
// s, so the run lands as a unit at index.
func (h *DocHandle) InsertText(field string, index int, s string) error {
	return h.c.edit(h.key, func(ed *crdt.Editor) ([]crdt.Op, error) { return ed.InsertText(field, index, s) })
}

// Delete builds, applies locally and queues a del op for the visible element
// at index of a list or text field.
func (h *DocHandle) Delete(field string, index int) error {
	return h.c.edit(h.key, func(ed *crdt.Editor) ([]crdt.Op, error) { return ed.Delete(field, index) })
}

// edit runs fn against the doc's view and queues the ops it returns. The
// editor applies each op to the view immediately, so the view always
// includes pending ops and stays valid (it is rebuilt from confirmed plus
// pending on the next confirmed change). Caller-visible side effects: the
// ops are persisted and, when connected, pushed.
func (c *Client) edit(key crdt.DocKey, fn func(*crdt.Editor) ([]crdt.Op, error)) error {
	c.mu.Lock()
	ds := c.ensureDocLocked(key)
	sch, ok := c.schemas[key.Coll]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("client: no schema for collection %q", key.Coll)
	}
	ed := &crdt.Editor{Actor: c.actor, Doc: c.viewLocked(ds), Schema: sch}
	ops, err := fn(ed)
	if err != nil {
		// The editor may have applied some ops before failing (it does not
		// for the handle methods, but stay safe): rebuild the view.
		ds.viewValid = false
		c.mu.Unlock()
		return err
	}
	ds.pending = append(ds.pending, ops...)
	if c.cw != nil {
		c.pushPendingLocked(ds)
	}
	c.persistLocked()
	c.mu.Unlock()
	c.fireChange(key)
	return nil
}
