package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/itsramhere/tidepool/internal/crdt"
)

// PebbleStore is the durable Store backend: one embedded LSM key-value store
// per node, keyed by the PLAN.md §4.8 layout.
type PebbleStore struct {
	db *pebble.DB
}

var _ Store = (*PebbleStore)(nil)

// Option customizes the Pebble options used by Open.
type Option func(*pebble.Options)

// WithFS overrides the filesystem Pebble stores its data on. Tests use it to
// inject an in-memory or crash-simulating FS.
func WithFS(fs vfs.FS) Option {
	return func(o *pebble.Options) { o.FS = fs }
}

// Open opens (or creates) a Pebble store rooted at dir.
func Open(dir string, opts ...Option) (*PebbleStore, error) {
	popts := &pebble.Options{}
	for _, opt := range opts {
		opt(popts)
	}
	db, err := pebble.Open(dir, popts)
	if err != nil {
		return nil, fmt.Errorf("open pebble store at %q: %w", dir, err)
	}
	return &PebbleStore{db: db}, nil
}

// Commit writes the batch in a single Pebble write, optionally followed by an
// fsync when b.Sync is set. A failed Commit leaves nothing behind.
func (s *PebbleStore) Commit(b *Batch) error {
	wb := s.db.NewBatch()
	defer wb.Close()
	for _, so := range b.Ops {
		raw, err := json.Marshal(so.Op)
		if err != nil {
			return fmt.Errorf("marshal op %v: %w", so.Op.ID, err)
		}
		if err := wb.Set(opKey(so.Op.Key, so.DocSeq), raw, nil); err != nil {
			return fmt.Errorf("stage op %v: %w", so.Op.ID, err)
		}
		if err := wb.Set(feedKey(so.FeedSeq), raw, nil); err != nil {
			return fmt.Errorf("stage feed entry %v: %w", so.Op.ID, err)
		}
	}
	for k, meta := range b.Metas {
		raw, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal meta %v: %w", k, err)
		}
		if err := wb.Set(metaKey(k), raw, nil); err != nil {
			return fmt.Errorf("stage meta %v: %w", k, err)
		}
	}
	for k, snap := range b.Snapshots {
		if err := wb.Set(snapshotKey(k), snap, nil); err != nil {
			return fmt.Errorf("stage snapshot %v: %w", k, err)
		}
	}
	for k, v := range b.Meta {
		if err := wb.Set([]byte(k), v, nil); err != nil {
			return fmt.Errorf("stage meta %q: %w", k, err)
		}
	}
	wopts := pebble.NoSync
	if b.Sync {
		wopts = pebble.Sync
	}
	if err := wb.Commit(wopts); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}
	return nil
}

// getRaw reads one point key; the value is cloned so it stays valid after the
// Pebble closer is released.
func (s *PebbleStore) getRaw(key []byte) ([]byte, bool, error) {
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get %q: %w", key, err)
	}
	out := bytes.Clone(val)
	if err := closer.Close(); err != nil {
		return nil, true, fmt.Errorf("release get %q: %w", key, err)
	}
	return out, true, nil
}

// DocMeta returns the document's meta, or ok=false when never committed.
func (s *PebbleStore) DocMeta(k crdt.DocKey) (DocMeta, bool, error) {
	raw, ok, err := s.getRaw(metaKey(k))
	if err != nil || !ok {
		return DocMeta{}, false, err
	}
	var meta DocMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return DocMeta{}, true, fmt.Errorf("parse meta %v: %w", k, err)
	}
	return meta, true, nil
}

// Snapshot returns the document's latest snapshot bytes, or ok=false.
func (s *PebbleStore) Snapshot(k crdt.DocKey) ([]byte, bool, error) {
	return s.getRaw(snapshotKey(k))
}

// GetMeta returns the raw value under key, or ok=false.
func (s *PebbleStore) GetMeta(key string) ([]byte, bool, error) {
	return s.getRaw([]byte(key))
}

// DocOps scans the document's op keys in ascending DocSeq order.
func (s *PebbleStore) DocOps(k crdt.DocKey, afterDocSeq uint64, fn func(StoredOp) error) error {
	prefix := docOpsPrefix(k)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return fmt.Errorf("iterate ops of %v: %w", k, err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		seq, err := parseDocSeq(iter.Key())
		if err != nil {
			return err
		}
		if seq <= afterDocSeq {
			continue
		}
		var op crdt.Op
		if err := json.Unmarshal(iter.Value(), &op); err != nil {
			return fmt.Errorf("parse op from %q: %w", iter.Key(), err)
		}
		if err := fn(StoredOp{Op: op, DocSeq: seq}); err != nil {
			return err
		}
	}
	return iter.Error()
}

// ListDocs calls fn with every doc ID that has meta under coll, sorted.
func (s *PebbleStore) ListDocs(coll string, fn func(docID string) error) error {
	prefix := docsPrefix(coll)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return fmt.Errorf("iterate docs of %q: %w", coll, err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		docID := string(iter.Key()[len(prefix):])
		if err := fn(docID); err != nil {
			return err
		}
	}
	return iter.Error()
}

// Feed returns ops from the feed in ascending FeedSeq order, up to limit
// (limit <= 0 means all).
func (s *PebbleStore) Feed(afterFeedSeq uint64, limit int) ([]StoredOp, error) {
	prefix := feedPrefix()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("iterate feed: %w", err)
	}
	defer iter.Close()
	var out []StoredOp
	for iter.First(); iter.Valid(); iter.Next() {
		seq, err := parseFeedSeq(iter.Key())
		if err != nil {
			return nil, err
		}
		if seq <= afterFeedSeq {
			continue
		}
		if limit > 0 && len(out) >= limit {
			break
		}
		var op crdt.Op
		if err := json.Unmarshal(iter.Value(), &op); err != nil {
			return nil, fmt.Errorf("parse op from %q: %w", iter.Key(), err)
		}
		out = append(out, StoredOp{Op: op, FeedSeq: seq})
	}
	return out, iter.Error()
}

// LastFeedSeq returns the highest feed seq stored, or 0 for an empty feed.
func (s *PebbleStore) LastFeedSeq() (uint64, error) {
	prefix := feedPrefix()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, fmt.Errorf("iterate feed: %w", err)
	}
	defer iter.Close()
	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, fmt.Errorf("seek last feed entry: %w", err)
		}
		return 0, nil
	}
	seq, err := parseFeedSeq(iter.Key())
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// Close closes the underlying Pebble database.
func (s *PebbleStore) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close pebble store: %w", err)
	}
	return nil
}
