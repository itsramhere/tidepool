package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/itsramhere/tidepool/internal/crdt"
)

// MemoryStore is an in-memory Store using the same key layout as the Pebble
// backend. It is not durable; unit tests and tooling use it.
type MemoryStore struct {
	// mu guards kv; every public method holds it for the whole call.
	mu sync.Mutex
	kv map[string][]byte
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{kv: make(map[string][]byte)}
}

// Commit writes the batch under the store's lock, making it atomic with
// respect to all other MemoryStore operations.
func (s *MemoryStore) Commit(b *Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, so := range b.Ops {
		raw, err := json.Marshal(so.Op)
		if err != nil {
			return fmt.Errorf("marshal op %v: %w", so.Op.ID, err)
		}
		s.kv[string(opKey(so.Op.Key, so.DocSeq))] = raw
		s.kv[string(feedKey(so.FeedSeq))] = raw
	}
	for k, meta := range b.Metas {
		raw, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal meta %v: %w", k, err)
		}
		s.kv[string(metaKey(k))] = raw
	}
	for k, snap := range b.Snapshots {
		s.kv[string(snapshotKey(k))] = snap
	}
	for k, v := range b.Meta {
		s.kv[k] = v
	}
	return nil
}

// DocMeta returns the document's meta, or ok=false when the document was
// never committed.
func (s *MemoryStore) DocMeta(k crdt.DocKey) (DocMeta, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.kv[string(metaKey(k))]
	if !ok {
		return DocMeta{}, false, nil
	}
	var meta DocMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return DocMeta{}, true, fmt.Errorf("parse meta %v: %w", k, err)
	}
	return meta, true, nil
}

// Snapshot returns the document's latest snapshot bytes, or ok=false.
func (s *MemoryStore) Snapshot(k crdt.DocKey) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.kv[string(snapshotKey(k))]
	if !ok {
		return nil, false, nil
	}
	return bytes.Clone(raw), true, nil
}

// DocOps scans the document's op keys in ascending DocSeq order (zero-padded,
// so lexicographic order is numeric order) and calls fn for each op after
// afterDocSeq.
func (s *MemoryStore) DocOps(k crdt.DocKey, afterDocSeq uint64, fn func(StoredOp) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(docOpsPrefix(k))
	var keys []string
	for key := range s.kv {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		seq, err := parseDocSeq([]byte(key))
		if err != nil {
			return err
		}
		if seq <= afterDocSeq {
			continue
		}
		var op crdt.Op
		if err := json.Unmarshal(s.kv[key], &op); err != nil {
			return fmt.Errorf("parse op from %q: %w", key, err)
		}
		if err := fn(StoredOp{Op: op, DocSeq: seq}); err != nil {
			return err
		}
	}
	return nil
}

// ListDocs calls fn with every doc ID that has meta under coll, sorted.
func (s *MemoryStore) ListDocs(coll string, fn func(docID string) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(docsPrefix(coll))
	var ids []string
	for key := range s.kv {
		if strings.HasPrefix(key, prefix) {
			ids = append(ids, key[len(prefix):])
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// Feed returns ops from the feed in ascending FeedSeq order, up to limit
// (limit <= 0 means all).
func (s *MemoryStore) Feed(afterFeedSeq uint64, limit int) ([]StoredOp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := feedPrefix()
	var keys []string
	for key := range s.kv {
		if bytes.HasPrefix([]byte(key), prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var out []StoredOp
	for _, key := range keys {
		seq, err := parseFeedSeq([]byte(key))
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
		if err := json.Unmarshal(s.kv[key], &op); err != nil {
			return nil, fmt.Errorf("parse op from %q: %w", key, err)
		}
		out = append(out, StoredOp{Op: op, FeedSeq: seq})
	}
	return out, nil
}

// LastFeedSeq returns the highest feed seq stored, or 0 for an empty feed.
func (s *MemoryStore) LastFeedSeq() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := feedPrefix()
	var last uint64
	for key := range s.kv {
		if !bytes.HasPrefix([]byte(key), prefix) {
			continue
		}
		seq, err := parseFeedSeq([]byte(key))
		if err != nil {
			return 0, err
		}
		if seq > last {
			last = seq
		}
	}
	return last, nil
}

// GetMeta returns the raw value stored under key, or ok=false.
func (s *MemoryStore) GetMeta(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.kv[key]
	if !ok {
		return nil, false, nil
	}
	return bytes.Clone(raw), true, nil
}

// Close is a no-op for the memory backend.
func (s *MemoryStore) Close() error { return nil }
