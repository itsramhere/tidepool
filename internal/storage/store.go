// Package storage defines the durable Store interface shared by the in-memory
// and Pebble backends, the Batch type used for group commit, and helpers to
// load documents from a store.
package storage

import (
	"errors"
	"fmt"

	"github.com/itsramhere/tidepool/internal/crdt"
)

// StoredOp is one op as persisted on a node. DocSeq is the document's local
// commit counter (the o/ key ordering, always causally valid to replay),
// FeedSeq the node-wide feed position (the f/ key). Reads reconstruct the
// relevant seq from the key: DocOps leaves FeedSeq zero and Feed leaves
// DocSeq zero, since each seq lives only in its own key space.
type StoredOp struct {
	Op      crdt.Op `json:"op"`
	DocSeq  uint64  `json:"docSeq"`
	FeedSeq uint64  `json:"feedSeq"`
}

// DocMeta tracks how far a document has been committed and how far its
// latest snapshot reaches.
type DocMeta struct {
	LastDocSeq uint64 `json:"lastDocSeq"`
	SnapDocSeq uint64 `json:"snapDocSeq"`
}

// Batch is one atomic commit: every entry becomes visible together, or none
// of it does.
type Batch struct {
	Ops       []StoredOp
	Metas     map[crdt.DocKey]DocMeta
	Snapshots map[crdt.DocKey][]byte
	Meta      map[string][]byte // raw keys like "a/<actor>", "p/<peer>", "m/node"
	Sync      bool
}

// Store is the durable op/meta storage behind a Tidepool node.
// Callbacks passed to DocOps and ListDocs must not call back into the same
// store (the memory implementation holds its lock across the scan).
type Store interface {
	// Commit writes the whole batch atomically; Sync controls whether the
	// write is durable before Commit returns.
	Commit(b *Batch) error
	DocMeta(k crdt.DocKey) (DocMeta, bool, error)
	Snapshot(k crdt.DocKey) ([]byte, bool, error)
	// DocOps calls fn for every stored op of the document with
	// DocSeq > afterDocSeq, in ascending DocSeq order.
	DocOps(k crdt.DocKey, afterDocSeq uint64, fn func(StoredOp) error) error
	ListDocs(coll string, fn func(docID string) error) error
	// Feed returns at most limit ops committed after afterFeedSeq in
	// ascending FeedSeq order; limit <= 0 means no limit.
	Feed(afterFeedSeq uint64, limit int) ([]StoredOp, error)
	LastFeedSeq() (uint64, error)
	GetMeta(key string) ([]byte, bool, error)
	Close() error
}

// LoadDocument rebuilds a document from the store: its latest snapshot (or an
// empty document) plus every stored op after that snapshot's DocSeq, applied
// in order. Already-applied ops (a snapshot racing a newer replay) are
// skipped; any other Apply error means the stored log diverged from the
// document and is returned.
func LoadDocument(s Store, k crdt.DocKey, sch crdt.Schema) (*crdt.Document, DocMeta, error) {
	meta, _, err := s.DocMeta(k)
	if err != nil {
		return nil, DocMeta{}, fmt.Errorf("load meta %v: %w", k, err)
	}
	doc := crdt.NewDocument(k)
	if raw, ok, err := s.Snapshot(k); err != nil {
		return nil, DocMeta{}, fmt.Errorf("load snapshot %v: %w", k, err)
	} else if ok {
		doc, err = crdt.UnmarshalSnapshot(raw)
		if err != nil {
			return nil, DocMeta{}, fmt.Errorf("parse snapshot %v: %w", k, err)
		}
	}
	err = s.DocOps(k, meta.SnapDocSeq, func(so StoredOp) error {
		if err := doc.Apply(so.Op, sch); err != nil && !errors.Is(err, crdt.ErrDuplicate) {
			return fmt.Errorf("replay %v into %v: %w", so.Op.ID, k, err)
		}
		return nil
	})
	if err != nil {
		return nil, DocMeta{}, err
	}
	return doc, meta, nil
}
