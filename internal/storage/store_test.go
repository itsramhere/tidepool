package storage

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/itsramhere/tidepool/internal/crdt"
)

var (
	suiteKey  = crdt.DocKey{Coll: "cards", Doc: "c1"}
	suiteKey2 = crdt.DocKey{Coll: "cards", Doc: "c2"}
	suiteEd   = crdt.ActorID("a1b2c3d4e5f60708")
)

// suiteOps builds five valid ops over suiteKey, one per field type.
func suiteOps(t *testing.T) []crdt.Op {
	t.Helper()
	doc := crdt.NewDocument(suiteKey)
	ed := &crdt.Editor{Actor: suiteEd, Doc: doc, Schema: crdt.ScenarioSchema}
	var ops []crdt.Op
	for _, edit := range []func() ([]crdt.Op, error){
		func() ([]crdt.Op, error) { return ed.Set("title", "hello") },
		func() ([]crdt.Op, error) { return ed.Inc("votes", 3) },
		func() ([]crdt.Op, error) { return ed.Add("labels", "red") },
		func() ([]crdt.Op, error) { return ed.Insert("checklist", 0, "first") },
		func() ([]crdt.Op, error) { return ed.InsertText("body", 0, "hi") },
	} {
		created, err := edit()
		if err != nil {
			t.Fatalf("suite edit: %v", err)
		}
		ops = append(ops, created...)
	}
	return ops
}

// toStoredOps stamps ops with consecutive DocSeq/FeedSeq from the given
// starting values.
func toStoredOps(ops []crdt.Op, docStart, feedStart uint64) []StoredOp {
	out := make([]StoredOp, len(ops))
	for i, op := range ops {
		out[i] = StoredOp{Op: op, DocSeq: docStart + uint64(i), FeedSeq: feedStart + uint64(i)}
	}
	return out
}

// applyOps applies ops to a document, failing the test on any error.
func applyOps(t *testing.T, doc *crdt.Document, ops []crdt.Op) {
	t.Helper()
	for i, op := range ops {
		if err := doc.Apply(op, crdt.ScenarioSchema); err != nil {
			t.Fatalf("apply op %d (%v): %v", i, op.ID, err)
		}
	}
}

// runStoreSuite runs the storage test suite against one implementation.
func runStoreSuite(t *testing.T, newStore func(*testing.T) Store) {
	t.Run("CommitAndDocOps", func(t *testing.T) {
		s := newStore(t)
		defer s.Close()
		ops := suiteOps(t)
		if err := s.Commit(&Batch{Ops: toStoredOps(ops, 1, 1), Sync: true}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		var got []StoredOp
		if err := s.DocOps(suiteKey, 0, func(so StoredOp) error { got = append(got, so); return nil }); err != nil {
			t.Fatalf("DocOps: %v", err)
		}
		if len(got) != len(ops) {
			t.Fatalf("DocOps returned %d ops, want %d", len(got), len(ops))
		}
		for i, so := range got {
			if so.DocSeq != uint64(i+1) {
				t.Errorf("op %d has DocSeq %d, want %d (ascending order)", i, so.DocSeq, i+1)
			}
			if !reflect.DeepEqual(so.Op, ops[i]) {
				t.Errorf("op %d differs:\ngot  %+v\nwant %+v", i, so.Op, ops[i])
			}
		}

		got = nil
		if err := s.DocOps(suiteKey, 2, func(so StoredOp) error { got = append(got, so); return nil }); err != nil {
			t.Fatalf("DocOps(after 2): %v", err)
		}
		if len(got) != len(ops)-2 || got[0].DocSeq != 3 {
			t.Errorf("DocOps(after 2) returned %d ops starting at %d, want %d starting at 3", len(got), got[0].DocSeq, len(ops)-2)
		}
	})

	t.Run("DocMeta", func(t *testing.T) {
		s := newStore(t)
		defer s.Close()
		if _, ok, err := s.DocMeta(suiteKey); err != nil || ok {
			t.Fatalf("DocMeta(unknown) = (ok=%v, err=%v), want ok=false", ok, err)
		}
		want := DocMeta{LastDocSeq: 3, SnapDocSeq: 2}
		if err := s.Commit(&Batch{Metas: map[crdt.DocKey]DocMeta{suiteKey: want}}); err != nil {
			t.Fatalf("commit meta: %v", err)
		}
		got, ok, err := s.DocMeta(suiteKey)
		if err != nil || !ok {
			t.Fatalf("DocMeta = (ok=%v, err=%v), want ok=true", ok, err)
		}
		if got != want {
			t.Errorf("DocMeta = %+v, want %+v", got, want)
		}
	})

	t.Run("Snapshot", func(t *testing.T) {
		s := newStore(t)
		defer s.Close()
		if _, ok, err := s.Snapshot(suiteKey); err != nil || ok {
			t.Fatalf("Snapshot(unknown) = (ok=%v, err=%v), want ok=false", ok, err)
		}
		want := []byte(`{"key":{"coll":"cards","doc":"c1"}}`)
		if err := s.Commit(&Batch{Snapshots: map[crdt.DocKey][]byte{suiteKey: want}}); err != nil {
			t.Fatalf("commit snapshot: %v", err)
		}
		got, ok, err := s.Snapshot(suiteKey)
		if err != nil || !ok {
			t.Fatalf("Snapshot = (ok=%v, err=%v), want ok=true", ok, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Snapshot = %s, want %s", got, want)
		}
	})

	t.Run("Feed", func(t *testing.T) {
		s := newStore(t)
		defer s.Close()
		ops := suiteOps(t)
		if err := s.Commit(&Batch{Ops: toStoredOps(ops, 1, 1)}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		all, err := s.Feed(0, 0)
		if err != nil {
			t.Fatalf("Feed(0,0): %v", err)
		}
		if len(all) != len(ops) {
			t.Fatalf("Feed(0,0) = %d ops, want %d", len(all), len(ops))
		}
		for i, so := range all {
			if so.FeedSeq != uint64(i+1) {
				t.Errorf("feed op %d has FeedSeq %d, want %d (ascending)", i, so.FeedSeq, i+1)
			}
			if !reflect.DeepEqual(so.Op, ops[i]) {
				t.Errorf("feed op %d differs", i)
			}
		}
		head, err := s.Feed(0, 2)
		if err != nil {
			t.Fatalf("Feed(0,2): %v", err)
		}
		if len(head) != 2 || head[0].FeedSeq != 1 || head[1].FeedSeq != 2 {
			t.Errorf("Feed(0,2) = %d ops starting at feed %d, want 2 starting at 1", len(head), head[0].FeedSeq)
		}
		tail, err := s.Feed(2, 0)
		if err != nil {
			t.Fatalf("Feed(2,0): %v", err)
		}
		if len(tail) != len(ops)-2 || tail[0].FeedSeq != 3 {
			t.Errorf("Feed(2,0) = %d ops starting at feed %d, want %d starting at 3", len(tail), tail[0].FeedSeq, len(ops)-2)
		}
		empty, err := s.Feed(uint64(len(ops)), 0)
		if err != nil || len(empty) != 0 {
			t.Errorf("Feed(past end) = %d ops, err %v, want 0/nil", len(empty), err)
		}
		last, err := s.LastFeedSeq()
		if err != nil {
			t.Fatalf("LastFeedSeq: %v", err)
		}
		if last != uint64(len(ops)) {
			t.Errorf("LastFeedSeq = %d, want %d", last, len(ops))
		}
	})

	t.Run("ListDocs", func(t *testing.T) {
		s := newStore(t)
		defer s.Close()
		err := s.Commit(&Batch{Metas: map[crdt.DocKey]DocMeta{
			suiteKey:                   {LastDocSeq: 1},
			suiteKey2:                  {LastDocSeq: 1},
			{Coll: "notes", Doc: "n1"}: {LastDocSeq: 1},
		}})
		if err != nil {
			t.Fatalf("commit metas: %v", err)
		}
		var cards []string
		if err := s.ListDocs("cards", func(docID string) error { cards = append(cards, docID); return nil }); err != nil {
			t.Fatalf("ListDocs(cards): %v", err)
		}
		if !reflect.DeepEqual(cards, []string{"c1", "c2"}) {
			t.Errorf("ListDocs(cards) = %v, want [c1 c2]", cards)
		}
		var notes []string
		if err := s.ListDocs("notes", func(docID string) error { notes = append(notes, docID); return nil }); err != nil {
			t.Fatalf("ListDocs(notes): %v", err)
		}
		if !reflect.DeepEqual(notes, []string{"n1"}) {
			t.Errorf("ListDocs(notes) = %v, want [n1]", notes)
		}
		var none []string
		if err := s.ListDocs("boards", func(docID string) error { none = append(none, docID); return nil }); err != nil {
			t.Fatalf("ListDocs(boards): %v", err)
		}
		if len(none) != 0 {
			t.Errorf("ListDocs(boards) = %v, want empty", none)
		}
	})

	t.Run("GetMeta", func(t *testing.T) {
		s := newStore(t)
		defer s.Close()
		if _, ok, err := s.GetMeta("a/" + string(suiteEd)); err != nil || ok {
			t.Fatalf("GetMeta(unknown) = (ok=%v, err=%v), want ok=false", ok, err)
		}
		err := s.Commit(&Batch{Meta: map[string][]byte{
			"a/" + string(suiteEd): []byte("alice"),
			"m/node":               []byte("node1"),
		}})
		if err != nil {
			t.Fatalf("commit meta: %v", err)
		}
		got, ok, err := s.GetMeta("a/" + string(suiteEd))
		if err != nil || !ok {
			t.Fatalf("GetMeta = (ok=%v, err=%v), want ok=true", ok, err)
		}
		if string(got) != "alice" {
			t.Errorf("GetMeta = %q, want alice", got)
		}
		got, ok, err = s.GetMeta("m/node")
		if err != nil || !ok || string(got) != "node1" {
			t.Errorf("GetMeta(m/node) = (%q, ok=%v, err=%v), want (node1, true, nil)", got, ok, err)
		}
	})

	t.Run("LoadDocument", func(t *testing.T) {
		s := newStore(t)
		defer s.Close()
		ops := suiteOps(t)

		// Snapshot after the first two ops; the rest replay from the log.
		snapDoc := crdt.NewDocument(suiteKey)
		applyOps(t, snapDoc, ops[:2])
		snap, err := snapDoc.MarshalSnapshot()
		if err != nil {
			t.Fatalf("marshal snapshot: %v", err)
		}
		err = s.Commit(&Batch{
			Ops:       toStoredOps(ops[:2], 1, 1),
			Metas:     map[crdt.DocKey]DocMeta{suiteKey: {LastDocSeq: 2, SnapDocSeq: 2}},
			Snapshots: map[crdt.DocKey][]byte{suiteKey: snap},
			Sync:      true,
		})
		if err != nil {
			t.Fatalf("commit snapshot batch: %v", err)
		}
		err = s.Commit(&Batch{
			Ops:   toStoredOps(ops[2:], 3, 3),
			Metas: map[crdt.DocKey]DocMeta{suiteKey: {LastDocSeq: 5, SnapDocSeq: 2}},
		})
		if err != nil {
			t.Fatalf("commit log batch: %v", err)
		}

		doc, meta, err := LoadDocument(s, suiteKey, crdt.ScenarioSchema)
		if err != nil {
			t.Fatalf("LoadDocument: %v", err)
		}
		if meta != (DocMeta{LastDocSeq: 5, SnapDocSeq: 2}) {
			t.Errorf("meta = %+v, want {5 2}", meta)
		}
		want := crdt.NewDocument(suiteKey)
		applyOps(t, want, ops)
		if got := doc.Values(); !reflect.DeepEqual(got, want.Values()) {
			t.Errorf("Values() = %v, want %v", got, want.Values())
		}
		if got := doc.VV; !reflect.DeepEqual(got, want.VV) {
			t.Errorf("VV = %v, want %v", got, want.VV)
		}

		// An unknown document loads as empty.
		empty, meta, err := LoadDocument(s, suiteKey2, crdt.ScenarioSchema)
		if err != nil {
			t.Fatalf("LoadDocument(unknown): %v", err)
		}
		if meta != (DocMeta{}) || len(empty.Values()) != 0 {
			t.Errorf("unknown doc = (meta %+v, values %v), want zero meta and no values", meta, empty.Values())
		}
	})
}

func TestMemoryStoreSuite(t *testing.T) {
	runStoreSuite(t, func(t *testing.T) Store { return NewMemoryStore() })
}

func TestPebbleStoreSuite(t *testing.T) {
	runStoreSuite(t, func(t *testing.T) Store {
		ps, err := Open(filepath.Join(t.TempDir(), "db"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		// Subtests in runStoreSuite defer s.Close(); no cleanup here, or the
		// store would be closed twice.
		return ps
	})
}

// TestPebbleStoreReopen verifies durability across a clean close and reopen
// (pebble only; the memory store has nothing to persist).
func TestPebbleStoreReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	ops := suiteOps(t)

	ps, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	err = ps.Commit(&Batch{
		Ops:   toStoredOps(ops, 1, 1),
		Metas: map[crdt.DocKey]DocMeta{suiteKey: {LastDocSeq: uint64(len(ops))}},
		Sync:  true,
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := ps.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ps2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ps2.Close()

	var got []StoredOp
	if err := ps2.DocOps(suiteKey, 0, func(so StoredOp) error { got = append(got, so); return nil }); err != nil {
		t.Fatalf("DocOps: %v", err)
	}
	if len(got) != len(ops) {
		t.Errorf("DocOps after reopen = %d ops, want %d", len(got), len(ops))
	}
	last, err := ps2.LastFeedSeq()
	if err != nil {
		t.Fatalf("LastFeedSeq: %v", err)
	}
	if last != uint64(len(ops)) {
		t.Errorf("LastFeedSeq after reopen = %d, want %d", last, len(ops))
	}
	if meta, ok, err := ps2.DocMeta(suiteKey); err != nil || !ok || meta.LastDocSeq != uint64(len(ops)) {
		t.Errorf("DocMeta after reopen = (%+v, ok=%v, err=%v)", meta, ok, err)
	}
}
