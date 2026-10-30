package storage

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/itsramhere/tidepool/internal/crdt"
)

// TestPebbleCrashLosesUnsynced simulates a process crash between batches:
// one batch committed with Sync:true and one with Sync:false, then a crash
// clone of the FS (which by definition contains only data that was synced).
// The synced batch must survive intact and the store must reopen cleanly;
// the unsynced batch must be gone.
func TestPebbleCrashLosesUnsynced(t *testing.T) {
	fs := vfs.NewCrashableMem()
	const dir = "tidepool-db"
	ops := suiteOps(t)

	ps, err := Open(dir, WithFS(fs))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := ps.Commit(&Batch{
		Ops:   toStoredOps(ops[:2], 1, 1),
		Metas: map[crdt.DocKey]DocMeta{suiteKey: {LastDocSeq: 2}},
		Sync:  true,
	}); err != nil {
		t.Fatalf("synced commit: %v", err)
	}
	if err := ps.Commit(&Batch{
		Ops:   toStoredOps(ops[2:], 3, 3),
		Metas: map[crdt.DocKey]DocMeta{suiteKey: {LastDocSeq: 5}},
		Sync:  false,
	}); err != nil {
		t.Fatalf("unsynced commit: %v", err)
	}

	// Snapshot the FS as of the last sync, then abandon the live store: a
	// crash is a process death, so nothing written afterwards reaches the
	// clone. (pebble/v2 replaced NewStrictMem/ResetToSyncedState with
	// CrashClone; see DEVIATIONS.md.)
	crashed := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	if err := ps.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ps2, err := Open(dir, WithFS(crashed))
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer ps2.Close()

	var got []StoredOp
	if err := ps2.DocOps(suiteKey, 0, func(so StoredOp) error { got = append(got, so); return nil }); err != nil {
		t.Fatalf("DocOps after crash: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("DocOps after crash = %d ops, want the 2 synced ops", len(got))
	}
	if len(got) == 2 && !reflect.DeepEqual(got[0].Op, ops[0]) {
		t.Errorf("first surviving op differs from the synced batch")
	}

	last, err := ps2.LastFeedSeq()
	if err != nil {
		t.Fatalf("LastFeedSeq after crash: %v", err)
	}
	if last != 2 {
		t.Errorf("LastFeedSeq after crash = %d, want 2 (unsynced feed entries lost)", last)
	}

	meta, ok, err := ps2.DocMeta(suiteKey)
	if err != nil || !ok {
		t.Fatalf("DocMeta after crash = (ok=%v, err=%v), want ok=true", ok, err)
	}
	if meta.LastDocSeq != 2 {
		t.Errorf("meta.LastDocSeq after crash = %d, want 2 (the synced value)", meta.LastDocSeq)
	}
}
