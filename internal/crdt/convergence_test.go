package crdt

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

// scenarioSteps returns the fuzz step count: TIDEPOOL_ITER when set,
// otherwise the 300 steps PLAN.md M3 specifies.
func scenarioSteps(t *testing.T) int {
	t.Helper()
	if s := os.Getenv("TIDEPOOL_ITER"); s != "" {
		return testIter(t)
	}
	return 300
}

func TestConvergenceFuzz(t *testing.T) {
	rng := newTestRand(t)
	steps := scenarioSteps(t)
	key := DocKey{Coll: "cards", Doc: "conv1"}

	reps, err := RunScenario(rng, key, steps)
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}

	// After the final sync every replica holds every op.
	wantOps := len(reps[0].Log)
	if wantOps == 0 {
		t.Fatal("scenario generated no ops")
	}
	for _, r := range reps {
		if len(r.Log) != wantOps {
			t.Fatalf("replica %s holds %d ops, replica %s holds %d", r.Actor, len(r.Log), reps[0].Actor, wantOps)
		}
	}

	base := reps[0]
	for _, r := range reps[1:] {
		if got := r.Doc.Values(); !reflect.DeepEqual(got, base.Doc.Values()) {
			t.Errorf("replica %s values = %v, want %v", r.Actor, got, base.Doc.Values())
		}
		if got := r.Doc.VV; !reflect.DeepEqual(got, base.Doc.VV) {
			t.Errorf("replica %s vv = %v, want %v", r.Actor, got, base.Doc.VV)
		}
		if got := r.Doc.ActorClock; !reflect.DeepEqual(got, base.Doc.ActorClock) {
			t.Errorf("replica %s clocks = %v, want %v", r.Actor, got, base.Doc.ActorClock)
		}
		if r.Doc.MaxLamport != base.Doc.MaxLamport {
			t.Errorf("replica %s maxLamport = %d, want %d", r.Actor, r.Doc.MaxLamport, base.Doc.MaxLamport)
		}
	}

	// Snapshot round-trip per replica, byte-stable as well as deep-equal.
	for _, r := range reps {
		raw, err := r.Doc.MarshalSnapshot()
		if err != nil {
			t.Fatalf("marshal snapshot for %s: %v", r.Actor, err)
		}
		back, err := UnmarshalSnapshot(raw)
		if err != nil {
			t.Fatalf("unmarshal snapshot for %s: %v", r.Actor, err)
		}
		if !reflect.DeepEqual(back, r.Doc) {
			t.Errorf("replica %s: snapshot round-trip differs from the document", r.Actor)
		}
		raw2, err := back.MarshalSnapshot()
		if err != nil {
			t.Fatalf("re-marshal snapshot for %s: %v", r.Actor, err)
		}
		if !bytes.Equal(raw, raw2) {
			t.Errorf("replica %s: snapshot bytes not stable across round-trip", r.Actor)
		}
	}
}

// TestApplyErrorLeavesDocUnchanged applies one op per §4.4 error case and
// requires the document snapshot to be byte-identical afterwards.
func TestApplyErrorLeavesDocUnchanged(t *testing.T) {
	key := DocKey{Coll: "cards", Doc: "c1"}
	d := NewDocument(key)
	ed := &Editor{Actor: actorA, Doc: d, Schema: testSchema}
	for _, edit := range []func() error{
		func() error { _, err := ed.Set("title", "hello"); return err },
		func() error { _, err := ed.Inc("votes", 2); return err },
		func() error { _, err := ed.Add("labels", "x"); return err },
		func() error { _, err := ed.InsertText("body", 0, "hi"); return err },
	} {
		if err := edit(); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	stranger := ActorID("9999999999999999")
	addY := Op{ID: OpID{Actor: stranger, Seq: 1}, Key: key, Lamport: d.MaxLamport + 1, Field: "labels", Kind: KindAdd, Value: jsonString(t, "y")}
	if err := d.Apply(addY, testSchema); err != nil {
		t.Fatalf("setup stranger add: %v", err)
	}

	before, err := d.MarshalSnapshot()
	if err != nil {
		t.Fatalf("marshal before: %v", err)
	}
	nextSeq := d.VV[stranger] + 1 // stranger continues from its setup add
	nextLam := d.MaxLamport + 1
	ref := func(id OpID) *OpID { return &id }

	cases := []struct {
		name    string
		op      Op
		wantErr error
	}{
		{"wrong doc key", Op{ID: OpID{stranger, nextSeq}, Key: DocKey{Coll: "cards", Doc: "zz"}, Lamport: nextLam, Field: "title", Kind: KindSet, Value: jsonString(t, "x")}, ErrInvalid},
		{"bad actor id", Op{ID: OpID{"nothex", 1}, Key: key, Lamport: nextLam, Field: "title", Kind: KindSet, Value: jsonString(t, "x")}, ErrInvalid},
		{"bad field name", Op{ID: OpID{stranger, nextSeq}, Key: key, Lamport: nextLam, Field: "Title", Kind: KindSet, Value: jsonString(t, "x")}, ErrInvalid},
		{"duplicate seq", Op{ID: OpID{actorA, 1}, Key: key, Lamport: nextLam + 100, Field: "title", Kind: KindSet, Value: jsonString(t, "x")}, ErrDuplicate},
		{"out of order seq", Op{ID: OpID{stranger, nextSeq + 3}, Key: key, Lamport: nextLam, Field: "title", Kind: KindSet, Value: jsonString(t, "x")}, ErrOutOfOrder},
		{"stale lamport", Op{ID: OpID{actorA, d.VV[actorA] + 1}, Key: key, Lamport: 1, Field: "title", Kind: KindSet, Value: jsonString(t, "x")}, ErrInvalid},
		{"field not in schema", Op{ID: OpID{stranger, nextSeq}, Key: key, Lamport: nextLam, Field: "ghost", Kind: KindSet, Value: jsonString(t, "x")}, ErrInvalid},
		{"kind not allowed for type", Op{ID: OpID{stranger, nextSeq}, Key: key, Lamport: nextLam, Field: "votes", Kind: KindSet, Value: jsonString(t, "x")}, ErrInvalid},
		{"object into register", Op{ID: OpID{stranger, nextSeq}, Key: key, Lamport: nextLam, Field: "title", Kind: KindSet, Value: json.RawMessage(`{"a":1}`)}, ErrInvalid},
		{"ins ref not covered", Op{ID: OpID{stranger, nextSeq}, Key: key, Lamport: nextLam, Field: "checklist", Kind: KindIns, Ref: ref(OpID{actorA, 999}), Value: jsonString(t, "x")}, ErrMissingDeps},
		{"del ref absent from field", Op{ID: OpID{stranger, nextSeq}, Key: key, Lamport: nextLam, Field: "checklist", Kind: KindDel, Ref: ref(OpID{actorA, 1})}, ErrMissingDeps},
		{"observed tag not covered", Op{ID: OpID{stranger, nextSeq}, Key: key, Lamport: nextLam, Field: "labels", Kind: KindRem, Value: jsonString(t, "y"), Observed: []OpID{{Actor: stranger, Seq: 9}}}, ErrMissingDeps},
		{"text ins two code points", Op{ID: OpID{stranger, nextSeq}, Key: key, Lamport: nextLam, Field: "body", Kind: KindIns, Value: jsonString(t, "ab")}, ErrInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := d.Apply(tc.op, testSchema)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Apply = %v, want %v", err, tc.wantErr)
			}
			after, err := d.MarshalSnapshot()
			if err != nil {
				t.Fatalf("marshal after: %v", err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("failed Apply changed the document")
			}
		})
	}
}
