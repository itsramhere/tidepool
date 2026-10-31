package crdt

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestAdaptiveIndexSnapshotRoundTripAfterLargeShift(t *testing.T) {
	d := NewDocument(DocKey{Coll: "cards", Doc: "roundtrip"})
	sch := Schema{"text": FieldText}
	actor := ActorID("aaaaaaaaaaaaaaaa")
	for seq := uint64(1); seq <= 101; seq++ {
		op := Op{ID: OpID{Actor: actor, Seq: seq}, Key: d.Key, Field: "text", Kind: KindIns, Lamport: seq, Value: json.RawMessage(`"x"`)}
		if seq > 1 && seq <= 100 {
			ref := OpID{Actor: actor, Seq: seq - 1}
			op.Ref = &ref
		}
		if err := d.Apply(op, sch); err != nil {
			t.Fatal(err)
		}
	}
	if d.Fields["text"].Seq.positions != nil {
		t.Fatal("expected unindexed large shift")
	}
	check := func() {
		t.Helper()
		raw, err := d.MarshalSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		back, err := UnmarshalSnapshot(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(back, d) || !reflect.DeepEqual(d.Clone(), d) {
			t.Fatal("derived index state differs after round trip")
		}
	}
	check()
	ref := OpID{Actor: actor, Seq: 49}
	late := Op{ID: OpID{Actor: "bbbbbbbbbbbbbbbb", Seq: 1}, Key: d.Key, Field: "text", Kind: KindIns, Lamport: 50, Ref: &ref, Value: json.RawMessage(`"y"`)}
	if err := d.Apply(late, sch); err != nil {
		t.Fatal(err)
	}
	check()
	ref = OpID{Actor: actor, Seq: 100}
	if err := d.Apply(Op{ID: OpID{Actor: actor, Seq: 102}, Key: d.Key, Field: "text", Kind: KindIns, Lamport: 102, Ref: &ref, Value: json.RawMessage(`"z"`)}, sch); err != nil {
		t.Fatal(err)
	}
	check()
}

func TestRGAIndexAcrossInsertDeleteCloneSnapshot(t *testing.T) {
	d := NewDocument(DocKey{Coll: "cards", Doc: "indexed"})
	sch := Schema{"text": FieldText}
	actor := ActorID("aaaaaaaaaaaaaaaa")
	refs := []*OpID{nil, nil, nil}
	for i := range 3 {
		op := Op{ID: OpID{Actor: actor, Seq: uint64(i + 1)}, Key: d.Key, Lamport: uint64(i + 1), Field: "text", Kind: KindIns, Value: json.RawMessage(`"x"`), Ref: refs[i]}
		if err := d.Apply(op, sch); err != nil {
			t.Fatal(err)
		}
	}
	first := OpID{Actor: actor, Seq: 1}
	op := Op{ID: OpID{Actor: actor, Seq: 4}, Key: d.Key, Lamport: 4, Field: "text", Kind: KindDel, Ref: &first}
	if err := d.Apply(op, sch); err != nil {
		t.Fatal(err)
	}
	snap, err := d.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := UnmarshalSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range []*Document{d, d.Clone(), restored} {
		g := doc.Fields["text"].Seq
		for i, item := range g.Items {
			if g.positions[item.ID] != i || g.indexOf(item.ID) != i {
				t.Fatal("stale RGA index")
			}
		}
		ref := OpID{Actor: actor, Seq: 2}
		op = Op{ID: OpID{Actor: actor, Seq: 5}, Key: d.Key, Lamport: 5, Field: "text", Kind: KindIns, Value: json.RawMessage(`"y"`), Ref: &ref}
		if err := doc.Apply(op, sch); err != nil {
			t.Fatal(err)
		}
		for i, item := range g.Items {
			if g.positions[item.ID] != i {
				t.Fatal("stale shifted position")
			}
		}
	}
}

func TestRGAIndexInvalidationAndRebuild(t *testing.T) {
	g := &RGA{}
	actor := ActorID("aaaaaaaaaaaaaaaa")
	for seq := uint64(1); seq <= 100; seq++ {
		op := Op{ID: OpID{Actor: actor, Seq: seq}, Kind: KindIns, Lamport: seq, Value: json.RawMessage(`"x"`)}
		if seq > 1 {
			ref := OpID{Actor: actor, Seq: seq - 1}
			op.Ref = &ref
		}
		g.apply(op)
	}
	g.apply(Op{ID: OpID{Actor: actor, Seq: 101}, Kind: KindIns, Lamport: 101, Value: json.RawMessage(`"y"`)})
	if g.positions != nil {
		t.Fatal("large shift should discard index")
	}
	ref := OpID{Actor: actor, Seq: 100}
	if g.indexOf(ref) != 100 {
		t.Fatal("unindexed lookup after shift")
	}
	g.apply(Op{ID: OpID{Actor: actor, Seq: 102}, Kind: KindIns, Lamport: 102, Ref: &ref, Value: json.RawMessage(`"z"`)})
	for i, item := range g.Items {
		if g.positions[item.ID] != i {
			t.Fatal("append did not rebuild exact positions")
		}
	}
}
