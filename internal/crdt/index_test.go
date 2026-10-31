package crdt

import (
	"encoding/json"
	"testing"
)

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
