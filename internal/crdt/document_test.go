package crdt

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// testSchema has one field of every type for document and editor tests.
var testSchema = Schema{
	"title":     FieldRegister,
	"votes":     FieldCounter,
	"labels":    FieldSet,
	"checklist": FieldList,
	"body":      FieldText,
}

// jsonString returns s as a raw JSON string value.
func jsonString(t *testing.T, s string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal %q: %v", s, err)
	}
	return raw
}

func TestDocumentApplyBasics(t *testing.T) {
	key := DocKey{Coll: "cards", Doc: "c1"}
	d := NewDocument(key)

	set1 := Op{ID: OpID{Actor: actorA, Seq: 1}, Key: key, Lamport: 5, Field: "title", Kind: KindSet, Value: jsonString(t, "hello")}
	if err := d.Apply(set1, testSchema); err != nil {
		t.Fatalf("Apply(set1): %v", err)
	}
	if d.VV[actorA] != 1 || d.ActorClock[actorA] != 5 || d.MaxLamport != 5 {
		t.Errorf("after set1: vv=%d clock=%d max=%d, want 1/5/5", d.VV[actorA], d.ActorClock[actorA], d.MaxLamport)
	}
	st, ok := d.Fields["title"]
	if !ok || st.Type != FieldRegister || st.Reg == nil || !st.Reg.Set {
		t.Fatalf("field title not created: %+v", st)
	}
	if got := d.Values(); !reflect.DeepEqual(got, map[string]any{"title": "hello"}) {
		t.Errorf("Values() = %v, want {title: hello}", got)
	}

	// A noop advances only the VV: no clock update, no field checks, so a
	// field missing from the schema is fine.
	noop := Op{ID: OpID{Actor: actorB, Seq: 1}, Key: key, Lamport: 1, Field: "ghost", Kind: KindNoop}
	if err := d.Apply(noop, testSchema); err != nil {
		t.Fatalf("Apply(noop): %v", err)
	}
	if d.VV[actorB] != 1 {
		t.Errorf("VV[actorB] = %d, want 1", d.VV[actorB])
	}
	if _, ok := d.ActorClock[actorB]; ok {
		t.Error("noop must not update ActorClock")
	}
	if d.MaxLamport != 5 || len(d.Fields) != 1 {
		t.Errorf("noop changed state: max=%d fields=%d, want 5/1", d.MaxLamport, len(d.Fields))
	}

	dup := Op{ID: OpID{Actor: actorB, Seq: 1}, Key: key, Lamport: 9, Field: "title", Kind: KindSet, Value: jsonString(t, "x")}
	if err := d.Apply(dup, testSchema); !errors.Is(err, ErrDuplicate) {
		t.Errorf("Apply(dup) = %v, want ErrDuplicate", err)
	}
	oops := Op{ID: OpID{Actor: actorB, Seq: 3}, Key: key, Lamport: 9, Field: "title", Kind: KindSet, Value: jsonString(t, "x")}
	if err := d.Apply(oops, testSchema); !errors.Is(err, ErrOutOfOrder) {
		t.Errorf("Apply(gap) = %v, want ErrOutOfOrder", err)
	}

	set2 := Op{ID: OpID{Actor: actorB, Seq: 2}, Key: key, Lamport: 9, Field: "title", Kind: KindSet, Value: jsonString(t, "bye")}
	if err := d.Apply(set2, testSchema); err != nil {
		t.Fatalf("Apply(set2): %v", err)
	}
	// lamport 9 > 5: register replaced; clocks track per actor.
	if got := d.Values()["title"]; got != "bye" {
		t.Errorf("title = %v, want bye", got)
	}
	if d.ActorClock[actorA] != 5 || d.ActorClock[actorB] != 9 || d.MaxLamport != 9 {
		t.Errorf("clocks: A=%d B=%d max=%d, want 5/9/9", d.ActorClock[actorA], d.ActorClock[actorB], d.MaxLamport)
	}
}

func TestDocumentApplyWrongKey(t *testing.T) {
	key := DocKey{Coll: "cards", Doc: "c1"}
	d := NewDocument(key)
	op := Op{ID: OpID{Actor: actorA, Seq: 1}, Key: DocKey{Coll: "cards", Doc: "other"}, Lamport: 1, Field: "title", Kind: KindSet, Value: jsonString(t, "x")}
	if err := d.Apply(op, testSchema); !errors.Is(err, ErrInvalid) {
		t.Errorf("Apply(wrong key) = %v, want ErrInvalid", err)
	}
	if len(d.Fields) != 0 || d.MaxLamport != 0 {
		t.Error("failed Apply changed the document")
	}
}

func TestDocumentCovers(t *testing.T) {
	d := NewDocument(DocKey{Coll: "cards", Doc: "c1"})
	d.VV[actorA] = 3
	if !d.Covers(OpID{Actor: actorA, Seq: 3}) {
		t.Error("Covers(A,3) = false, want true")
	}
	if d.Covers(OpID{Actor: actorA, Seq: 4}) {
		t.Error("Covers(A,4) = true, want false")
	}
	if d.Covers(OpID{Actor: actorB, Seq: 1}) {
		t.Error("Covers(B,1) = true, want false")
	}
}

func TestDocumentValuesSkipsUntouchedFields(t *testing.T) {
	d := NewDocument(DocKey{Coll: "cards", Doc: "c1"})
	op := Op{ID: OpID{Actor: actorA, Seq: 1}, Key: d.Key, Lamport: 1, Field: "title", Kind: KindSet, Value: jsonString(t, "x")}
	if err := d.Apply(op, testSchema); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got := d.Values()
	if len(got) != 1 {
		t.Fatalf("Values() has %d fields, want 1 (untouched fields are absent)", len(got))
	}
	if _, ok := got["body"]; ok {
		t.Error("untouched text field present in Values()")
	}
}

func TestDocumentCloneDeep(t *testing.T) {
	key := DocKey{Coll: "cards", Doc: "c1"}
	d := NewDocument(key)
	ref := func(id OpID) *OpID { return &id }
	ops := []Op{
		{ID: OpID{actorA, 1}, Key: key, Lamport: 1, Field: "title", Kind: KindSet, Value: jsonString(t, "v1")},
		{ID: OpID{actorA, 2}, Key: key, Lamport: 2, Field: "title", Kind: KindSet, Value: jsonString(t, "v2")},
		{ID: OpID{actorA, 3}, Key: key, Lamport: 3, Field: "votes", Kind: KindInc, Delta: 5},
		{ID: OpID{actorA, 4}, Key: key, Lamport: 4, Field: "labels", Kind: KindAdd, Value: jsonString(t, "x")},
		{ID: OpID{actorB, 1}, Key: key, Lamport: 5, Field: "labels", Kind: KindAdd, Value: jsonString(t, "y")},
		{ID: OpID{actorA, 5}, Key: key, Lamport: 6, Field: "checklist", Kind: KindIns, Value: jsonString(t, "a")},
		{ID: OpID{actorA, 6}, Key: key, Lamport: 7, Field: "checklist", Kind: KindIns, Ref: ref(OpID{actorA, 5}), Value: jsonString(t, "b")},
		{ID: OpID{actorA, 7}, Key: key, Lamport: 8, Field: "checklist", Kind: KindDel, Ref: ref(OpID{actorA, 6})},
		{ID: OpID{actorA, 8}, Key: key, Lamport: 9, Field: "body", Kind: KindIns, Value: jsonString(t, "h")},
		{ID: OpID{actorA, 9}, Key: key, Lamport: 10, Field: "body", Kind: KindIns, Ref: ref(OpID{actorA, 8}), Value: jsonString(t, "i")},
		{ID: OpID{actorA, 10}, Key: key, Lamport: 11, Field: "title", Kind: KindSet, Value: json.RawMessage("null")},
	}
	for _, op := range ops {
		if err := d.Apply(op, testSchema); err != nil {
			t.Fatalf("apply %+v: %v", op, err)
		}
	}
	wantOriginal := map[string]any{
		"title":     nil,
		"votes":     int64(5),
		"labels":    []string{"x", "y"},
		"checklist": []any{"a"},
		"body":      "hi",
	}

	clone := d.Clone()
	if !reflect.DeepEqual(clone, d) {
		t.Fatal("Clone() is not deep-equal to the original")
	}

	// Mutate every part of the clone; the original must not move.
	mut := []Op{
		{ID: OpID{actorB, 2}, Key: key, Lamport: 12, Field: "title", Kind: KindSet, Value: jsonString(t, "mutated")},
		{ID: OpID{actorB, 3}, Key: key, Lamport: 13, Field: "votes", Kind: KindInc, Delta: -5},
		{ID: OpID{actorB, 4}, Key: key, Lamport: 14, Field: "labels", Kind: KindAdd, Value: jsonString(t, "zzz")},
		{ID: OpID{actorB, 5}, Key: key, Lamport: 15, Field: "labels", Kind: KindRem, Value: jsonString(t, "x"), Observed: []OpID{{Actor: actorA, Seq: 4}}},
		{ID: OpID{actorB, 6}, Key: key, Lamport: 16, Field: "checklist", Kind: KindIns, Value: jsonString(t, "new")},
		{ID: OpID{actorB, 7}, Key: key, Lamport: 17, Field: "body", Kind: KindIns, Value: jsonString(t, "Z")},
	}
	for _, op := range mut {
		if err := clone.Apply(op, testSchema); err != nil {
			t.Fatalf("apply mutation %+v: %v", op, err)
		}
	}
	if reflect.DeepEqual(clone, d) {
		t.Fatal("clone mutations did not change the clone")
	}
	if got := d.Values(); !reflect.DeepEqual(got, wantOriginal) {
		t.Errorf("original Values() = %v, want %v (clone mutation leaked)", got, wantOriginal)
	}
	if got := len(d.Fields["checklist"].Seq.Items); got != 2 {
		t.Errorf("original checklist has %d items, want 2 (clone insert leaked)", got)
	}
}
