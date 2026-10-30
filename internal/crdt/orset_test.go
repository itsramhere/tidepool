package crdt

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

const (
	actorA = ActorID("0123456789abcdef")
	actorB = ActorID("fedcba9876543210")
	actorC = ActorID("00000000000000ff")
)

// setAddOp builds an add op for elem with the given write timestamp.
func setAddOp(t *testing.T, lamport uint64, actor ActorID, elem string) Op {
	t.Helper()
	raw, err := json.Marshal(elem)
	if err != nil {
		t.Fatalf("marshal %q: %v", elem, err)
	}
	return Op{ID: OpID{Actor: actor, Seq: 1}, Lamport: lamport, Kind: KindAdd, Value: raw}
}

// applySetOps checks and applies ops in order to a fresh ORSet.
func applySetOps(t *testing.T, ops ...Op) *ORSet {
	t.Helper()
	s := &ORSet{}
	for _, op := range ops {
		if err := s.check(op); err != nil {
			t.Fatalf("check(%+v): %v", op, err)
		}
		s.apply(op)
	}
	return s
}

func TestORSetAddWins(t *testing.T) {
	// A adds e; B removes it observing only A's tag; C's add is concurrent
	// with B's rem and must survive it.
	addA := setAddOp(t, 1, actorA, "e")
	remB := Op{ID: OpID{Actor: actorB, Seq: 1}, Lamport: 2, Kind: KindRem, Value: addA.Value, Observed: []OpID{addA.ID}}
	addC := setAddOp(t, 3, actorC, "e")

	s := applySetOps(t, addA, remB, addC)
	if got := s.Materialize(); !reflect.DeepEqual(got, []string{"e"}) {
		t.Errorf("Materialize() = %v, want [e] (add wins)", got)
	}
	if tags := s.Tags("e"); len(tags) != 1 || tags[0] != addC.ID {
		t.Errorf("Tags(e) = %v, want [%v]", tags, addC.ID)
	}
}

func TestORSetRemoveObserved(t *testing.T) {
	addA := setAddOp(t, 1, actorA, "e")
	remB := Op{ID: OpID{Actor: actorB, Seq: 1}, Lamport: 2, Kind: KindRem, Value: addA.Value, Observed: []OpID{addA.ID}}

	s := applySetOps(t, addA, remB)
	if got := s.Materialize(); len(got) != 0 {
		t.Errorf("Materialize() = %v, want empty after observed remove", got)
	}
	if tags := s.Tags("e"); tags != nil {
		t.Errorf("Tags(e) = %v, want nil after removal", tags)
	}
}

func TestORSetRemoveOfAlreadyRemovedTagIsHarmless(t *testing.T) {
	addA := setAddOp(t, 1, actorA, "e")
	remB := Op{ID: OpID{Actor: actorB, Seq: 1}, Lamport: 2, Kind: KindRem, Value: addA.Value, Observed: []OpID{addA.ID}}
	remB2 := Op{ID: OpID{Actor: actorB, Seq: 2}, Lamport: 3, Kind: KindRem, Value: addA.Value, Observed: []OpID{addA.ID}}
	remC := Op{ID: OpID{Actor: actorC, Seq: 1}, Lamport: 4, Kind: KindRem, Value: addA.Value, Observed: []OpID{{Actor: actorA, Seq: 99}}}

	s := applySetOps(t, addA, remB, remB2, remC) // second rem repeats the tag; third removes an unknown tag
	if got := s.Materialize(); len(got) != 0 {
		t.Errorf("Materialize() = %v, want empty", got)
	}

	// rem of an element that was never added must be harmless too.
	s2 := applySetOps(t, remC)
	if got := s2.Materialize(); len(got) != 0 {
		t.Errorf("Materialize() = %v, want empty", got)
	}
}

func TestORSetPartialRemoveKeepsOtherTags(t *testing.T) {
	addA := setAddOp(t, 1, actorA, "e")
	addC := Op{ID: OpID{Actor: actorC, Seq: 1}, Lamport: 2, Kind: KindAdd, Value: addA.Value}
	remB := Op{ID: OpID{Actor: actorB, Seq: 1}, Lamport: 3, Kind: KindRem, Value: addA.Value, Observed: []OpID{addA.ID}}

	s := applySetOps(t, addA, addC, remB)
	if got := s.Materialize(); !reflect.DeepEqual(got, []string{"e"}) {
		t.Errorf("Materialize() = %v, want [e] (C's tag still live)", got)
	}
	if tags := s.Tags("e"); len(tags) != 1 || tags[0] != addC.ID {
		t.Errorf("Tags(e) = %v, want [%v]", tags, addC.ID)
	}
}

func TestORSetMaterializeSorted(t *testing.T) {
	s := applySetOps(t,
		setAddOp(t, 1, actorA, "b"),
		setAddOp(t, 2, actorB, "a"),
		setAddOp(t, 3, actorC, "c"),
	)
	if got := s.Materialize(); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("Materialize() = %v, want [a b c]", got)
	}
}

func TestORSetTagsSortedByActorSeq(t *testing.T) {
	// Same element, tags added so that arrival order differs from
	// (actor, seq) order.
	s := applySetOps(t,
		setAddOp(t, 1, actorB, "e"), // tag (B,1)
		Op{ID: OpID{Actor: actorA, Seq: 1}, Lamport: 2, Kind: KindAdd, Value: json.RawMessage(`"e"`)}, // tag (A,1)
		Op{ID: OpID{Actor: actorB, Seq: 2}, Lamport: 3, Kind: KindAdd, Value: json.RawMessage(`"e"`)}, // tag (B,2)
	)
	want := []OpID{{Actor: actorA, Seq: 1}, {Actor: actorB, Seq: 1}, {Actor: actorB, Seq: 2}}
	if got := s.Tags("e"); !reflect.DeepEqual(got, want) {
		t.Errorf("Tags(e) = %v, want %v", got, want)
	}
}

func TestORSetCheck(t *testing.T) {
	tests := []struct {
		name    string
		op      Op
		wantErr error
	}{
		{"add string", setAddOp(t, 1, actorA, "e"), nil},
		{"rem with string value", Op{Kind: KindRem, Value: json.RawMessage(`"e"`), Observed: []OpID{{Actor: actorA, Seq: 1}}}, nil},
		{"rem without observed tags", Op{Kind: KindRem, Value: json.RawMessage(`"e"`)}, nil},
		{"wrong kind", Op{Kind: KindSet, Value: json.RawMessage(`"e"`)}, ErrInvalid},
		{"add number", Op{Kind: KindAdd, Value: json.RawMessage(`3`)}, ErrInvalid},
		{"add object", Op{Kind: KindAdd, Value: json.RawMessage(`{"x":1}`)}, ErrInvalid},
		{"add missing value", Op{Kind: KindAdd}, ErrInvalid},
		{"rem missing value", Op{Kind: KindRem, Observed: []OpID{{Actor: actorA, Seq: 1}}}, ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ORSet{}
			err := s.check(tt.op)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("check() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("check() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
