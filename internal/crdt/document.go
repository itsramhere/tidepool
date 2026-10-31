package crdt

import (
	"encoding/json"
	"fmt"
)

// Document is one replicated document: every op of every actor applied in
// seq order per actor, plus the derived per-actor clocks. All replicas that
// have applied the same op set have identical Documents.
type Document struct {
	Key DocKey
	// VV maps each actor to the last seq applied from it; seqs are
	// contiguous per actor, so the VV alone says which ops are known.
	VV map[ActorID]uint64
	// ActorClock maps each actor to its latest applied op's lamport; an
	// actor's lamports must strictly increase.
	ActorClock map[ActorID]uint64
	MaxLamport uint64
	// Fields holds the CRDT state per field; fields that never received an
	// op are absent. In every FieldState exactly one of the pointers is
	// non-nil.
	Fields map[string]*FieldState
}

// FieldState is one field's CRDT state: its type plus exactly one of the
// typed pointers.
type FieldState struct {
	Type FieldType
	Reg  *Register
	Ctr  *Counter
	Set  *ORSet
	Seq  *RGA
}

// NewDocument returns an empty document for the given key.
func NewDocument(k DocKey) *Document {
	return &Document{
		Key:        k,
		VV:         make(map[ActorID]uint64),
		ActorClock: make(map[ActorID]uint64),
		Fields:     make(map[string]*FieldState),
	}
}

// newFieldState returns a fresh empty state for the type. The caller has
// already verified the type is one of the five known ones.
func newFieldState(ft FieldType) *FieldState {
	st := &FieldState{Type: ft}
	switch ft {
	case FieldRegister:
		st.Reg = &Register{}
	case FieldCounter:
		st.Ctr = &Counter{}
	case FieldSet:
		st.Set = &ORSet{}
	case FieldList:
		st.Seq = &RGA{}
	case FieldText:
		st.Seq = &RGA{Text: true}
	}
	return st
}

// check validates op against the field state without writing anything.
func (st *FieldState) check(op Op, vv map[ActorID]uint64) error {
	switch st.Type {
	case FieldRegister:
		return st.Reg.check(op)
	case FieldCounter:
		return st.Ctr.check(op)
	case FieldSet:
		return st.Set.check(op)
	case FieldList, FieldText:
		return st.Seq.check(op, vv)
	default:
		return fmt.Errorf("%w: unknown field type %q", ErrInvalid, st.Type)
	}
}

// apply applies a checked op to the field state.
func (st *FieldState) apply(op Op) {
	switch st.Type {
	case FieldRegister:
		st.Reg.apply(op)
	case FieldCounter:
		st.Ctr.apply(op)
	case FieldSet:
		st.Set.apply(op)
	case FieldList, FieldText:
		st.Seq.apply(op)
	}
}

// Apply validates and applies op following PLAN.md §4.4, in that exact order,
// and mutates nothing when any check fails. ErrDuplicate is reported for
// already-applied ops (callers usually treat it as success); ErrOutOfOrder
// and ErrMissingDeps mean the op may become applicable after other ops
// arrive; ErrInvalid means it never will.
func (d *Document) Apply(op Op, s Schema) error {
	if op.Key != d.Key {
		return fmt.Errorf("%w: op key %+v does not match document %+v", ErrInvalid, op.Key, d.Key)
	}
	if err := ValidateKey(op.Key); err != nil {
		return err
	}
	if err := ValidateActor(op.ID.Actor); err != nil {
		return err
	}
	if err := ValidateField(op.Field); err != nil {
		return err
	}

	if op.ID.Seq <= d.VV[op.ID.Actor] {
		return fmt.Errorf("%w: seq %d <= vv %d for actor %s", ErrDuplicate, op.ID.Seq, d.VV[op.ID.Actor], op.ID.Actor)
	}
	if op.ID.Seq != d.VV[op.ID.Actor]+1 {
		return fmt.Errorf("%w: seq %d, want %d for actor %s", ErrOutOfOrder, op.ID.Seq, d.VV[op.ID.Actor]+1, op.ID.Actor)
	}

	// A rejected op stored as a noop only advances the VV: seq contiguity
	// survives, every replica converges on "this op did nothing".
	if op.Kind == KindNoop {
		d.VV[op.ID.Actor] = op.ID.Seq
		return nil
	}

	if op.Lamport <= d.ActorClock[op.ID.Actor] {
		return fmt.Errorf("%w: lamport %d <= actor clock %d for actor %s", ErrInvalid, op.Lamport, d.ActorClock[op.ID.Actor], op.ID.Actor)
	}

	ft, ok := s[op.Field]
	if !ok {
		return fmt.Errorf("%w: field %q is not in the schema", ErrInvalid, op.Field)
	}
	if !allowedKind(ft, op.Kind) {
		return fmt.Errorf("%w: kind %q is not allowed on a %s field", ErrInvalid, op.Kind, ft)
	}

	// Validate against a throwaway state when the field does not exist yet,
	// so a failed check leaves the document untouched.
	st, exists := d.Fields[op.Field]
	if !exists {
		st = newFieldState(ft)
	}
	if err := st.check(op, d.VV); err != nil {
		return err
	}
	if op.Ref != nil && op.Ref.Seq > d.VV[op.Ref.Actor] {
		return fmt.Errorf("%w: ref %v not yet applied", ErrMissingDeps, *op.Ref)
	}
	for _, id := range op.Observed {
		if id.Seq > d.VV[id.Actor] {
			return fmt.Errorf("%w: observed %v not yet applied", ErrMissingDeps, id)
		}
	}

	if !exists {
		d.Fields[op.Field] = st
	}
	st.apply(op)
	d.VV[op.ID.Actor] = op.ID.Seq
	d.ActorClock[op.ID.Actor] = op.Lamport
	if op.Lamport > d.MaxLamport {
		d.MaxLamport = op.Lamport
	}
	return nil
}

// Clone returns a deep copy: maps, tag sets, RGA items and raw values are
// duplicated, so mutating the clone can never corrupt the original.
func (d *Document) Clone() *Document {
	c := &Document{
		Key:        d.Key,
		VV:         make(map[ActorID]uint64, len(d.VV)),
		ActorClock: make(map[ActorID]uint64, len(d.ActorClock)),
		MaxLamport: d.MaxLamport,
		Fields:     make(map[string]*FieldState, len(d.Fields)),
	}
	for actor, seq := range d.VV {
		c.VV[actor] = seq
	}
	for actor, clock := range d.ActorClock {
		c.ActorClock[actor] = clock
	}
	for name, st := range d.Fields {
		c.Fields[name] = st.clone()
	}
	return c
}

func (st *FieldState) clone() *FieldState {
	c := &FieldState{Type: st.Type}
	if st.Reg != nil {
		reg := *st.Reg
		reg.Value = append(json.RawMessage(nil), st.Reg.Value...)
		c.Reg = &reg
	}
	if st.Ctr != nil {
		c.Ctr = &Counter{N: st.Ctr.N}
	}
	if st.Set != nil {
		c.Set = &ORSet{Elems: make(map[string]map[OpID]struct{}, len(st.Set.Elems))}
		for elem, tags := range st.Set.Elems {
			t := make(map[OpID]struct{}, len(tags))
			for id := range tags {
				t[id] = struct{}{}
			}
			c.Set.Elems[elem] = t
		}
	}
	if st.Seq != nil {
		c.Seq = &RGA{Text: st.Seq.Text, Items: make([]RGAItem, len(st.Seq.Items))}
		for i, item := range st.Seq.Items {
			item.Value = append(json.RawMessage(nil), item.Value...)
			c.Seq.Items[i] = item
		}
		c.Seq.reindex()
	}
	return c
}

// Values returns the materialized map: every field that received an op with
// its current value. Fields that never did are absent.
func (d *Document) Values() map[string]any {
	out := make(map[string]any, len(d.Fields))
	for name, st := range d.Fields {
		switch st.Type {
		case FieldRegister:
			out[name] = st.Reg.Materialize()
		case FieldCounter:
			out[name] = st.Ctr.Materialize()
		case FieldSet:
			out[name] = st.Set.Materialize()
		case FieldList, FieldText:
			out[name] = st.Seq.Materialize()
		}
	}
	return out
}

// Covers reports whether the document has applied the op with the given ID.
func (d *Document) Covers(id OpID) bool {
	return id.Seq <= d.VV[id.Actor]
}
