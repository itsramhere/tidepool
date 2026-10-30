package crdt

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// Editor creates valid local ops for one actor against one document: it
// stamps Seq from the document's VV and Lamport from its MaxLamport, applies
// each op immediately, and returns the ops it created so callers can queue
// and push them. Methods that address positions report ErrNotFound when the
// position does not exist and ErrInvalid for negative indexes or wrong field
// types.
type Editor struct {
	Actor  ActorID
	Doc    *Document
	Schema Schema
}

// nextOp stamps an op with the actor's next seq and lamport.
func (e *Editor) nextOp(field string, kind OpKind) Op {
	return Op{
		ID:      OpID{Actor: e.Actor, Seq: e.Doc.VV[e.Actor] + 1},
		Key:     e.Doc.Key,
		Lamport: e.Doc.MaxLamport + 1,
		Field:   field,
		Kind:    kind,
	}
}

// marshalValue marshals v into a raw JSON scalar for an op value.
func marshalValue(v any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal value %T: %w", v, err)
	}
	return raw, nil
}

// Set builds and applies a register set op.
func (e *Editor) Set(field string, v any) ([]Op, error) {
	raw, err := marshalValue(v)
	if err != nil {
		return nil, err
	}
	op := e.nextOp(field, KindSet)
	op.Value = raw
	if err := e.Doc.Apply(op, e.Schema); err != nil {
		return nil, err
	}
	return []Op{op}, nil
}

// Inc builds and applies a counter inc op.
func (e *Editor) Inc(field string, delta int64) ([]Op, error) {
	op := e.nextOp(field, KindInc)
	op.Delta = delta
	if err := e.Doc.Apply(op, e.Schema); err != nil {
		return nil, err
	}
	return []Op{op}, nil
}

// Add builds and applies a set add op.
func (e *Editor) Add(field string, s string) ([]Op, error) {
	raw, err := marshalValue(s)
	if err != nil {
		return nil, err
	}
	op := e.nextOp(field, KindAdd)
	op.Value = raw
	if err := e.Doc.Apply(op, e.Schema); err != nil {
		return nil, err
	}
	return []Op{op}, nil
}

// Remove builds and applies a set rem op observing every live tag of s.
// ErrNotFound when s is not in the set.
func (e *Editor) Remove(field string, s string) ([]Op, error) {
	ft, ok := e.Schema[field]
	if !ok || ft != FieldSet {
		return nil, fmt.Errorf("%w: field %q is not a set", ErrInvalid, field)
	}
	st, ok := e.Doc.Fields[field]
	if !ok {
		return nil, fmt.Errorf("%w: element %q not in set %q", ErrNotFound, s, field)
	}
	tags := st.Set.Tags(s)
	if len(tags) == 0 {
		return nil, fmt.Errorf("%w: element %q not in set %q", ErrNotFound, s, field)
	}
	raw, err := marshalValue(s)
	if err != nil {
		return nil, err
	}
	op := e.nextOp(field, KindRem)
	op.Value = raw
	op.Observed = tags
	if err := e.Doc.Apply(op, e.Schema); err != nil {
		return nil, err
	}
	return []Op{op}, nil
}

// seqState returns the RGA state of a list/text field, or nil when the field
// has no state yet (an empty sequence). requireList/requireText narrow the
// accepted type for the list-only and text-only methods.
func (e *Editor) seqState(field string, requireList, requireText bool) (*RGA, error) {
	ft, ok := e.Schema[field]
	if !ok || (ft != FieldList && ft != FieldText) {
		return nil, fmt.Errorf("%w: field %q is not a list or text field", ErrInvalid, field)
	}
	if requireList && ft != FieldList {
		return nil, fmt.Errorf("%w: field %q is a %s, not a list", ErrInvalid, field, ft)
	}
	if requireText && ft != FieldText {
		return nil, fmt.Errorf("%w: field %q is a %s, not text", ErrInvalid, field, ft)
	}
	if st, ok := e.Doc.Fields[field]; ok {
		return st.Seq, nil
	}
	return nil, nil
}

// seqIndex resolves index into a Ref: for inserts, nil for the head or the
// visible element at index-1; for deletes, the visible element at index.
// ErrNotFound when the position is past the end.
func seqIndex(g *RGA, index int, forInsert bool) (*OpID, error) {
	if index < 0 {
		return nil, fmt.Errorf("%w: negative index %d", ErrInvalid, index)
	}
	var vis []OpID
	if g != nil {
		vis = g.VisibleIDs()
	}
	if forInsert {
		if index == 0 {
			return nil, nil // insert at the head
		}
		if index > len(vis) {
			return nil, fmt.Errorf("%w: index %d past end (%d elements)", ErrNotFound, index, len(vis))
		}
		id := vis[index-1]
		return &id, nil
	}
	if index >= len(vis) {
		return nil, fmt.Errorf("%w: index %d past end (%d elements)", ErrNotFound, index, len(vis))
	}
	id := vis[index]
	return &id, nil
}

// Insert builds and applies a list ins op. index 0 inserts at the head,
// otherwise the new element lands after the visible element at index-1.
func (e *Editor) Insert(field string, index int, v any) ([]Op, error) {
	g, err := e.seqState(field, true, false)
	if err != nil {
		return nil, err
	}
	ref, err := seqIndex(g, index, true)
	if err != nil {
		return nil, err
	}
	raw, err := marshalValue(v)
	if err != nil {
		return nil, err
	}
	op := e.nextOp(field, KindIns)
	op.Ref = ref
	op.Value = raw
	if err := e.Doc.Apply(op, e.Schema); err != nil {
		return nil, err
	}
	return []Op{op}, nil
}

// InsertText builds and applies one ins op per code point of s, each
// referencing the previous new element, so the run lands as a unit at index.
func (e *Editor) InsertText(field string, index int, s string) ([]Op, error) {
	g, err := e.seqState(field, false, true)
	if err != nil {
		return nil, err
	}
	if !utf8.ValidString(s) {
		return nil, fmt.Errorf("%w: text is not valid UTF-8", ErrInvalid)
	}
	ref, err := seqIndex(g, index, true)
	if err != nil {
		return nil, err
	}
	var ops []Op
	for _, r := range s {
		raw, err := marshalValue(string(r))
		if err != nil {
			return nil, err
		}
		op := e.nextOp(field, KindIns)
		op.Ref = ref
		op.Value = raw
		if err := e.Doc.Apply(op, e.Schema); err != nil {
			return nil, err
		}
		ops = append(ops, op)
		ref = &ops[len(ops)-1].ID
	}
	return ops, nil
}

// Delete builds and applies a del op for the visible element at index of a
// list or text field.
func (e *Editor) Delete(field string, index int) ([]Op, error) {
	g, err := e.seqState(field, false, false)
	if err != nil {
		return nil, err
	}
	ref, err := seqIndex(g, index, false)
	if err != nil {
		return nil, err
	}
	op := e.nextOp(field, KindDel)
	op.Ref = ref
	if err := e.Doc.Apply(op, e.Schema); err != nil {
		return nil, err
	}
	return []Op{op}, nil
}
