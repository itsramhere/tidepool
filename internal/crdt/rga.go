package crdt

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// RGAItem is one element of an RGA sequence. Items are never physically
// removed; del only marks them, so indexOf stays stable and replicas that
// have not yet seen a delete still integrate new inserts identically.
type RGAItem struct {
	ID      OpID
	Lamport uint64
	Value   json.RawMessage
	Deleted bool
}

// RGA is the sequence CRDT behind list and text fields. Concurrent inserts
// after the same element sort by (Lamport, Actor), higher first, which keeps
// every causally valid delivery order in identical document order.
// Text=true materializes the visible values as one string.
type RGA struct {
	Items []RGAItem
	Text  bool
}

// check validates an ins or del op against the sequence and writes nothing.
// Ref semantics: nil Ref means insert at head; otherwise the referenced
// element must already be applied to the document (per vv) and present in
// this sequence, and an ins must carry a strictly higher lamport than its
// Ref. A del must not carry a value.
func (g *RGA) check(op Op, vv map[ActorID]uint64) error {
	switch op.Kind {
	case KindIns:
		if g.Text {
			v, err := ParseScalar(op.Value)
			if err != nil {
				return err
			}
			s, ok := v.(string)
			if !ok || !IsSingleCodePoint(s) {
				return fmt.Errorf("%w: text ins value must be a string with exactly one code point", ErrInvalid)
			}
		} else if _, err := ParseScalar(op.Value); err != nil {
			return err
		}
	case KindDel:
		if len(op.Value) > 0 {
			return fmt.Errorf("%w: del must not carry a value", ErrInvalid)
		}
		if op.Ref == nil {
			return fmt.Errorf("%w: del requires a Ref", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: kind %q is not allowed on a sequence", ErrInvalid, op.Kind)
	}
	if op.Ref == nil {
		return nil
	}
	if op.Ref.Seq > vv[op.Ref.Actor] {
		return fmt.Errorf("%w: ref %v not yet applied to the document", ErrMissingDeps, *op.Ref)
	}
	idx := g.indexOf(*op.Ref)
	if idx < 0 {
		return fmt.Errorf("%w: ref %v not found in this sequence", ErrMissingDeps, *op.Ref)
	}
	if op.Kind == KindIns && op.Lamport <= g.Items[idx].Lamport {
		return fmt.Errorf("%w: ins lamport %d must be greater than ref lamport %d", ErrInvalid, op.Lamport, g.Items[idx].Lamport)
	}
	return nil
}

// apply applies a checked ins or del op following PLAN.md §4.5: the new item
// lands after its Ref and after every sibling subtree that sorts above it,
// so the skip loop stops at the first item that is not greater.
func (g *RGA) apply(op Op) {
	switch op.Kind {
	case KindIns:
		i := 0
		if op.Ref != nil {
			i = g.indexOf(*op.Ref) + 1
		}
		for i < len(g.Items) && rgaItemGreater(g.Items[i], op) {
			i++
		}
		g.Items = slices.Insert(g.Items, i, RGAItem{ID: op.ID, Lamport: op.Lamport, Value: op.Value})
	case KindDel:
		if op.Ref == nil {
			return
		}
		if idx := g.indexOf(*op.Ref); idx >= 0 {
			g.Items[idx].Deleted = true
		}
	}
}

// rgaItemGreater reports whether item sorts after op under the total order
// (lamport, actor) — the condition the insertion skip loop tests.
func rgaItemGreater(item RGAItem, op Op) bool {
	if item.Lamport != op.Lamport {
		return item.Lamport > op.Lamport
	}
	return item.ID.Actor > op.ID.Actor
}

// indexOf returns the document-order position of the item with the given ID,
// or -1 if absent. Linear scan in v1; M12 adds an index map if benchmarks
// demand it.
func (g *RGA) indexOf(id OpID) int {
	for i := range g.Items {
		if g.Items[i].ID == id {
			return i
		}
	}
	return -1
}

// VisibleIDs returns the IDs of non-deleted items in document order.
func (g *RGA) VisibleIDs() []OpID {
	out := make([]OpID, 0, len(g.Items))
	for _, item := range g.Items {
		if !item.Deleted {
			out = append(out, item.ID)
		}
	}
	return out
}

// Materialize returns the visible values: []any of scalars in document order
// for a list, or the concatenation of visible single code points for text.
func (g *RGA) Materialize() any {
	if g.Text {
		var b strings.Builder
		for _, item := range g.Items {
			if item.Deleted {
				continue
			}
			v, err := ParseScalar(item.Value)
			if err != nil {
				continue // values are validated by check; unreachable in practice
			}
			if s, ok := v.(string); ok {
				b.WriteString(s)
			}
		}
		return b.String()
	}
	out := []any{}
	for _, item := range g.Items {
		if item.Deleted {
			continue
		}
		if v, err := ParseScalar(item.Value); err == nil {
			out = append(out, v)
		}
	}
	return out
}
