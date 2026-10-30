package crdt

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
)

// ORSet is an add-wins observed-remove set: an element is present while it
// has at least one live add tag, and a rem only deletes the tags it
// explicitly observed. An add concurrent with a rem therefore survives it.
type ORSet struct {
	Elems map[string]map[OpID]struct{}
}

// check validates an add or rem op against a set field: the kind must be
// allowed and the value a JSON string naming the element. It writes nothing.
func (s *ORSet) check(op Op) error {
	switch op.Kind {
	case KindAdd, KindRem:
	default:
		return fmt.Errorf("%w: kind %q is not allowed on a set", ErrInvalid, op.Kind)
	}
	if _, err := setElem(op.Value); err != nil {
		return err
	}
	return nil
}

// apply applies a checked add or rem op. rem deletes exactly the observed
// tags and drops the element when nothing is left; removing unknown or
// already-removed tags is harmless.
func (s *ORSet) apply(op Op) {
	elem, err := setElem(op.Value)
	if err != nil {
		return // apply never sees an op that failed check
	}
	switch op.Kind {
	case KindAdd:
		if s.Elems == nil {
			s.Elems = make(map[string]map[OpID]struct{})
		}
		tags, ok := s.Elems[elem]
		if !ok {
			tags = make(map[OpID]struct{})
			s.Elems[elem] = tags
		}
		tags[op.ID] = struct{}{}
	case KindRem:
		tags, ok := s.Elems[elem]
		if !ok {
			return
		}
		for _, tag := range op.Observed {
			delete(tags, tag)
		}
		if len(tags) == 0 {
			delete(s.Elems, elem)
		}
	}
}

// Materialize returns the live elements sorted ascending.
func (s *ORSet) Materialize() []string {
	out := make([]string, 0, len(s.Elems))
	for elem := range s.Elems {
		out = append(out, elem)
	}
	slices.Sort(out)
	return out
}

// Tags returns the live tags of elem sorted by (actor, seq), or nil if the
// element is absent. The editor uses this to build rem ops.
func (s *ORSet) Tags(elem string) []OpID {
	tags, ok := s.Elems[elem]
	if !ok {
		return nil
	}
	out := make([]OpID, 0, len(tags))
	for id := range tags {
		out = append(out, id)
	}
	slices.SortFunc(out, func(a, b OpID) int {
		if c := cmp.Compare(a.Actor, b.Actor); c != 0 {
			return c
		}
		return cmp.Compare(a.Seq, b.Seq)
	})
	return out
}

// setElem decodes the JSON string naming a set element.
func setElem(raw json.RawMessage) (string, error) {
	v, err := ParseScalar(raw)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%w: set element must be a JSON string, got %T", ErrInvalid, v)
	}
	return s, nil
}
