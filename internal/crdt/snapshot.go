package crdt

import (
	"encoding/json"
	"fmt"
)

// Snapshot JSON shapes (PLAN.md §4.6). One struct per field type keeps the
// emitted objects exact; encoding/json sorts map keys, and set tag lists are
// sorted by (actor, seq) via ORSet.Tags.
type snapshotDoc struct {
	Key        DocKey                     `json:"key"`
	VV         map[ActorID]uint64         `json:"vv"`
	Clock      map[ActorID]uint64         `json:"clock"`
	MaxLamport uint64                     `json:"maxLamport"`
	Fields     map[string]json.RawMessage `json:"fields"`
}

type snapRegister struct {
	Type    FieldType       `json:"type"`
	Lamport uint64          `json:"lamport"`
	Actor   ActorID         `json:"actor"`
	Value   json.RawMessage `json:"value"`
}

type snapCounter struct {
	Type FieldType `json:"type"`
	N    int64     `json:"n"`
}

type snapSet struct {
	Type  FieldType         `json:"type"`
	Elems map[string][]OpID `json:"elems"`
}

type snapRGA struct {
	Type  FieldType  `json:"type"`
	Items []snapItem `json:"items"`
}

type snapItem struct {
	ID      OpID            `json:"id"`
	Lamport uint64          `json:"lamport"`
	Value   json.RawMessage `json:"value"`
	Deleted bool            `json:"deleted"`
}

// MarshalSnapshot serializes the full document state — including tombstones,
// so a snapshot alone is a complete replica — in the exact JSON format of
// PLAN.md §4.6, shared with the TypeScript SDK.
func (d *Document) MarshalSnapshot() ([]byte, error) {
	if err := ValidateKey(d.Key); err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	fields := make(map[string]json.RawMessage, len(d.Fields))
	for name, st := range d.Fields {
		var raw []byte
		var err error
		switch st.Type {
		case FieldRegister:
			if st.Reg == nil || !st.Reg.Set {
				return nil, fmt.Errorf("marshal snapshot: register field %q has no value", name)
			}
			raw, err = json.Marshal(snapRegister{Type: st.Type, Lamport: st.Reg.Lamport, Actor: st.Reg.Actor, Value: st.Reg.Value})
		case FieldCounter:
			if st.Ctr == nil {
				return nil, fmt.Errorf("marshal snapshot: counter field %q has no state", name)
			}
			raw, err = json.Marshal(snapCounter{Type: st.Type, N: st.Ctr.N})
		case FieldSet:
			if st.Set == nil {
				return nil, fmt.Errorf("marshal snapshot: set field %q has no state", name)
			}
			elems := make(map[string][]OpID, len(st.Set.Elems))
			for elem := range st.Set.Elems {
				elems[elem] = st.Set.Tags(elem)
			}
			raw, err = json.Marshal(snapSet{Type: st.Type, Elems: elems})
		case FieldList, FieldText:
			if st.Seq == nil {
				return nil, fmt.Errorf("marshal snapshot: sequence field %q has no state", name)
			}
			items := make([]snapItem, len(st.Seq.Items))
			for i, item := range st.Seq.Items {
				items[i] = snapItem{ID: item.ID, Lamport: item.Lamport, Value: item.Value, Deleted: item.Deleted}
			}
			raw, err = json.Marshal(snapRGA{Type: st.Type, Items: items})
		default:
			return nil, fmt.Errorf("marshal snapshot: field %q has unknown type %q", name, st.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("marshal snapshot field %q: %w", name, err)
		}
		fields[name] = raw
	}
	out, err := json.Marshal(snapshotDoc{
		Key:        d.Key,
		VV:         d.VV,
		Clock:      d.ActorClock,
		MaxLamport: d.MaxLamport,
		Fields:     fields,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	return out, nil
}

// UnmarshalSnapshot rebuilds a document from MarshalSnapshot output.
func UnmarshalSnapshot(data []byte) (*Document, error) {
	var env snapshotDoc
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	if err := ValidateKey(env.Key); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	d := NewDocument(env.Key)
	if env.VV != nil {
		d.VV = env.VV
	}
	if env.Clock != nil {
		d.ActorClock = env.Clock
	}
	d.MaxLamport = env.MaxLamport
	for name, raw := range env.Fields {
		var head struct {
			Type FieldType `json:"type"`
		}
		if err := json.Unmarshal(raw, &head); err != nil {
			return nil, fmt.Errorf("unmarshal snapshot field %q: %w", name, err)
		}
		st := &FieldState{Type: head.Type}
		switch head.Type {
		case FieldRegister:
			var sr snapRegister
			if err := json.Unmarshal(raw, &sr); err != nil {
				return nil, fmt.Errorf("unmarshal snapshot field %q: %w", name, err)
			}
			if len(sr.Value) == 0 {
				return nil, fmt.Errorf("unmarshal snapshot: register field %q has no value", name)
			}
			st.Reg = &Register{Lamport: sr.Lamport, Actor: sr.Actor, Value: sr.Value, Set: true}
		case FieldCounter:
			var sc snapCounter
			if err := json.Unmarshal(raw, &sc); err != nil {
				return nil, fmt.Errorf("unmarshal snapshot field %q: %w", name, err)
			}
			st.Ctr = &Counter{N: sc.N}
		case FieldSet:
			var ss snapSet
			if err := json.Unmarshal(raw, &ss); err != nil {
				return nil, fmt.Errorf("unmarshal snapshot field %q: %w", name, err)
			}
			elems := make(map[string]map[OpID]struct{}, len(ss.Elems))
			for elem, tags := range ss.Elems {
				t := make(map[OpID]struct{}, len(tags))
				for _, id := range tags {
					t[id] = struct{}{}
				}
				elems[elem] = t
			}
			st.Set = &ORSet{Elems: elems}
		case FieldList, FieldText:
			var sq snapRGA
			if err := json.Unmarshal(raw, &sq); err != nil {
				return nil, fmt.Errorf("unmarshal snapshot field %q: %w", name, err)
			}
			items := make([]RGAItem, len(sq.Items))
			for i, si := range sq.Items {
				items[i] = RGAItem{ID: si.ID, Lamport: si.Lamport, Value: si.Value, Deleted: si.Deleted}
			}
			st.Seq = &RGA{Text: head.Type == FieldText, Items: items}
			st.Seq.reindex()
		default:
			return nil, fmt.Errorf("unmarshal snapshot: field %q has unknown type %q", name, head.Type)
		}
		d.Fields[name] = st
	}
	return d, nil
}
