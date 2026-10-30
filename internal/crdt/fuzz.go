package crdt

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"unicode/utf8"
)

// ScenarioSchema has one field of every type; the convergence fuzzer and the
// conformance vectors use it so every CRDT gets exercised.
var ScenarioSchema = Schema{
	"title":     FieldRegister,
	"votes":     FieldCounter,
	"labels":    FieldSet,
	"checklist": FieldList,
	"body":      FieldText,
}

// scenarioActors are fixed replica identities so conformance vectors are
// reproducible across regenerations.
var scenarioActors = []ActorID{
	"a1b2c3d4e5f60708",
	"0f1e2d3c4b5a6978",
	"deadbeefdeadbeef",
	"0123456789abcdef",
}

// Replica is one simulated client in a convergence scenario.
type Replica struct {
	Actor ActorID
	Doc   *Document
	Ed    *Editor
	Log   []Op // every op this replica applied, in apply order
}

// NewReplica creates an empty replica for the document key.
func NewReplica(key DocKey, actor ActorID) *Replica {
	doc := NewDocument(key)
	return &Replica{Actor: actor, Doc: doc, Ed: &Editor{Actor: actor, Doc: doc, Schema: ScenarioSchema}}
}

// DeliverFrom applies every op in src's log that r does not cover, in src's
// log order. ErrDuplicate ops are ignored; any other error is returned,
// because a log built by Apply can only be causally valid.
func (r *Replica) DeliverFrom(src *Replica) (int, error) {
	moved := 0
	for _, op := range src.Log {
		if r.Doc.Covers(op.ID) {
			continue
		}
		err := r.Doc.Apply(op, r.Ed.Schema)
		if errors.Is(err, ErrDuplicate) {
			continue
		}
		if err != nil {
			return moved, fmt.Errorf("replica %s applying %v from %s: %w", r.Actor, op.ID, src.Actor, err)
		}
		r.Log = append(r.Log, op)
		moved++
	}
	return moved, nil
}

var (
	scenarioWords = []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}
	scenarioChars = []rune("abcdefg")
)

// randomScalar returns a random JSON scalar for register and list edits.
func randomScalar(rng *rand.Rand) any {
	switch rng.IntN(4) {
	case 0:
		return scenarioWords[rng.IntN(len(scenarioWords))]
	case 1:
		return float64(rng.IntN(2000)) / 8
	case 2:
		return rng.IntN(2) == 0
	default:
		return nil
	}
}

// randomEdit performs one random valid edit on a random field of the
// scenario schema. It returns ErrNotFound when the random pick addresses an
// element that no longer exists; callers skip the step.
func randomEdit(rng *rand.Rand, r *Replica) ([]Op, error) {
	names := ScenarioSchema.SortedFields()
	field := names[rng.IntN(len(names))]
	switch ScenarioSchema[field] {
	case FieldRegister:
		return r.Ed.Set(field, randomScalar(rng))
	case FieldCounter:
		delta := int64(rng.IntN(11)) - 5
		if delta == 0 {
			delta = 1
		}
		return r.Ed.Inc(field, delta)
	case FieldSet:
		vals, _ := r.Doc.Values()[field].([]string)
		if len(vals) == 0 || rng.IntN(2) == 0 {
			return r.Ed.Add(field, scenarioWords[rng.IntN(len(scenarioWords))])
		}
		return r.Ed.Remove(field, vals[rng.IntN(len(vals))])
	case FieldList:
		var vis []OpID
		if st, ok := r.Doc.Fields[field]; ok {
			vis = st.Seq.VisibleIDs()
		}
		if len(vis) == 0 || rng.IntN(2) == 0 {
			return r.Ed.Insert(field, rng.IntN(len(vis)+1), randomScalar(rng))
		}
		return r.Ed.Delete(field, rng.IntN(len(vis)))
	case FieldText:
		runes := 0
		if st, ok := r.Doc.Fields[field]; ok {
			runes = utf8.RuneCountInString(st.Seq.Materialize().(string))
		}
		if runes == 0 || rng.IntN(2) == 0 {
			idx := rng.IntN(runes + 1)
			return r.Ed.InsertText(field, idx, string(scenarioChars[rng.IntN(len(scenarioChars))]))
		}
		return r.Ed.Delete(field, rng.IntN(runes))
	default:
		return nil, fmt.Errorf("field %q has unknown type %q", field, ScenarioSchema[field])
	}
}

// RunScenario runs the convergence fuzz loop: for steps rounds, with p=0.7 a
// random replica makes a random edit (ErrNotFound picks are skipped), with
// p=0.3 one replica delivers another's log. Afterwards every pair is synced
// repeatedly until nothing moves, so every replica holds every op. The
// returned error wraps the first out-of-order or missing-deps delivery —
// those would be convergence bugs, never legitimate outcomes.
func RunScenario(rng *rand.Rand, key DocKey, steps int) ([]*Replica, error) {
	reps := make([]*Replica, len(scenarioActors))
	for i, actor := range scenarioActors {
		reps[i] = NewReplica(key, actor)
	}
	for step := 0; step < steps; step++ {
		if rng.Float64() < 0.7 {
			r := reps[rng.IntN(len(reps))]
			ops, err := randomEdit(rng, r)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", step, err)
			}
			r.Log = append(r.Log, ops...)
			continue
		}
		a := reps[rng.IntN(len(reps))]
		b := reps[rng.IntN(len(reps))]
		if _, err := b.DeliverFrom(a); err != nil {
			return nil, fmt.Errorf("step %d: %w", step, err)
		}
	}
	for {
		moved := 0
		for _, a := range reps {
			for _, b := range reps {
				if a == b {
					continue
				}
				n, err := b.DeliverFrom(a)
				if err != nil {
					return nil, err
				}
				moved += n
			}
		}
		if moved == 0 {
			return reps, nil
		}
	}
}
