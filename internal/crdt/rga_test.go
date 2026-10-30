package crdt

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
)

// rgaInsOp builds an ins op with a JSON string value.
func rgaInsOp(t *testing.T, seq, lamport uint64, actor ActorID, ref *OpID, value string) Op {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %q: %v", value, err)
	}
	return Op{ID: OpID{Actor: actor, Seq: seq}, Lamport: lamport, Kind: KindIns, Ref: ref, Value: raw}
}

// applyRGAOps checks and applies ops in order to a fresh RGA, maintaining the
// version vector the way Document.Apply will.
func applyRGAOps(t *testing.T, text bool, ops ...Op) *RGA {
	t.Helper()
	g := &RGA{Text: text}
	vv := make(map[ActorID]uint64)
	for _, op := range ops {
		if err := g.check(op, vv); err != nil {
			t.Fatalf("check(%+v): %v", op, err)
		}
		g.apply(op)
		vv[op.ID.Actor] = op.ID.Seq
	}
	return g
}

func TestRGAInsertAtHead(t *testing.T) {
	g := applyRGAOps(t, false,
		rgaInsOp(t, 1, 1, actorA, nil, "b"),
		rgaInsOp(t, 2, 2, actorA, nil, "a"),
	)
	if got := g.Materialize(); !reflect.DeepEqual(got, []any{"a", "b"}) {
		t.Errorf("Materialize() = %v, want [a b]", got)
	}
}

func TestRGAInsertAfter(t *testing.T) {
	first := rgaInsOp(t, 1, 1, actorA, nil, "a")
	g := applyRGAOps(t, false,
		first,
		rgaInsOp(t, 2, 2, actorA, &first.ID, "b"),
		rgaInsOp(t, 3, 3, actorA, &first.ID, "c"),
	)
	// Both later inserts reference "a"; the higher lamport sorts first.
	if got := g.Materialize(); !reflect.DeepEqual(got, []any{"a", "c", "b"}) {
		t.Errorf("Materialize() = %v, want [a c b]", got)
	}
}

func TestRGAConcurrentInsertsHigherFirst(t *testing.T) {
	// Byte-wise actor order: low < high, so the assertion reads directly.
	const actorLow, actorHigh = ActorID("0000000000000001"), ActorID("ffffffffffffffff")
	base := rgaInsOp(t, 1, 1, actorA, nil, "a")
	x := rgaInsOp(t, 1, 5, actorLow, &base.ID, "x")  // (5, low)
	y := rgaInsOp(t, 1, 5, actorHigh, &base.ID, "y") // (5, high), concurrent with x

	gx := applyRGAOps(t, false, base, x, y)
	gy := applyRGAOps(t, false, base, y, x)
	want := []any{"a", "y", "x"} // higher (lamport, actor) sorts first
	for name, g := range map[string]*RGA{"low first": gx, "high first": gy} {
		if got := g.Materialize(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: Materialize() = %v, want %v", name, got, want)
		}
	}
}

func TestRGADeleteHidesButKeepsItem(t *testing.T) {
	first := rgaInsOp(t, 1, 1, actorA, nil, "a")
	second := rgaInsOp(t, 2, 2, actorA, &first.ID, "b")
	del := Op{ID: OpID{Actor: actorB, Seq: 1}, Lamport: 3, Kind: KindDel, Ref: &second.ID}

	g := applyRGAOps(t, false, first, second, del)
	if got := g.Materialize(); !reflect.DeepEqual(got, []any{"a"}) {
		t.Errorf("Materialize() = %v, want [a]", got)
	}
	if len(g.Items) != 2 {
		t.Errorf("len(Items) = %d, want 2 (items are never removed)", len(g.Items))
	}
	if got := g.VisibleIDs(); !reflect.DeepEqual(got, []OpID{first.ID}) {
		t.Errorf("VisibleIDs() = %v, want [%v]", got, first.ID)
	}

	// A second del of the same element (a distinct op) is harmless.
	delAgain := Op{ID: OpID{Actor: actorB, Seq: 2}, Lamport: 4, Kind: KindDel, Ref: &second.ID}
	g2 := applyRGAOps(t, false, first, second, del, delAgain)
	if got := g2.Materialize(); !reflect.DeepEqual(got, []any{"a"}) {
		t.Errorf("Materialize() after double delete = %v, want [a]", got)
	}
	if !reflect.DeepEqual(g2.Items, g.Items) {
		t.Errorf("double delete changed item state:\n%+v\n%+v", g2.Items, g.Items)
	}
}

func TestRGATextMaterialize(t *testing.T) {
	h := rgaInsOp(t, 1, 1, actorA, nil, "h")
	acute := rgaInsOp(t, 2, 2, actorA, &h.ID, "é")
	i := rgaInsOp(t, 3, 3, actorA, &h.ID, "i")
	g := applyRGAOps(t, true, h, acute, i)
	// acute (L2) and i (L3) both reference h; the higher lamport sorts
	// first after h, so i precedes acute.
	if got := g.Materialize(); got != "hié" {
		t.Errorf("Materialize() = %q, want %q", got, "hié")
	}

	del := Op{ID: OpID{Actor: actorB, Seq: 1}, Lamport: 4, Kind: KindDel, Ref: &acute.ID}
	g2 := applyRGAOps(t, true, h, acute, i, del)
	if got := g2.Materialize(); got != "hi" {
		t.Errorf("Materialize() after delete = %q, want %q", got, "hi")
	}
}

func TestRGACheck(t *testing.T) {
	tests := []struct {
		name    string
		run     func(t *testing.T) error
		wantErr error
	}{
		{"list ins scalar", func(t *testing.T) error {
			return (&RGA{}).check(rgaInsOp(t, 1, 1, actorA, nil, "x"), nil)
		}, nil},
		{"list ins bool", func(t *testing.T) error {
			return (&RGA{}).check(Op{Kind: KindIns, Value: json.RawMessage(`true`)}, nil)
		}, nil},
		{"text ins one code point", func(t *testing.T) error {
			return (&RGA{Text: true}).check(rgaInsOp(t, 1, 1, actorA, nil, "é"), nil)
		}, nil},
		{"text ins two code points", func(t *testing.T) error {
			return (&RGA{Text: true}).check(rgaInsOp(t, 1, 1, actorA, nil, "ab"), nil)
		}, ErrInvalid},
		{"text ins number", func(t *testing.T) error {
			return (&RGA{Text: true}).check(Op{Kind: KindIns, Value: json.RawMessage(`3`)}, nil)
		}, ErrInvalid},
		{"ins missing value", func(t *testing.T) error {
			return (&RGA{}).check(Op{Kind: KindIns}, nil)
		}, ErrInvalid},
		{"del with value", func(t *testing.T) error {
			return (&RGA{}).check(Op{Kind: KindDel, Value: json.RawMessage(`"x"`)}, nil)
		}, ErrInvalid},
		{"del without ref", func(t *testing.T) error {
			return (&RGA{}).check(Op{Kind: KindDel}, nil)
		}, ErrInvalid},
		{"wrong kind", func(t *testing.T) error {
			return (&RGA{}).check(Op{Kind: KindAdd, Value: json.RawMessage(`"x"`)}, nil)
		}, ErrInvalid},
		{"ins ref not in vv", func(t *testing.T) error {
			ref := OpID{Actor: actorB, Seq: 9}
			return (&RGA{}).check(rgaInsOp(t, 1, 2, actorA, &ref, "x"), make(map[ActorID]uint64))
		}, ErrMissingDeps},
		{"ins ref in vv but not in items", func(t *testing.T) error {
			ref := OpID{Actor: actorB, Seq: 5}
			vv := map[ActorID]uint64{actorB: 5}
			return (&RGA{}).check(rgaInsOp(t, 1, 6, actorA, &ref, "x"), vv)
		}, ErrMissingDeps},
		{"ins lamport equal to ref", func(t *testing.T) error {
			base := rgaInsOp(t, 1, 5, actorB, nil, "x")
			g := applyRGAOps(t, false, base)
			vv := map[ActorID]uint64{actorB: 1}
			return g.check(rgaInsOp(t, 1, 5, actorA, &base.ID, "y"), vv)
		}, ErrInvalid},
		{"ins lamport below ref", func(t *testing.T) error {
			base := rgaInsOp(t, 1, 5, actorB, nil, "x")
			g := applyRGAOps(t, false, base)
			vv := map[ActorID]uint64{actorB: 1}
			return g.check(rgaInsOp(t, 1, 4, actorA, &base.ID, "y"), vv)
		}, ErrInvalid},
		{"del ref not in items", func(t *testing.T) error {
			vv := map[ActorID]uint64{actorB: 5}
			return (&RGA{}).check(Op{Kind: KindDel, Ref: &OpID{Actor: actorB, Seq: 5}}, vv)
		}, ErrMissingDeps},
		{"del existing item", func(t *testing.T) error {
			base := rgaInsOp(t, 1, 5, actorB, nil, "x")
			g := applyRGAOps(t, false, base)
			vv := map[ActorID]uint64{actorB: 1}
			return g.check(Op{Kind: KindDel, Ref: &base.ID}, vv)
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run(t)
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

func TestRGACheckDoesNotMutate(t *testing.T) {
	base := rgaInsOp(t, 1, 1, actorA, nil, "a")
	second := rgaInsOp(t, 2, 2, actorA, &base.ID, "b")
	g := applyRGAOps(t, false, base, second)

	before := slices.Clone(g.Items)
	vv := map[ActorID]uint64{actorA: 2, actorB: 5}
	bad := Op{ID: OpID{Actor: actorB, Seq: 1}, Lamport: 9, Kind: KindDel, Ref: &OpID{Actor: actorB, Seq: 5}}
	if err := g.check(bad, vv); !errors.Is(err, ErrMissingDeps) {
		t.Fatalf("check(bad del) = %v, want ErrMissingDeps", err)
	}
	if !reflect.DeepEqual(g.Items, before) {
		t.Errorf("failed check mutated items:\nbefore %+v\nafter  %+v", before, g.Items)
	}
	if got := g.VisibleIDs(); len(got) != 2 {
		t.Errorf("VisibleIDs() after failed check = %v, want 2 visible", got)
	}
}

// rgaValue returns a distinct test value for op number idx: a single code
// point for text (the alphabet has more runes than the max ops per scenario),
// a unique string for lists.
func rgaValue(text bool, idx int) json.RawMessage {
	var v any
	if text {
		const alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZéßüñΩç"
		v = string([]rune(alphabet)[idx%len([]rune(alphabet))])
	} else {
		v = fmt.Sprintf("v%03d", idx)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Errorf("marshal rga test value: %w", err)) // strings always marshal
	}
	return raw
}

// rgaSim is one simulated actor's local replica while ops are generated.
type rgaSim struct {
	actor   ActorID
	local   *RGA
	vv      map[ActorID]uint64
	applied int // entries of the global log already applied
	maxLam  uint64
	seq     uint64
}

// generateRGAOps simulates three actors taking turns: each syncs the unseen
// prefix of the global log (in log order, which is causally valid), then
// inserts a run of 1-3 chained elements anchored at a random visible element
// or the head. The returned log is every op in creation order.
func generateRGAOps(t *testing.T, rng *rand.Rand, text bool) []Op {
	t.Helper()
	sims := []*rgaSim{
		{actor: actorA, local: &RGA{Text: text}, vv: make(map[ActorID]uint64)},
		{actor: actorB, local: &RGA{Text: text}, vv: make(map[ActorID]uint64)},
		{actor: actorC, local: &RGA{Text: text}, vv: make(map[ActorID]uint64)},
	}
	var log []Op
	for turn := 0; turn < 6; turn++ {
		for _, sim := range sims {
			for ; sim.applied < len(log); sim.applied++ {
				op := log[sim.applied]
				if op.ID.Actor == sim.actor {
					continue // own ops were applied at creation time
				}
				if err := sim.local.check(op, sim.vv); err != nil {
					t.Fatalf("sim %s: check(%+v): %v", sim.actor, op, err)
				}
				sim.local.apply(op)
				sim.vv[op.ID.Actor] = op.ID.Seq
				if op.Lamport > sim.maxLam {
					sim.maxLam = op.Lamport
				}
			}
			runLen := 1 + rng.IntN(3)
			var ref *OpID
			if vis := sim.local.VisibleIDs(); len(vis) > 0 && rng.IntN(5) != 0 {
				ref = &vis[rng.IntN(len(vis))]
			}
			for k := 0; k < runLen; k++ {
				sim.seq++
				sim.maxLam++
				op := Op{
					ID:      OpID{Actor: sim.actor, Seq: sim.seq},
					Lamport: sim.maxLam,
					Kind:    KindIns,
					Ref:     ref,
					Value:   rgaValue(text, len(log)),
				}
				if err := sim.local.check(op, sim.vv); err != nil {
					t.Fatalf("sim %s: check(%+v): %v", sim.actor, op, err)
				}
				sim.local.apply(op)
				sim.vv[op.ID.Actor] = op.ID.Seq
				log = append(log, op)
				ref = &op.ID
			}
		}
	}
	return log
}

// deliverRGA applies ops to a fresh RGA in a random causally valid order:
// per-actor seqs must be contiguous and refs must already be applied. It
// returns the materialized result.
func deliverRGA(t *testing.T, ops []Op, text bool, rng *rand.Rand) any {
	t.Helper()
	merged := &RGA{Text: text}
	vv := make(map[ActorID]uint64)
	delivered := make([]bool, len(ops))
	for remaining := len(ops); remaining > 0; remaining-- {
		cands := make([]int, 0, remaining)
		for i, op := range ops {
			if delivered[i] {
				continue
			}
			if op.ID.Seq != vv[op.ID.Actor]+1 {
				continue
			}
			if op.Ref != nil && op.Ref.Seq > vv[op.Ref.Actor] {
				continue
			}
			cands = append(cands, i)
		}
		if len(cands) == 0 {
			t.Fatalf("%d ops left but none deliverable — generator bug", remaining)
		}
		i := cands[rng.IntN(len(cands))]
		op := ops[i]
		if err := merged.check(op, vv); err != nil {
			t.Fatalf("check(%+v): %v", op, err)
		}
		merged.apply(op)
		vv[op.ID.Actor] = op.ID.Seq
		delivered[i] = true
	}
	if len(merged.Items) != len(ops) {
		t.Fatalf("merged replica holds %d items, want %d — ops were lost", len(merged.Items), len(ops))
	}
	return merged.Materialize()
}

// TestRGAInterleavingConvergence is the interleaving property: for every
// generated scenario, all causally valid delivery orders materialize
// identically (PLAN.md M2 task 3).
func TestRGAInterleavingConvergence(t *testing.T) {
	rng := newTestRand(t)
	iters := testIter(t)
	for _, text := range []bool{false, true} {
		name := "list"
		if text {
			name = "text"
		}
		t.Run(name, func(t *testing.T) {
			for round := 0; round < iters; round++ {
				ops := generateRGAOps(t, rng, text)
				want := deliverRGA(t, ops, text, rng)
				for order := 0; order < 5; order++ {
					got := deliverRGA(t, ops, text, rng)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("round %d order %d: materialized %#v, want %#v", round, order, got, want)
					}
				}
			}
		})
	}
}
