package crdt

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
)

// newTestRand returns a rand.Rand seeded from TIDEPOOL_SEED (default:
// time-based) so randomized failures are reproducible (PLAN.md golden rule 8).
// The seed is printed via t.Logf, which shows on failure.
func newTestRand(t *testing.T) *rand.Rand {
	t.Helper()
	var seed uint64
	if s := os.Getenv("TIDEPOOL_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("bad TIDEPOOL_SEED %q: %v", s, err)
		}
		seed = v
	} else {
		seed = rand.Uint64()
	}
	t.Logf("seed=%d", seed)
	return rand.New(rand.NewPCG(seed, seed))
}

// testIter returns the randomized-test iteration count from TIDEPOOL_ITER
// (default 200).
func testIter(t *testing.T) int {
	t.Helper()
	if s := os.Getenv("TIDEPOOL_ITER"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 1 {
			t.Fatalf("bad TIDEPOOL_ITER %q: %v", s, err)
		}
		return v
	}
	return 200
}

// setOp builds a register set op writing value at the given timestamp.
func setOp(t *testing.T, lamport uint64, actor ActorID, value string) Op {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %q: %v", value, err)
	}
	return Op{ID: OpID{Actor: actor, Seq: 1}, Lamport: lamport, Kind: KindSet, Value: raw}
}

func TestRegisterLWW(t *testing.T) {
	tests := []struct {
		name      string
		steps     []Op
		wantValue any
		wantLam   uint64
		wantActor ActorID
	}{
		{
			name:      "empty register takes first set",
			steps:     []Op{setOp(t, 7, "0123456789abcdef", "x")},
			wantValue: "x",
			wantLam:   7,
			wantActor: "0123456789abcdef",
		},
		{
			name:      "higher lamport wins",
			steps:     []Op{setOp(t, 1, "0123456789abcdef", "old"), setOp(t, 2, "0123456789abcdef", "new")},
			wantValue: "new",
			wantLam:   2,
			wantActor: "0123456789abcdef",
		},
		{
			name:      "lamport tie: higher actor wins",
			steps:     []Op{setOp(t, 5, "0123456789abcdef", "x"), setOp(t, 5, "fedcba9876543210", "y")},
			wantValue: "y",
			wantLam:   5,
			wantActor: "fedcba9876543210",
		},
		{
			name:      "lamport tie: higher actor wins regardless of arrival order",
			steps:     []Op{setOp(t, 5, "fedcba9876543210", "y"), setOp(t, 5, "0123456789abcdef", "x")},
			wantValue: "y",
			wantLam:   5,
			wantActor: "fedcba9876543210",
		},
		{
			name:      "lower lamport ignored",
			steps:     []Op{setOp(t, 10, "0123456789abcdef", "keep"), setOp(t, 9, "fedcba9876543210", "drop")},
			wantValue: "keep",
			wantLam:   10,
			wantActor: "0123456789abcdef",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Register{}
			for _, op := range tt.steps {
				if err := r.check(op); err != nil {
					t.Fatalf("check(%+v): %v", op, err)
				}
				r.apply(op)
			}
			if got := r.Materialize(); !reflect.DeepEqual(got, tt.wantValue) {
				t.Errorf("Materialize() = %#v, want %#v", got, tt.wantValue)
			}
			if !r.Set || r.Lamport != tt.wantLam || r.Actor != tt.wantActor {
				t.Errorf("register state = (set=%v, lamport=%d, actor=%s), want (true, %d, %s)",
					r.Set, r.Lamport, r.Actor, tt.wantLam, tt.wantActor)
			}
		})
	}
}

func TestRegisterNullValue(t *testing.T) {
	// JSON null is a valid register scalar; a set register holding null is
	// different from a never-set register.
	r := &Register{}
	op := Op{ID: OpID{Actor: "0123456789abcdef", Seq: 1}, Lamport: 1, Kind: KindSet, Value: json.RawMessage("null")}
	if err := r.check(op); err != nil {
		t.Fatalf("check: %v", err)
	}
	r.apply(op)
	if !r.Set {
		t.Error("register should be marked Set after a null write")
	}
	if got := r.Materialize(); got != nil {
		t.Errorf("Materialize() = %#v, want nil", got)
	}
}

func TestRegisterCheck(t *testing.T) {
	tests := []struct {
		name    string
		op      Op
		wantErr error
	}{
		{"string value", setOp(t, 1, "0123456789abcdef", "hi"), nil},
		{"number value", Op{Kind: KindSet, Value: json.RawMessage("3.5")}, nil},
		{"bool value", Op{Kind: KindSet, Value: json.RawMessage("true")}, nil},
		{"null value", Op{Kind: KindSet, Value: json.RawMessage("null")}, nil},
		{"wrong kind", Op{Kind: KindInc, Delta: 1}, ErrInvalid},
		{"object value", Op{Kind: KindSet, Value: json.RawMessage(`{"a":1}`)}, ErrInvalid},
		{"array value", Op{Kind: KindSet, Value: json.RawMessage(`[1]`)}, ErrInvalid},
		{"missing value", Op{Kind: KindSet}, ErrInvalid},
		{"invalid json", Op{Kind: KindSet, Value: json.RawMessage(`nope`)}, ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Register{}
			err := r.check(tt.op)
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

// randomSetOps returns n set ops whose (lamport, actor) pairs are unique but
// drawn from a small lamport range, so ties across actors are common.
func randomSetOps(rng *rand.Rand, actors []ActorID, n int) []Op {
	type stamp struct {
		lam uint64
		a   ActorID
	}
	used := make(map[stamp]bool, n)
	ops := make([]Op, 0, n)
	for len(ops) < n {
		s := stamp{uint64(rng.IntN(7)) + 1, actors[rng.IntN(len(actors))]}
		if used[s] {
			continue
		}
		used[s] = true
		raw, err := json.Marshal(fmt.Sprintf("v%d", len(ops)))
		if err != nil {
			panic(fmt.Errorf("marshal test value: %w", err)) // strings always marshal
		}
		ops = append(ops, Op{
			ID:      OpID{Actor: s.a, Seq: uint64(len(ops) + 1)},
			Lamport: s.lam,
			Kind:    KindSet,
			Value:   raw,
		})
	}
	return ops
}

// TestRegisterCommutativity applies 20 random set ops in 50 random
// permutations per round and requires every permutation to yield the same
// materialized value — and that value must be the write with the maximal
// (lamport, actor), computed independently.
func TestRegisterCommutativity(t *testing.T) {
	rng := newTestRand(t)
	rounds := testIter(t)
	actors := make([]ActorID, 6)
	for i := range actors {
		actors[i] = ActorID(fmt.Sprintf("%016x", i+1))
	}
	for round := 0; round < rounds; round++ {
		ops := randomSetOps(rng, actors, 20)

		winner := ops[0]
		for _, op := range ops[1:] {
			if OpLess(winner, op) {
				winner = op
			}
		}
		want, err := ParseScalar(winner.Value)
		if err != nil {
			t.Fatalf("round %d: ParseScalar: %v", round, err)
		}

		for p := 0; p < 50; p++ {
			perm := slices.Clone(ops)
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			r := &Register{}
			for _, op := range perm {
				if err := r.check(op); err != nil {
					t.Fatalf("round %d perm %d: check(%+v): %v", round, p, op, err)
				}
				r.apply(op)
			}
			if got := r.Materialize(); !reflect.DeepEqual(got, want) {
				t.Fatalf("round %d perm %d: got %#v, want %#v (winner lamport %d actor %s)",
					round, p, got, want, winner.Lamport, winner.ID.Actor)
			}
		}
	}
}
