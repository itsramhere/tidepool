package crdt

import (
	"errors"
	"slices"
	"testing"
)

func TestCounterCheck(t *testing.T) {
	tests := []struct {
		name    string
		op      Op
		wantErr error
	}{
		{"positive delta", Op{Kind: KindInc, Delta: 5}, nil},
		{"negative delta", Op{Kind: KindInc, Delta: -5}, nil},
		{"zero delta", Op{Kind: KindInc, Delta: 0}, ErrInvalid},
		{"wrong kind set", Op{Kind: KindSet, Value: []byte(`"x"`)}, ErrInvalid},
		{"wrong kind add", Op{Kind: KindAdd, Value: []byte(`"x"`)}, ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Counter{}
			err := c.check(tt.op)
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

func TestCounterSum(t *testing.T) {
	deltas := []int64{5, -3, 10, -1}
	var want int64
	for _, d := range deltas {
		want += d
	}
	if want != 11 {
		t.Fatalf("test setup: want %d, want 11", want)
	}

	mkOps := func() []Op {
		ops := make([]Op, len(deltas))
		for i, d := range deltas {
			ops[i] = Op{ID: OpID{Actor: "0123456789abcdef", Seq: uint64(i + 1)}, Lamport: uint64(i + 1), Kind: KindInc, Delta: d}
		}
		return ops
	}

	forward := mkOps()
	c := &Counter{}
	for _, op := range forward {
		if err := c.check(op); err != nil {
			t.Fatalf("check(%+v): %v", op, err)
		}
		c.apply(op)
	}
	if c.N != want {
		t.Errorf("forward order: N = %d, want %d", c.N, want)
	}

	reversed := mkOps()
	slices.Reverse(reversed)
	c2 := &Counter{}
	for _, op := range reversed {
		if err := c2.check(op); err != nil {
			t.Fatalf("check(%+v): %v", op, err)
		}
		c2.apply(op)
	}
	if c2.N != want {
		t.Errorf("reversed order: N = %d, want %d", c2.N, want)
	}
	if got := c2.Materialize(); got != any(want) {
		t.Errorf("Materialize() = %#v, want %#v", got, want)
	}
}

// TestCounterCommutativity sums 20 random non-zero deltas in 50 random
// permutations per round and requires the result to equal the plain sum.
func TestCounterCommutativity(t *testing.T) {
	rng := newTestRand(t)
	rounds := testIter(t)
	actors := []ActorID{"0123456789abcdef", "fedcba9876543210", "00000000000000ff"}
	for round := 0; round < rounds; round++ {
		const n = 20
		var want int64
		ops := make([]Op, 0, n)
		for i := 0; i < n; i++ {
			d := int64(rng.IntN(2001)) - 1000
			if d == 0 {
				d = 1
			}
			want += d
			ops = append(ops, Op{
				ID:      OpID{Actor: actors[i%len(actors)], Seq: uint64(i + 1)},
				Lamport: uint64(i + 1),
				Kind:    KindInc,
				Delta:   d,
			})
		}
		for p := 0; p < 50; p++ {
			perm := slices.Clone(ops)
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			c := &Counter{}
			for _, op := range perm {
				if err := c.check(op); err != nil {
					t.Fatalf("round %d perm %d: check(%+v): %v", round, p, op, err)
				}
				c.apply(op)
			}
			if c.N != want {
				t.Fatalf("round %d perm %d: N = %d, want %d", round, p, c.N, want)
			}
		}
	}
}
