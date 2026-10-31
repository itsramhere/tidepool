package crdt

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"
)

func benchmarkRand(b *testing.B) *rand.Rand {
	seed := uint64(time.Now().UnixNano())
	if raw := os.Getenv("TIDEPOOL_SEED"); raw != "" {
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			b.Fatal(err)
		}
		seed = n
	}
	b.Logf("seed=%d", seed)
	return rand.New(rand.NewPCG(seed, seed))
}

func BenchmarkRGAInsertAppend(b *testing.B) { benchmarkRGA(b, false) }
func BenchmarkRGAInsertRandom(b *testing.B) { benchmarkRGA(b, true) }

// Build a causal chain without timing setup. Measure check + insert into an
// n..2n-item sequence, resetting outside the timer to bound memory growth.
func benchmarkRGA(b *testing.B, random bool) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			rng := benchmarkRand(b)
			actor := ActorID("aaaaaaaaaaaaaaaa")
			var g RGA
			reset := func() {
				g = RGA{Items: make([]RGAItem, n, n*2+1)}
				for i := range n {
					g.Items[i] = RGAItem{ID: OpID{Actor: actor, Seq: uint64(i + 1)}, Lamport: uint64(i + 1), Value: json.RawMessage(`"x"`)}
				}
				g.reindex()
			}
			reset()
			vv := map[ActorID]uint64{actor: uint64(n)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if len(g.Items) >= n*2 {
					b.StopTimer()
					reset()
					vv[actor] = uint64(n)
					b.StartTimer()
				}
				index := len(g.Items) - 1
				if random {
					index = rng.IntN(len(g.Items))
				}
				ref := g.Items[index].ID
				seq := vv[actor] + 1
				op := Op{ID: OpID{Actor: actor, Seq: seq}, Kind: KindIns, Lamport: seq, Ref: &ref, Value: json.RawMessage(`"x"`)}
				if err := g.check(op, vv); err != nil {
					b.Fatal(err)
				}
				g.apply(op)
				vv[actor] = seq
			}
		})
	}
}

func BenchmarkDocumentApply(b *testing.B) {
	for _, ft := range []FieldType{FieldRegister, FieldCounter, FieldSet, FieldList, FieldText} {
		b.Run(string(ft), func(b *testing.B) {
			d := NewDocument(DocKey{Coll: "cards", Doc: "bench"})
			sch := Schema{"field": ft}
			actor := ActorID("aaaaaaaaaaaaaaaa")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seq := uint64(i + 1)
				op := Op{ID: OpID{Actor: actor, Seq: seq}, Key: d.Key, Field: "field", Lamport: seq, Value: json.RawMessage(`"x"`)}
				switch ft {
				case FieldRegister:
					op.Kind = KindSet
				case FieldCounter:
					op.Kind = KindInc
					op.Delta = 1
					op.Value = nil
				case FieldSet:
					op.Kind = KindAdd
				case FieldList, FieldText:
					op.Kind = KindIns
					if i > 0 {
						ref := OpID{Actor: actor, Seq: seq - 1}
						op.Ref = &ref
					}
				}
				if err := d.Apply(op, sch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
