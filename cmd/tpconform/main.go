package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"

	"github.com/itsramhere/tidepool/internal/crdt"
)

// conformanceCase is the on-disk shape of one testdata/conformance vector.
type conformanceCase struct {
	Schema           map[string]crdt.FieldType `json:"schema"`
	Ops              []crdt.Op                 `json:"ops"`
	ExpectedValues   map[string]any            `json:"expectedValues"`
	ExpectedSnapshot json.RawMessage           `json:"expectedSnapshot"`
}

const (
	caseCount    = 50
	stepsPerCase = 120
)

func main() {
	out := flag.String("out", "testdata/conformance", "output directory for case-XXX.json files")
	flag.Parse()
	if err := generate(*out); err != nil {
		fmt.Fprintln(os.Stderr, "tpconform:", err)
		os.Exit(1)
	}
}

// generate writes one case per seed 1..caseCount. Regenerate with
// `go run ./cmd/tpconform`: the fixed seeds, fixed scenario actors and fixed
// schema make the output reproducible byte for byte.
func generate(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	for seed := 1; seed <= caseCount; seed++ {
		rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)))
		key := crdt.DocKey{Coll: "cards", Doc: fmt.Sprintf("case%d", seed)}
		reps, err := crdt.RunScenario(rng, key, stepsPerCase)
		if err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
		base := reps[0]
		for _, r := range reps[1:] {
			if got := r.Doc.Values(); !reflect.DeepEqual(got, base.Doc.Values()) {
				return fmt.Errorf("seed %d: replica %s did not converge: %v vs %v", seed, r.Actor, got, base.Doc.Values())
			}
		}
		snap, err := base.Doc.MarshalSnapshot()
		if err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
		c := conformanceCase{
			Schema:           crdt.ScenarioSchema,
			Ops:              base.Log,
			ExpectedValues:   base.Doc.Values(),
			ExpectedSnapshot: snap,
		}
		data, err := json.MarshalIndent(c, "", "  ")
		if err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
		data = append(data, '\n')
		name := filepath.Join(dir, fmt.Sprintf("case-%03d.json", seed))
		if err := os.WriteFile(name, data, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
		fmt.Printf("wrote %s (%d ops)\n", name, len(base.Log))
	}
	return nil
}
