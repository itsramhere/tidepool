package crdt

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const conformanceDir = "../../testdata/conformance"

// TestConformanceVectors replays every committed vector through Apply and
// requires Go to still reproduce the expected values and snapshot, guarding
// against accidental behavior changes. Regenerate with: go run ./cmd/tpconform
func TestConformanceVectors(t *testing.T) {
	entries, err := os.ReadDir(conformanceDir)
	if err != nil {
		t.Fatalf("read %s: %v (run go run ./cmd/tpconform to generate the vectors)", conformanceDir, err)
	}
	found := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		found++
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(conformanceDir, name))
			if err != nil {
				t.Fatalf("read vector: %v", err)
			}
			var c struct {
				Schema           Schema          `json:"schema"`
				Ops              []Op            `json:"ops"`
				ExpectedValues   map[string]any  `json:"expectedValues"`
				ExpectedSnapshot json.RawMessage `json:"expectedSnapshot"`
			}
			if err := json.Unmarshal(data, &c); err != nil {
				t.Fatalf("parse vector: %v", err)
			}
			if len(c.Ops) == 0 {
				t.Fatal("vector has no ops")
			}
			doc := NewDocument(c.Ops[0].Key)
			for i, op := range c.Ops {
				if err := doc.Apply(op, c.Schema); err != nil {
					t.Fatalf("op %d (%v): %v", i, op.ID, err)
				}
			}

			// Values compared as canonical JSON: expectedValues decode set
			// fields as []any while Materialize returns []string, but their
			// marshaled forms are identical, including map key order.
			want, err := json.Marshal(c.ExpectedValues)
			if err != nil {
				t.Fatalf("marshal expected values: %v", err)
			}
			got, err := json.Marshal(doc.Values())
			if err != nil {
				t.Fatalf("marshal actual values: %v", err)
			}
			if !bytes.Equal(want, got) {
				t.Errorf("values mismatch:\nwant %s\ngot  %s", want, got)
			}

			snap, err := doc.MarshalSnapshot()
			if err != nil {
				t.Fatalf("marshal snapshot: %v", err)
			}
			if !compactEqual(snap, c.ExpectedSnapshot) {
				t.Errorf("snapshot mismatch:\nwant %s\ngot  %s", c.ExpectedSnapshot, snap)
			}
		})
	}
	if found != 50 {
		t.Errorf("found %d conformance vectors, want 50", found)
	}
}

// compactEqual compares two JSON documents ignoring whitespace, preserving
// token order so it also catches reordering.
func compactEqual(a, b []byte) bool {
	var bufA, bufB bytes.Buffer
	if json.Compact(&bufA, a) != nil || json.Compact(&bufB, b) != nil {
		return false
	}
	return bytes.Equal(bufA.Bytes(), bufB.Bytes())
}
