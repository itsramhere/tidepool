package schema

import (
	"strings"
	"testing"

	"github.com/itsramhere/tidepool/internal/crdt"
)

const exampleRules = `collections:
  cards:
    fields:
      title: register
      status: register
      board: register
      owner: register
      votes: counter
      labels: set
      checklist: list
      description: text
    rules:
      read: "true"
      write: "true"
`

func TestParseExample(t *testing.T) {
	cfg, err := Parse([]byte(exampleRules))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, ok := cfg.Collections["cards"]; !ok {
		t.Fatal("cards collection missing")
	}
	sch, ok := cfg.Schema("cards")
	if !ok {
		t.Fatal("Schema(cards) = not ok")
	}
	want := crdt.Schema{
		"title":       crdt.FieldRegister,
		"status":      crdt.FieldRegister,
		"board":       crdt.FieldRegister,
		"owner":       crdt.FieldRegister,
		"votes":       crdt.FieldCounter,
		"labels":      crdt.FieldSet,
		"checklist":   crdt.FieldList,
		"description": crdt.FieldText,
	}
	if len(sch) != len(want) {
		t.Fatalf("Schema(cards) has %d fields, want %d", len(sch), len(want))
	}
	for field, ft := range want {
		if got := sch[field]; got != ft {
			t.Errorf("field %q = %q, want %q", field, got, ft)
		}
	}
	if _, ok := cfg.Schema("ghost"); ok {
		t.Error("Schema(unknown collection) = ok, want false")
	}
}

// TestLoadDeployExample pins deploy/rules.yaml to stay parseable.
func TestLoadDeployExample(t *testing.T) {
	cfg, err := Load("../../deploy/rules.yaml")
	if err != nil {
		t.Fatalf("Load(deploy/rules.yaml): %v", err)
	}
	if _, ok := cfg.Schema("cards"); !ok {
		t.Error("deploy/rules.yaml does not define cards")
	}
}

func TestParseRejects(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantSub string
	}{
		{
			name:    "unknown field type",
			yaml:    "collections:\n  cards:\n    fields:\n      x: blob\n",
			wantSub: "unknown type",
		},
		{
			name:    "bad collection name",
			yaml:    "collections:\n  Cards:\n    fields:\n      x: register\n",
			wantSub: "collection",
		},
		{
			name:    "bad field name",
			yaml:    "collections:\n  cards:\n    fields:\n      X: register\n",
			wantSub: "field",
		},
		{
			name:    "no collections",
			yaml:    "collections: {}\n",
			wantSub: "no collections",
		},
		{
			name:    "not yaml",
			yaml:    "\t-broken: [",
			wantSub: "parse rules.yaml",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil {
				t.Fatal("Parse = nil error, want error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not contain %q", err, tt.wantSub)
			}
		})
	}
}
