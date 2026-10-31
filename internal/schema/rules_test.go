package schema

import (
	"strings"
	"testing"
)

// ruleYAML builds a one-collection config body with the given rules.
func ruleYAML(read, write string) []byte {
	return []byte(`collections:
  cards:
    fields:
      title: register
      owner: register
      votes: counter
    rules:
      read: ` + quoteYAML(read) + `
      write: ` + quoteYAML(write) + `
`)
}

// quoteYAML renders an expression as a YAML double-quoted scalar, or an
// empty scalar when the rule is absent.
func quoteYAML(expr string) string {
	if expr == "" {
		return `""`
	}
	return `"` + expr + `"`
}

func TestParseCompilesRules(t *testing.T) {
	cfg, err := Parse(ruleYAML("true", "auth.uid == doc.owner"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	coll, ok := cfg.Collection("cards")
	if !ok {
		t.Fatal("cards collection missing")
	}
	if _, ok := coll.ReadRule(); !ok {
		t.Fatal("read rule not compiled")
	}
	if _, ok := coll.WriteRule(); !ok {
		t.Fatal("write rule not compiled")
	}
}

func TestParseRejectsBadRuleSyntax(t *testing.T) {
	_, err := Parse(ruleYAML("doc.board in", "true"))
	if err == nil || !strings.Contains(err.Error(), `doc.board in`) {
		t.Fatalf("Parse = %v, want error naming the rule text", err)
	}
}

func TestParseRejectsNonBoolOutput(t *testing.T) {
	_, err := Parse(ruleYAML("doc.owner", "true"))
	if err == nil || !strings.Contains(err.Error(), "want bool") {
		t.Fatalf("Parse = %v, want non-bool output error", err)
	}
}

func TestParseRejectsUnknownVariable(t *testing.T) {
	// map(string, dyn) keys are dynamic (runtime "no such key" errors), but
	// an undeclared variable must fail compilation.
	_, err := Parse(ruleYAML("bogus == true", "true"))
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("Parse = %v, want undeclared-variable error", err)
	}
}

// compile compiles src through the config's startup path.
func compile(t *testing.T, src string) func(RuleInputs) (bool, error) {
	t.Helper()
	// Compile once through the config so the startup path is exercised.
	cfg, err := Parse(ruleYAML("true", src))
	if err != nil {
		t.Fatalf("Parse write rule: %v", err)
	}
	coll, _ := cfg.Collection("cards")
	prog, ok := coll.WriteRule()
	if !ok {
		t.Fatal("write rule missing")
	}
	return func(in RuleInputs) (bool, error) { return Eval(prog, in) }
}

// auth builds the auth variable: uid plus all JWT claims.
func auth(uid string, claims map[string]any) map[string]any {
	m := map[string]any{"uid": uid}
	for k, v := range claims {
		m[k] = v
	}
	return m
}

// exampleWrite compiles the write-rule shape from PLAN.md M7 task 5.
func exampleWrite(t *testing.T) func(RuleInputs) (bool, error) {
	return compile(t, "isNew ? newDoc.owner == auth.uid : (doc.owner == auth.uid || op.field in ['votes', 'labels', 'checklist'])")
}

func TestExampleRuleCreatorClaimsOwnership(t *testing.T) {
	w := exampleWrite(t)
	// A field creation on an unowned doc is allowed only when the op makes
	// the creator the owner.
	allowed, err := w(RuleInputs{
		Auth:   auth("user-alice", nil),
		Doc:    map[string]any{},
		NewDoc: map[string]any{"owner": "user-alice"},
		IsNew:  true,
		Field:  "owner",
		Kind:   "set",
	})
	if err != nil || !allowed {
		t.Fatalf("create owner = %v, %v; want allowed", allowed, err)
	}
	allowed, err = w(RuleInputs{
		Auth:   auth("user-alice", nil),
		Doc:    map[string]any{},
		NewDoc: map[string]any{"owner": "user-bob"},
		IsNew:  true,
		Field:  "owner",
		Kind:   "set",
	})
	if err != nil || allowed {
		t.Fatalf("create owner as someone else = %v, %v; want denied", allowed, err)
	}
	// Creating any other field first is denied: owner does not exist yet.
	// The rule text evaluates newDoc.owner, which errors ("no such key") on
	// an unowned doc — evaluation errors count as deny.
	allowed, err = w(RuleInputs{
		Auth:   auth("user-alice", nil),
		Doc:    map[string]any{},
		NewDoc: map[string]any{"title": "hi"},
		IsNew:  true,
		Field:  "title",
		Kind:   "set",
	})
	if allowed {
		t.Fatalf("create title before owner = %v, %v; want denied", allowed, err)
	}
}

func TestExampleRuleOwnerAndSharedFields(t *testing.T) {
	w := exampleWrite(t)
	doc := map[string]any{"owner": "user-alice", "title": "hello", "votes": int64(1)}
	// The owner may edit anything.
	allowed, err := w(RuleInputs{
		Auth: auth("user-alice", nil), Doc: doc, NewDoc: map[string]any{"owner": "user-alice", "title": "changed", "votes": int64(1)},
		Field: "title", Kind: "set",
	})
	if err != nil || !allowed {
		t.Fatalf("owner title write = %v, %v; want allowed", allowed, err)
	}
	// A non-owner may touch the shared fields only.
	bob := auth("user-bob", nil)
	shared := RuleInputs{Auth: bob, Doc: doc, Field: "votes", Kind: "inc"}
	shared.NewDoc = map[string]any{"owner": "user-alice", "title": "hello", "votes": int64(2)}
	if allowed, err := w(shared); err != nil || !allowed {
		t.Fatalf("non-owner votes inc = %v, %v; want allowed", allowed, err)
	}
	// ...but not the title.
	if allowed, err := w(RuleInputs{
		Auth: bob, Doc: doc, NewDoc: map[string]any{"owner": "user-alice", "title": "stolen", "votes": int64(1)},
		Field: "title", Kind: "set",
	}); err != nil || allowed {
		t.Fatalf("non-owner title write = %v, %v; want denied", allowed, err)
	}
}

func TestExampleRuleReadShape(t *testing.T) {
	read := compile(t, "doc.board in auth.boards")
	allowed, err := read(RuleInputs{
		Auth:   auth("user-alice", map[string]any{"boards": []any{"b1", "b2"}}),
		Doc:    map[string]any{"board": "b1"},
		NewDoc: map[string]any{"board": "b1"},
	})
	if err != nil || !allowed {
		t.Fatalf("board in claim = %v, %v; want allowed", allowed, err)
	}
	allowed, err = read(RuleInputs{
		Auth:   auth("user-bob", map[string]any{"boards": []any{"b1"}}),
		Doc:    map[string]any{"board": "b9"},
		NewDoc: map[string]any{"board": "b9"},
	})
	if err != nil || allowed {
		t.Fatalf("board outside claim = %v, %v; want denied", allowed, err)
	}
}

// Eval must surface evaluation errors so callers can deny.
func TestEvalErrorsSurface(t *testing.T) {
	prog := compile(t, "doc.board in auth.boards")
	// auth.boards missing: the membership test cannot run.
	_, err := prog(RuleInputs{Auth: auth("user-alice", nil), Doc: map[string]any{"board": "b1"}})
	if err == nil {
		t.Fatal("Eval with missing claim = nil error, want error (deny)")
	}
	// doc.board missing: same.
	_, err = prog(RuleInputs{Auth: auth("user-bob", map[string]any{"boards": []any{"b1"}}), Doc: map[string]any{}})
	if err == nil {
		t.Fatal("Eval with missing doc field = nil error, want error (deny)")
	}
}
