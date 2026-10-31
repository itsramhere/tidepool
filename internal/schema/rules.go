package schema

import (
	"fmt"

	"github.com/google/cel-go/cel"
)

// ruleEnv is the one environment every rule compiles against, with the
// variables from PLAN.md milestone M7:
//
//	auth   map(string, dyn) — uid plus all JWT claims
//	doc    map(string, dyn) — materialized values before the op
//	newDoc map(string, dyn) — materialized values after the op
//	isNew  bool             — the op creates its field (no prior state)
//	op     map(string, dyn) — {field, kind} of the op being judged
//
// Construction cannot fail with only variable declarations, but the error
// is carried so a failure surfaces at first compile, not as a panic.
var ruleEnv, ruleEnvErr = cel.NewEnv(
	cel.Variable("auth", cel.MapType(cel.StringType, cel.DynType)),
	cel.Variable("doc", cel.MapType(cel.StringType, cel.DynType)),
	cel.Variable("newDoc", cel.MapType(cel.StringType, cel.DynType)),
	cel.Variable("isNew", cel.BoolType),
	cel.Variable("op", cel.MapType(cel.StringType, cel.DynType)),
)

// compileRule compiles one rule text against the shared environment. The
// rule text is part of every error so a bad deploy fails fast with the
// offending rule visible.
func CompileRule(name, src string) (cel.Program, error) {
	if ruleEnvErr != nil {
		return nil, fmt.Errorf("build rule environment: %w", ruleEnvErr)
	}
	ast, iss := ruleEnv.Compile(src)
	if err := iss.Err(); err != nil {
		return nil, fmt.Errorf("compile %s rule %q: %w", name, src, err)
	}
	if ast.OutputType() != cel.BoolType {
		return nil, fmt.Errorf("compile %s rule %q: output type %s, want bool", name, src, ast.OutputType())
	}
	prg, err := ruleEnv.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("program %s rule %q: %w", name, src, err)
	}
	return prg, nil
}

// RuleInputs carries the CEL variables for one rule evaluation. Doc and
// NewDoc hold materialized values (Document.Values maps); Auth holds uid
// plus every JWT claim. Read evaluations pass Doc == NewDoc and a zero op.
type RuleInputs struct {
	Auth   map[string]any
	Doc    map[string]any
	NewDoc map[string]any
	IsNew  bool
	Field  string
	Kind   string
}

// activation renders the inputs as a CEL map activation.
func (in RuleInputs) activation() map[string]any {
	return map[string]any{
		"auth":   in.Auth,
		"doc":    in.Doc,
		"newDoc": in.NewDoc,
		"isNew":  in.IsNew,
		"op":     map[string]any{"field": in.Field, "kind": in.Kind},
	}
}

// Eval evaluates one compiled rule. A returned error means the rule could
// not be decided (missing claim, type mismatch, ...) — callers must treat
// that as deny.
func Eval(prg cel.Program, in RuleInputs) (bool, error) {
	out, _, err := prg.Eval(in.activation())
	if err != nil {
		return false, fmt.Errorf("eval rule: %w", err)
	}
	allowed, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("eval rule: output %T is not bool", out.Value())
	}
	return allowed, nil
}
