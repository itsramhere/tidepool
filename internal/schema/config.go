package schema

import (
	"fmt"
	"os"

	"github.com/google/cel-go/cel"
	"github.com/itsramhere/tidepool/internal/crdt"
	"gopkg.in/yaml.v3"
)

// Rules holds the per-collection CEL rule texts.
type Rules struct {
	Read  string `yaml:"read"`
	Write string `yaml:"write"`
}

// Collection declares one collection's fields and rules. The compiled CEL
// programs are built at Parse time (fail fast) and are immutable afterwards.
type Collection struct {
	Fields map[string]string `yaml:"fields"`
	Rules  Rules             `yaml:"rules"`

	readProg  cel.Program
	writeProg cel.Program
}

// ReadRule returns the compiled read rule, or false when the collection
// declares none (reads are then allowed).
func (c *Collection) ReadRule() (cel.Program, bool) {
	return c.readProg, c.readProg != nil
}

// WriteRule returns the compiled write rule, or false when the collection
// declares none (writes are then allowed).
func (c *Collection) WriteRule() (cel.Program, bool) {
	return c.writeProg, c.writeProg != nil
}

// Config is the parsed rules.yaml.
type Config struct {
	Collections map[string]Collection `yaml:"collections"`
}

// Load reads and validates a rules.yaml file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load rules: %w", err)
	}
	return Parse(data)
}

// Parse decodes and validates rules.yaml content: collection names and field
// names must match their formats and every field type must be known.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse rules.yaml: %w", err)
	}
	if len(cfg.Collections) == 0 {
		return nil, fmt.Errorf("parse rules.yaml: no collections defined")
	}
	for name, coll := range cfg.Collections {
		if err := crdt.ValidateCollection(name); err != nil {
			return nil, fmt.Errorf("rules.yaml: %w", err)
		}
		if len(coll.Fields) == 0 {
			return nil, fmt.Errorf("rules.yaml: collection %q has no fields", name)
		}
		for field, ft := range coll.Fields {
			if err := crdt.ValidateField(field); err != nil {
				return nil, fmt.Errorf("rules.yaml: collection %q: %w", name, err)
			}
			switch crdt.FieldType(ft) {
			case crdt.FieldRegister, crdt.FieldCounter, crdt.FieldSet, crdt.FieldList, crdt.FieldText:
			default:
				return nil, fmt.Errorf("rules.yaml: collection %q field %q has unknown type %q", name, field, ft)
			}
		}
		// Compile every declared rule at startup: a bad rule must fail the
		// config load, not surface at first use.
		if coll.Rules.Read != "" {
			prog, err := compileRule(name+"/read", coll.Rules.Read)
			if err != nil {
				return nil, fmt.Errorf("rules.yaml: collection %q: %w", name, err)
			}
			coll.readProg = prog
		}
		if coll.Rules.Write != "" {
			prog, err := compileRule(name+"/write", coll.Rules.Write)
			if err != nil {
				return nil, fmt.Errorf("rules.yaml: collection %q: %w", name, err)
			}
			coll.writeProg = prog
		}
		cfg.Collections[name] = coll
	}
	return &cfg, nil
}

// Collection returns one collection's declaration with its compiled rules.
func (c *Config) Collection(name string) (Collection, bool) {
	coll, ok := c.Collections[name]
	return coll, ok
}

// Schema returns the field schema for one collection.
func (c *Config) Schema(coll string) (crdt.Schema, bool) {
	decl, ok := c.Collections[coll]
	if !ok {
		return nil, false
	}
	sch := make(crdt.Schema, len(decl.Fields))
	for field, ft := range decl.Fields {
		sch[field] = crdt.FieldType(ft)
	}
	return sch, true
}
