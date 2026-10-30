package schema

import (
	"fmt"
	"os"

	"github.com/itsramhere/tidepool/internal/crdt"
	"gopkg.in/yaml.v3"
)

// Rules holds the per-collection CEL rule texts. They are parsed and carried
// through from M5 but not evaluated until M7 wires in the rule engine;
// everything is allowed until then.
type Rules struct {
	Read  string `yaml:"read"`
	Write string `yaml:"write"`
}

// Collection declares one collection's fields and rules.
type Collection struct {
	Fields map[string]string `yaml:"fields"`
	Rules  Rules             `yaml:"rules"`
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
	}
	return &cfg, nil
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
