package crdt

import "slices"

// FieldType is the CRDT type of a document field.
type FieldType string

// The five field types (PLAN.md §4.2).
const (
	FieldRegister FieldType = "register"
	FieldCounter  FieldType = "counter"
	FieldSet      FieldType = "set"
	FieldList     FieldType = "list"
	FieldText     FieldType = "text"
)

// Schema maps field names to their types for one collection. Fields absent
// from the schema cannot receive ops.
type Schema map[string]FieldType

// AllowedKinds returns the op kinds a field type accepts (PLAN.md §4.2), or
// nil for an unknown type.
func AllowedKinds(ft FieldType) []OpKind {
	switch ft {
	case FieldRegister:
		return []OpKind{KindSet}
	case FieldCounter:
		return []OpKind{KindInc}
	case FieldSet:
		return []OpKind{KindAdd, KindRem}
	case FieldList, FieldText:
		return []OpKind{KindIns, KindDel}
	default:
		return nil
	}
}

// allowedKind reports whether kind may be applied to a field of type ft.
func allowedKind(ft FieldType, kind OpKind) bool {
	for _, k := range AllowedKinds(ft) {
		if k == kind {
			return true
		}
	}
	return false
}

// SortedFields returns the schema's field names sorted ascending, so callers
// never depend on Go's randomized map iteration order.
func (s Schema) SortedFields() []string {
	names := make([]string, 0, len(s))
	for name := range s {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
