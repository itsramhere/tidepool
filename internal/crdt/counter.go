package crdt

import "fmt"

// Counter is a grow-only counter: the value is the sum of all inc deltas.
// Duplicate ops are already excluded at the document level by the version
// vector, so the field itself only ever sums distinct ops.
type Counter struct {
	N int64
}

// check validates an inc op against a counter field: the kind must be `inc`
// and the delta non-zero. It writes nothing.
func (c *Counter) check(op Op) error {
	if op.Kind != KindInc {
		return fmt.Errorf("%w: kind %q is not allowed on a counter", ErrInvalid, op.Kind)
	}
	if op.Delta == 0 {
		return fmt.Errorf("%w: counter delta must be non-zero", ErrInvalid)
	}
	return nil
}

// apply applies a checked inc op.
func (c *Counter) apply(op Op) {
	c.N += op.Delta
}

// Materialize returns the counter value.
func (c *Counter) Materialize() any {
	return c.N
}
