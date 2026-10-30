package crdt

import (
	"encoding/json"
	"fmt"
)

// Register is a last-writer-wins register: the value written by the set op
// with the highest (Lamport, Actor) wins.
type Register struct {
	Lamport uint64
	Actor   ActorID
	Value   json.RawMessage
	Set     bool
}

// check validates a set op against a register field: the kind must be `set`
// and the value a JSON scalar (see ParseScalar). It writes nothing, so a
// failed check leaves the field untouched.
func (r *Register) check(op Op) error {
	if op.Kind != KindSet {
		return fmt.Errorf("%w: kind %q is not allowed on a register", ErrInvalid, op.Kind)
	}
	if _, err := ParseScalar(op.Value); err != nil {
		return err
	}
	return nil
}

// apply applies a checked set op. A strictly lower (Lamport, Actor) never
// replaces the current value; equal timestamps cannot occur between distinct
// ops of one actor because lamports strictly increase per actor.
func (r *Register) apply(op Op) {
	if r.Set && !registerWriteWins(*r, op) {
		return
	}
	r.Lamport = op.Lamport
	r.Actor = op.ID.Actor
	r.Value = op.Value
	r.Set = true
}

// registerWriteWins reports whether op's timestamp beats the register's
// current write under the total order (Lamport, Actor).
func registerWriteWins(r Register, op Op) bool {
	if op.Lamport != r.Lamport {
		return op.Lamport > r.Lamport
	}
	return op.ID.Actor > r.Actor
}

// Materialize returns the stored value parsed as a JSON scalar, or nil if
// nothing was ever set. Values are validated by check on the way in, so a
// parse failure here is impossible in practice; it degrades to nil.
func (r *Register) Materialize() any {
	if !r.Set {
		return nil
	}
	v, err := ParseScalar(r.Value)
	if err != nil {
		return nil
	}
	return v
}
