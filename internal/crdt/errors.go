package crdt

import "errors"

// Sentinel errors returned by validation and (from M3 on) Document.Apply.
// Callers must compare with errors.Is; implementations wrap them with
// context via fmt.Errorf("%w: ...", sentinel).
var (
	// ErrInvalid reports an op that violates a format, type or ordering rule
	// and must never be applied as-is.
	ErrInvalid = errors.New("invalid op")

	// ErrDuplicate reports an op whose Seq the document has already applied.
	// Callers treat it as success with no change.
	ErrDuplicate = errors.New("duplicate op")

	// ErrOutOfOrder reports an op whose Seq is not the next expected for its
	// actor on that document; it may become applicable after other ops arrive.
	ErrOutOfOrder = errors.New("out-of-order op")

	// ErrMissingDeps reports an op referencing elements or versions the
	// document has not seen yet; it may become applicable after syncing.
	ErrMissingDeps = errors.New("missing dependencies")

	// ErrNotFound reports a referenced element that does not exist.
	ErrNotFound = errors.New("not found")
)
