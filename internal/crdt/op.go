package crdt

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
)

// ActorID identifies one client install or device: 16 lowercase hex chars
// (8 random bytes).
type ActorID string

// OpID uniquely identifies one operation. Seq starts at 1 and is contiguous
// per actor per document, so version vectors alone say which ops are known.
type OpID struct {
	Actor ActorID `json:"actor"`
	Seq   uint64  `json:"seq"`
}

// Less orders OpIDs by (Actor, Seq) with byte-wise actor comparison. Snapshot
// tag lists use this order; it is mirrored exactly by the TypeScript SDK.
func (a OpID) Less(b OpID) bool {
	if a.Actor != b.Actor {
		return a.Actor < b.Actor
	}
	return a.Seq < b.Seq
}

// DocKey names one document: a collection plus an ID within it.
type DocKey struct {
	Coll string `json:"coll"`
	Doc  string `json:"doc"`
}

// OpKind is the kind of change an Op makes to a field.
type OpKind string

// Op kinds; which kinds each field type accepts is fixed by PLAN.md §4.2.
const (
	KindSet  OpKind = "set"  // register: Value
	KindInc  OpKind = "inc"  // counter: Delta (non-zero)
	KindAdd  OpKind = "add"  // set: Value (JSON string); tag = op.ID
	KindRem  OpKind = "rem"  // set: Value + Observed (tags being removed)
	KindIns  OpKind = "ins"  // list/text: Ref (nil = insert at head) + Value
	KindDel  OpKind = "del"  // list/text: Ref = element to delete
	KindNoop OpKind = "noop" // stored in place of a rejected op
)

// Op is one immutable change to one field of one document.
type Op struct {
	ID       OpID            `json:"id"`
	Key      DocKey          `json:"key"`
	Lamport  uint64          `json:"lamport"`
	Field    string          `json:"field,omitempty"`
	Kind     OpKind          `json:"kind"`
	Value    json.RawMessage `json:"value,omitempty"`
	Delta    int64           `json:"delta,omitempty"`
	Ref      *OpID           `json:"ref,omitempty"`
	Observed []OpID          `json:"observed,omitempty"`
}

// OpLess reports whether a sorts before b in the total order over ops:
// (Lamport, then Actor byte-wise). Every order-dependent decision in the CRDT
// engine must use this comparison so all replicas converge.
func OpLess(a, b Op) bool {
	if a.Lamport != b.Lamport {
		return a.Lamport < b.Lamport
	}
	return a.ID.Actor < b.ID.Actor
}

// Validation patterns from PLAN.md §4.1. IDs and collection names never
// contain '/', which keeps the storage key layout unambiguous.
var (
	actorRe = regexp.MustCompile(`^[0-9a-f]{16}$`)
	collRe  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	docRe   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	fieldRe = regexp.MustCompile(`^[a-z][a-zA-Z0-9_]{0,63}$`)
)

// ValidateActor checks the actor ID format.
func ValidateActor(a ActorID) error {
	if !actorRe.MatchString(string(a)) {
		return fmt.Errorf("%w: actor %q does not match %s", ErrInvalid, a, actorRe)
	}
	return nil
}

// ValidateCollection checks the collection name format.
func ValidateCollection(coll string) error {
	if !collRe.MatchString(coll) {
		return fmt.Errorf("%w: collection %q does not match %s", ErrInvalid, coll, collRe)
	}
	return nil
}

// ValidateKey checks collection and document ID formats.
func ValidateKey(k DocKey) error {
	if !collRe.MatchString(k.Coll) {
		return fmt.Errorf("%w: collection %q does not match %s", ErrInvalid, k.Coll, collRe)
	}
	if !docRe.MatchString(k.Doc) {
		return fmt.Errorf("%w: doc %q does not match %s", ErrInvalid, k.Doc, docRe)
	}
	return nil
}

// ValidateField checks the field name format.
func ValidateField(f string) error {
	if !fieldRe.MatchString(f) {
		return fmt.Errorf("%w: field %q does not match %s", ErrInvalid, f, fieldRe)
	}
	return nil
}

// NewActorID returns a fresh random actor ID: 8 crypto-random bytes as 16
// lowercase hex chars. It panics if the system CSPRNG fails, which is not
// recoverable for a local-first client.
func NewActorID() ActorID {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("crdt.NewActorID: %w", err))
	}
	return ActorID(hex.EncodeToString(b[:]))
}
