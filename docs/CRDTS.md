# CRDT semantics

Go (`internal/crdt`) and TypeScript (`sdk/ts/src/crdt`) implement the same
operation-based model. Replicas converge when they have the same accepted
operations delivered in causally valid order: operations from an actor are
contiguous per document, and references/observed tags arrive before their uses.
The server asks clients to retry missing dependencies rather than buffering
arbitrarily ordered operations inside a document.

## Identity and clocks

An actor is 16 lowercase hexadecimal characters, generated from eight random
bytes. An operation ID is `{actor, seq}`, with `seq` starting at one separately
for every actor/document pair. The document's VV is `map[actor]lastSeq`; a
sequence at or below its entry is a duplicate. Replaying an increment therefore
does not double-count it.

Each actor's Lamport timestamps strictly increase per document. The editor
chooses a timestamp above the document's maximum observed Lamport. Compare
operations using `(lamport, actor)`, with byte-wise actor comparison breaking
ties. This is a deterministic logical order, not time-of-day order.

| Identifier | Validation |
|---|---|
| Actor | `^[0-9a-f]{16}$` |
| Collection | `^[a-z][a-z0-9_]{0,63}$` |
| Document | `^[A-Za-z0-9_-]{1,128}$` |
| Field | `^[a-z][a-zA-Z0-9_]{0,63}$` |

## Apply order

`Document.Apply(op, schema)` checks these gates in order. Failure changes
neither fields nor vectors/clocks; duplicate is a distinguished error that
callers normally treat as success without a state change.

1. Require `op.key == document.key` and valid collection, document, actor and
   field identifiers. Otherwise return `ErrInvalid`. Noops retain a valid field
   name even though the wire struct marks `field` optional.
2. If `seq <= VV[actor]`, return `ErrDuplicate`.
3. If `seq != VV[actor] + 1`, return `ErrOutOfOrder`.
4. For `noop`, advance only `VV[actor]` and return. Do not update clocks or
   fields, and do not check its Lamport or schema field type.
5. Require `lamport > ActorClock[actor]`; otherwise return `ErrInvalid`.
6. Require a configured field type, an allowed operation kind and valid value.
   A scalar is string, number, boolean or null, with at most 64 KiB of JSON.
   Set elements are strings; list elements are scalars; text insertions are
   exactly one valid Unicode code point. Counter deltas must be nonzero;
   sequence deletion requires a reference and must not carry a value.
7. Require every `ref` and `observed` ID to be covered by the VV, or return
   `ErrMissingDeps`. Referenced sequence items must exist in that field, and
   an insertion's Lamport must exceed its referenced item's Lamport.
8. Apply the checked operation, creating field state only now. Set
   `VV[actor]=seq`, `ActorClock[actor]=lamport`, and
   `MaxLamport=max(MaxLamport,lamport)`.

Field-specific validation runs before mutation, including sequence dependency
checks. Go's position index is derived state and is never populated by a failed
validation. Missing fields remain absent from materialized values until a real
operation succeeds; a noop alone does not create a field.

## Register: last writer wins

State is `{Lamport, Actor, Value}` plus an empty/set flag. A `set` replaces the
value only if its `(Lamport, Actor)` is greater than the current winner, or the
register is empty. Materialization returns the winning JSON scalar.

For concurrent writes `(5, a) = "red"` and `(5, b) = "blue"`, with `a < b`,
every replica chooses `"blue"`, whichever write arrives first. This resolves
the conflict deterministically; it does not retain both user-visible values.

## Counter: increments

State is one `int64` sum. `inc` adds its signed, nonzero `delta` after document
deduplication. Concurrent `+3` and `-1` produce `2` in either order. Keep values
within the supported integer range; there is no overflow arbitration protocol.

## Set: add wins, observed remove

State maps each string to its live add tags. `add` uses its own operation ID as
a unique tag. `rem` names an element in `value` and removes only the tags listed
in `observed`; empty tag sets are dropped. Materialization returns live elements
sorted ascending, not arrival order.

Suppose Alice adds `"urgent"` with tag `a1`. Bob removes the observed `a1`, while
Carol concurrently adds `"urgent"` with tag `c1`. After all three operations,
`c1` remains, so `"urgent"` is present. A later remove that observes both tags
can remove the element. An unobserved concurrent add cannot be erased by an
older remove.

## List and text: RGA sequences

Each item stores `{ID, Lamport, Value, Deleted}` in document order. A list's
visible value is a scalar array; text concatenates visible single-code-point
strings. Deletes mark tombstones rather than removing items, so future
operations may still refer to deleted items.

Insertion follows this algorithm exactly:

```text
i = 0 if ref is null, otherwise indexOf(ref) + 1
while i < length(items) and (items[i].Lamport, items[i].ID.Actor)
                            > (op.Lamport, op.ID.Actor):
    i++
insert {op.ID, op.Lamport, op.Value, Deleted:false} at i
```

Concurrent siblings sort by descending logical timestamp. An item inserted
after observing a predecessor has a larger Lamport, so its descendants sort
ahead of that predecessor in the scan; skipping a higher-ranked sibling also
skips the descendants that must stay with it. The causal reference and clock
checks are necessary to this ordering argument.

Use abbreviated actor names `a < b < c` for this worked example (real actors
must match the identifier regex):

| Item ID | Lamport | Reference | Value |
|---|---:|---|---|
| a1 | 1 | head | A |
| b1 | 2 | a1 | B |
| c1 | 2 | a1 | C |
| b2 | 3 | b1 | ! |

Delivery `a1, b1, b2` first produces `AB!`. Inserting `c1` after a1 puts C
before B because `(2,c) > (2,b)`, giving `ACB!`. Delivery `a1, c1, b1, b2`
also gives `ACB!`: b1 skips the higher-ranked C, then b2 inserts after B.
Deleting b1 hides B but retains its slot, so the visible text becomes `AC!`.

Go keeps a `map[OpID]int` for fast references during append-heavy editing.
The map is present when the greatest `(Lamport, Actor)` item is within the
last 64 slots, and absent otherwise; absent maps use a linear lookup. This
state-based policy lets clone and snapshot load reconstruct the same derived
state regardless of delivery history. This does not change ordering or snapshot JSON;
TypeScript retains the same algorithm with linear lookup. See the measured
trade-off in [bench/RESULTS.md](../bench/RESULTS.md#rga-insertions).

Text positions count Unicode code points. `Array.from(text)` in TypeScript
avoids treating an emoji's surrogate pair as two CRDT elements. A grapheme
such as a combining sequence can still occupy several code points.

## Noops and rejection

A rejected operation is stored with the same ID/key/Lamport/field and
`kind: "noop"`; payload, delta, reference and observed tags are cleared.
Its sequence is consumed everywhere, but it does not change fields or clocks.
The client's confirmed-plus-pending view then loses the rejected optimistic
effect. This is why rejecting sequence 4 does not strand sequence 5 forever.

## Snapshots and conformance

A snapshot contains key, VV, actor clocks, maximum Lamport and all field state:
register winners, counter sums, live set tags and every sequence item including
tombstones. For example, a single accepted register write can be represented as:

```json
{
  "key": {"coll": "cards", "doc": "c1"},
  "vv": {"0000000000000001": 1},
  "clock": {"0000000000000001": 1},
  "maxLamport": 1,
  "fields": {
    "title": {"type": "register", "lamport": 1,
              "actor": "0000000000000001", "value": "Hello"}
  }
}
```

Counter fields encode `{type:"counter",n}`; sets encode
`{type:"set",elems:{element:[tag,...]}}`; lists/text encode `{type,items}` with
item keys `id`, `lamport`, `value`, `deleted`. Set tags sort by `(actor, seq)`.
Go sorts map keys when encoding JSON; TypeScript canonicalization sorts keys
recursively. Go generates 50 conformance vectors, and SDK tests compare values,
canonical snapshots and snapshot-loaded values.

Registers/lists use JSON number semantics (float64 in Go). TypeScript uses
`number` for counters, sequences and Lamports too: applications must remain
within `Number.MAX_SAFE_INTEGER` (2^53 − 1) for cross-language exactness.
