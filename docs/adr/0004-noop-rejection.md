# ADR 0004: Store denied operations as noops

Date: 2026-10-31
Status: Accepted; records the M5/M7 design in M13

## Context

An offline client allocates contiguous per-document actor sequences before a
server can authorize its edits. If a server silently drops denied sequence 4,
sequence 5 cannot pass the next-sequence gate on any replica. Optimistic clients
also need to learn which local effect to remove.

## Decision

After sequence gates succeed, store a denied rule decision or invalid
type/value/Lamport as a noop with the original ID, key, Lamport and field.
Clear its value, delta, reference and observed tags. Applying a noop advances
only the VV, leaving fields, actor clocks and maximum Lamport unchanged.

Commit the noop through the same durable batch and feed as a real edit. Return
`ack` status `rejected` and echo the noop to its author exactly once. The
author removes the corresponding pending operation and rebuilds its visible
state from confirmed state plus remaining pending edits. Peers replay the
origin's decision without reevaluating write rules.

Bad identity/ownership is `invalid` and consumes no sequence; clients drop that
and all later pending edits of the same document and surface an error. Missing
sequences/dependencies are `retry`, while previously stored IDs are `dup`.

## Alternatives and consequences

Silently dropping a denied edit would leave a permanent sequence gap. Requiring
clients to renumber later operations would change their identities and any
references to them. Persisting a noop gives every replica a stable decision
without such rewrites, at the cost of log space for rejected attempts.

This is an authorization outcome, not an undo of already confirmed history.
Write rules use origin-local state, which can be stale; coordinated policy
enforcement would need an additional design. Later pending operations that
depend on rejected effects still undergo normal validation and can fail too.

See [protocol rollback](../PROTOCOL.md#rejection-rollback) and
[multi-master trade-offs](0003-multi-master.md).
