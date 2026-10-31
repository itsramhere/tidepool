# ADR 0001: Operation-based CRDTs with causal delivery

Date: 2026-10-31
Status: Accepted; records the M1–M3 design in M13

## Context

Clients need immediate offline edits, deterministic merging across nodes, and
authorization that can reject an individual edit. Documents have five field
types and may be large, while one edit often changes a single field.

## Decision

Use immutable operation-based CRDT mutations, identified by `(actor, seq)`
within a document. Deliver contiguous actor sequences and require observed
dependencies before applying an operation. A per-document VV provides dedup
and missing-operation selection. Lamport/actor order resolves register and
sequence ties; counter increments and observed-remove tags preserve concurrent
intent within their respective semantics.

Keep full snapshots for bootstrap and local recovery, including causal metadata
and sequence tombstones. Snapshots complement the operation log; they are not
a separate state-merge protocol. The same operation semantics run in Go and
TypeScript, checked against 50 generated vectors.

## Alternatives and consequences

A state-based design would require mergeable representations and authorization
semantics for incoming aggregate state. We chose individual operations because
the server can validate and record the result of each attempted edit, including
a noop for a rejection. This is a design choice, not a measured claim that
operation-based replication is always smaller.

Operations require causal integration: missing dependencies produce retries,
and an arbitrary unordered stream is not directly applicable. History and RGA
tombstones grow until a future retention/GC protocol is introduced. CRDT
convergence does not enforce application-wide invariants such as unique names
or transactional transfers between documents.

See [CRDT semantics](../CRDTS.md), [noop rejection](0004-noop-rejection.md), and
[multi-master replication](0003-multi-master.md).
