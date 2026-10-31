# ADR 0005: Pebble for durable operation and feed storage

Date: 2026-10-31
Status: Accepted; records the M4/M5 design in M13

## Context

A commit must publish document operations, document metadata, snapshots, actor
ownership and the node feed together. Replication additionally needs its cursor
to commit with the received operations. Recovery must replay operations in
local document order and support prefix scans.

## Decision

Use Pebble v2 behind the `storage.Store` interface, with a memory implementation
for tests. Encode document/feed positions as zero-padded decimal key suffixes
so key order is sequence order. One storage batch covers all affected keyspaces;
the single-writer committer groups up to 256 waiting requests per commit.

`fsync: always` requests a synced batch and is the default. `fsync: never` is an
explicit weaker durability option. Acknowledgments follow successful batch
commit. Keep periodic full CRDT snapshots and replay only later document ops
for local reconstruction; peers still consume the retained feed.

## Alternatives and consequences

Memory-only storage is useful for tests but cannot meet restart durability.
A custom append-only file would also need indexing, atomic metadata/cursor
publication and crash recovery; we chose an existing embedded store rather
than implementing those mechanisms in this project. The Store interface keeps
those engine details out of CRDT and sync semantics.

Pebble does not supply application conflict resolution or cluster consensus;
CRDTs and feed replay do that work. Snapshots do not currently prune the log or
tombstones, so disk usage grows. The crash-safety test uses Pebble v2's
crashable in-memory filesystem to distinguish synced from unsynced data, and
the shared storage suite runs against both backends.

The [M12 results](../../bench/RESULTS.md#pebble-group-commits) measure durable
group commit gains on one development machine; they do not establish a
general engine comparison or production capacity guarantee.
