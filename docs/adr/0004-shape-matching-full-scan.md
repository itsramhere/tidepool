# ADR 0004: Shape matching scans the whole collection

Date: 2026-10-31
Status: Accepted (M8)

## Context

A subscription's shape is "read rule AND filter", both CEL programs evaluated
against a document's current values and the subscriber's auth map. Two paths
need matching:

- **Subscribe reconcile**: every doc of the collection is loaded and matched,
  so the subscriber starts with exactly the matching set.
- **Broadcast**: after each committed op, every subscription on the collection
  re-evaluates `matches` for that one doc to detect move-in / move-out.

The naive implementation is O(docs in collection) per subscribe and O(subs on
collection) per committed op.

## Decision

Accept the full scan. Subscribe loads every doc of the collection and
evaluates `matches` (a handful of CEL evaluations over a small values map).
Broadcast evaluates at most one read rule and one filter per subscription per
op — no scan at all, since only the op's own document changes membership.

## Consequences

- **Subscribe cost is O(collection).** A client subscribing to a collection
  with N docs triggers N loads plus N×(read rule + filter) evaluations on the
  committer goroutine. For the target deployment size (thousands of docs,
  short subscribes) this is acceptable; Pebble loads are prefix scans.
- **The committer goroutine does the CEL work.** Rule and filter evaluation
  happens on the single writer. CEL programs over small maps evaluate in
  microseconds; if this ever dominates, the move is to precompute a
  materialized-values cache in `docEntry` (updated on apply) rather than
  moving matching off the writer (which would reopen ADR 0002's race).
- **Stretch goal (not built):** an optional secondary index on one register
  field (e.g. `status`) mapping value → doc IDs, consulted when a filter has
  the form `doc.<field> == <literal>`. Revisit if benchmark data (M12) shows
  subscribe or broadcast matching as a bottleneck.
