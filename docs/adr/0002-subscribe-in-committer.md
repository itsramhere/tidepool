# ADR 0002: Subscribe reconciles inside the committer

Date: 2026-10-30
Status: Accepted (M5)

## Context

When a client sends `sub`, the server must reconcile the subscription: send a
snapshot (or the ops the client is missing) for every document in the
collection, evict docs the client claims that do not exist, then send `ready`.
Afterwards, every newly committed op on the collection must be streamed to the
subscription.

The committer is the single goroutine that applies ops and commits batches.
The subscription registry lives inside the committer (see
`internal/server/subscriptions.go`).

## Decision

The whole reconciliation happens on the committer goroutine, inside the
handling of the `subscribeReq`. The subscription is registered only after its
`ready` frame has been enqueued.

## Consequences

- **No races and no gaps.** A committed op is broadcast either before the
  subscription exists (and the reconcile reads therefore already include it)
  or after (and the broadcast delivers it). There is no window where an op
  could fall between the reconcile snapshot and the live feed, so a client
  that honors the frames it received converges without a second reconcile.
- **Reconcile cost is serialized with commits.** A subscribe over a large
  collection scans its docs on the committer goroutine, stalling other
  groups for the duration. This is O(collection size) per subscribe. For
  M5's scale this is acceptable; if it ever shows up in benchmarks (M12), the
  fix is moving reconcile reads to a snapshot-isolated reader (e.g. reading a
  consistent feed cut) while keeping registration on the committer — the
  registration is the part that genuinely needs serialization.
- **Slow consumers during reconcile.** Reconcile frames go through the same
  bounded per-session buffer as live frames; a client that cannot keep up is
  disconnected and reconnects (`slow_consumer`), which is the same recovery
  path as ordinary overload.
