# ADR 0003: Multi-master replication via op feeds, no consensus

Date: 2026-10-31
Status: Accepted (M9)

## Context

Tidepool wants any client connected to any node, all nodes converging, with
no leader and no consensus protocol. Every node already keeps a node-wide
feed (`f/<feedSeq>`) of every op it applied, in apply order, and every op it
applied — own commits and replicated ones alike — lands in that feed through
the committer.

## Decision

Peers replicate by pulling each other's feed over WebSocket:

- `GET /v1/replicate?after=N` (Bearer peer secret) replays every feed op with
  `FeedSeq > N`, then streams live. The committer notifies feed subscribers
  after each commit; a subscriber whose buffer fills is disconnected and
  resumes from its own cursor.
- One puller goroutine per configured peer reads frames, batches up to 500,
  and hands them to the committer, which applies them **without rule
  checks**, skips duplicates, persists the peer cursor (`p/<peerID>`) in the
  same storage batch as the ops it covers, and broadcasts to local
  subscribers.
- A node configured with peers both serves the feed and pulls from its peers.
  The `peers` config couples the two directions for simplicity.

**Why no consensus.** Ops are CRDT mutations: applying the same set in any
order that respects §4.4's per-actor seq gates converges. A node's feed is
its apply log — every op's dependencies were applied on that node earlier, so
they appear earlier in the feed. Therefore replaying any single feed, from
any cursor, in order, is always causally valid on an arbitrary replica: no
coordination, no vector-clock negotiation, no anti-entropy beyond the cursor.
Out-of-order or missing-dep errors from a feed are consequently bugs (cluster
misconfiguration), logged and skipped rather than retried.

**Write-rule trade-off.** A write rule is evaluated once, on the origin node,
against that node's state — which may be stale relative to the cluster (the
origin may not yet have replicated a concurrent change the rule reads). All
nodes honor the origin's decision: replicated ops bypass rule checks so
every replica converges on exactly what the origin stored. Deployments that
need stricter arbitration must route a collection's writes through one node.

## Consequences

- **Ownership is not enforced on replication.** `a/<actor>` is claimed by
  client sessions; a node that receives an op for an actor it has never seen
  does not fabricate ownership — a migrated client claims normally when it
  connects. Cluster deployments share the same uid space.
- **Local schema/rule drift degrades to noops.** A replicated op that is
  invalid under the receiving node's rules.yaml is stored as a noop to keep
  per-actor seqs contiguous (logged loudly); feeds are only guaranteed
  round-trippable between nodes running the same configuration.
- **Cursors survive restarts** because they commit with the ops they cover;
  re-pulling after a crash yields duplicates, which are skipped by the VV.
- **Slow peers are cheap.** A lagging peer is disconnected rather than
  buffered unboundedly; its cursor makes the catch-up a plain replay.
- **A peered node never evicts a doc it does not have.** Reconcile evicts
  known docs that no longer match the shape, but a doc missing from the
  store of a node WITH peers is treated as replication lag, never as
  deletion: evicting it would make the client drop real data and later
  re-stamp colliding op IDs, which permanently splits replicas. The
  eventual move-in snapshot reconciles the client's copy instead.
  Single-node deployments keep the strict evict.
