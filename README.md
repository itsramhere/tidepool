# Tidepool

> **status: under construction** — see [PLAN.md](PLAN.md) for the roadmap and
> [PROGRESS.md](PROGRESS.md) for what is done.

Tidepool is a local-first sync server: apps keep a full copy of their data on
every device, edit it offline instantly, and sync later — conflicts never need
manual resolution because every field type is a conflict-free replicated data
type (CRDT). A single-writer committer group-commits operations to Pebble, an
embedded LSM-tree key-value store, so one fsync covers a whole batch of
requests; writes denied by per-collection CEL authorization rules are stored as
no-ops so that operation sequences stay contiguous and every replica converges
on the fact that the op did nothing; clients subscribe to *shapes* — a
collection plus a CEL filter — and partially replicate exactly the documents
they need, with live move-in snapshots and move-out evictions as documents
cross the shape's boundary. Nodes stream their operation feeds to each other
and converge without consensus, and Go and TypeScript client SDKs are held
byte-compatible by generated cross-language conformance vectors.

## Development

```powershell
gofmt -l .      # must print nothing
go vet ./...
go build ./...
go test ./...
```

CI additionally runs `go test -race ./...` on Linux.
