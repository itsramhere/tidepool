# M12 measurements

Measured on 2026-10-31 against the M12 working tree based on `a3f3265`.
These are short, single-run measurements on a shared development laptop, not
capacity guarantees. Raw output is committed alongside this report.

## Environment and reproduction

- Windows 11 Home Single Language, build 10.0.26200, amd64.
- AMD Ryzen 7 7445HS, 6 physical cores / 12 logical processors; Go default
  GOMAXPROCS=12. Physical memory reported by Windows: 16,357,158,912 bytes.
- Samsung MZVL8512HFLU-00BH1 512 GB SSD; Pebble v2.1.7 on the local filesystem.
- Go 1.26.3; Node 22.20.0 for SDK checks. Server and load generator share the
  same machine and communicate over loopback; no peers, TLS or authorization
  rules in the benchmark collection. Load runs use `fsync: always`.

Run from the repository root in PowerShell:

```powershell
$env:TIDEPOOL_SEED = '12345'
go test ./internal/crdt -run '^$' -bench 'BenchmarkRGAInsert' -benchtime=200x -count=1 -benchmem
go test ./internal/crdt -run '^$' -bench '^BenchmarkDocumentApply$' -benchtime=200x -count=1 -benchmem
go test ./internal/server -run '^$' -bench '^BenchmarkCommit$' -benchtime=100x -count=1 -benchmem
python -B bench/run.py --duration 10s
```

`rga-before.txt` was captured before adding the derived index; `rga-after.txt`
is the final adaptive implementation. To reproduce the baseline with the new
benchmark file, use an isolated checkout with the original `rga.go` from
`a3f3265` and omit `g.reindex()` from benchmark setup. Setup and reset are
excluded from RGA timing; index memory and construction cost are therefore
excluded from its per-op allocations. Run longer repetitions before comparing
small differences; fixed iteration counts here make the workload explicit.

## RGA insertions

The benchmark starts with a causal chain of N items, uses PCG seed 12345, and
measures validation plus insertion. New insertions use increasing logical clocks;
random references select an existing item. Tombstones retain their positions.

| Pattern | Initial items | Before, µs/op | After, µs/op |
|---|---:|---:|---:|
| Append | 1,000 | 4.606 | 1.391 |
| Append | 10,000 | 26.936 | 0.634 |
| Append | 100,000 | 272.388 | 1.228 |
| Random | 1,000 | 3.454 | 2.308 |
| Random | 10,000 | 19.354 | 22.698 |
| Random | 100,000 | 479.148 | 657.436 |

The index removes repeated reference scans on append: the 100k case is about
222× faster in this run. Random insertion still moves a slice suffix and shows
no consistent improvement; the final 100k observation is slower, so this change
does not claim a random-insertion speedup. Updating every shifted map entry
experimentally cost about 4.85 ms/op at 100k, so large middle insertions now
discard the index and a subsequent append rebuilds it; shifts of at most 64
items update exact positions directly.

## Document apply

One actor edits one field for 200 successful operations. This includes ID,
sequence, dependency and scalar validation; it excludes storage and networking.

| Field | ns/op | B/op | Allocations/op |
|---|---:|---:|---:|
| Register | 2,557 | 192 | 4 |
| Counter | 1,567 | 16 | 1 |
| Set | 3,039 | 463 | 7 |
| List | 1,756 | 507 | 5 |
| Text | 1,860 | 507 | 5 |

The counter has the lowest observed allocation cost because it does not decode
a scalar or allocate sequence/tag state. These small-document measurements
do not predict the cost of large sets, clones, materialization, or long text.

## Pebble group commits

Each group contains exactly 1, 16 or 256 requests, one counter increment per
request. Each request edits its own stable document; the benchmark includes
validation, actor lookup, periodic snapshots, metrics, ack/echo encoding and
Pebble commit, but no sockets or CEL rules. There are 100 groups per case.

| Sync | Requests/group | ns/group | Operations/s | B/group |
|---|---:|---:|---:|---:|
| Never | 1 | 13,911 | 71,948 | 3,148 |
| Never | 16 | 233,434 | 68,548 | 64,779 |
| Never | 256 | 9,077,640 | 28,201 | 2,409,509 |
| Always | 1 | 1,218,403 | 820.8 | 3,422 |
| Always | 16 | 2,123,110 | 7,536 | 65,420 |
| Always | 256 | 7,947,758 | 32,210 | 2,416,281 |

With durable writes, grouping 256 requests raises observed throughput about
39× versus one request per fsync. Without fsync, larger groups did not improve
this short run; allocation, snapshot and encoding costs remain, and the
unexpected ordering of the two batch-256 results calls for longer runs rather
than a claim that fsync improves performance.

## End-to-end load

`run.py` builds binaries, starts a fresh durable database for each scenario,
seeds 100 documents outside the measurement, and connects every client to
`doc.run == "<unique run id>"`. All clients receive every measured edit; authors'
own echoes are excluded from fan-out latency. Every writer sends a numeric
Unix-nanosecond `sentAt` register operation and correlates the real ack by request
ID, while remote receivers read the timestamp directly from the op.

| Clients | Offered ops/s | Offered / acked | Completed ops/s | Remote samples | Errors |
|---:|---:|---:|---:|---:|---:|
| 2 | 200 | 2,000 / 2,000 | 199.79 | 2,000 | 0 |
| 16 | 500 | 5,000 / 5,000 | 498.97 | 75,000 | 0 |
| 64 | 1,000 | 10,000 / 10,000 | 998.90 | 630,000 | 0 |

All three scenarios delivered the expected `(clients - 1) × acked` remote
samples and drained without errors. Throughput includes the final drain
(about 10–20 ms here); these rate-limited runs demonstrate the offered loads,
not the server's saturation point or the default 1,000-client / 5,000-ops/s load.

| Clients | Ack p50 / p95 / p99, ms | Remote fan-out p50 / p95 / p99, ms |
|---:|---:|---:|
| 2 | 1.227 / 2.296 / 80.130 | 1.239 / 2.319 / 80.130 |
| 16 | 1.067 / 2.121 / 35.790 | 1.089 / 2.185 / 36.874 |
| 64 | 3.126 / 9.157 / 23.100 | 3.702 / 12.219 / 29.331 |

Quantiles use bounded-memory logarithmic histograms and report bucket upper
bounds within 1% of sampled positive latencies. Tail latency is variable on this
shared laptop, and fewer clients did not yield uniformly better p99; repeat
isolated runs before attributing that variation to a server change.

## Running and inspecting a node

Start the supplied config from the repository root, then use another terminal:

```powershell
go run ./cmd/tidepoold --config bench/node.yaml
# In the other terminal:
$env:TIDEPOOL_TOKEN = go run ./cmd/tpctl token --secret benchmark-secret-local --uid benchmark
go run ./cmd/tpload --url ws://127.0.0.1:7300/v1/sync --clients 16 --docs 100 --rate 500 --duration 10s
Invoke-RestMethod http://127.0.0.1:7300/metrics
$headers = @{ Authorization = 'Bearer benchmark-admin-local' }
Invoke-RestMethod http://127.0.0.1:7301/admin/docs/load -Headers $headers
# Substitute a document ID returned above:
go run ./cmd/tpctl doc get --url http://127.0.0.1:7301 --token benchmark-admin-local load/DOC_ID
Invoke-WebRequest http://127.0.0.1:7301/debug/pprof/goroutine -Headers $headers
```

`admin_listen` is optional and disabled when empty; a configured listener
requires a nonempty `admin_token`. Both document inspection and pprof require
`Authorization: Bearer <admin_token>` and are absent from the public listener.
The document response contains `key`, `values`, `vv`, and full `snapshot`;
collection inspection returns `coll` and sorted `docs`. Inspection runs through
the committer after storage commits, never directly against mutable cache state.

The public `/metrics` endpoint exports client operation outcomes, storage commit
latency/group sizes, open sessions, active subscriptions, slow-client disconnects
and peer lag. Peer lag uses the latest observed remote feed position minus its
durable cursor, so it is a lower bound when the remote node advances unseen.
Each embedded node owns its own registry. The benchmark configs use local-only
development credentials and bind both listeners to loopback.

Raw evidence: `rga-before.txt`, `rga-after.txt`, `document.txt`, `commit.txt`,
`load-{2,16,64}.json`, matching metrics scrapes and server logs. The Python runner
keeps the databases under ignored `data/benchmark-*` directories for inspection.
