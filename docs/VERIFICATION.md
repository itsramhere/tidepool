# M13 final verification

Verified on 2026-10-31 against the M13 working tree based on `a47f130`.
Windows: Go 1.26.3, Node 22.20.0, Docker Engine 29.8.0. Linux race checks ran
in the installed Ubuntu WSL environment with Go 1.27.3 and GCC. Each M13 task
also ran gofmt, vet, build, Go tests, SDK tests and TypeScript typechecking
before the next task started.

## Milestone completion checks

| Milestones | Command or check | Result |
|---|---|---|
| M0 | `gofmt -l .`, `go vet ./...`, `go build ./...`, `go test ./... -count=1` | Pass; formatting produced no output |
| M1 | `go test ./internal/crdt/...` | Pass in full suite |
| M2 | `TIDEPOOL_SEED=20261031 TIDEPOOL_ITER=2000 go test ./internal/crdt/... -count=1` | Pass, 5.643 s after the index fix |
| M3 | Same command with `TIDEPOOL_ITER=1000` | Pass, 3.081 s |
| M3 | `go run ./cmd/tpconform --out data/m13-conformance` | 50 regenerated files; byte-for-byte equal to committed vectors |
| M4 | Shared memory/Pebble suite and crash-safety tests through full Go run | Pass |
| M5 | `go run ./cmd/tidepoold --config deploy/dev.yaml` | Started; `/healthz` returned 200; verification process tree stopped afterward |
| M6 | `go test -count=20 ./test/integration/...` | All 20 repetitions passed, 94.286 s |
| M7–M8 | Authorization, rollback and shape tests through full Go run | Pass |
| M9 | `TIDEPOOL_SEED=20261031 go test ./test/chaos/... -count=5` | All five passed after the fix, 76.655 s |
| M10 | `npm --prefix sdk/ts test` and `npx tsc --noEmit` inside sdk/ts | 55 tests passed, including 50 vector cases and real-server E2E |
| M11 | `npm run demo:record` inside examples/kanban | Real two-session walkthrough passed; GIF regenerated |
| M12 | `bench/RESULTS.md` and raw benchmark/load evidence | Present; ack/fan-out sample counts and zero-error reports checked |
| M13 / CI | `TIDEPOOL_SEED=20261031 go test -race ./...` in Ubuntu WSL | All packages passed |
| Demo build | SDK build and `npm --prefix examples/kanban run build` | Pass |

The table uses POSIX-style inline environment assignments for compactness.
In PowerShell use `$env:TIDEPOOL_SEED='20261031'` and
`$env:TIDEPOOL_ITER='2000'` before the command, and remove `TIDEPOOL_ITER`
before running the default-iteration chaos checks.

## Docker and browser walkthrough

Built and started all three nodes using the README's demo Compose overlay.
The distroless nonroot image successfully wrote fresh named volumes. Ran
`node scripts/verify-cluster.mjs` from `examples/kanban`: it starts two real
Vite processes with distinct server URLs and uses browser sessions connected
to node 1 (`7200`) and node 3 (`7202`). It asserts the actual WebSocket URLs.

The walkthrough created a card, took one browser offline, edited text and
votes there, edited votes and a set label on the other node, then reconnected.
Both showed two votes, the merged label/text and zero pending operations.
Moving the card to b2 evicted it from b1 on both nodes and re-subscribing to b2
restored it. No browser page errors occurred. This supplements the longer
partition/migration chaos test; it is not a browser test of every protocol case.

Also started the base Compose configuration without the demo overlay:
all three health endpoints returned 200 and all three `/dev/token` requests
returned 404. Stopped the verification cluster with `docker compose down`;
named data volumes were retained.

To reproduce the cross-node browser check, first follow the README's install
and demo-cluster commands, then run:

```powershell
cd examples/kanban
npx playwright install chromium
node scripts/verify-cluster.mjs
```

## Failure found and fixed

The initial 2,000-step run failed the existing deep-equality snapshot test.
M12's adaptive RGA index could be absent after a large middle insertion, while
snapshot load always reconstructed a full map. The wire snapshot and visible
values matched, but the reconstructed document's derived state differed.

The index policy now depends only on sequence state: keep the map when the
greatest `(Lamport, Actor)` item lies in the final 64 slots; otherwise omit it.
Clone and snapshot loading reconstruct that same policy. A deterministic
regression test covers large shifts, late delivery and subsequent appends;
no existing test was weakened, skipped or deleted. The stress runs and Linux
race suite pass with the fix. [Updated measurements](../bench/RESULTS.md#m13-snapshot-safe-index-verification)
retain the original M12 evidence and record the M13 remeasurement separately.

## Documentation checks

Checked relative file links, parsed JSON examples, confirmed the 50-vector
count, and reviewed Mermaid sequence diagrams against the frame constructors
and handlers. ADR 0004 now documents noop rejection; the former shape-scan ADR
is preserved as 0006. Benchmark tables are short-run development-machine
observations, not production capacity claims.
