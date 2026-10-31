# Tidepool

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

## Features

- **Five CRDT field types** — last-writer-wins registers, counters, add-wins
  OR-Sets, and RGA-backed lists and text — with deterministic op semantics
  shared verbatim between server and clients.
- **Offline-first Go client** (`pkg/client`): local confirmed state plus a
  pending op queue, automatic reconnect with jittered backoff, re-subscribe
  and re-push on every reconnect, pluggable `LocalStore` persistence.
- **Byte-compatible TypeScript client** (`sdk/ts`, `@tidepool/client`): the
  same CRDT engine ported 1:1, held identical to Go by 50 generated
  conformance vectors (values and snapshots compared as canonical JSON),
  with a reactive API, memory and IndexedDB persistence, and zero runtime
  dependencies beyond `idb`.
- **Group commit to Pebble** — one fsync per batch of up to 256 requests, a
  10k-document LRU cache, automatic snapshots every 100 ops per document.
- **CEL authorization rules** — per-collection `read`/`write` rules over
  `auth` (uid + JWT claims), `doc`/`newDoc` values, and the op itself;
  denied writes become noops, so rejection is just another convergent op.
- **Shapes** — subscriptions with CEL filters; documents move in (snapshot)
  and out (op, then evict) live, and partial-replication state survives
  reconnects.
- **Multi-master replication** — every node serves its op feed
  (`GET /v1/replicate?after=N`) and pulls its peers' feeds with durable
  cursors; no leader, no consensus, three nodes behind a docker-compose file.
- **Chaos-tested** — a seeded 3-node/6-client chaos test randomly partitions
  links and moves clients between nodes, then asserts byte-identical
  convergence everywhere.

## Quickstart

Run one node (development config, listens on `127.0.0.1:7100`):

```powershell
go run ./cmd/tidepoold --config deploy/dev.yaml
```

Mint a client token (HS256 JWT; `sub` is the user id):

```powershell
go run ./cmd/tpctl token --secret dev-secret-change-me --uid alice --ttl 24h
```

Edit a document from Go:

```go
c, err := client.New(client.Options{
    URL:   "ws://127.0.0.1:7100/v1/sync",
    Token: token,
    Schemas: map[string]crdt.Schema{"cards": {
        "title": crdt.FieldRegister, "votes": crdt.FieldCounter,
        "labels": crdt.FieldSet, "description": crdt.FieldText,
    }},
})
if err != nil { panic(err) }
go c.Run(ctx)                       // connect/reconnect loop until ctx is done
c.Subscribe("cards", "")            // shape: this collection, every doc

card := c.Doc("cards", "card1")     // offline edits queue locally
card.Set("title", "hello")
card.Inc("votes", 1)
card.InsertText("description", 0, "hi ")

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
c.WaitSynced(ctx)                   // pending ops pushed, subscriptions ready
fmt.Println(card.Values())
```

The same edits from TypeScript (`npm install @tidepool/client`):

```ts
import { Client } from "@tidepool/client";

const c = await Client.create({
  url: "ws://127.0.0.1:7100/v1/sync",
  token,
  schemas: { cards: { title: "register", votes: "counter",
                      labels: "set", description: "text" } },
});
c.run();                                 // connect/reconnect loop
c.subscribe("cards", "");                // shape: this collection, every doc

const card = c.doc("cards", "card1");    // offline edits queue locally
card.set("title", "hello");
card.inc("votes", 1);

await c.waitSynced();                    // pending ops pushed, subs ready
console.log(card.values());
```

Run a three-node cluster with replication:

```powershell
docker compose -f deploy/docker-compose.yml up
# clients: ws://localhost:7200/v1/sync, :7201, :7202
```

## Try it: collaborative kanban

![Two windows edit offline, reconnect, and move a card between board shapes](docs/kanban-demo.gif)

Use Go plus Node.js 22.12+ (or 20.19+). From the repository root, start the
demo node in one terminal:

```powershell
go run ./cmd/tidepoold --config examples/kanban/dev.yaml
```

In another terminal, build the local SDK, install the demo, and start Vite:

```powershell
npm --prefix sdk/ts ci
npm --prefix sdk/ts run build
cd examples/kanban
npm ci
npm run dev
```

Open the printed Vite URL in two windows. Each window has a separate actor,
even when both select Alice. Then follow this roughly 60-second walkthrough:

1. On board **b1**, click **+ new card**. Edit its title (Enter or blur to save),
   description, labels (Enter), and checklist. The second window updates live.
2. In the first window, click **Go offline**. Change the description and vote.
   Edits appear immediately and the pending counter grows.
3. In the second window, vote and add a different label while still online.
4. Click **Go online** in the first window. Both show the merged description,
   labels, and votes; pending returns to zero and status becomes **synced**.
5. Change a card's **Column** to move it to doing/done. Change its **Board** to
   **b2**: it leaves b1 in both windows. Select b2 in the header to receive it
   through the new `doc.board == "b2"` shape. **Archive card** moves it outside
   both demo boards.
6. Switch users in the header to try distinct identities. A user's queued edits
   survive switching away and back within that window.

The demo uses memory storage: keep the window open while offline; a reload
discards unconfirmed edits. Confirmed data lives in `data/demo`. Its dedicated
rules allow shared editing of every card. The unauthenticated `/dev/token?uid=`
endpoint is mounted only with `dev_mode: true`; the included config binds to
loopback. Leave dev mode disabled on deployed nodes. To use another node, set
`VITE_SERVER_URL` before starting Vite.

To repeat the browser verification and regenerate the GIF (requires FFmpeg on
PATH), run from `examples/kanban`:

```powershell
npx playwright install chromium
npm run demo:record
```

The script starts an isolated node with temporary storage and two browser
sessions, asserts convergence and board move-in/out, captures real screenshots,
then shuts down its node and removes the temporary data.

## Wire protocol

WebSocket text frames, one JSON object per frame (`hello`/`welcome`,
`sub`/`snap`/`op`/`evict`/`ready`, `push`/`ack`, `err`, `ping`/`pong`) — see
`internal/protocol` for the message definitions. The server guarantees, per
connection, commit-ordered frames (G1), exactly-once echo of every accepted op
to its author (G2), and post-`ready` equality between the client's copy and
the server's (G3).

## Project layout

```
cmd/tidepoold/       server binary        internal/replication/  peer feed + puller
cmd/tpctl/           token CLI            internal/schema/       rules.yaml + CEL rules
cmd/tpconform/       conformance vectors  internal/protocol/     wire messages
cmd/tpload/          load generator (M12) internal/auth/         JWT issue/verify
internal/crdt/       CRDT core            internal/storage/      Store: memory + Pebble
internal/server/     sessions, committer  pkg/client/            Go client SDK
sdk/ts/              TypeScript client    test/integration/      end-to-end tests + Go helper
docs/adr/            architecture decisions
deploy/              docker-compose cluster, node configs, rules example
testdata/conformance/ generated cross-language test vectors (M10 uses these)
test/integration/    end-to-end server + client tests
test/chaos/          partition/convergence tests + blockable proxy
```

## Development

```powershell
gofmt -l .      # must print nothing
go vet ./...
go build ./...
go test ./...
```

CI runs `go test -race ./...` for Go and `npm test` + `tsc --noEmit` for the
TypeScript SDK (both reproducible locally — the Go job verbatim inside
WSL). Randomized tests take `TIDEPOOL_SEED` (printed on failure) and
`TIDEPOOL_ITER`; the chaos test skips under `-short`. Architecture
decisions live in
[docs/adr](docs/adr): subscribe-in-committer (0002), multi-master
replication without consensus (0003), and the shape-matching full scan
(0004).

## Status

Milestones M0–M11 are complete: CRDT core with cross-language conformance
vectors, durable storage, the single-node server, the Go and TypeScript
client SDKs, CEL authorization rules, shape-based partial replication, and
multi-node replication with the chaos test, and the collaborative offline-first
kanban demo. Remaining milestones cover observability, benchmarks, and
documentation polish.
