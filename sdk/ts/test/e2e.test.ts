// End-to-end: the TypeScript client against a real tidepoold, converging
// with a Go client (the helper binary under test/integration/helper).
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import { Client, canonicalStringify, type DocKey } from "../src/client";

const repoRoot = join(import.meta.dirname, "..", "..", "..");
const jwtSecret = "e2e-secret";
const port = await freePort();
const url = `ws://127.0.0.1:${port}/v1/sync`;

let workDir = "";
let serverProc: ChildProcess | null = null;
let serverStderr = "";
let token = "";

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      if (addr && typeof addr === "object") {
        srv.close(() => resolve(addr.port));
      } else {
        reject(new Error("no port"));
      }
    });
    srv.on("error", reject);
  });
}

async function waitHealthy(): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/healthz`);
      if (res.ok) {
        return;
      }
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`server did not become healthy: ${serverStderr.slice(-2000)}`);
}

beforeAll(async () => {
  workDir = mkdtempSync(join(tmpdir(), "tidepool-e2e-"));
  writeFileSync(
    join(workDir, "rules.yaml"),
    `collections:\n  cards:\n    fields:\n      title: register\n      status: register\n      votes: counter\n      labels: set\n      checklist: list\n      description: text\n`,
  );
  writeFileSync(
    join(workDir, "node.yaml"),
    `node_id: e2e\nlisten: "127.0.0.1:${port}"\ndata_dir: ${join(workDir, "data").replace(/\\/g, "\\\\")}\nfsync: never\njwt_secret: ${jwtSecret}\nrules_file: ${join(workDir, "rules.yaml").replace(/\\/g, "\\\\")}\nmax_ops_per_push: 1000\nsession_send_buffer: 1024\npeers: []\n`,
  );

  // Build the server binary (spawning `go run` on Windows leaks children).
  const build = spawnSync("go", ["build", "-o", join(workDir, "tidepoold.exe"), "./cmd/tidepoold"], {
    cwd: repoRoot,
    encoding: "utf-8",
  });
  if (build.status !== 0) {
    throw new Error(`go build: ${build.stderr}`);
  }
  serverProc = spawn(join(workDir, "tidepoold.exe"), ["--config", join(workDir, "node.yaml")], {
    stdio: ["ignore", "ignore", "pipe"],
  });
  serverProc.stderr?.on("data", (d) => {
    serverStderr += String(d);
  });
  await waitHealthy();

  const tok = spawnSync(
    "go",
    ["run", "./cmd/tpctl", "token", "--secret", jwtSecret, "--uid", "e2e-user", "--ttl", "1h"],
    { cwd: repoRoot, encoding: "utf-8" },
  );
  if (tok.status !== 0) {
    throw new Error(`tpctl: ${tok.stderr}`);
  }
  token = tok.stdout.trim();
}, 90_000);

afterAll(() => {
  serverProc?.kill();
  try {
    rmSync(workDir, { recursive: true, force: true });
  } catch {
    // best effort
  }
});

// runGoHelper runs the Go client helper and returns its printed doc values.
function runGoHelper(actor: string, ops: string): Promise<Record<string, Record<string, unknown>>> {
  return new Promise((resolve, reject) => {
    const proc = spawn(
      "go",
      ["run", "./test/integration/helper", "-url", url, "-token", token, "-actor", actor, "-ops", ops],
      { cwd: repoRoot, encoding: "utf-8" },
    );
    let out = "";
    let err = "";
    proc.stdout.on("data", (d) => (out += d));
    proc.stderr.on("data", (d) => (err += d));
    proc.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`helper exited ${code}: ${err}`));
        return;
      }
      try {
        resolve(JSON.parse(out));
      } catch (e) {
        reject(new Error(`helper output: ${out} (${String(e)})`));
      }
    });
  });
}

function waitFor(repr: () => string, want: string, timeoutMs = 20_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const tick = () => {
      if (repr() === want) {
        resolve();
        return;
      }
      if (Date.now() > deadline) {
        reject(new Error(`never matched: want ${want}, have ${repr()}`));
        return;
      }
      setTimeout(tick, 100);
    };
    tick();
  });
}

describe("TypeScript client against a real server", () => {
  it("converges with a Go client across every field type", async () => {
    const ts = await Client.create({
      url,
      token,
      actor: "00000000000000a1",
      schemas: {
        cards: {
          title: "register",
          status: "register",
          votes: "counter",
          labels: "set",
          checklist: "list",
          description: "text",
        },
      },
    });
    ts.run();
    ts.subscribe("cards", "");
    let changes = 0;
    ts.onChange((_: DocKey) => {
      changes++;
    });

    // TS client edits; the Go client edits the same docs.
    const shared = ts.doc("cards", "shared");
    shared.set("title", "from-ts");
    shared.inc("votes", 3);
    shared.add("labels", "red");
    shared.insertText("description", 0, "héllo");
    shared.insert("checklist", 0, "step1");

    const goView = await runGoHelper("00000000000000b2", [
      "set:cards:shared:title:from-go",
      "inc:cards:shared:votes:4",
      "add:cards:shared:labels:blue",
      "set:cards:from-go:status:open",
      "inc:cards:from-go:votes:1",
    ].join(";"));

    // The TS client must converge on the merged state.
    const wantShared: Record<string, unknown> = {
      title: "from-go",
      votes: 7,
      labels: ["blue", "red"],
      checklist: ["step1"],
      description: "héllo",
    };
    await waitFor(
      () => canonicalStringify(ts.doc("cards", "shared").values()),
      canonicalStringify(wantShared),
    );

    // The Go client's view of shared must match too.
    expect(canonicalStringify(goView["cards/shared"])).toBe(canonicalStringify(wantShared));
    // And the Go client must see the TS-created doc state replicated in.
    expect(goView["cards/from-go"]).toEqual({ status: "open", votes: 1 });

    // The reactive onChange callback must have fired for the convergence.
    expect(changes).toBeGreaterThan(0);
  }, 90_000);
});
