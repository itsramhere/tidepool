// Replays the README walkthrough against an isolated real node and captures a GIF.
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer as netServer } from "node:net";
import { chromium } from "playwright";
import { createServer } from "vite";

const appRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repo = resolve(appRoot, "../..");
const work = mkdtempSync(join(tmpdir(), "tidepool-demo-"));
let node, vite, browser;
let stderr = "";
let frame = 0;
const errors = [];
async function freePort() {
  const server = netServer();
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const port = server.address().port;
  await new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve()));
  return port;
}
function command(exe, args, cwd = repo) {
  const result = spawnSync(exe, args, { cwd, encoding: "utf8", windowsHide: true });
  if (result.error) throw result.error;
  assert.equal(result.status, 0, result.stderr);
}
async function healthy(url) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try { if ((await fetch(url)).ok) return; } catch { /* node starting */ }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`Node did not start: ${stderr}`);
}
async function capture(page) {
  await page.screenshot({ path: join(work, `${String(frame++).padStart(3, "0")}.png`) });
  console.log(`Captured frame ${frame}`);
}
async function synced(page) { await page.locator(".pill.synced").waitFor(); }
try {
  const port = await freePort();
  const config = join(work, "node.yaml");
  writeFileSync(join(work, "rules.yaml"), readFileSync(join(appRoot, "rules.yaml")));
  writeFileSync(config, `node_id: demo-recording\nlisten: "127.0.0.1:${port}"\ndata_dir: ${JSON.stringify(join(work, "data"))}\nfsync: always\njwt_secret: recording-only\ndev_mode: true\nrules_file: ${JSON.stringify(join(work, "rules.yaml"))}\n`);
  const binary = join(work, process.platform === "win32" ? "node.exe" : "node");
  command("go", ["build", "-o", binary, "./cmd/tidepoold"]);
  node = spawn(binary, ["--config", config], { windowsHide: true, stdio: ["ignore", "ignore", "pipe"] });
  node.stderr.on("data", (chunk) => { stderr += chunk; });
  await healthy(`http://127.0.0.1:${port}/healthz`);
  process.env.VITE_SERVER_URL = `ws://127.0.0.1:${port}/v1/sync`;
  vite = await createServer({ root: appRoot, server: { host: "127.0.0.1", port: 0 } });
  await vite.listen();
  browser = await chromium.launch();
  const first = await browser.newPage({ viewport: { width: 1100, height: 800 } });
  const second = await browser.newPage({ viewport: { width: 1100, height: 800 } });
  for (const page of [first, second]) {
    page.setDefaultTimeout(15_000);
    page.on("pageerror", (error) => errors.push(error.message));
    await page.goto(vite.resolvedUrls.local[0]);
    await synced(page);
  }
  await capture(first);
  await first.getByRole("button", { name: "+ new card", exact: true }).first().click();
  const title = first.getByRole("textbox", { name: "Card title" });
  await title.fill("Ship the weekend project"); await title.press("Enter");
  await first.getByPlaceholder("Description (collaborative)").fill("Build with 🌊 Tidepool");
  await first.getByPlaceholder("+ label").fill("sync");
  await first.getByPlaceholder("+ label").press("Enter");
  await first.getByPlaceholder("+ checklist item").fill("Try two windows");
  await first.getByRole("button", { name: "Add", exact: true }).click();
  await synced(first);
  await second.getByRole("textbox", { name: "Card title" }).waitFor();
  await capture(first);
  await first.getByRole("button", { name: "Go offline", exact: true }).click();
  await first.locator(".pill.offline").waitFor();
  await capture(first);
  await first.getByPlaceholder("Description (collaborative)").fill("Build with 🌊 Tidepool offline");
  await first.getByRole("button", { name: "Vote", exact: true }).click();
  assert.notEqual(await first.locator(".pending").innerText(), "0 pending");
  await capture(first);
  await second.getByRole("button", { name: "Vote", exact: true }).click();
  await second.getByPlaceholder("+ label").fill("review");
  await second.getByPlaceholder("+ label").press("Enter");
  await synced(second);
  await capture(second);
  await capture(first);
  await first.getByRole("button", { name: "Go online", exact: true }).click();
  await synced(first);
  for (const page of [first, second]) {
    await page.getByText("2 votes", { exact: true }).waitFor();
    await page.getByRole("button", { name: "remove review", exact: true }).waitFor();
    await page.waitForFunction(() => document.querySelector("textarea").value === "Build with 🌊 Tidepool offline");
    assert.equal(await page.locator(".pending").innerText(), "0 pending");
    await capture(page);
  }
  await first.getByRole("combobox", { name: "Column", exact: true }).selectOption("doing");
  await synced(first);
  await capture(first);
  await first.getByRole("combobox", { name: "Card board", exact: true }).selectOption("b2");
  await second.locator(".card").waitFor({ state: "detached" });
  await capture(first);
  for (const page of [first, second]) {
    await page.getByRole("combobox", { name: "Board", exact: true }).selectOption("b2");
    await synced(page);
    await page.locator(".card").waitFor();
    await capture(page);
  }
  await second.getByRole("combobox", { name: "User", exact: true }).selectOption("bob");
  await synced(second);
  await second.getByRole("button", { name: "Go offline", exact: true }).click();
  await second.getByRole("button", { name: "Vote", exact: true }).click();
  await second.getByRole("combobox", { name: "User", exact: true }).selectOption("carol");
  await second.getByRole("button", { name: "+ new card", exact: true }).first().waitFor();
  await second.getByRole("combobox", { name: "User", exact: true }).selectOption("bob");
  await second.getByText("3 votes", { exact: true }).waitFor();
  await capture(second);
  await second.getByRole("button", { name: "Go online", exact: true }).click();
  await synced(second);
  await first.getByText("3 votes", { exact: true }).waitFor();
  await capture(first);
  assert.deepEqual(errors, [], `Browser errors: ${errors.join(", ")}`);
  const output = join(repo, "docs", "kanban-demo.gif");
  mkdirSync(dirname(output), { recursive: true });
  command("ffmpeg", ["-y", "-framerate", "1/2", "-i", join(work, "%03d.png"), "-filter_complex", "split[a][b];[a]palettegen[p];[b][p]paletteuse", "-loop", "0", output]);
  console.log(`Two-window walkthrough passed; ${frame} real screenshots recorded to ${output}`);
} finally {
  await browser?.close();
  await vite?.close();
  if (node && node.exitCode === null) {
    node.kill();
    await new Promise((resolve) => node.once("exit", resolve));
  }
  assert.equal(dirname(resolve(work)), resolve(tmpdir()));
  rmSync(work, { recursive: true, force: true });
}
