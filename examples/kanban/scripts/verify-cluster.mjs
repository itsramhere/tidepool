// Run after the README's demo Compose cluster is healthy. No mocked transport.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const servers = [];
let browser;
const errors = [];
async function freePort() {
  const server = createServer();
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const port = server.address().port;
  await new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve()));
  return port;
}
async function healthy(url) {
  for (let i = 0; i < 200; i++) {
    try { if ((await fetch(url)).ok) return; } catch { /* process starting */ }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`health timeout: ${url}`);
}
try {
  browser = await chromium.launch();
  const pages = [];
  for (const node of [7200, 7202]) {
    await healthy(`http://127.0.0.1:${node}/healthz`);
    const port = await freePort();
    const server = spawn(process.execPath, [resolve(root, "node_modules/vite/bin/vite.js"), "--host", "127.0.0.1", "--port", String(port), "--strictPort"], {
      cwd: root, windowsHide: true, env: { ...process.env, VITE_SERVER_URL: `ws://127.0.0.1:${node}/v1/sync` }, stdio: "ignore",
    });
    servers.push(server);
    await healthy(`http://127.0.0.1:${port}`);
    const page = await browser.newPage();
    page.setDefaultTimeout(30_000);
    page.on("pageerror", (e) => errors.push(e.message));
    const sockets = [];
    page.on("websocket", (socket) => sockets.push(socket.url()));
    await page.goto(`http://127.0.0.1:${port}`);
    await page.locator(".pill.synced").waitFor();
    assert(sockets.includes(`ws://127.0.0.1:${node}/v1/sync`), "wrong sync endpoint");
    pages.push(page);
  }
  const [first, second] = pages;
  const title = `M13-${Date.now()}`;
  const card = (page) => page.locator(".card").filter({ has: page.locator(`input.title[value="${title}"]`) });
  await first.getByRole("button", { name: "+ new card", exact: true }).first().click();
  const input = first.locator('input.title[value="New card"]').first();
  await input.fill(title);
  await first.locator(`input.title[value="${title}"]`).press("Enter");
  await card(second).waitFor();
  await first.getByRole("button", { name: "Go offline", exact: true }).click();
  await first.locator(".pill.offline").waitFor();
  await card(first).getByPlaceholder("Description (collaborative)").fill("Offline across nodes");
  await card(first).getByRole("button", { name: "Vote", exact: true }).click();
  assert.notEqual(await first.locator(".pending").innerText(), "0 pending");
  await card(second).getByRole("button", { name: "Vote", exact: true }).click();
  await card(second).getByPlaceholder("+ label").fill("node3");
  await card(second).getByPlaceholder("+ label").press("Enter");
  await second.locator(".pill.synced").waitFor();
  await first.getByRole("button", { name: "Go online", exact: true }).click();
  for (const page of pages) {
    await card(page).getByText("2 votes", { exact: true }).waitFor();
    await card(page).getByRole("button", { name: "remove node3", exact: true }).waitFor();
    await page.waitForFunction((title) => [...document.querySelectorAll(".card")].some((el) => el.querySelector("input.title").value === title && el.querySelector("textarea").value === "Offline across nodes"), title);
    await page.locator(".pill.synced").waitFor();
    assert.equal(await page.locator(".pending").innerText(), "0 pending");
  }
  await card(first).getByRole("combobox", { name: "Card board", exact: true }).selectOption("b2");
  await card(second).waitFor({ state: "detached" });
  for (const page of pages) {
    await page.getByRole("combobox", { name: "Board", exact: true }).selectOption("b2");
    await card(page).getByText("2 votes", { exact: true }).waitFor();
  }
  assert.deepEqual(errors, []);
  console.log("PASS: real Docker nodes 1 and 3, offline edits, counter/set/text convergence, zero pending, shape move-out/in.");
} finally {
  await browser?.close();
  await Promise.all(servers.map(async (server) => {
    if (server.exitCode === null) { server.kill(); await new Promise((resolve) => server.once("exit", resolve)); }
  }));
}
