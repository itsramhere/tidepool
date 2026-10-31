// Reactive client mirroring the Go SDK (PLAN.md M6 tasks 2–4, M10 task 4):
// per-document confirmed state plus a pending op queue, an editable view,
// shape subscriptions reconciled on every reconnect, jittered exponential
// backoff, and non-blocking sends (a full send queue tears the connection
// down; Run reconnects).
import { canonicalStringify } from "./canonical.js";
import { CrdtError } from "./crdt/errors.js";
import { Editor } from "./crdt/editor.js";
import { Document, newFieldState, type FieldState } from "./crdt/document.js";
import { newActorID, validateActor, validateKey, type ActorID, type DocKey, type Op, type OpID } from "./crdt/op.js";
import type { Schema } from "./crdt/schema.js";
import { marshalSnapshot, unmarshalSnapshot, type SnapshotObject } from "./crdt/snapshot.js";
import type { ClientStorage, PersistedState } from "./storage.js";
import { MemoryStorage } from "./storage.js";

const baseBackoffMs = 200;
const maxBackoffMs = 10_000;
const handshakeTimeoutMs = 10_000;
const writeTimeoutMs = 5_000;
const sendBuffer = 64;
const maxOpsPerPush = 1000;
const maxPushBytes = 512 << 10;
const perOpFrameOverhead = 128;
const protocolVersion = 1;

/** Client status: derived from the connection and sync state. */
export type ClientStatus = "offline" | "connecting" | "syncing" | "synced";

export interface ClientOptions {
  /** The server's sync endpoint, e.g. ws://127.0.0.1:7100/v1/sync. */
  url: string;
  /** JWT presented in the hello frame. */
  token: string;
  /** Actor identifying this device; generated (or restored) when omitted. */
  actor?: ActorID;
  /** Collection schemas: docs in collections without one can be read, not edited. */
  schemas: Record<string, Schema>;
  /** Persistence for the local model; memory-only when omitted. */
  store?: ClientStorage;
}

export interface OpError {
  opId: OpID;
  key: DocKey;
  status: "rejected" | "invalid";
  reason: string;
}

export class Subscription {
  failed: Error | null = null;
  ready = false;
  constructor(
    public id: string,
    public coll: string,
    public filter: string,
  ) {}
}

/** DocHandle edits and reads one document; handles are cheap. */
export class DocHandle {
  constructor(
    private client: Client,
    public key: DocKey,
  ) {}

  get coll(): string {
    return this.key.coll;
  }

  get id(): string {
    return this.key.doc;
  }

  /** values returns the view: confirmed state plus pending local ops. */
  values(): Record<string, unknown> {
    return this.client.withDoc(this.key, (ds) => this.client.viewOf(ds).values());
  }

  set(field: string, v: unknown): void {
    this.client.edit(this.key, (ed) => ed.set(field, v));
  }

  inc(field: string, delta: number): void {
    this.client.edit(this.key, (ed) => ed.inc(field, delta));
  }

  add(field: string, s: string): void {
    this.client.edit(this.key, (ed) => ed.add(field, s));
  }

  remove(field: string, s: string): void {
    this.client.edit(this.key, (ed) => ed.remove(field, s));
  }

  insert(field: string, index: number, v: unknown): void {
    this.client.edit(this.key, (ed) => ed.insert(field, index, v));
  }

  insertText(field: string, index: number, s: string): void {
    this.client.edit(this.key, (ed) => ed.insertText(field, index, s));
  }

  delete(field: string, index: number): void {
    this.client.edit(this.key, (ed) => ed.delete(field, index));
  }
}

interface DocState {
  key: DocKey;
  confirmed: Document;
  pending: Op[];
  sent: Set<string>;
  view: Document | null;
  retryAttempt: number;
  retryTimer: ReturnType<typeof setTimeout> | null;
}

/**
 * Client is one local replica. Create it with Client.create (async: it
 * loads persisted state), then run() to connect; edits work while offline.
 */
export class Client {
  actor: ActorID;

  private opts: ClientOptions;
  private store: ClientStorage;
  private schemas: Record<string, Schema>;

  private mu = false; // JS is single-threaded per turn; guards async sections
  private docs = new Map<string, DocState>();
  private subs = new Map<string, Subscription>();
  private subOrder: string[] = [];
  private nextSub = 0;
  private nextReq = 0;
  private ackDoc = new Map<number, DocKey>();
  private ws: WebSocket | null = null;
  private sendQueue: string[] = [];
  private stopped = false;
  private runStarted = false;
  private runGeneration = 0;
  private saveChain: Promise<void> = Promise.resolve();
  private waiters: Array<() => void> = [];
  private changeListeners: Array<(key: DocKey) => void> = [];
  private statusListeners: Array<(s: ClientStatus) => void> = [];
  private errorListeners: Array<(e: Error | OpError) => void> = [];
  private connState: "offline" | "connecting" | "connected" = "offline";
  private handshakeDeadline = 0;
  private alive = true;

  private constructor(opts: ClientOptions, actor: ActorID) {
    this.opts = opts;
    this.actor = actor;
    this.store = opts.store ?? new MemoryStorage();
    this.schemas = opts.schemas;
  }

  /** create builds a Client, restoring the local model from the store. */
  static async create(o: ClientOptions): Promise<Client> {
    if (!o.url) {
      throw new Error("client: url is required");
    }
    let actor = o.actor ?? "";
    const state = o.store ? await o.store.load() : null;
    if (!actor && state?.actor) {
      actor = state.actor;
    }
    if (!actor) {
      actor = newActorID();
    }
    validateActor(actor);
    const c = new Client(o, actor);
    for (const pd of state?.docs ?? []) {
      const key = { coll: pd.coll, doc: pd.doc };
      validateKey(key);
      const confirmed = pd.snapshot ? unmarshalSnapshot(pd.snapshot) : new Document(key);
      c.docs.set(docId(key), {
        key,
        confirmed,
        pending: pd.pending ?? [],
        sent: new Set(),
        view: null,
        retryAttempt: 0,
        retryTimer: null,
      });
    }
    return c;
  }

  /**
   * run starts the connect/reconnect loop; it returns immediately. It may
   * be called again after stop() — the demo's offline toggle does exactly
   * that.
   */
  run(): void {
    if (this.runStarted) {
      throw new Error("client: run is already running");
    }
    this.stopped = false;
    this.runStarted = true;
    void this.runLoop(++this.runGeneration);
  }

  /** stop tears the connection down and ends the run loop. */
  stop(): void {
    this.stopped = true;
    this.runGeneration++;
    this.runStarted = false;
    this.closeConn();
    for (const sub of this.subs.values()) {
      sub.ready = false;
    }
    this.notify();
  }

  /** subscribe adds a shape subscription; it is (re-)sent on every connect. */
  subscribe(coll: string, filter: string): Subscription {
    this.nextSub++;
    const sub = new Subscription(`s${this.nextSub}`, coll, filter);
    if (!this.schemas[coll]) {
      sub.failed = new Error(`client: no schema for collection "${coll}"`);
      this.emitError(sub.failed);
    }
    this.subs.set(sub.id, sub);
    this.subOrder.push(sub.id);
    if (!sub.failed && this.ws) {
      this.sendSub(sub);
    }
    this.notify();
    return sub;
  }

  /**
   * unsubscribe stops one subscription: the server stops delivering its
   * shape and the client drops it from status/waitSynced accounting.
   */
  unsubscribe(sub: Subscription): void {
    if (!this.subs.has(sub.id)) {
      return;
    }
    this.send({ t: "unsub", id: sub.id });
    this.subs.delete(sub.id);
    this.subOrder = this.subOrder.filter((id) => id !== sub.id);
    this.notify();
  }

  /**
   * listDocs returns the doc ids the client currently holds for a
   * collection — every doc that arrived via snapshot or op, sorted.
   */
  listDocs(coll: string): string[] {
    const out: string[] = [];
    for (const ds of this.docs.values()) {
      if (ds.key.coll === coll) {
        out.push(ds.key.doc);
      }
    }
    return out.sort();
  }

  /** pendingCount reports ops created locally but not yet confirmed. */
  get pendingCount(): number {
    return this.countPending();
  }

  /** running reports whether the connect/reconnect loop is active. */
  get running(): boolean {
    return this.runStarted && !this.stopped;
  }

  /** doc returns a handle for one document, creating it when unknown. */
  doc(coll: string, id: string): DocHandle {
    const key = { coll, doc: id };
    this.withDoc(key, () => {});
    return new DocHandle(this, key);
  }

  /** onChange registers a callback fired after any visible document change;
   * the returned function removes it. */
  onChange(cb: (key: DocKey) => void): () => void {
    this.changeListeners.push(cb);
    return () => {
      this.changeListeners = this.changeListeners.filter((x) => x !== cb);
    };
  }

  /** onStatus registers a callback fired on every status transition; the
   * returned function removes it. */
  onStatus(cb: (s: ClientStatus) => void): () => void {
    this.statusListeners.push(cb);
    return () => {
      this.statusListeners = this.statusListeners.filter((x) => x !== cb);
    };
  }

  /** onError registers a callback for rejected/invalid ops and fatal
   * errors; the returned function removes it. */
  onError(cb: (e: Error | OpError) => void): () => void {
    this.errorListeners.push(cb);
    return () => {
      this.errorListeners = this.errorListeners.filter((x) => x !== cb);
    };
  }

  /** status reflects connection and sync progress. */
  get status(): ClientStatus {
    if (this.connState !== "connected") {
      return this.connState === "connecting" ? "connecting" : "offline";
    }
    if (this.countPending() === 0 && this.allSubsReady()) {
      return "synced";
    }
    return "syncing";
  }

  /** waitSynced resolves when no pending ops remain and all subs are ready. */
  async waitSynced(timeoutMs = 30_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if (this.syncedNow()) {
        return;
      }
      if (Date.now() > deadline) {
        throw new Error("client: WaitSynced timed out");
      }
      await new Promise<void>((resolve) => {
        const w = () => {
          this.waiters = this.waiters.filter((x) => x !== w);
          resolve();
        };
        this.waiters.push(w);
        setTimeout(w, Math.max(0, Math.min(deadline - Date.now(), 50)));
      });
    }
  }

  // ---- internals -------------------------------------------------------

  private syncedNow(): boolean {
    for (const id of this.subOrder) {
      const sub = this.subs.get(id);
      if (!sub || (sub.failed === null && !sub.ready)) {
        // failed subs never ready; they do not block forever
        if (sub && sub.failed === null) {
          return false;
        }
        if (!sub) {
          return false;
        }
      }
    }
    return this.countPending() === 0;
  }

  private allSubsReady(): boolean {
    for (const id of this.subOrder) {
      const sub = this.subs.get(id);
      if (sub && sub.failed === null && !sub.ready) {
        return false;
      }
    }
    return true;
  }

  private countPending(): number {
    let n = 0;
    for (const ds of this.docs.values()) {
      n += ds.pending.length;
    }
    return n;
  }

  private async runLoop(generation: number): Promise<void> {
    let attempt = 0;
    while (!this.stopped && generation === this.runGeneration) {
      this.setConnState("connecting");
      const err = await this.connectAndServe();
      if (this.stopped || generation !== this.runGeneration) {
        break;
      }
      if (err) {
        attempt++;
      } else {
        attempt = 0;
      }
      const delay = backoffDelay(attempt);
      await sleep(delay);
    }
    if (generation === this.runGeneration) this.setConnState("offline");
  }

  private connectAndServe(): Promise<Error | null> {
    return new Promise((resolve) => {
      let settled = false;
      let ws: WebSocket | undefined;
      let welcomed = false;
      const done = (err: Error | null) => {
        clearTimeout(watchdog);
        if (!settled) {
          settled = true;
          if (ws) this.teardownConn(ws);
          resolve(err);
        }
      };
      const watchdog = setTimeout(() => {
        if (!welcomed && !this.stopped) {
          done(new Error("client: welcome timed out"));
        }
      }, handshakeTimeoutMs);
      try {
        ws = new WebSocket(this.opts.url);
      } catch (e) {
        done(e instanceof Error ? e : new Error(String(e)));
        return;
      }
      this.ws = ws;
      this.sendQueue = [];
      this.handshakeDeadline = Date.now() + handshakeTimeoutMs;
      const connection = ws;

      ws.onopen = () => {
        if (this.stopped || this.ws !== connection) return;
        this.sendRaw({ t: "hello", v: protocolVersion, token: this.opts.token, actor: this.actor });
      };
      ws.onmessage = (ev) => {
        if (this.stopped || this.ws !== connection) return;
        try {
          const frame = JSON.parse(String(ev.data)) as Record<string, unknown>;
          if (!welcomed) {
            if (frame.t !== "welcome") {
              done(
                new Error(
                  `client: expected welcome, got ${JSON.stringify(frame).slice(0, 200)}`,
                ),
              );
              return;
            }
            welcomed = true;
            this.setConnState("connected");
            this.resubscribeAll();
            this.pushAllPending();
            this.notify();
            return;
          }
          this.dispatch(frame);
            } catch (e) {
          done(e instanceof Error ? e : new Error(String(e)));
        }
      };
      ws.onerror = () => {
        done(new Error("client: connection error"));
      };
      ws.onclose = () => {
        this.teardownConn(connection);
        done(welcomed ? new Error("client: connection closed") : new Error("client: connection closed before welcome"));
      };

    });
  }

  private teardownConn(ws: WebSocket): void {
    if (this.ws === ws) {
      this.ws = null;
      this.sendQueue = [];
      this.ackDoc.clear();
      for (const ds of this.docs.values()) {
        ds.sent.clear();
      }
      for (const sub of this.subs.values()) {
        sub.ready = false;
      }
      this.setConnState("offline");
      this.notify();
    }
    try {
      ws.close();
    } catch {
      // already closed
    }
  }

  private closeConn(): void {
    const ws = this.ws;
    if (ws) {
      this.teardownConn(ws);
    }
    this.setConnState("offline");
  }

  private setConnState(s: "offline" | "connecting" | "connected"): void {
    if (this.connState !== s) {
      this.connState = s;
      for (const cb of this.statusListeners) {
        cb(this.status);
      }
    }
  }

  private dispatch(frame: Record<string, unknown>): void {
    switch (frame.t) {
      case "snap":
        this.handleSnapshot(frame);
        return;
      case "op":
        this.handleOpFrame(frame);
        return;
      case "evict":
        this.handleEvict(frame);
        return;
      case "ready":
        this.handleReady(frame);
        return;
      case "ack":
        this.handleAck(frame);
        return;
      case "err": {
        const sub = typeof frame.sub === "string" ? frame.sub : "";
        const msg = `client: server error ${frame.code}: ${frame.msg}`;
        if (sub) {
          // Subscription-scoped error: only that sub fails.
          const target = this.subs.get(sub);
          if (target) {
            target.failed = new Error(msg);
          }
          this.emitError(new Error(msg));
          this.notify();
          return;
        }
        this.emitError(new Error(msg));
        return; // non-fatal for the dispatch loop; the server closes anyway
      }
      case "welcome":
      case "pong":
        return;
      default:
        return;
    }
  }

  private handleSnapshot(frame: Record<string, unknown>): void {
    // The sub id names the subscription that delivered the snapshot; the
    // snapshot itself is keyed by its document.
    const snap = frame.snapshot as SnapshotObject;
    const doc = unmarshalSnapshot(snap);
    const key = doc.key;
    const ds = this.ensureDoc(key);
    ds.confirmed = doc;
    ds.view = null;
    this.persist();
    this.notify();
    this.fireChange(key);
  }

  private handleOpFrame(frame: Record<string, unknown>): void {
    const op = frame.op as Op;
    const ds = this.ensureDoc(op.key);
    let changed = false;
    try {
      ds.confirmed.apply(op, this.schemas[op.key.coll] ?? {});
      ds.view = null;
      changed = true;
    } catch (e) {
      if (!isDuplicate(e)) {
        this.emitError(e instanceof Error ? e : new Error(String(e)));
        return;
      }
    }
    const i = pendingIndexOf(ds, op.id);
    if (i >= 0) {
      ds.pending.splice(i, 1);
      ds.sent.delete(opIdString(op.id));
      changed = true;
      if (op.kind === "noop") {
        // The server stored our op as a noop: it was rejected.
        this.emitError({ opId: op.id, key: op.key, status: "rejected", reason: "rejected" });
      }
    }
    if (changed) {
      this.persist();
      this.notify();
      this.fireChange(op.key);
    }
  }

  private handleEvict(frame: Record<string, unknown>): void {
    const key = frame.key as DocKey;
    const ds = this.docs.get(docId(key));
    if (!ds) {
      return;
    }
    if (ds.pending.length > 0) {
      return; // pending ops win over eviction
    }
    this.docs.delete(docId(key));
    this.persist();
    this.notify();
    this.fireChange(key);
  }

  private handleReady(frame: Record<string, unknown>): void {
    const sub = this.subs.get(String(frame.sub ?? ""));
    if (sub) {
      sub.ready = true;
    }
    this.notify();
  }

  private handleAck(frame: Record<string, unknown>): void {
    const req = Number(frame.req ?? 0);
    const key = this.ackDoc.get(req);
    if (!key) {
      return;
    }
    this.ackDoc.delete(req);
    const ds = this.docs.get(docId(key));
    if (!ds) {
      return;
    }
    let touched = false;
    for (const r of (frame.results ?? []) as Array<{ id: OpID; status: string; reason?: string }>) {
      switch (r.status) {
        case "ok":
          // The G2 echo removes the op from pending.
          ds.sent.delete(opIdString(r.id));
          ds.retryAttempt = 0;
          break;
        case "dup": {
          // No echo follows a dup: resolve the op locally instead.
          ds.sent.delete(opIdString(r.id));
          const i = pendingIndexOf(ds, r.id);
          if (i >= 0) {
            const op = ds.pending[i]!;
            try {
              ds.confirmed.apply(op, this.schemas[key.coll] ?? {});
              ds.view = null;
              touched = true;
            } catch (e) {
              if (!isDuplicate(e)) {
                this.emitError(e instanceof Error ? e : new Error(String(e)));
              }
            }
            ds.pending.splice(i, 1);
          }
          ds.retryAttempt = 0;
          break;
        }
        case "rejected":
          // The noop echo carries the error report.
          ds.sent.delete(opIdString(r.id));
          break;
        case "retry": {
          // Keep the op pending; resend it and everything after it after
          // the backoff (PLAN.md: "resend from that op").
          for (const op of ds.pending) {
            if (op.id.seq >= r.id.seq && op.id.actor === r.id.actor) {
              ds.sent.delete(opIdString(op.id));
            }
          }
          this.scheduleResend(ds);
          break;
        }
        case "invalid": {
          // Drop that op and all later pending ops of the same doc.
          const i = pendingIndexOf(ds, r.id);
          if (i >= 0) {
            for (const op of ds.pending.slice(i)) {
              ds.sent.delete(opIdString(op.id));
            }
            ds.pending = ds.pending.slice(0, i);
            ds.view = null;
            touched = true;
          }
          this.emitError({ opId: r.id, key, status: "invalid", reason: r.reason ?? "" });
          break;
        }
      }
    }
    if (touched) {
      this.persist();
      this.notify();
      this.fireChange(key);
    }
  }

  private scheduleResend(ds: DocState): void {
    ds.retryAttempt++;
    const delay = Math.min(baseBackoffMs * 2 ** Math.min(ds.retryAttempt - 1, 4), 2000);
    if (ds.retryTimer) {
      clearTimeout(ds.retryTimer);
    }
    ds.retryTimer = setTimeout(() => {
      ds.retryTimer = null;
      this.withDoc(ds.key, () => {});
      const cur = this.docs.get(docId(ds.key));
      if (cur && this.ws) {
        this.pushPending(cur);
      }
    }, jitter(delay));
  }

  private resubscribeAll(): void {
    for (const id of this.subOrder) {
      const sub = this.subs.get(id);
      if (sub && !sub.failed) {
        this.sendSub(sub);
      }
    }
    for (const key of [...this.docs.keys()].sort()) {
      const ds = this.docs.get(key)!;
      if (ds.pending.length > 0) {
        this.pushPending(ds);
      }
    }
  }

  private sendSub(sub: Subscription): void {
    const known: Record<string, Record<string, number>> = {};
    for (const ds of this.docs.values()) {
      if (ds.key.coll === sub.coll) {
        known[ds.key.doc] = { ...ds.confirmed.vv };
      }
    }
    this.send({ t: "sub", id: sub.id, coll: sub.coll, filter: sub.filter, known });
  }

  private pushPending(ds: DocState): void {
    let chunk: Op[] = [];
    let bytes = 0;
    const flush = () => {
      if (chunk.length === 0) {
        return;
      }
      this.nextReq++;
      const req = this.nextReq;
      if (this.send({ t: "push", req, ops: chunk })) {
        this.ackDoc.set(req, ds.key);
        for (const op of chunk) {
          ds.sent.add(opIdString(op.id));
        }
      }
      chunk = [];
      bytes = 0;
    };
    for (const op of ds.pending) {
      if (ds.sent.has(opIdString(op.id))) {
        continue;
      }
      const size = perOpFrameOverhead + JSON.stringify(op.value ?? null).length + 24 * (op.observed?.length ?? 0);
      if (chunk.length > 0 && (chunk.length >= maxOpsPerPush || bytes + size > maxPushBytes)) {
        flush();
      }
      chunk.push(op);
      bytes += size;
    }
    flush();
  }

  private pushAllPending(): void {
    for (const key of [...this.docs.keys()].sort()) {
      const ds = this.docs.get(key)!;
      if (ds.pending.length > 0) {
        this.pushPending(ds);
      }
    }
  }

  /** send queues one frame; a full queue tears the connection down. */
  private send(frame: Record<string, unknown>): boolean {
    const ws = this.ws;
    if (!ws) {
      return false;
    }
    const data = JSON.stringify(frame);
    if (this.sendQueue.length >= sendBuffer) {
      this.closeConn();
      return false;
    }
    this.sendQueue.push(data);
    this.flushSendQueue();
    return true;
  }

  private sendRaw(data: Record<string, unknown>): void {
    const ws = this.ws;
    if (!ws) {
      return;
    }
    try {
      ws.send(JSON.stringify(data));
    } catch {
      this.closeConn();
    }
  }

  private flushSendQueue(): void {
    const ws = this.ws;
    if (!ws) {
      return;
    }
    while (this.sendQueue.length > 0) {
      const frame = this.sendQueue[0]!;
      try {
        ws.send(frame);
        this.sendQueue.shift();
      } catch {
        this.closeConn();
        return;
      }
    }
  }

  // ---- local model -----------------------------------------------------

  private docKeyOf(id: string): DocKey | null {
    const ds = this.docs.get(id);
    return ds ? ds.key : null;
  }

  private ensureDoc(key: DocKey): DocState {
    const id = docId(key);
    let ds = this.docs.get(id);
    if (!ds) {
      ds = {
        key: { ...key },
        confirmed: new Document(key),
        pending: [],
        sent: new Set(),
        view: null,
        retryAttempt: 0,
        retryTimer: null,
      };
      this.docs.set(id, ds);
    }
    return ds;
  }

  /** @internal */ withDoc<T>(key: DocKey, fn: (ds: DocState) => T): T {
    const ds = this.ensureDoc(key);
    return fn(ds);
  }

  /** @internal */ viewOf(ds: DocState): Document {
    if (ds.view) {
      return ds.view;
    }
    const sch = this.schemas[ds.key.coll] ?? {};
    const view = ds.confirmed.clone();
    const kept: Op[] = [];
    for (const op of ds.pending) {
      try {
        view.apply(op, sch);
        kept.push(op);
      } catch (e) {
        if (isDuplicate(e)) {
          // Already confirmed server-side; the echo will remove it too.
        } else {
          kept.push(op);
        }
      }
    }
    ds.pending = kept;
    ds.view = view;
    return view;
  }

  /** @internal */ edit(key: DocKey, fn: (ed: Editor) => Op[]): void {
    const ds = this.ensureDoc(key);
    const sch = this.schemas[key.coll];
    if (!sch) {
      throw new Error(`client: no schema for collection "${key.coll}"`);
    }
    const ed = new Editor(this.actor, this.viewOf(ds), sch);
    let ops: Op[];
    try {
      ops = fn(ed);
    } catch (e) {
      ds.view = null;
      throw e;
    }
    ds.pending.push(...ops);
    if (this.ws) {
      this.pushPending(ds);
    }
    this.persist();
    this.notify();
    this.fireChange(key);
  }

  private persist(): void {
    if (!this.store) {
      return;
    }
    const state: PersistedState = { actor: this.actor, docs: [] };
    for (const key of [...this.docs.keys()].sort()) {
      const ds = this.docs.get(key)!;
      const pd = {
        coll: ds.key.coll,
        doc: ds.key.doc,
        snapshot: marshalSnapshot(ds.confirmed) as SnapshotObject,
        pending: ds.pending.map((op) => structuredClone(op)),
      };
      state.docs.push(pd);
    }
    // Serialize saves so an older state can never land after a newer one.
    this.saveChain = this.saveChain
      .then(() => this.store.save(state))
      .catch((e) => {
        if (this.alive) {
          this.emitError(e instanceof Error ? e : new Error(String(e)));
        }
      });
  }

  private notify(): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const w of waiters) {
      w();
    }
    // Status may have moved without a connection event.
    for (const cb of this.statusListeners) cb(this.status);
  }

  private fireChange(key: DocKey): void {
    for (const cb of this.changeListeners) {
      try {
        cb(key);
      } catch (e) {
        this.emitError(e instanceof Error ? e : new Error(String(e)));
      }
    }
  }

  private emitError(e: Error | OpError): void {
    for (const cb of this.errorListeners) {
      try {
        cb(e);
      } catch {
        // listener errors must not break the client
      }
    }
  }
}

export type { DocKey, FieldState };

function docId(k: DocKey): string {
  return `${k.coll}/${k.doc}`;
}

function opIdString(id: OpID): string {
  return `${id.actor}:${id.seq}`;
}

function pendingIndexOf(ds: DocState, id: OpID): number {
  return ds.pending.findIndex((op) => op.id.actor === id.actor && op.id.seq === id.seq);
}

function isDuplicate(e: unknown): boolean {
  return e instanceof CrdtError && e.kind === "duplicate";
}

function backoffDelay(attempt: number): number {
  const d = Math.min(baseBackoffMs * 2 ** Math.min(Math.max(attempt, 0), 6), maxBackoffMs);
  return d / 2 + Math.random() * (d / 2);
}

function jitter(d: number): number {
  return d / 2 + Math.random() * (d / 2);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// canonicalStringify is re-exported for snapshot debugging parity with Go.
export { canonicalStringify };
export type { Schema, SnapshotObject, PersistedState };
export { marshalSnapshot, unmarshalSnapshot };
export { Document, Editor, newActorID, validateActor, validateKey };
export { MemoryStorage, IndexedDBStorage } from "./storage.js";
export type { ClientStorage } from "./storage.js";
export { isCrdtKind } from "./crdt/errors.js";
export { opIDLess, opLess } from "./crdt/op.js";

