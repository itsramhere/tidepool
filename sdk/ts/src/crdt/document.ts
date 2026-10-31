// Document: one replicated document (PLAN.md §4.4), mirroring
// internal/crdt/document.go — same checks, same order, mutating nothing
// when any check fails.
import type { DocKey, Op, OpID } from "./op.js";
import { errDuplicate, errInvalid, errOutOfOrder, errMissingDeps, CrdtError } from "./errors.js";
import { validateActor, validateField, validateKey } from "./op.js";
import { allowedKind, type FieldType, type Schema } from "./schema.js";
import { Counter } from "./counter.js";
import { ORSet } from "./orset.js";
import { Register } from "./register.js";
import { RGA } from "./rga.js";

/** FieldState is one field's CRDT state: type plus exactly one state object. */
export interface FieldState {
  type: FieldType;
  reg?: Register;
  ctr?: Counter;
  set?: ORSet;
  seq?: RGA;
}

export class Document {
  /** vv maps each actor to its last applied seq (contiguous per actor). */
  vv: Record<string, number> = {};
  /** actorClock maps each actor to its latest applied op's lamport. */
  actorClock: Record<string, number> = {};
  maxLamport = 0;
  fields: Record<string, FieldState> = {};

  constructor(public key: DocKey) {}

  /**
   * apply validates and applies op following PLAN.md §4.4, in that exact
   * order, mutating nothing when any check fails. Throws CrdtError:
   * duplicate is reported for already-applied ops (callers usually treat it
   * as success); outOfOrder/missingDeps may become applicable later;
   * invalid never will.
   */
  apply(op: Op, schema: Schema): void {
    if (op.key.coll !== this.key.coll || op.key.doc !== this.key.doc) {
      throw errInvalid(`op key ${keyString(op.key)} does not match document ${keyString(this.key)}`);
    }
    validateKey(op.key);
    validateActor(op.id.actor);
    validateField(op.field ?? "");

    const seen = this.vv[op.id.actor] ?? 0;
    if (op.id.seq <= seen) {
      throw errDuplicate(`seq ${op.id.seq} <= vv ${seen} for actor ${op.id.actor}`);
    }
    if (op.id.seq !== seen + 1) {
      throw errOutOfOrder(`seq ${op.id.seq}, want ${seen + 1} for actor ${op.id.actor}`);
    }

    // A rejected op stored as a noop only advances the VV: seq contiguity
    // survives, every replica converges on "this op did nothing".
    if (op.kind === "noop") {
      this.vv[op.id.actor] = op.id.seq;
      return;
    }

    if (op.lamport <= (this.actorClock[op.id.actor] ?? 0)) {
      throw errInvalid(
        `lamport ${op.lamport} <= actor clock ${this.actorClock[op.id.actor] ?? 0} for actor ${op.id.actor}`,
      );
    }

    const ft = schema[op.field ?? ""];
    if (ft === undefined) {
      throw errInvalid(`field "${op.field}" is not in the schema`);
    }
    if (!allowedKind(ft, op.kind)) {
      throw errInvalid(`kind "${op.kind}" is not allowed on a ${ft} field`);
    }

    // Validate against a throwaway state when the field does not exist
    // yet, so a failed check leaves the document untouched.
    const existing = this.fields[op.field ?? ""];
    const st = existing ?? newFieldState(ft);
    stTypeCheck(st, op, this.vv);
    if (op.ref != null && op.ref.seq > (this.vv[op.ref.actor] ?? 0)) {
      throw errMissingDeps(`ref ${op.ref.actor}/${op.ref.seq} not yet applied`);
    }
    for (const id of op.observed ?? []) {
      if (id.seq > (this.vv[id.actor] ?? 0)) {
        throw errMissingDeps(`observed ${id.actor}/${id.seq} not yet applied`);
      }
    }

    if (!existing) {
      this.fields[op.field ?? ""] = st;
    }
    stApply(st, op);
    this.vv[op.id.actor] = op.id.seq;
    this.actorClock[op.id.actor] = op.lamport;
    if (op.lamport > this.maxLamport) {
      this.maxLamport = op.lamport;
    }
  }

  /** clone returns a deep copy; mutating the clone never corrupts the original. */
  clone(): Document {
    const c = new Document(this.key);
    c.vv = { ...this.vv };
    c.actorClock = { ...this.actorClock };
    c.maxLamport = this.maxLamport;
    for (const [name, st] of Object.entries(this.fields)) {
      c.fields[name] = cloneFieldState(st);
    }
    return c;
  }

  /** values returns the materialized map; fields without ops are absent. */
  values(): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [name, st] of Object.entries(this.fields)) {
      out[name] = stMaterialize(st);
    }
    return out;
  }

  /** covers reports whether the op with the given ID has been applied. */
  covers(id: OpID): boolean {
    return id.seq <= (this.vv[id.actor] ?? 0);
  }
}

export function newFieldState(ft: FieldType): FieldState {
  const st: FieldState = { type: ft };
  switch (ft) {
    case "register":
      st.reg = new Register();
      break;
    case "counter":
      st.ctr = new Counter();
      break;
    case "set":
      st.set = new ORSet();
      break;
    case "list":
      st.seq = new RGA(false);
      break;
    case "text":
      st.seq = new RGA(true);
      break;
  }
  return st;
}

function stTypeCheck(st: FieldState, op: Op, vv: Record<string, number>): void {
  switch (st.type) {
    case "register":
      st.reg!.check(op);
      return;
    case "counter":
      st.ctr!.check(op);
      return;
    case "set":
      st.set!.check(op);
      return;
    case "list":
    case "text":
      st.seq!.check(op, vv);
      return;
  }
}

function stApply(st: FieldState, op: Op): void {
  switch (st.type) {
    case "register":
      st.reg!.apply(op);
      return;
    case "counter":
      st.ctr!.apply(op);
      return;
    case "set":
      st.set!.apply(op);
      return;
    case "list":
    case "text":
      st.seq!.apply(op);
      return;
  }
}

function stMaterialize(st: FieldState): unknown {
  switch (st.type) {
    case "register":
      return st.reg!.materialize();
    case "counter":
      return st.ctr!.materialize();
    case "set":
      return st.set!.materialize();
    case "list":
    case "text":
      return st.seq!.materialize();
  }
}

function cloneFieldState(st: FieldState): FieldState {
  const c: FieldState = { type: st.type };
  if (st.reg) {
    c.reg = new Register();
    c.reg.lamport = st.reg.lamport;
    c.reg.actor = st.reg.actor;
    c.reg.value = st.reg.value;
    c.reg.set = st.reg.set;
  }
  if (st.ctr) {
    c.ctr = new Counter();
    c.ctr.n = st.ctr.n;
  }
  if (st.set) {
    c.set = new ORSet();
    for (const [elem, tags] of Object.entries(st.set.elems)) {
      c.set.elems[elem] = { ...tags };
    }
  }
  if (st.seq) {
    c.seq = new RGA(st.seq.text);
    c.seq.items = st.seq.items.map((item) => ({ ...item }));
  }
  return c;
}

function keyString(k: DocKey): string {
  return `{${k.coll} ${k.doc}}`;
}

export { CrdtError };
