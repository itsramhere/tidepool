// Op model (PLAN.md §4.3), mirroring internal/crdt/op.go. Lamports and seqs
// are `number`: JSON-safe integers up to 2^53, far beyond any realistic
// document lifetime.
import { errInvalid } from "./errors.js";

export type ActorID = string;

export interface OpID {
  actor: ActorID;
  seq: number;
}

export interface DocKey {
  coll: string;
  doc: string;
}

export type OpKind = "set" | "inc" | "add" | "rem" | "ins" | "del" | "noop";

export interface Op {
  id: OpID;
  key: DocKey;
  lamport: number;
  field?: string;
  kind: OpKind;
  /** JSON scalar (may be null); `undefined` means the op carries no value. */
  value?: unknown;
  delta?: number;
  ref?: OpID | null;
  observed?: OpID[];
}

/** opIDLess orders OpIDs by (actor byte-wise, seq). */
export function opIDLess(a: OpID, b: OpID): boolean {
  if (a.actor !== b.actor) {
    return a.actor < b.actor;
  }
  return a.seq < b.seq;
}

/** opLess is the total order over ops: (lamport, then actor byte-wise). */
export function opLess(a: Op, b: Op): boolean {
  if (a.lamport !== b.lamport) {
    return a.lamport < b.lamport;
  }
  return a.id.actor < b.id.actor;
}

const actorRe = /^[0-9a-f]{16}$/;
const collRe = /^[a-z][a-z0-9_]{0,63}$/;
const docRe = /^[A-Za-z0-9_-]{1,128}$/;
const fieldRe = /^[a-z][a-zA-Z0-9_]{0,63}$/;

export function validateActor(a: ActorID): void {
  if (!actorRe.test(a)) {
    throw errInvalid(`actor "${a}" does not match ${actorRe.source}`);
  }
}

export function validateCollection(coll: string): void {
  if (!collRe.test(coll)) {
    throw errInvalid(`collection "${coll}" does not match ${collRe.source}`);
  }
}

export function validateKey(k: DocKey): void {
  if (!collRe.test(k.coll)) {
    throw errInvalid(`collection "${k.coll}" does not match ${collRe.source}`);
  }
  if (!docRe.test(k.doc)) {
    throw errInvalid(`doc "${k.doc}" does not match ${docRe.source}`);
  }
}

export function validateField(f: string): void {
  if (!fieldRe.test(f)) {
    throw errInvalid(`field "${f}" does not match ${fieldRe.source}`);
  }
}

/** newActorID returns 8 crypto-random bytes as 16 lowercase hex chars. */
export function newActorID(): ActorID {
  const b = new Uint8Array(8);
  crypto.getRandomValues(b);
  return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
}
