// Editor creates valid local ops for one actor against one document,
// mirroring internal/crdt/editor.go: it stamps seq from the document's VV
// and lamport from its maxLamport, applies each op immediately, and returns
// the ops it created so callers can queue and push them.
import type { Op, OpID } from "./op.js";
import { errInvalid, errNotFound } from "./errors.js";
import { isSingleCodePoint } from "./value.js";
import type { Schema } from "./schema.js";
import type { Document } from "./document.js";
import type { ORSet } from "./orset.js";
import type { RGA } from "./rga.js";

export class Editor {
  constructor(
    public actor: string,
    public doc: Document,
    public schema: Schema,
  ) {}

  private nextOp(field: string, kind: Op["kind"]): Op {
    return {
      id: { actor: this.actor, seq: (this.doc.vv[this.actor] ?? 0) + 1 },
      key: this.doc.key,
      lamport: this.doc.maxLamport + 1,
      field,
      kind,
    } as Op;
  }

  /** set builds and applies a register set op. */
  set(field: string, v: unknown): Op[] {
    const op = this.nextOp(field, "set");
    op.value = v;
    this.doc.apply(op, this.schema);
    return [op];
  }

  /** inc builds and applies a counter inc op. */
  inc(field: string, delta: number): Op[] {
    const op = this.nextOp(field, "inc");
    op.delta = delta;
    this.doc.apply(op, this.schema);
    return [op];
  }

  /** add builds and applies a set add op. */
  add(field: string, s: string): Op[] {
    const op = this.nextOp(field, "add");
    op.value = s;
    this.doc.apply(op, this.schema);
    return [op];
  }

  /** remove builds and applies a set rem op observing every live tag of s. */
  remove(field: string, s: string): Op[] {
    const ft = this.schema[field];
    if (ft === undefined || ft !== "set") {
      throw errInvalid(`field "${field}" is not a set`);
    }
    const st = this.doc.fields[field];
    const tags = st?.set ? orsetTags(st.set, s) : null;
    if (tags === null || tags.length === 0) {
      throw errNotFound(`element "${s}" not in set "${field}"`);
    }
    const op = this.nextOp(field, "rem");
    op.value = s;
    op.observed = tags;
    this.doc.apply(op, this.schema);
    return [op];
  }

  /**
   * insert builds and applies a list ins op. Index 0 inserts at the head;
   * otherwise the element lands after the visible element at index-1.
   */
  insert(field: string, index: number, v: unknown): Op[] {
    const g = this.seqState(field, true, false);
    const ref = seqIndex(g, index, true);
    const op = this.nextOp(field, "ins");
    op.ref = ref;
    op.value = v;
    this.doc.apply(op, this.schema);
    return [op];
  }

  /**
   * insertText builds and applies one ins op per code point of s, each
   * referencing the previous new element, so the run lands as a unit at
   * index. Code points come from Array.from, never UTF-16 positions.
   */
  insertText(field: string, index: number, s: string): Op[] {
    const g = this.seqState(field, false, true);
    for (const ch of Array.from(s)) {
      if (!isSingleCodePoint(ch)) {
        throw errInvalid("text ins value must be a string with exactly one code point");
      }
    }
    const ref = seqIndex(g, index, true);
    const ops: Op[] = [];
    let prev: OpID | null = ref;
    for (const ch of Array.from(s)) {
      const op = this.nextOp(field, "ins");
      op.ref = prev;
      op.value = ch;
      this.doc.apply(op, this.schema);
      ops.push(op);
      prev = op.id;
    }
    return ops;
  }

  /** delete builds and applies a del op for the visible element at index. */
  delete(field: string, index: number): Op[] {
    const g = this.seqState(field, false, false);
    const ref = seqIndex(g, index, false);
    const op = this.nextOp(field, "del");
    op.ref = ref;
    this.doc.apply(op, this.schema);
    return [op];
  }

  private seqState(field: string, requireList: boolean, requireText: boolean): RGA | null {
    const ft = this.schema[field];
    if (ft === undefined || (ft !== "list" && ft !== "text")) {
      throw errInvalid(`field "${field}" is not a list or text field`);
    }
    if (requireList && ft !== "list") {
      throw errInvalid(`field "${field}" is a ${ft}, not a list`);
    }
    if (requireText && ft !== "text") {
      throw errInvalid(`field "${field}" is ${ft}, not text`);
    }
    const st = this.doc.fields[field];
    return st?.seq ?? null;
  }
}

function orsetTags(s: ORSet, elem: string): OpID[] | null {
  return s.tags(elem);
}

/**
 * seqIndex resolves index into a ref: for inserts, null for the head or the
 * visible element at index-1; for deletes, the visible element at index.
 * Throws notFound past the end and invalid for negative indexes.
 */
function seqIndex(g: RGA | null, index: number, forInsert: boolean): OpID | null {
  if (index < 0) {
    throw errInvalid(`negative index ${index}`);
  }
  const vis = g ? g.visibleIDs() : [];
  if (forInsert) {
    if (index === 0) {
      return null; // insert at the head
    }
    if (index > vis.length) {
      throw errNotFound(`index ${index} past end (${vis.length} elements)`);
    }
    return vis[index - 1]!;
  }
  if (index >= vis.length) {
    throw errNotFound(`index ${index} past end (${vis.length} elements)`);
  }
  return vis[index]!;
}
