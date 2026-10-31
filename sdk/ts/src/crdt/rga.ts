// RGA sequence CRDT behind list and text fields (PLAN.md §4.5), mirroring
// internal/crdt/rga.go. Items are never physically removed.
import type { Op, OpID } from "./op";
import { errInvalid, errMissingDeps } from "./errors";
import { isSingleCodePoint, parseScalar } from "./value";
import type { FieldType } from "./schema";

export interface RGAItem {
  id: OpID;
  lamport: number;
  value: unknown;
  deleted: boolean;
}

export class RGA {
  items: RGAItem[] = [];
  text = false;

  constructor(text = false) {
    this.text = text;
  }

  /**
   * check validates an ins or del op and writes nothing. Ref semantics:
   * nil ref inserts at head; otherwise the ref must be applied (per vv) and
   * present, and an ins must carry a strictly higher lamport than its ref.
   */
  check(op: Op, vv: Record<string, number>): void {
    switch (op.kind) {
      case "ins":
        if (this.text) {
          const v = parseScalar(op.value);
          if (typeof v !== "string" || !isSingleCodePoint(v)) {
            throw errInvalid("text ins value must be a string with exactly one code point");
          }
        } else {
          parseScalar(op.value);
        }
        break;
      case "del":
        if (op.value !== undefined) {
          throw errInvalid("del must not carry a value");
        }
        if (op.ref == null) {
          throw errInvalid("del requires a ref");
        }
        break;
      default:
        throw errInvalid(`kind "${op.kind}" is not allowed on a sequence`);
    }
    if (op.ref == null) {
      return;
    }
    if (op.ref.seq > (vv[op.ref.actor] ?? 0)) {
      throw errMissingDeps(`ref ${refString(op.ref)} not yet applied to the document`);
    }
    const idx = this.indexOf(op.ref);
    if (idx < 0) {
      throw errMissingDeps(`ref ${refString(op.ref)} not found in this sequence`);
    }
    if (op.kind === "ins" && op.lamport <= this.items[idx]!.lamport) {
      throw errInvalid(
        `ins lamport ${op.lamport} must be greater than ref lamport ${this.items[idx]!.lamport}`,
      );
    }
  }

  /** apply applies a checked ins or del op per PLAN.md §4.5's skip loop. */
  apply(op: Op): void {
    if (op.kind === "ins") {
      let i = 0;
      if (op.ref != null) {
        i = this.indexOf(op.ref) + 1;
      }
      while (i < this.items.length && rgaItemGreater(this.items[i]!, op)) {
        i++;
      }
      this.items.splice(i, 0, { id: op.id, lamport: op.lamport, value: op.value, deleted: false });
      return;
    }
    if (op.kind === "del" && op.ref != null) {
      const idx = this.indexOf(op.ref);
      if (idx >= 0) {
        this.items[idx]!.deleted = true;
      }
    }
  }

  /** indexOf returns the document-order position of id, or -1. */
  indexOf(id: OpID): number {
    return this.items.findIndex((item) => item.id.actor === id.actor && item.id.seq === id.seq);
  }

  /** visibleIDs returns the non-deleted item IDs in document order. */
  visibleIDs(): OpID[] {
    return this.items.filter((item) => !item.deleted).map((item) => item.id);
  }

  /** materialize: []any of scalars for a list, one string for text. */
  materialize(): unknown {
    if (this.text) {
      let out = "";
      for (const item of this.items) {
        if (item.deleted) {
          continue;
        }
        const v = safeScalar(item.value);
        if (typeof v === "string") {
          out += v;
        }
      }
      return out;
    }
    const out: unknown[] = [];
    for (const item of this.items) {
      if (item.deleted) {
        continue;
      }
      const v = safeScalar(item.value);
      if (v !== undefined) {
        out.push(v);
      }
    }
    return out;
  }
}

function rgaItemGreater(item: RGAItem, op: Op): boolean {
  if (item.lamport !== op.lamport) {
    return item.lamport > op.lamport;
  }
  return item.id.actor > op.id.actor;
}

function safeScalar(v: unknown): unknown {
  try {
    return parseScalar(v);
  } catch {
    return undefined; // values are validated by check; unreachable in practice
  }
}

function refString(id: OpID): string {
  return `{${id.actor} ${id.seq}}`;
}

export type { FieldType };
