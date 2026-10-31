// Add-wins observed-remove set (PLAN.md §4.5), mirroring internal/crdt/orset.go.
import type { Op, OpID } from "./op";
import { errInvalid } from "./errors";
import { parseScalar } from "./value";

export class ORSet {
  /** element → live add tags. */
  elems: Record<string, Record<string, true>> = {};

  /** check validates an add or rem op; it writes nothing. */
  check(op: Op): void {
    if (op.kind !== "add" && op.kind !== "rem") {
      throw errInvalid(`kind "${op.kind}" is not allowed on a set`);
    }
    setElem(op.value);
  }

  /** apply applies a checked add or rem op; unknown removes are harmless. */
  apply(op: Op): void {
    const elem = setElem(op.value);
    if (op.kind === "add") {
      const tags = this.elems[elem] ?? {};
      tags[tagKey(op.id)] = true;
      this.elems[elem] = tags;
      return;
    }
    // rem: delete exactly the observed tags; drop empty elements.
    const tags = this.elems[elem];
    if (!tags) {
      return;
    }
    for (const id of op.observed ?? []) {
      delete tags[tagKey(id)];
    }
    if (Object.keys(tags).length === 0) {
      delete this.elems[elem];
    }
  }

  /** materialize returns the live elements sorted ascending. */
  materialize(): string[] {
    return Object.keys(this.elems).sort();
  }

  /** tags returns elem's live tags sorted by (actor, seq), or null. */
  tags(elem: string): OpID[] | null {
    const tags = this.elems[elem];
    if (!tags) {
      return null;
    }
    const out = Object.keys(tags).map((k) => untagKey(k));
    out.sort((a, b) => (a.actor !== b.actor ? (a.actor < b.actor ? -1 : 1) : a.seq - b.seq));
    return out;
  }
}

// Tag keys: Go keys a map by the OpID struct; JS keys a Record by string, so
// tags are keyed "actor:seq" — actors are fixed 16-char hex, unambiguous.
function tagKey(id: OpID): string {
  return `${id.actor}:${id.seq}`;
}

function untagKey(k: string): OpID {
  const i = k.indexOf(":");
  return { actor: k.slice(0, i), seq: Number(k.slice(i + 1)) };
}

function setElem(raw: unknown): string {
  const v = parseScalar(raw);
  if (typeof v !== "string") {
    throw errInvalid(`set element must be a JSON string, got ${typeof v}`);
  }
  return v;
}
