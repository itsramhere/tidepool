// Snapshot serialization (PLAN.md §4.6), mirroring internal/crdt/snapshot.go.
// marshalSnapshot returns the JSON-shaped object (callers canonicalStringify
// or JSON.stringify it); unmarshalSnapshot rebuilds the document.
import { validateKey } from "./op.js";
import type { DocKey, OpID } from "./op.js";
import { Document, newFieldState, type FieldState } from "./document.js";
import { Register } from "./register.js";
import { Counter } from "./counter.js";
import { ORSet } from "./orset.js";
import { RGA } from "./rga.js";
import type { FieldType } from "./schema.js";

interface SnapRegister {
  type: FieldType;
  lamport: number;
  actor: string;
  value: unknown;
}

interface SnapCounter {
  type: FieldType;
  n: number;
}

interface SnapSet {
  type: FieldType;
  elems: Record<string, OpID[]>;
}

interface SnapItem {
  id: OpID;
  lamport: number;
  value: unknown;
  deleted: boolean;
}

interface SnapRGA {
  type: FieldType;
  items: SnapItem[];
}

export interface SnapshotObject {
  key: DocKey;
  vv: Record<string, number>;
  clock: Record<string, number>;
  maxLamport: number;
  fields: Record<string, SnapRegister | SnapCounter | SnapSet | SnapRGA>;
}

/** marshalSnapshot serializes the full state, tombstones included. */
export function marshalSnapshot(doc: Document): SnapshotObject {
  validateKey(doc.key);
  const fields: SnapshotObject["fields"] = {};
  for (const [name, st] of Object.entries(doc.fields)) {
    switch (st.type) {
      case "register":
        if (!st.reg || !st.reg.set) {
          throw new Error(`marshal snapshot: register field "${name}" has no value`);
        }
        fields[name] = {
          type: st.type,
          lamport: st.reg.lamport,
          actor: st.reg.actor,
          value: st.reg.value,
        };
        break;
      case "counter":
        if (!st.ctr) {
          throw new Error(`marshal snapshot: counter field "${name}" has no state`);
        }
        fields[name] = { type: st.type, n: st.ctr.n };
        break;
      case "set": {
        if (!st.set) {
          throw new Error(`marshal snapshot: set field "${name}" has no state`);
        }
        const elems: Record<string, OpID[]> = {};
        for (const elem of Object.keys(st.set.elems)) {
          elems[elem] = st.set.tags(elem) ?? [];
        }
        fields[name] = { type: st.type, elems };
        break;
      }
      case "list":
      case "text": {
        if (!st.seq) {
          throw new Error(`marshal snapshot: sequence field "${name}" has no state`);
        }
        fields[name] = {
          type: st.type,
          items: st.seq.items.map((item) => ({
            id: { ...item.id },
            lamport: item.lamport,
            value: item.value,
            deleted: item.deleted,
          })),
        };
        break;
      }
    }
  }
  return {
    key: { ...doc.key },
    vv: { ...doc.vv },
    clock: { ...doc.actorClock },
    maxLamport: doc.maxLamport,
    fields,
  };
}

/** unmarshalSnapshot rebuilds a document from marshalSnapshot output. */
export function unmarshalSnapshot(data: SnapshotObject): Document {
  validateKey(data.key);
  const doc = new Document(data.key);
  if (data.vv) {
    doc.vv = { ...data.vv };
  }
  if (data.clock) {
    doc.actorClock = { ...data.clock };
  }
  doc.maxLamport = data.maxLamport ?? 0;
  for (const [name, raw] of Object.entries(data.fields ?? {})) {
    const st: FieldState = newFieldState(raw.type);
    switch (raw.type) {
      case "register": {
        const sr = raw as SnapRegister;
        if (sr.value === undefined) {
          throw new Error(`unmarshal snapshot: register field "${name}" has no value`);
        }
        st.reg = new Register();
        st.reg.lamport = sr.lamport;
        st.reg.actor = sr.actor;
        st.reg.value = sr.value;
        st.reg.set = true;
        break;
      }
      case "counter": {
        const sc = raw as SnapCounter;
        st.ctr = new Counter();
        st.ctr.n = sc.n;
        break;
      }
      case "set": {
        const ss = raw as SnapSet;
        st.set = new ORSet();
        for (const [elem, tags] of Object.entries(ss.elems ?? {})) {
          const t: Record<string, true> = {};
          for (const id of tags) {
            t[`${id.actor}:${id.seq}`] = true;
          }
          st.set.elems[elem] = t;
        }
        break;
      }
      case "list":
      case "text": {
        const sr = raw as SnapRGA;
        st.seq = new RGA(raw.type === "text");
        st.seq.items = (sr.items ?? []).map((item) => ({ ...item }));
        break;
      }
    }
    doc.fields[name] = st;
  }
  return doc;
}
