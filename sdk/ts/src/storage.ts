// Client storage adapters: where the SDK persists the local model (actor
// identity, confirmed snapshots, pending ops) across restarts. Mirrors the
// Go SDK's LocalStore; persistence is asynchronous here because
// IndexedDBStorage is.
import type { ActorID, Op } from "./crdt/op";
import type { SnapshotObject } from "./crdt/snapshot";

export interface PersistedDoc {
  coll: string;
  doc: string;
  /** §4.6 snapshot of the confirmed state, when the doc has any. */
  snapshot?: SnapshotObject;
  /** Local ops the server has not confirmed yet, in creation order. */
  pending: Op[];
}

export interface PersistedState {
  actor?: ActorID;
  docs: PersistedDoc[];
}

export interface ClientStorage {
  load(): Promise<PersistedState | null>;
  save(state: PersistedState): Promise<void>;
}

/** MemoryStorage keeps the state in process memory. */
export class MemoryStorage implements ClientStorage {
  private state: PersistedState | null = null;

  async load(): Promise<PersistedState | null> {
    return this.state ? structuredClone(this.state) : null;
  }

  async save(state: PersistedState): Promise<void> {
    this.state = structuredClone(state);
  }
}

/**
 * IndexedDBStorage persists the state in a browser's IndexedDB (single
 * record). Built on the tiny `idb` promise wrapper.
 */
export class IndexedDBStorage implements ClientStorage {
  constructor(private dbName = "tidepool") {}

  async load(): Promise<PersistedState | null> {
    const { openDB } = await import("idb");
    const db = await openDB(this.dbName, 1, {
      upgrade(db) {
        if (!db.objectStoreNames.contains("state")) {
          db.createObjectStore("state");
        }
      },
    });
    try {
      const state = (await db.get("state", "state")) as PersistedState | undefined;
      return state ?? null;
    } finally {
      db.close();
    }
  }

  async save(state: PersistedState): Promise<void> {
    const { openDB } = await import("idb");
    const db = await openDB(this.dbName, 1, {
      upgrade(db) {
        if (!db.objectStoreNames.contains("state")) {
          db.createObjectStore("state");
        }
      },
    });
    try {
      await db.put("state", state, "state");
    } finally {
      db.close();
    }
  }
}
