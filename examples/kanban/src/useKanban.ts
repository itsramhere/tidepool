// useKanban manages the demo's client lifecycle: one client per user (with
// a per-user store), the board shape subscription, and the offline toggle.
import { useEffect, useRef, useState } from "react";
import { Client, MemoryStorage, type ClientStatus, type Subscription } from "@tidepool/client";
import { devToken } from "./api";

interface ViteEnv {
  readonly VITE_SERVER_URL?: string;
}

export const syncUrl = (import.meta as unknown as { env?: ViteEnv }).env?.VITE_SERVER_URL ?? "ws://127.0.0.1:7100/v1/sync";

export const cardSchema = {
  title: "register",
  status: "register",
  board: "register",
  votes: "counter",
  labels: "set",
  checklist: "list",
  description: "text",
} as const;

// Per-user stores survive user switches within a session; a fresh visitor
// starts empty and reconciles from the server.
const stores = new Map<string, MemoryStorage>();
const clients = new Map<string, Client>();
function storeFor(uid: string): MemoryStorage {
  let s = stores.get(uid);
  if (!s) {
    s = new MemoryStorage();
    stores.set(uid, s);
  }
  return s;
}

export interface Kanban {
  client: Client | null;
  status: ClientStatus;
  pending: number;
  error: string;
}

export function useKanban(uid: string, board: string, offline: boolean): Kanban {
  const [client, setClient] = useState<Client | null>(null);
  const [status, setStatus] = useState<ClientStatus>("offline");
  const [pending, setPending] = useState(0);
  const [error, setError] = useState("");
  const subRef = useRef<Subscription | null>(null);

  // (Re)create the client when the user changes.
  useEffect(() => {
    let cancelled = false;
    let created: Client | null = null;
    let detach: Array<() => void> = [];
    setClient(null);
    setError("");
    (async () => {
      let c = clients.get(uid);
      if (!c) {
        const token = await devToken(syncUrl, uid);
        if (cancelled) return;
        c = await Client.create({
          url: syncUrl, token, schemas: { cards: cardSchema }, store: storeFor(uid),
        });
        if (cancelled) return;
        clients.set(uid, c);
      }
      created = c;
      const refresh = () => { setPending(c.pendingCount); setStatus(c.status); };
      detach = [c.onChange(refresh), c.onStatus(refresh), c.onError((e) => {
        setError(e instanceof Error ? e.message : `${e.status}: ${e.reason}`);
      })];
      refresh();
      setClient(c);
    })().catch((e: unknown) => {
      if (!cancelled) setError(e instanceof Error ? e.message : String(e));
    });
    return () => {
      cancelled = true;
      detach.forEach((off) => off());
      created?.stop();
    };
  }, [uid]); // eslint-disable-line react-hooks/exhaustive-deps

  // The board subscription is a shape: switching boards unsubscribes the
  // old filter and subscribes the new one — move-in/out on the wire.
  useEffect(() => {
    if (!client) {
      return;
    }
    if (subRef.current) {
      client.unsubscribe(subRef.current);
    }
    if (board) {
      subRef.current = client.subscribe("cards", `doc.board == ${JSON.stringify(board)}`);
    }
    return () => {
      if (subRef.current) {
        client.unsubscribe(subRef.current);
        subRef.current = null;
      }
    };
  }, [client, board]);

  // The offline toggle stops/starts the run loop.
  useEffect(() => {
    if (!client) {
      return;
    }
    if (offline) {
      client.stop();
    } else if (!client.running) {
      client.run();
    }
  }, [client, offline]);

  return { client, status, pending, error };
}
