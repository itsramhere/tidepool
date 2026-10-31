// Tidepool Kanban demo: columns from the status register, cards over every
// field type, a header with the dev-token user switcher / status pill /
// offline toggle / pending counter, and a board shape filter (doc.board).
import { useEffect, useMemo, useState } from "react";
import { Card } from "./Card";
import { useKanban } from "./useKanban";

const USERS = ["alice", "bob", "carol"];
const STATUSES = ["todo", "doing", "done"];
const BOARDS = ["b1", "b2"];

export default function App() {
  const [uid, setUid] = useState(USERS[0]!);
  const [board, setBoard] = useState(BOARDS[0]!);
  const [offline, setOffline] = useState(false);
  const { client, status, pending, error } = useKanban(uid, board, offline);

  // Re-render on every visible doc change: handle.values() is read fresh
  // per render, which is the SDK's reactive contract.
  const [tick, bump] = useState(0);
  useEffect(() => {
    if (!client) {
      return;
    }
    const offChange = client.onChange(() => bump((n) => n + 1));
    const offStatus = client.onStatus(() => bump((n) => n + 1));
    return () => {
      offChange();
      offStatus();
    };
  }, [client]);

  const cards = useMemo(() => {
    if (!client) {
      return [];
    }
    void tick;
    return client
      .listDocs("cards")
      .map((id) => ({
        id,
        handle: client.doc("cards", id),
        values: client.doc("cards", id).values() as CardValues,
      }))
      .filter((c) => c.values.board === board);
  }, [client, board, tick]);

  const addCard = (status: string) => {
    if (!client) {
      return;
    }
    const id = `card-${crypto.randomUUID()}`;
    const card = client.doc("cards", id);
    card.set("board", board);
    card.set("status", status);
    card.set("title", "New card");
  };

  // The SDK has no document deletion (M11 scope); the demo archives a card
  // by moving it to a board id no board subscribes to — a shape move-out.
  const archiveCard = (id: string) => {
    client?.doc("cards", id).set("board", "archived");
  };

  return (
    <>
      <header className="header">
        <h1>Tidepool Kanban</h1>
        <label className="board-picker">
          user{" "}
          <select aria-label="User" value={uid} onChange={(e) => setUid(e.target.value)}>
            {USERS.map((u) => (
              <option key={u}>{u}</option>
            ))}
          </select>
        </label>
        <span className={`pill ${status}`}>{status}</span>
        <button
          className={offline ? "offline-on" : ""}
          onClick={() => setOffline(!offline)}
          title="stops the client loop; edits queue locally"
        >
          {offline ? "Go online" : "Go offline"}
        </button>
        <span className={`pending ${pending > 0 ? "hot" : ""}`}>{pending} pending</span>
        <span className="spacer" />
        <label className="board-picker">
          board{" "}
          <select aria-label="Board" value={board} onChange={(e) => setBoard(e.target.value)}>
            {BOARDS.map((b) => (
              <option key={b}>{b}</option>
            ))}
          </select>
        </label>
      </header>

      {error && <div className="notice" role="alert">{error}</div>}

      {offline && (
        <div className="notice">
          Offline — edits queue locally as pending ops and sync when you go
          online again.
        </div>
      )}

      <main className="board">
        {STATUSES.map((st) => {
          const inColumn = cards.filter((c) => columnOf(c.values) === st);
          return (
            <section className="column" key={st}>
              <h2>
                {st} <span className="count">{inColumn.length}</span>
              </h2>
              <button className="newcard" disabled={!client} onClick={() => addCard(st)}>
                + new card
              </button>
              {inColumn.length === 0 && <div className="empty">nothing here</div>}
              {inColumn.map((c) => (
                <Card key={c.id} handle={c.handle} onDelete={() => archiveCard(c.id)} />
              ))}
            </section>
          );
        })}
      </main>
    </>
  );
}

interface CardValues {
  title?: string;
  status?: string;
  board?: string;
  votes?: number;
  labels?: string[];
  checklist?: string[];
  description?: string;
}

function columnOf(values: CardValues): string {
  const st = values.status;
  return STATUSES.includes(st ?? "") ? (st as string) : "todo";
}
