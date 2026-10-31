// Card renders and edits one kanban card over the SDK doc handle: title
// (register, committed on blur/Enter), votes (counter button), labels (set
// chips), checklist (list add/remove), and a collaborative textarea whose
// changes diff into delete + insertText ops.
import { useEffect, useState } from "react";
import type { DocHandle } from "@tidepool/client";
import { diffText } from "./diff";

export interface CardValues {
  title?: string;
  status?: string;
  board?: string;
  votes?: number;
  labels?: string[];
  checklist?: string[];
  description?: string;
}

export function Card(props: { handle: DocHandle; onDelete: () => void }) {
  const { handle } = props;
  const values = handle.values() as CardValues;

  return (
    <div className="card">
      <Title handle={handle} initial={values.title ?? ""} />
      <label className="card-select">Column
        <select aria-label="Column" value={values.status ?? "todo"}
          onChange={(e) => handle.set("status", e.target.value)}>
          {["todo", "doing", "done"].map((status) => <option key={status}>{status}</option>)}
        </select>
      </label>
      <label className="card-select">Board
        <select aria-label="Card board" value={values.board ?? "b1"}
          onChange={(e) => handle.set("board", e.target.value)}>
          {["b1", "b2"].map((board) => <option key={board}>{board}</option>)}
        </select>
      </label>
      <div className="votes">
        <button onClick={() => handle.inc("votes", 1)} aria-label="Vote" title="Vote">
          ▲
        </button>
        <span>{values.votes ?? 0} votes</span>
      </div>
      <Labels handle={handle} labels={values.labels ?? []} />
      <Checklist handle={handle} items={values.checklist ?? []} />
      <Description handle={handle} text={values.description ?? ""} />
      <div className="addrow">
        <button className="newcard" onClick={props.onDelete}>
          Archive card
        </button>
      </div>
    </div>
  );
}

function Title(props: { handle: DocHandle; initial: string }) {
  const [draft, setDraft] = useState(props.initial);
  useEffect(() => setDraft(props.initial), [props.initial]);
  const commit = () => {
    if (draft !== (props.handle.values() as CardValues).title) {
      props.handle.set("title", draft);
    }
  };
  return (
    <input
      className="title"
      aria-label="Card title"
      value={draft}
      placeholder="Card title"
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === "Enter") {
          commit();
          e.currentTarget.blur();
        }
      }}
    />
  );
}

function Labels(props: { handle: DocHandle; labels: string[] }) {
  const [adding, setAdding] = useState("");
  return (
    <div className="chips">
      {props.labels.map((label) => (
        <span className="chip" key={label}>
          {label}
          <button aria-label={`remove ${label}`} title={`remove ${label}`} onClick={() => props.handle.remove("labels", label)}>
            ×
          </button>
        </span>
      ))}
      <span className="chip add">
        <input
          placeholder="+ label"
          value={adding}
          size={6}
          style={{ border: "none", background: "transparent", font: "inherit", outline: "none" }}
          onChange={(e) => setAdding(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && adding.trim()) {
              props.handle.add("labels", adding.trim());
              setAdding("");
            }
          }}
          onBlur={() => {
            if (adding.trim()) {
              props.handle.add("labels", adding.trim());
              setAdding("");
            }
          }}
        />
      </span>
    </div>
  );
}

function Checklist(props: { handle: DocHandle; items: string[] }) {
  const [item, setItem] = useState("");
  return (
    <div>
      <ul className="checklist">
        {props.items.map((text, index) => (
          <li key={index}>
            <span>{text}</span>
            <button aria-label={`remove checklist item ${index + 1}`} title="remove item" onClick={() => props.handle.delete("checklist", index)}>
              ✕
            </button>
          </li>
        ))}
      </ul>
      <div className="addrow">
        <input
          placeholder="+ checklist item"
          value={item}
          onChange={(e) => setItem(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && item.trim()) {
              props.handle.insert("checklist", props.items.length, item.trim());
              setItem("");
            }
          }}
        />
        <button
          onClick={() => {
            if (item.trim()) {
              props.handle.insert("checklist", props.items.length, item.trim());
              setItem("");
            }
          }}
        >
          Add
        </button>
      </div>
    </div>
  );
}

/**
 * Description is the collaborative textarea: every change diffs the old and
 * new text by common prefix/suffix and lands as delete + insertText ops,
 * so concurrent edits merge character-wise.
 */
function Description(props: { handle: DocHandle; text: string }) {
  return (
    <textarea
      className="description"
      placeholder="Description (collaborative)"
      value={props.text}
      onChange={(e) => {
        const next = e.target.value;
        const current = (props.handle.values() as CardValues).description ?? "";
        const { pos, delCount, insert } = diffText(current, next);
        if (delCount > 0) {
          for (let i = 0; i < delCount; i++) {
            props.handle.delete("description", pos);
          }
        }
        if (insert.length > 0) {
          props.handle.insertText("description", pos, insert);
        }

      }}

    />
  );
}
