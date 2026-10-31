import { expect, it } from "vitest";
import { diffText } from "../../../examples/kanban/src/diff";
import { Client } from "../src/client";

it("translates textarea replacements into Unicode CRDT deletes and inserts", async () => {
  const client = await Client.create({ url: "ws://localhost", token: "", schemas: { cards: { description: "text" } } });
  const doc = client.doc("cards", "text");
  for (const next of ["Hello 🌊 world", "Hello 🐙 world", "🌊 world", "🌊 world!", "", "hello"]) {
    const before = String(doc.values().description ?? "");
    const diff = diffText(before, next);
    for (let i = 0; i < diff.delCount; i++) doc.delete("description", diff.pos);
    if (diff.insert) doc.insertText("description", diff.pos, diff.insert);
    expect(doc.values().description ?? "").toBe(next);
  }
});
