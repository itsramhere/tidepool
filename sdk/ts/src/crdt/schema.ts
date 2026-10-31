// Field types (PLAN.md §4.2) and which op kinds each accepts. Mirrors
// internal/crdt/schema.go 1:1.
export type FieldType = "register" | "counter" | "set" | "list" | "text";

export type Schema = Record<string, FieldType>;

const allowed: Record<FieldType, OpKindList> = {
  register: ["set"],
  counter: ["inc"],
  set: ["add", "rem"],
  list: ["ins", "del"],
  text: ["ins", "del"],
};

type OpKindList = readonly string[];

/** allowedKind reports whether kind may be applied to a field of type ft. */
export function allowedKind(ft: FieldType, kind: string): boolean {
  return (allowed[ft] as OpKindList).includes(kind);
}
