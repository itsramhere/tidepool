// canonicalStringify: a canonical JSON form shared with the Go SDK. Object
// keys are sorted recursively; strings carry the same escaping Go's
// encoding/json produces (HTML characters and U+2028/2029); numbers use the
// JS shortest round-trip form, which matches Go for the values these
// documents contain (JSON-safe integers and typical decimals).
export function canonicalStringify(v: unknown): string {
  if (v === null) {
    return "null";
  }
  switch (typeof v) {
    case "boolean":
      return v ? "true" : "false";
    case "number":
      return numberToJSON(v);
    case "string":
      return escapeGoJSON(v);
    case "object":
      break;
    default:
      return "null"; // undefined and functions have no JSON form
  }
  if (Array.isArray(v)) {
    const items = v.map((item) => (item === undefined ? "null" : canonicalStringify(item)));
    return `[${items.join(",")}]`;
  }
  const rec = v as Record<string, unknown>;
  const keys = Object.keys(rec)
    .filter((k) => rec[k] !== undefined)
    .sort();
  const parts = keys.map((k) => `${escapeGoJSON(k)}:${canonicalStringify(rec[k])}`);
  return `{${parts.join(",")}}`;
}

/**
 * numberToJSON formats a number the way encoding/json formats float64:
 * plain decimal inside JS's exponential range, exponent form outside.
 */
function numberToJSON(n: number): string {
  if (!Number.isFinite(n)) {
    throw new Error(`canonicalStringify: non-finite number ${n}`);
  }
  if (Object.is(n, -0)) {
    return "-0"; // matches Go's float64(-0)
  }
  return String(n);
}

function escapeGoJSON(s: string): string {
  return JSON.stringify(s)
    .replace(/</g, "\\u003c")
    .replace(/>/g, "\\u003e")
    .replace(/&/g, "\\u0026")
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029");
}
