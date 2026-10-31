// Scalar values (PLAN.md §4.2), mirroring internal/crdt/value.go.
import { errInvalid } from "./errors.js";

/** MaxScalarBytes is the largest JSON size accepted for one scalar value. */
export const MAX_SCALAR_BYTES = 64 << 10;

/**
 * parseScalar validates a JSON scalar: string, number, bool or null.
 * Objects, arrays and oversized values are rejected. `raw` is the already
 * parsed JSON value; the size check measures its compact JSON form, which
 * matches the Go implementation's raw-byte check for values that arrived
 * as JSON.
 */
export function parseScalar(raw: unknown): unknown {
  const size = raw === undefined ? 0 : JSON.stringify(raw).length;
  if (size > MAX_SCALAR_BYTES) {
    throw errInvalid(`scalar value is ${size} bytes, max is ${MAX_SCALAR_BYTES}`);
  }
  if (raw === null) {
    return null;
  }
  switch (typeof raw) {
    case "string":
    case "boolean":
    case "number":
      return raw;
    default:
      throw errInvalid(`scalar must be string, number, bool or null, got ${typeof raw}`);
  }
}

/**
 * isSingleCodePoint reports whether s is exactly one valid Unicode code
 * point (lone UTF-16 surrogates are rejected, mirroring Go's UTF-8 check).
 */
export function isSingleCodePoint(s: string): boolean {
  const cps = Array.from(s);
  if (cps.length !== 1) {
    return false;
  }
  const cp = s.codePointAt(0) ?? 0;
  return !(cp >= 0xd800 && cp <= 0xdfff);
}
