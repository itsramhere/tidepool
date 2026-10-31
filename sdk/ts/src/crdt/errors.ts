// Sentinel error kinds mirroring internal/crdt/errors.go: callers test the
// `kind` property the way Go callers use errors.Is.
export type ErrorKind = "invalid" | "duplicate" | "outOfOrder" | "missingDeps" | "notFound";

export class CrdtError extends Error {
  constructor(
    public kind: ErrorKind,
    message: string,
  ) {
    super(message);
    this.name = "CrdtError";
  }
}

export const errInvalid = (message: string): CrdtError => new CrdtError("invalid", message);
export const errDuplicate = (message: string): CrdtError => new CrdtError("duplicate", message);
export const errOutOfOrder = (message: string): CrdtError => new CrdtError("outOfOrder", message);
export const errMissingDeps = (message: string): CrdtError => new CrdtError("missingDeps", message);
export const errNotFound = (message: string): CrdtError => new CrdtError("notFound", message);

/** isCrdtKind reports whether e is a CrdtError of the given kind. */
export function isCrdtKind(e: unknown, kind: ErrorKind): boolean {
  return e instanceof CrdtError && e.kind === kind;
}
