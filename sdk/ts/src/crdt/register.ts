// Last-writer-wins register (PLAN.md §4.5), mirroring internal/crdt/register.go.
import type { Op } from "./op.js";
import { errInvalid, type CrdtError } from "./errors.js";
import { parseScalar } from "./value.js";

export class Register {
  lamport = 0;
  actor = "";
  value: unknown = undefined;
  set = false;

  /** check validates a set op; it writes nothing. */
  check(op: Op): void {
    if (op.kind !== "set") {
      throw errInvalid(`kind "${op.kind}" is not allowed on a register`);
    }
    parseScalar(op.value);
  }

  /** apply applies a checked set op; a lower (lamport, actor) never wins. */
  apply(op: Op): void {
    if (this.set && !registerWriteWins(this, op)) {
      return;
    }
    this.lamport = op.lamport;
    this.actor = op.id.actor;
    this.value = op.value;
    this.set = true;
  }

  /** materialize returns the stored scalar, or null if never set. */
  materialize(): unknown {
    if (!this.set) {
      return null;
    }
    try {
      return parseScalar(this.value);
    } catch {
      return null; // values are validated by check; unreachable in practice
    }
  }
}

function registerWriteWins(r: Register, op: Op): boolean {
  if (op.lamport !== r.lamport) {
    return op.lamport > r.lamport;
  }
  return op.id.actor > r.actor;
}

export type { CrdtError };
