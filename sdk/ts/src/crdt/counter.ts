// Grow-only counter (PLAN.md §4.5), mirroring internal/crdt/counter.go.
import type { Op } from "./op";
import { errInvalid } from "./errors";

export class Counter {
  n = 0;

  /** check validates an inc op: non-zero delta. It writes nothing. */
  check(op: Op): void {
    if (op.kind !== "inc") {
      throw errInvalid(`kind "${op.kind}" is not allowed on a counter`);
    }
    if ((op.delta ?? 0) === 0) {
      throw errInvalid("counter delta must be non-zero");
    }
  }

  apply(op: Op): void {
    this.n += op.delta ?? 0;
  }

  materialize(): number {
    return this.n;
  }
}
