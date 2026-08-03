/**
 * A ceiling on how many paid model calls this process may make in a rolling window.
 *
 * The thing being protected is a quota, not data: the Cerebras key belongs to whoever runs this,
 * and one loop in a client — or one person leaning on the demo — can spend it in minutes. One
 * question costs up to `MAX_MODEL_TURNS` HTTP calls, so "how many questions" and "how many calls"
 * are not the same number and only the second one is billed.
 *
 * Two decisions worth stating, because both could reasonably have gone the other way:
 *
 * - **Exhaustion is not an error.** This system has a complete answer path that costs nothing —
 *   deterministic routing over the reference snapshot, local embeddings, local vector search — and
 *   it produces a fully evidenced answer with provenance. Refusing to answer when the *free* path
 *   is right there would be a worse product for no gain, so the caller degrades to it instead of
 *   returning 429.
 * - **Whole questions, or none.** A budget consumed per HTTP call can run out between the tool
 *   call and the answer, leaving a question half-answered and the money already spent. So a
 *   question reserves its worst case up front and refunds what it did not use.
 *
 * The window slides rather than resetting on the hour: a fixed window lets a client spend the
 * whole budget twice across the boundary, which is exactly when a runaway client would do it.
 */

/** Calls per window, when `CEREBRAS_CALLS_PER_WINDOW` says nothing. */
export const DEFAULT_CALLS_PER_WINDOW = 120;

/** One hour. */
export const DEFAULT_WINDOW_MS = 60 * 60 * 1000;

export interface CallBudget {
  /**
   * Reserves `count` calls, or refuses. All or nothing: a partial reservation would be a
   * half-answered question.
   */
  reserve(count: number): boolean;
  /** Returns calls reserved but not made. */
  refund(count: number): void;
  /** How many calls are still available right now. */
  remaining(): number;
}

export interface CallBudgetConfig {
  readonly limit: number;
  readonly windowMs: number;
  /** Injectable so a test can move time without sleeping. */
  readonly now?: () => number;
}

export function createCallBudget(config: CallBudgetConfig): CallBudget {
  const { limit, windowMs } = config;
  const now = config.now ?? (() => Date.now());
  // One timestamp per reserved call. At a budget of a few hundred per hour this is a few hundred
  // numbers — small enough that the exactness of a true sliding window is free.
  let spent: number[] = [];

  const forget = (): void => {
    const cutoff = now() - windowMs;
    if (spent.length > 0 && spent[0]! <= cutoff) {
      spent = spent.filter((at) => at > cutoff);
    }
  };

  return {
    reserve(count: number): boolean {
      forget();
      if (spent.length + count > limit) return false;
      const at = now();
      for (let i = 0; i < count; i++) spent.push(at);
      return true;
    },

    refund(count: number): void {
      // Removes the most recent entries: they are the ones this caller just reserved, and
      // dropping the oldest instead would silently extend the window for everybody else.
      if (count > 0) spent.splice(Math.max(0, spent.length - count), count);
    },

    remaining(): number {
      forget();
      return Math.max(0, limit - spent.length);
    },
  };
}

/**
 * Reads the budget from the environment, failing startup on a malformed value.
 *
 * Never silently reverts to the default: the failure mode where an operator believes a spending
 * limit is in force and it is not is the one this module exists to prevent.
 */
export function callBudgetFromEnv(env: NodeJS.ProcessEnv = process.env): CallBudget {
  const raw = env.CEREBRAS_CALLS_PER_WINDOW;
  let limit = DEFAULT_CALLS_PER_WINDOW;
  if (raw !== undefined && raw.length > 0) {
    const value = Number(raw);
    if (!Number.isInteger(value) || value < 0) {
      throw new Error(`CEREBRAS_CALLS_PER_WINDOW must be a non-negative integer, got '${raw}'`);
    }
    limit = value;
  }

  const rawWindow = env.CEREBRAS_WINDOW_MS;
  let windowMs = DEFAULT_WINDOW_MS;
  if (rawWindow !== undefined && rawWindow.length > 0) {
    const value = Number(rawWindow);
    if (!Number.isInteger(value) || value <= 0) {
      throw new Error(`CEREBRAS_WINDOW_MS must be a positive integer, got '${rawWindow}'`);
    }
    windowMs = value;
  }

  return createCallBudget({ limit, windowMs });
}
