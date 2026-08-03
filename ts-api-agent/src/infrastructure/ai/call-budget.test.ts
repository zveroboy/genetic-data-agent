/**
 * The spending ceiling on paid model calls.
 *
 * What is being protected is a quota someone pays for, so the properties worth pinning are the
 * ones that decide whether it actually bounds spending: that a reservation is all-or-nothing,
 * that unused turns come back, and that the window slides rather than resetting — a fixed window
 * lets a runaway client spend the whole budget twice across the boundary, which is exactly when
 * a runaway client would do it.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  DEFAULT_CALLS_PER_WINDOW,
  callBudgetFromEnv,
  createCallBudget,
} from './call-budget.ts';

/** A clock a test can move by hand. */
function fakeClock(start = 1_000_000) {
  let at = start;
  return { now: () => at, advance: (ms: number) => (at += ms) };
}

describe('createCallBudget', () => {
  it('reserves all or nothing, so a question is never half-funded', () => {
    const budget = createCallBudget({ limit: 10, windowMs: 1000, now: fakeClock().now });

    assert.equal(budget.reserve(4), true);
    assert.equal(budget.reserve(4), true);
    assert.equal(budget.remaining(), 2);
    // Two left, four wanted: refused outright rather than partially granted.
    assert.equal(budget.reserve(4), false);
    assert.equal(budget.remaining(), 2, 'a refused reservation spends nothing');
  });

  it('gives back turns that were reserved but not made', () => {
    const budget = createCallBudget({ limit: 10, windowMs: 1000, now: fakeClock().now });

    budget.reserve(4);
    // The question answered in two turns; the other two were never billed.
    budget.refund(2);

    assert.equal(budget.remaining(), 8);
  });

  it('slides the window instead of resetting it', () => {
    const clock = fakeClock();
    const budget = createCallBudget({ limit: 4, windowMs: 1000, now: clock.now });

    budget.reserve(4);
    assert.equal(budget.reserve(1), false);

    // Half a window later the earlier calls are still inside it.
    clock.advance(600);
    assert.equal(budget.reserve(1), false, 'a fixed window would have reset here');

    clock.advance(500);
    assert.equal(budget.reserve(4), true, 'the first four have aged out');
  });

  it('a zero limit refuses every call', () => {
    // The way to turn the paid path off entirely without unsetting the key.
    const budget = createCallBudget({ limit: 0, windowMs: 1000, now: fakeClock().now });

    assert.equal(budget.reserve(1), false);
    assert.equal(budget.remaining(), 0);
  });
});

describe('callBudgetFromEnv', () => {
  it('uses the default when unset', () => {
    assert.equal(callBudgetFromEnv({}).remaining(), DEFAULT_CALLS_PER_WINDOW);
  });

  it('reads an explicit limit', () => {
    assert.equal(callBudgetFromEnv({ CEREBRAS_CALLS_PER_WINDOW: '7' }).remaining(), 7);
  });

  it('fails loudly on a malformed limit rather than reverting to the default', () => {
    // The failure mode this prevents: an operator believes a spending limit is in force, a typo
    // silently restored the default, and the first bill says otherwise.
    assert.throws(
      () => callBudgetFromEnv({ CEREBRAS_CALLS_PER_WINDOW: 'a lot' }),
      /must be a non-negative integer/,
    );
    assert.throws(
      () => callBudgetFromEnv({ CEREBRAS_CALLS_PER_WINDOW: '-1' }),
      /must be a non-negative integer/,
    );
    assert.throws(() => callBudgetFromEnv({ CEREBRAS_WINDOW_MS: '0' }), /must be a positive integer/);
  });
});
