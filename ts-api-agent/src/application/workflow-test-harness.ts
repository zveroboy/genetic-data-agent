/**
 * Server-free Workflow activator harness.
 *
 * `GenomicIngestionWorkflow` is exercised as a *real* Temporal Workflow function: the Activity
 * proxies it builds with `proxyActivities` are the real ones, and `runWorkflow` drives them by
 * installing a minimal Workflow activator on `globalThis.__TEMPORAL_ACTIVATOR__` — the same hook
 * the SDK's own Workflow sandbox installs. Callers get back the `scheduleActivity` commands the
 * Workflow actually emits (Activity type, task queue, timeouts, retry policy, cancellation type),
 * the order in which it emits them, and the state the `getProgress` query reports at each point.
 * Nothing here asserts "a mock was called".
 *
 * `@temporalio/testing`'s time-skipping environment was not used. It was not a dependency of
 * this workspace when this harness was written and could not be installed in that session's
 * network-blocked sandbox — that was a statement about the sandbox, not the package or the
 * environment generally; a later fix pass re-attempted the install with the sandbox disabled
 * and it succeeded cleanly (`ts-api-agent/package.json` now lists it as a devDependency). Even
 * installed, `TestWorkflowEnvironment`'s ephemeral test server still downloads a binary lazily
 * on first *use* (not at install time), which nothing here has attempted, and a rewrite onto it
 * is out of scope for this fix pass. This harness is the server-free substitute that was used
 * instead; it is confined to `runWorkflow` and touches three SDK internals, all of them stable
 * across 1.x: the activator global, the `lib/cancellation-scope` subpath, and the fact that
 * `AsyncLocalStorage` degrades to an empty class outside the sandbox.
 *
 * FOLLOW-UP (explicit, not yet scheduled to a task): replace this harness with
 * `TestWorkflowEnvironment.createTimeSkipping()` now that `@temporalio/testing` is installed.
 * The assertions themselves (scheduled-activity shape, task queue, retry policy, cancellation,
 * progress query) should carry over largely unchanged; only `runWorkflow`'s plumbing goes away.
 */
import assert from 'node:assert/strict';

import {
  ActivityFailure,
  ApplicationFailure,
  CancelledFailure,
  decodeActivityCancellationType,
  defaultPayloadConverter,
} from '@temporalio/common';
import {
  AsyncLocalStorage as WorkflowAsyncLocalStorage,
  RootCancellationScope,
} from '@temporalio/workflow/lib/cancellation-scope.js';

import { CONTROL_PLANE_TASK_QUEUE, type IngestionProgress } from './workflows.ts';

/**
 * Outside the Workflow sandbox the SDK swaps `AsyncLocalStorage` for an empty class, which
 * leaves `CancellationScope.current()` without a `getStore`. The Workflow under test creates no
 * nested scopes, so a `getStore` that always reports "no scope entered" is exact: every
 * Activity it schedules belongs to the root scope, which is what a real Workflow's main
 * function runs in.
 *
 * WARNING — process-global side effect on a third-party module: this permanently patches
 * `@temporalio/workflow`'s internal `AsyncLocalStorage` prototype for the lifetime of whatever
 * process evaluates this file, and there is no matching teardown. It is safe ONLY because
 * `node --test` isolates each test *file* into its own process — if this harness is ever
 * extracted into a shared fixture imported by multiple test files running in the same process,
 * or the test runner stops process-isolating files, this mutation will leak across them.
 */
const storagePrototype = WorkflowAsyncLocalStorage.prototype as { getStore?: () => undefined };
storagePrototype.getStore ??= () => undefined;

interface ScheduledActivity {
  readonly seq: number;
  readonly activityType: string;
  readonly taskQueue: string;
  readonly args: readonly unknown[];
  readonly scheduleToCloseSeconds: number | null;
  readonly startToCloseSeconds: number | null;
  readonly heartbeatSeconds: number | null;
  readonly cancellationType: string | undefined;
  readonly maximumAttempts: number | undefined;
  readonly nonRetryableErrorTypes: readonly string[];
}

/** Reads a protobuf `Duration` back into whole seconds. */
function durationSeconds(duration: unknown): number | null {
  if (duration === null || duration === undefined) return null;
  const seconds = (duration as { seconds?: number | { toNumber(): number } }).seconds;
  if (typeof seconds === 'number') return seconds;
  if (seconds && typeof seconds.toNumber === 'function') return seconds.toNumber();
  return null;
}

export interface WorkflowEnvironment<T> {
  /** Every `scheduleActivity` command the Workflow has emitted, in emission order. */
  readonly scheduled: readonly ScheduledActivity[];
  /** Sequence numbers the Workflow has asked the server to cancel. */
  readonly cancelRequests: readonly number[];
  /** The current `getProgress` answer, produced by the Workflow's own query handler. */
  progress(): IngestionProgress;
  /** Delivers a successful Activity result and lets the Workflow advance. */
  complete(seq: number, value: unknown): Promise<void>;
  /** Delivers an Activity failure, wrapped the way the server wraps one. */
  fail(seq: number, cause: Error): Promise<void>;
  /** Requests cancellation of the Workflow, exactly as a `WorkflowHandle.cancel()` does. */
  cancel(): Promise<void>;
  /** The Workflow's own promise. */
  readonly result: Promise<T>;
}

/** Lets every already-scheduled microtask run, the way a Workflow Task activation does. */
function drain(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

/**
 * Runs `workflow(input)` under a throwaway activator and returns handles for driving it.
 *
 * The activator is only as complete as the Workflow needs: Activity scheduling, query handler
 * registration and the root cancellation scope. Anything the Workflow is forbidden to touch —
 * timers, child Workflows, signals, `uuid4`, `Date.now` — is deliberately absent, so reaching
 * for one fails the test loudly instead of silently succeeding.
 */
export function runWorkflow<I, T>(
  workflow: (input: I) => Promise<T>,
  input: I,
): WorkflowEnvironment<T> {
  const scheduled: ScheduledActivity[] = [];
  const cancelRequests: number[] = [];
  const queryHandlers = new Map<string, { handler: (...args: unknown[]) => unknown }>();
  const completions = {
    activity: new Map<number, { resolve(value: unknown): void; reject(err: unknown): void }>(),
    timer: new Map(),
    childWorkflowStart: new Map(),
    childWorkflowComplete: new Map(),
    signalWorkflow: new Map(),
    cancelWorkflow: new Map(),
  };
  const rootScope = new RootCancellationScope();

  const activator = {
    info: {
      namespace: 'default',
      workflowId: 'workflow-under-test',
      runId: 'run-under-test',
      workflowType: 'GenomicIngestionWorkflow',
      taskQueue: CONTROL_PLANE_TASK_QUEUE,
    },
    payloadConverter: defaultPayloadConverter,
    nextSeqs: { activity: 1, timer: 1, childWorkflow: 1, signalWorkflow: 1, cancelWorkflow: 1 },
    completions,
    interceptors: { inbound: [], outbound: [] },
    queryHandlers,
    signalHandlers: new Map(),
    updateHandlers: new Map(),
    rootScope,
    hasFlag: () => true,
    dispatchBufferedSignals() {},
    dispatchBufferedUpdates() {},
    pushCommand(command: Record<string, any>): void {
      if (command.requestCancelActivity) {
        cancelRequests.push(command.requestCancelActivity.seq);
        return;
      }
      const schedule = command.scheduleActivity;
      if (!schedule) return;
      scheduled.push({
        seq: schedule.seq,
        activityType: schedule.activityType,
        taskQueue: schedule.taskQueue,
        args: (schedule.arguments ?? []).map((payload: unknown) =>
          defaultPayloadConverter.fromPayload(payload as never),
        ),
        scheduleToCloseSeconds: durationSeconds(schedule.scheduleToCloseTimeout),
        startToCloseSeconds: durationSeconds(schedule.startToCloseTimeout),
        heartbeatSeconds: durationSeconds(schedule.heartbeatTimeout),
        cancellationType: decodeActivityCancellationType(schedule.cancellationType),
        maximumAttempts: schedule.retryPolicy?.maximumAttempts,
        nonRetryableErrorTypes: schedule.retryPolicy?.nonRetryableErrorTypes ?? [],
      });
    },
  };

  const previousActivator = (globalThis as any).__TEMPORAL_ACTIVATOR__;
  (globalThis as any).__TEMPORAL_ACTIVATOR__ = activator;
  const result = workflow(input);
  // The Workflow's rejection is asserted by the individual tests; keep Node quiet until then.
  result.catch(() => undefined);
  (globalThis as any).__TEMPORAL_ACTIVATOR__ = previousActivator;

  function settle(seq: number, deliver: (completion: { resolve(v: unknown): void; reject(e: unknown): void }) => void) {
    const completion = completions.activity.get(seq);
    assert.ok(completion, `no activity is waiting on sequence ${seq}`);
    completions.activity.delete(seq);
    (globalThis as any).__TEMPORAL_ACTIVATOR__ = activator;
    deliver(completion);
    return drain().finally(() => {
      (globalThis as any).__TEMPORAL_ACTIVATOR__ = previousActivator;
    });
  }

  return {
    scheduled,
    cancelRequests,
    progress(): IngestionProgress {
      const registered = queryHandlers.get('getProgress');
      assert.ok(registered, 'the workflow registered no getProgress query handler');
      return registered.handler() as IngestionProgress;
    },
    complete(seq, value) {
      return settle(seq, (completion) => completion.resolve(value));
    },
    fail(seq, cause) {
      const activity = scheduled.find((entry) => entry.seq === seq);
      // A real `CancelledFailure` reaches Workflow code as itself — the server does not run it
      // through `ApplicationFailure.fromError`, which would relabel it as an ApplicationFailure
      // of type 'CancelledFailure' and make `isCancellation()` false. Every other cause here is
      // already the exact shape `ApplicationFailure.fromError` would classify it as (either an
      // `ApplicationFailure` built via `.create`, or an Error whose constructor name is the wire
      // type), so routing those through `fromError` still reproduces what Temporal does.
      const wrappedCause =
        cause instanceof CancelledFailure ? cause : ApplicationFailure.fromError(cause);
      return settle(seq, (completion) =>
        completion.reject(
          new ActivityFailure(
            `Activity task failed: ${cause.message}`,
            activity?.activityType ?? 'unknown',
            String(seq),
            undefined,
            'test-worker',
            wrappedCause,
          ),
        ),
      );
    },
    cancel() {
      (globalThis as any).__TEMPORAL_ACTIVATOR__ = activator;
      rootScope.cancel();
      return drain().finally(() => {
        (globalThis as any).__TEMPORAL_ACTIVATOR__ = previousActivator;
      });
    },
    result,
  };
}
