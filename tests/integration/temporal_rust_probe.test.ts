/**
 * Feasibility gate 1: a TypeScript Temporal Workflow schedules a genuine Rust Temporal
 * Activity Worker across task queues, and payload compatibility, heartbeats,
 * cancellation and retry all work over that boundary.
 *
 * Runs against a real `temporal server start-dev` instance on a free port.
 */
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { execFile } from 'node:child_process';
import { once } from 'node:events';
import net from 'node:net';
import { after, before, describe, it } from 'node:test';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

import { Client, Connection, WorkflowFailedError } from '@temporalio/client';
import type { Worker } from '@temporalio/worker';

import {
  PROBE_WORKFLOW_TASK_QUEUE,
  createProbeWorker,
} from '../../ts-api-agent/src/application/temporal_probe_worker.ts';

const execFileAsync = promisify(execFile);

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const CARGO_PATH = `/opt/homebrew/opt/rustup/bin:${process.env.PATH ?? ''}`;
const RUST_TASK_QUEUE = 'genomic-ingestion-rust';
const RUST_ACTIVITY_TYPE = 'rustActivityProbe';
/** `temporal.api.enums.v1.EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED` */
const EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED = 15;

async function freePort(): Promise<number> {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address() as net.AddressInfo;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}

async function waitFor(
  label: string,
  predicate: () => Promise<boolean>,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch (err) {
      lastError = err;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`timed out waiting for ${label}: ${String(lastError)}`);
}

function decodeJsonPayload(payload: { data?: Uint8Array | null } | null | undefined): unknown {
  if (!payload?.data) return undefined;
  return JSON.parse(Buffer.from(payload.data).toString('utf8'));
}

describe('temporal rust probe (cross-language feasibility gate)', () => {
  let temporalServer: ChildProcess;
  let rustWorker: ChildProcess;
  let tsWorker: Worker;
  let tsWorkerRun: Promise<void>;
  let connection: Connection;
  let client: Client;
  let address: string;
  const rustWorkerLog: string[] = [];

  before(async () => {
    const port = await freePort();
    address = `127.0.0.1:${port}`;

    temporalServer = spawn(
      'temporal',
      ['server', 'start-dev', '--ip', '127.0.0.1', '--port', String(port), '--headless', '--log-level', 'error'],
      { stdio: ['ignore', 'pipe', 'pipe'] },
    );
    temporalServer.stderr?.setEncoding('utf8');

    await waitFor('temporal dev server', async () => {
      const probe = await Connection.connect({ address, connectTimeout: 1000 });
      await probe.workflowService.getSystemInfo({ namespace: 'default' });
      await probe.close();
      return true;
    });

    connection = await Connection.connect({ address });
    client = new Client({ connection, namespace: 'default' });

    // Build and start the real Rust activity worker.
    await execFileAsync(
      'cargo',
      ['build', '--manifest-path', 'rust-ingestion-worker/Cargo.toml', '--bin', 'temporal_probe_worker'],
      { cwd: REPO_ROOT, env: { ...process.env, PATH: CARGO_PATH }, maxBuffer: 32 * 1024 * 1024 },
    );

    rustWorker = spawn(path.join(REPO_ROOT, 'target/debug/temporal_probe_worker'), [], {
      cwd: REPO_ROOT,
      env: { ...process.env, TEMPORAL_ADDRESS: address, TEMPORAL_NAMESPACE: 'default' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    rustWorker.stdout?.setEncoding('utf8');
    rustWorker.stderr?.setEncoding('utf8');
    rustWorker.stdout?.on('data', (chunk: string) => rustWorkerLog.push(chunk));
    rustWorker.stderr?.on('data', (chunk: string) => rustWorkerLog.push(chunk));

    await waitFor('rust probe worker', async () =>
      rustWorkerLog.join('').includes('[rust-probe-worker] ready'),
    );

    tsWorker = await createProbeWorker(address);
    tsWorkerRun = tsWorker.run();
  });

  after(async () => {
    if (tsWorker) {
      tsWorker.shutdown();
      await tsWorkerRun.catch(() => undefined);
    }
    await connection?.close().catch(() => undefined);
    rustWorker?.kill('SIGINT');
    temporalServer?.kill('SIGINT');
    await Promise.all(
      [rustWorker, temporalServer]
        .filter(Boolean)
        .map((proc) => once(proc, 'exit').catch(() => undefined)),
    );
  });

  it('round-trips camelCase payloads through a Rust activity on its own task queue', async () => {
    const handle = await client.workflow.start('temporalRustProbeWorkflow', {
      taskQueue: PROBE_WORKFLOW_TASK_QUEUE,
      workflowId: `probe-payload-${Date.now()}`,
      args: [{ message: 'hello', iterations: 20 }],
    });

    const result = await handle.result();
    assert.deepEqual(result, { echoed: 'hello', workerLanguage: 'rust' });

    const history = await handle.fetchHistory();
    const scheduled = (history.events ?? []).filter((e) => e.activityTaskScheduledEventAttributes);
    assert.equal(scheduled.length, 1, 'exactly one activity should be scheduled');
    const attrs = scheduled[0].activityTaskScheduledEventAttributes!;
    assert.equal(attrs.activityType?.name, RUST_ACTIVITY_TYPE);
    assert.equal(attrs.taskQueue?.name, RUST_TASK_QUEUE);

    // The input the Rust worker actually received, as recorded by the server.
    assert.deepEqual(decodeJsonPayload(attrs.input?.payloads?.[0]), {
      message: 'hello',
      iterations: 20,
    });

    const completed = (history.events ?? []).find((e) => e.activityTaskCompletedEventAttributes);
    assert.deepEqual(decodeJsonPayload(completed?.activityTaskCompletedEventAttributes?.result?.payloads?.[0]), {
      echoed: 'hello',
      workerLanguage: 'rust',
    });
  });

  it('heartbeats from Rust and is cancellable mid-flight', async () => {
    const handle = await client.workflow.start('temporalRustProbeWorkflow', {
      taskQueue: PROBE_WORKFLOW_TASK_QUEUE,
      workflowId: `probe-cancel-${Date.now()}`,
      args: [{ message: 'cancel-me', iterations: 200 }],
    });

    let identity = '';
    let heartbeat: unknown;
    await waitFor('rust heartbeat detail on pending activity', async () => {
      const description = await handle.describe();
      const pending = description.raw.pendingActivities?.[0];
      if (!pending?.heartbeatDetails?.payloads?.length) return false;
      identity = pending.lastWorkerIdentity ?? '';
      heartbeat = decodeJsonPayload(pending.heartbeatDetails.payloads[0]);
      return true;
    });

    assert.ok(
      identity.startsWith('rust-ingestion-worker@'),
      `pending activity worker identity should be the Rust worker, got "${identity}"`,
    );
    assert.equal(typeof (heartbeat as { iteration?: number })?.iteration, 'number');

    const logLengthBeforeCancel = rustWorkerLog.join('').length;
    await handle.cancel();
    await assert.rejects(handle.result(), (err: unknown) => err instanceof WorkflowFailedError);

    const history = await handle.fetchHistory();
    const eventTypes = (history.events ?? []).map((e) => e.eventType);
    assert.ok(
      eventTypes.includes(EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED),
      `expected a cancel request event, got [${eventTypes.join(',')}]`,
    );

    // The activity uses the SDK default cancellation type (TRY_CANCEL), so the workflow
    // closes without waiting for the activity's response and no ActivityTaskCanceled event
    // is written. The Rust worker's own log is therefore the proof that the cancellation
    // actually reached and stopped the Rust activity.
    await waitFor('rust activity to observe cancellation', async () =>
      rustWorkerLog.join('').slice(logLengthBeforeCancel).includes('rustActivityProbe cancelled'),
    );
  });

  it('retries the Rust activity after a failed first attempt', async () => {
    const handle = await client.workflow.start('temporalRustProbeWorkflow', {
      taskQueue: PROBE_WORKFLOW_TASK_QUEUE,
      workflowId: `probe-retry-${Date.now()}`,
      args: [{ message: 'fail-once', iterations: 2 }],
    });

    const result = await handle.result();
    assert.deepEqual(result, { echoed: 'fail-once', workerLanguage: 'rust' });

    const history = await handle.fetchHistory();
    const started = (history.events ?? [])
      .filter((e) => e.activityTaskStartedEventAttributes)
      .map((e) => e.activityTaskStartedEventAttributes!);
    // Temporal only persists the final ActivityTaskStarted event of a retried activity; it
    // carries the attempt counter and the previous attempt's failure.
    assert.equal(started.length, 1);
    assert.equal(started[0].attempt, 2, 'the successful run must be the second attempt');
    assert.match(
      started[0].lastFailure?.message ?? '',
      /probe failing deliberately on attempt 1/,
      'the retried attempt must carry the Rust-side failure from attempt 1',
    );
  });
});
