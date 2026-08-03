/**
 * Cross-language ingestion gate: the *production* `GenomicIngestionWorkflow` schedules the
 * *production* `buildDatasetArtifact` Activity on `genomic-ingestion-rust`, a real Rust worker
 * serves it against a real MinIO, and the artifact it publishes satisfies the TypeScript half of
 * the frozen `ingestion-v1` contract.
 *
 * Nothing here is a stand-in for the thing under test:
 *
 * - The Workflow is `ts-api-agent/src/application/workflows.ts`, with its real Activity options —
 *   the 15-second `heartbeatTimeout`, `WAIT_CANCELLATION_COMPLETED`, and the non-retryable
 *   failure-type list are the ones production uses.
 * - The Activity is the registered `buildDatasetArtifact`, run by `target/debug/temporal_worker`.
 * - The dataset checksum and the inventory invariants are re-derived here with the production
 *   TypeScript implementations (`dataset-checksum.ts`), so the two languages have to agree.
 * - Every uploaded object is HEADed straight out of MinIO and compared against the inventory,
 *   which is exactly what `publishDataset` refuses to publish without.
 *
 * Three workflow executions share one server, one Rust worker and one staging root:
 *
 * 1. **Success.** The whole contract, end to end.
 * 2. **Retry.** Attempt 1 fails on a genuinely retryable condition and attempt 2 succeeds under
 *    its *own* S3 prefix and its *own* local workspace. "No retry appends to an existing local
 *    database or a published prefix" is the load-bearing property of the attempt scoping, and it
 *    is only a property if something actually runs attempt 2.
 * 3. **Cancellation.** A running Activity is cancelled and Temporal history has to record an
 *    `ActivityTaskCanceled` — not a failure, not a timeout. The Workflow uses
 *    `WAIT_CANCELLATION_COMPLETED`, so it *blocks* on that response: a Rust side that recorded a
 *    failure instead would stall the Workflow until `scheduleToCloseTimeout`.
 *
 * The two control-plane Activities are the only substitutes: `inspectDatasetSource` is replaced
 * by a stub that pins the per-scenario source object this test seeded, and `publishDataset` runs
 * the production validation but does not write a manifest — no manifest is written anywhere in
 * this test, which is also the assertion that the Rust Activity does not write one either.
 *
 * Requires a running MinIO (`docker compose up -d minio`) and the `temporal` CLI. Every S3
 * bucket, object, local directory and server process it uses is created by this run, under a
 * per-run name, and removed again in `after`. Nothing it did not create is ever deleted.
 */
import assert from 'node:assert/strict';
import { execFile, spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectsCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { Client, Connection, type WorkflowHandle } from '@temporalio/client';
import { NativeConnection, Worker } from '@temporalio/worker';

import {
  computeDatasetChecksumSha256,
  assertValidArtifactResult,
} from '../../ts-api-agent/src/application/dataset-checksum.ts';
import {
  ARTIFACT_FORMAT,
  type BuildDatasetArtifactInput,
  BuildDatasetArtifactInputSchema,
  type BuildDatasetArtifactResult,
  BuildDatasetArtifactResultSchema,
  CONTRACT_VERSION,
  type DatasetManifest,
  DatasetManifestSchema,
  INGESTION_PHASES,
  IngestionHeartbeatSchema,
  LAYOUT_VERSION,
  PARQUET_SCHEMA_FINGERPRINT,
  PARTITION_SPEC,
  SCHEMA_VERSION,
  SORT_ORDER,
  VARIANTS_SEGMENT,
  allowedPrefixFor,
} from '../../ts-api-agent/src/application/ingestion-contracts.ts';
import {
  CONTROL_PLANE_TASK_QUEUE,
  RUST_INGESTION_TASK_QUEUE,
} from '../../ts-api-agent/src/application/workflows.ts';

const execFileAsync = promisify(execFile);

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const CARGO_PATH = `/opt/homebrew/opt/rustup/bin:${process.env.PATH ?? ''}`;

const RUST_ACTIVITY_TYPE = 'buildDatasetArtifact';
const WORKER_IDENTITY_PREFIX = 'rust-ingestion-worker@';

/**
 * `contracts/ingestion-v1.md`: "Phases, **in order**". A consumer polling heartbeats may read a
 * phase as progress, so the published sequence must never regress through this list.
 *
 * Sourced from `ingestion-contracts.ts::INGESTION_PHASES` — the same shared contract module this
 * file already imports schemas and constants from — rather than duplicated as a literal, so this
 * test cannot drift from the one the TypeScript application code and its own tests use.
 */
const PHASE_ORDER = INGESTION_PHASES;

const S3_ENDPOINT = process.env.S3_ENDPOINT ?? 'http://localhost:9000';
const S3_ACCESS_KEY = process.env.S3_ACCESS_KEY ?? 'admin';
const S3_SECRET_KEY = process.env.S3_SECRET_KEY ?? 'password123';

/**
 * Per-run names. This test runs against the developer's shared MinIO, so it must never touch a
 * bucket it did not create. Every bucket it creates is appended to `ownedBuckets`, and `after`
 * removes exactly those and nothing else.
 */
const RUN_ID = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const SOURCE_BUCKET = `rust-ingest-src-${RUN_ID}`;
const ownedBuckets: string[] = [];

const SOURCE_KEY = 'samples/demo_user.vcf';
const RETRY_SOURCE_KEY = 'samples/retry_user.vcf';
const CANCEL_SOURCE_KEY = 'samples/cancel_user.vcf.gz';

const DATASET_SUFFIX = RUN_ID.replace(/[^a-z0-9]/gi, '');
const ARTIFACT_VERSION = 'iv-e2e-1';

/** The happy-path scenario. */
const ARTIFACT_BUCKET = `rust-ingest-art-${RUN_ID}`;
const DATASET_ID = `ds-e2e-${DATASET_SUFFIX}`;
const ALLOWED_PREFIX = allowedPrefixFor(DATASET_ID, ARTIFACT_VERSION);
const ATTEMPT_PREFIX = `${ALLOWED_PREFIX}attempt-1/`;

/** The retry scenario. Its artifact bucket deliberately does not exist when the run starts. */
const RETRY_ARTIFACT_BUCKET = `rust-ingest-retry-${RUN_ID}`;
const RETRY_DATASET_ID = `ds-retry-${DATASET_SUFFIX}`;
const RETRY_ALLOWED_PREFIX = allowedPrefixFor(RETRY_DATASET_ID, ARTIFACT_VERSION);

/** The cancellation scenario. */
const CANCEL_ARTIFACT_BUCKET = `rust-ingest-cancel-${RUN_ID}`;
const CANCEL_DATASET_ID = `ds-cancel-${DATASET_SUFFIX}`;
const CANCEL_ALLOWED_PREFIX = allowedPrefixFor(CANCEL_DATASET_ID, ARTIFACT_VERSION);
/**
 * An object the cancellation must leave alone. The Activity's cleanup is local-only by design —
 * an abandoned attempt prefix stays orphaned rather than being deleted — and this is the sentinel
 * that proves it deletes nothing in S3 at all.
 */
const CANARY_KEY = 'canary/do-not-delete.txt';
const CANARY_BODY = 'a cancelled activity must delete nothing in the object store';

/**
 * The synthetic source. Chromosomes are chosen so the inventory is genuinely multi-file and so
 * byte-wise `(chrom, relativePath)` ordering is not the same as numeric ordering — `10` and `2`
 * are both present, and `X`/`MT` sort after every digit.
 */
const CHROMOSOMES = ['1', '2', '10', '22', 'X', 'MT'] as const;
/**
 * Variants per chromosome. Large enough that the Activity stays in flight long enough for the
 * server-side heartbeat poll below to observe a pending Activity, small enough to keep the run
 * to a few seconds.
 */
const VARIANTS_PER_CHROMOSOME = 40_000;
/** Deliberately malformed lines, counted rather than fatal. */
const MALFORMED_RECORDS = 3;
const EXPECTED_VARIANTS = CHROMOSOMES.length * VARIANTS_PER_CHROMOSOME;
/** `pos` of the n-th variant on a chromosome; keeps min/max predictable per partition. */
const firstPos = (index: number): number => 10_000 + index * 100;
const lastPos = firstPos(VARIANTS_PER_CHROMOSOME - 1);

/** The retry scenario runs the whole build twice, so its source is deliberately small. */
const RETRY_VARIANTS_PER_CHROMOSOME = 2_000;
const RETRY_EXPECTED_VARIANTS = CHROMOSOMES.length * RETRY_VARIANTS_PER_CHROMOSOME;

/**
 * The cancellation scenario's source is deliberately the largest, and this is why.
 *
 * A Temporal Activity learns that it has been cancelled from the *response* to one of its own
 * heartbeats (`RecordActivityTaskHeartbeatResponse.cancel_requested`). sdk-core throttles those
 * requests to `min(heartbeatTimeout × 0.8, max_heartbeat_throttle_interval)`. The worker sets
 * `max_heartbeat_throttle_interval` to 5 seconds (`temporal_worker.rs`), well under
 * `heartbeatTimeout × 0.8` = 12 seconds under the Workflow's frozen `heartbeatTimeout: '15
 * seconds'`, so the 5-second cap is what governs: however promptly the Workflow asks, the Rust
 * side cannot observe the cancellation for up to ~5 seconds — and an Activity that finishes first
 * is simply never cancelled.
 *
 * This is a *smaller* fixture than a previous version of this test used, and the 5-second figure
 * above is why it could shrink: before the worker set `max_heartbeat_throttle_interval`, the
 * governing window was the full `heartbeatTimeout × 0.8` = 12 seconds, so the fixture had to keep
 * the Activity in flight that long (5.4 M variants, ~25 s of runtime).
 *
 * Measured on this fixture shape and this machine: the Activity gets through roughly
 * 165 000-190 000 variants/second once it reaches `WRITING_DUCKDB`, so 1.5 M variants
 * (`CANCEL_VARIANTS_PER_CHROMOSOME * CHROMOSOMES.length`) takes ~9 s to finish if left uncancelled.
 * Across repeated runs, the cancellation was consistently observed ~5.3 s after the Activity
 * started (matching the 5-second throttle cap plus the RPC round trip), leaving ~3.5-4 s of margin
 * before the Activity would have finished on its own — comfortably mid-parse, in the
 * `WRITING_DUCKDB` phase, with roughly 40% of the variants still unprocessed.
 *
 * Do not shrink this further without re-measuring: the margin above is the whole safety factor.
 * A fixture sized so total runtime is only slightly more than the 5-second worst-case observation
 * window has no slack for a slower CI machine or a slow tick of the JS watcher's 250 ms poll
 * before `handle.cancel()` fires, and the Activity would sometimes complete before the
 * cancellation could ever reach it — turning this into a flaky "activity finished, was never
 * cancelled" failure instead of a reliable proof of the cross-language cancellation contract.
 *
 * The fixture is gzipped so that "large" costs disk and network almost nothing — and, as a
 * bonus, this is the one end-to-end exercise of the gzip source path.
 */
const CANCEL_VARIANTS_PER_CHROMOSOME = 250_000;

interface HeadFacts {
  contentLength: number;
  etag: string;
  checksumSha256: string | undefined;
}

const s3 = new S3Client({
  endpoint: S3_ENDPOINT,
  region: 'us-east-1',
  forcePathStyle: true,
  credentials: { accessKeyId: S3_ACCESS_KEY, secretAccessKey: S3_SECRET_KEY },
});

/** The canonical cross-language ETag form: the header value with its quotes removed. */
function canonicalEtag(raw: string | undefined): string {
  const value = raw ?? '';
  return value.startsWith('"') && value.endsWith('"') ? value.slice(1, -1) : value;
}

async function head(bucket: string, key: string): Promise<HeadFacts> {
  const response = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
  return {
    contentLength: Number(response.ContentLength),
    etag: canonicalEtag(response.ETag),
    checksumSha256: response.Metadata?.sha256,
  };
}

async function listKeys(bucket: string): Promise<string[]> {
  const keys: string[] = [];
  let token: string | undefined;
  do {
    const page = await s3.send(
      new ListObjectsV2Command({ Bucket: bucket, ContinuationToken: token }),
    );
    for (const object of page.Contents ?? []) if (object.Key) keys.push(object.Key);
    token = page.NextContinuationToken;
  } while (token !== undefined);
  return keys.sort();
}

/** Creates a bucket and records it as this run's to remove. The only way a bucket is created. */
async function createOwnBucket(bucket: string): Promise<void> {
  await s3.send(new CreateBucketCommand({ Bucket: bucket }));
  ownedBuckets.push(bucket);
}

/** Empties and removes one bucket this run created. Never called on any other. */
async function removeOwnBucket(bucket: string): Promise<void> {
  const keys = await listKeys(bucket).catch(() => []);
  for (let index = 0; index < keys.length; index += 1000) {
    await s3.send(
      new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: { Objects: keys.slice(index, index + 1000).map((Key) => ({ Key })) },
      }),
    );
  }
  await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
}

/**
 * Writes a synthetic VCF to `destination`, a few thousand lines at a time.
 *
 * Streamed rather than joined into one string because the cancellation fixture is millions of
 * records: materialising it would cost hundreds of megabytes of heap for no reason.
 */
async function writeSyntheticVcf(
  destination: string,
  perChromosome: number,
  withMalformed: boolean,
  compress: boolean,
): Promise<void> {
  const file = fs.createWriteStream(destination);
  const stream = compress ? zlib.createGzip({ level: 1 }) : file;
  if (compress) stream.pipe(file);
  const write = async (text: string): Promise<void> => {
    if (!stream.write(text)) await once(stream, 'drain');
  };

  await write(
    '##fileformat=VCFv4.2\n' +
      '##source=TemporalRustActivityIntegrationTest\n' +
      '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tDEMO_USER\n',
  );
  for (const chrom of CHROMOSOMES) {
    let chunk = '';
    for (let index = 0; index < perChromosome; index += 1) {
      chunk += `${chrom}\t${firstPos(index)}\trs${chrom}_${index}\tA\tG\t99\tPASS\tGENE=SYN\tGT\t${
        index % 3 === 0 ? '0/1' : '1/1'
      }\n`;
      if (chunk.length >= 1 << 20) {
        await write(chunk);
        chunk = '';
      }
    }
    if (chunk.length > 0) await write(chunk);
  }
  if (withMalformed) {
    // Rejected, not fatal: too few columns, an unparseable position, an unknown contig.
    await write(
      '1\t123\trs_bad_columns\n' +
        '1\tnot-a-position\trs_bad_pos\tA\tG\t99\tPASS\tGENE=SYN\tGT\t0/1\n' +
        'chrUn_gl000220\t500\trs_bad_contig\tA\tG\t99\tPASS\tGENE=SYN\tGT\t0/1\n',
    );
  }
  stream.end();
  await once(file, 'close');
}

/** Where the generated sources are staged before upload; removed with the rest in `after`. */
let fixtureRoot = '';

/** Seeds one source object and returns the identity the contract input has to pin. */
async function seedSource(
  key: string,
  perChromosome: number,
  withMalformed: boolean,
  compress = false,
): Promise<HeadFacts> {
  const local = path.join(fixtureRoot, key.replace(/\//g, '_'));
  await writeSyntheticVcf(local, perChromosome, withMalformed, compress);
  await s3.send(
    new PutObjectCommand({
      Bucket: SOURCE_BUCKET,
      Key: key,
      Body: fs.createReadStream(local),
      ContentLength: fs.statSync(local).size,
      ContentType: 'text/plain',
    }),
  );
  fs.rmSync(local, { force: true });
  return head(SOURCE_BUCKET, key);
}

function artifactInputFor(
  datasetId: string,
  artifactBucket: string,
  sourceKey: string,
  source: HeadFacts,
): BuildDatasetArtifactInput {
  return BuildDatasetArtifactInputSchema.parse({
    contractVersion: CONTRACT_VERSION,
    datasetId,
    datasetKey: 'demo-small',
    source: {
      bucket: SOURCE_BUCKET,
      key: sourceKey,
      etag: source.etag,
      versionId: null,
      contentLength: source.contentLength,
    },
    reference: { build: 'GRCh38', version: 'demo-clinvar-grch38-v3' },
    target: {
      bucket: artifactBucket,
      artifactVersion: ARTIFACT_VERSION,
      allowedPrefix: allowedPrefixFor(datasetId, ARTIFACT_VERSION),
    },
  });
}

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

/**
 * Races `promise` against a bound, well under the Workflow's 45-minute `scheduleToCloseTimeout`.
 *
 * `handle.result()` has no timeout of its own, and this suite configures no `--test-timeout`
 * either. So the exact regression the retry and cancellation scenarios exist to catch — the Rust
 * side never producing the event the Workflow is waiting on (a repaired attempt 2 that never
 * starts, an `ActivityTaskCanceled` that never arrives) — would hang the whole suite for up to 45
 * minutes instead of failing it in seconds.
 */
async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const bound = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`timed out after ${timeoutMs}ms waiting for ${label}`)),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([promise, bound]);
  } finally {
    clearTimeout(timer);
  }
}

function decodeJsonPayload(payload: { data?: Uint8Array | null } | null | undefined): unknown {
  if (!payload?.data) return undefined;
  return JSON.parse(Buffer.from(payload.data).toString('utf8'));
}

type History = Awaited<ReturnType<WorkflowHandle['fetchHistory']>>;
type HistoryEvent = NonNullable<History['events']>[number];

/**
 * Every history event that belongs to the *Rust* activity, matched by the scheduled event it
 * refers back to.
 *
 * Filtering on the event type alone is not enough: the Workflow also runs two control-plane
 * activities, so a bare `ActivityTaskCompleted` count answers a question about the wrong
 * activity.
 */
function rustActivityEvents(history: History): HistoryEvent[] {
  const events = history.events ?? [];
  const scheduled = events.filter(
    (event) => event.activityTaskScheduledEventAttributes?.activityType?.name === RUST_ACTIVITY_TYPE,
  );
  assert.equal(scheduled.length, 1, 'exactly one buildDatasetArtifact should be scheduled');
  const scheduledEventId = String(scheduled[0]!.eventId);

  return events.filter((event) => {
    const attributes =
      event.activityTaskStartedEventAttributes ??
      event.activityTaskCompletedEventAttributes ??
      event.activityTaskFailedEventAttributes ??
      event.activityTaskCanceledEventAttributes ??
      event.activityTaskTimedOutEventAttributes ??
      event.activityTaskCancelRequestedEventAttributes;
    if (!attributes) return false;
    return String((attributes as { scheduledEventId?: unknown }).scheduledEventId) === scheduledEventId;
  });
}

/**
 * The published phase sequence must be non-decreasing through the contract's ordered phase list.
 *
 * A `Set` comparison — which is what this test used to do — cannot see a regression, and a
 * regression is exactly what a consumer polling heartbeats would misread: the run reaching
 * `FINALIZING`, falling back to `UPLOADING_PARTITION`, and reaching it again.
 */
function assertPhasesNeverRegress(phases: readonly string[], context: string): void {
  const ranks = phases.map((phase) => {
    const rank = PHASE_ORDER.indexOf(phase as (typeof PHASE_ORDER)[number]);
    assert.notEqual(rank, -1, `'${phase}' is not a contract phase`);
    return rank;
  });
  for (let index = 1; index < ranks.length; index += 1) {
    assert.ok(
      ranks[index]! >= ranks[index - 1]!,
      `${context}: the phase sequence regresses from '${phases[index - 1]}' to '${phases[index]}' — ${JSON.stringify(phases)}`,
    );
  }
}

describe('rust buildDatasetArtifact activity (cross-language ingestion gate)', () => {
  let temporalServer: ChildProcess;
  let rustWorker: ChildProcess;
  let tsWorker: Worker;
  let tsWorkerRun: Promise<void>;
  let connection: Connection;
  let nativeConnection: NativeConnection;
  let client: Client;
  let stagingRoot: string;
  const rustWorkerLog: string[] = [];

  /** The inputs the stub `inspectDatasetSource` hands to the Rust Activity, keyed by dataset. */
  const activityInputs = new Map<string, BuildDatasetArtifactInput>();
  /** The Rust Activity's results, captured by the stub `publishDataset`, keyed by dataset. */
  const artifacts = new Map<string, BuildDatasetArtifactResult>();

  /** The whole worker log with colour escapes stripped. */
  const workerLog = (): string => rustWorkerLog.join('').replace(/\[[0-9;]*m/g, '');

  /** Every attempt workspace the worker named for one dataset, in the order it started them. */
  const workspacesFor = (datasetId: string): string[] =>
    [...workerLog().matchAll(/workspace=(\S+)/g)]
      .map((match) => match[1]!)
      .filter((name) => name.includes(datasetId));

  before(async () => {
    // --- MinIO: one source bucket owned by this run, seeded with three synthetic objects -----
    await execFileAsync('docker', ['compose', 'up', '-d', 'minio'], { cwd: REPO_ROOT });
    await waitFor('minio', async () => (await fetch(`${S3_ENDPOINT}/minio/health/live`)).ok);
    fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'rust-ingestion-fixtures-'));
    await createOwnBucket(SOURCE_BUCKET);
    await createOwnBucket(ARTIFACT_BUCKET);
    // `RETRY_ARTIFACT_BUCKET` is deliberately *not* created: see the retry scenario.

    activityInputs.set(
      DATASET_ID,
      artifactInputFor(
        DATASET_ID,
        ARTIFACT_BUCKET,
        SOURCE_KEY,
        await seedSource(SOURCE_KEY, VARIANTS_PER_CHROMOSOME, true),
      ),
    );
    activityInputs.set(
      RETRY_DATASET_ID,
      artifactInputFor(
        RETRY_DATASET_ID,
        RETRY_ARTIFACT_BUCKET,
        RETRY_SOURCE_KEY,
        await seedSource(RETRY_SOURCE_KEY, RETRY_VARIANTS_PER_CHROMOSOME, false),
      ),
    );

    // --- Temporal dev server on a free port ------------------------------------------------
    const port = await freePort();
    const address = `127.0.0.1:${port}`;
    temporalServer = spawn(
      'temporal',
      ['server', 'start-dev', '--ip', '127.0.0.1', '--port', String(port), '--headless', '--log-level', 'error'],
      { stdio: ['ignore', 'pipe', 'pipe'] },
    );
    await waitFor('temporal dev server', async () => {
      const probe = await Connection.connect({ address, connectTimeout: 1000 });
      await probe.workflowService.getSystemInfo({ namespace: 'default' });
      await probe.close();
      return true;
    });
    connection = await Connection.connect({ address });
    client = new Client({ connection, namespace: 'default' });

    // --- The real Rust activity worker -----------------------------------------------------
    await execFileAsync(
      'cargo',
      ['build', '--manifest-path', 'rust-ingestion-worker/Cargo.toml', '--bin', 'temporal_worker'],
      { cwd: REPO_ROOT, env: { ...process.env, PATH: CARGO_PATH }, maxBuffer: 32 * 1024 * 1024 },
    );

    stagingRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'rust-ingestion-staging-'));
    rustWorker = spawn(path.join(REPO_ROOT, 'target/debug/temporal_worker'), [], {
      cwd: REPO_ROOT,
      env: {
        ...process.env,
        TEMPORAL_ADDRESS: address,
        TEMPORAL_NAMESPACE: 'default',
        S3_ENDPOINT,
        S3_ACCESS_KEY,
        S3_SECRET_KEY,
        S3_FORCE_PATH_STYLE: 'true',
        INGESTION_STAGING_ROOT: stagingRoot,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    rustWorker.stdout?.setEncoding('utf8');
    rustWorker.stderr?.setEncoding('utf8');
    rustWorker.stdout?.on('data', (chunk: string) => rustWorkerLog.push(chunk));
    rustWorker.stderr?.on('data', (chunk: string) => rustWorkerLog.push(chunk));
    await waitFor('rust ingestion worker', async () =>
      workerLog().includes('[rust-ingestion-worker] ready'),
    );

    // --- The production Workflow, with the two control-plane activities stubbed -------------
    // `buildDatasetArtifact` is deliberately absent: it exists only in Rust, on its own queue.
    nativeConnection = await NativeConnection.connect({ address });
    tsWorker = await Worker.create({
      connection: nativeConnection,
      namespace: 'default',
      taskQueue: CONTROL_PLANE_TASK_QUEUE,
      workflowsPath: new URL('../../ts-api-agent/src/application/workflows.ts', import.meta.url)
        .pathname,
      activities: {
        async inspectDatasetSource(datasetId: string): Promise<BuildDatasetArtifactInput> {
          const input = activityInputs.get(datasetId);
          if (!input) throw new Error(`no seeded source for '${datasetId}'`);
          return input;
        },
        async publishDataset(
          input: BuildDatasetArtifactInput,
          result: BuildDatasetArtifactResult,
        ): Promise<DatasetManifest> {
          // The production inventory validation, unchanged: prefix containment, canonical
          // ordering, partition agreement and the dataset checksum all have to hold before the
          // Rust result is allowed to become a manifest.
          const parsed = BuildDatasetArtifactResultSchema.parse(result);
          assertValidArtifactResult(input, parsed);
          artifacts.set(input.datasetId, parsed);
          // Assembled but never written: the manifest is `publishDataset`'s object to store, and
          // storing nothing is what lets this test assert the Rust Activity published none.
          return DatasetManifestSchema.parse({
            datasetId: input.datasetId,
            artifactFormat: ARTIFACT_FORMAT,
            layoutVersion: LAYOUT_VERSION,
            schemaVersion: SCHEMA_VERSION,
            schemaFingerprint: PARQUET_SCHEMA_FINGERPRINT,
            artifactVersion: input.target.artifactVersion,
            referenceVersion: input.reference.version,
            partitionSpec: [...PARTITION_SPEC],
            sortOrder: [...SORT_ORDER],
            ...parsed,
          });
        },
      },
    });
    tsWorkerRun = tsWorker.run();
  });

  after(async () => {
    if (tsWorker) {
      tsWorker.shutdown();
      await tsWorkerRun.catch(() => undefined);
    }
    await nativeConnection?.close().catch(() => undefined);
    await connection?.close().catch(() => undefined);
    rustWorker?.kill('SIGINT');
    temporalServer?.kill('SIGINT');
    await Promise.all(
      [rustWorker, temporalServer]
        .filter(Boolean)
        .map((proc) => once(proc, 'exit').catch(() => undefined)),
    );

    // The Rust worker's stdout is captured into `rustWorkerLog` rather than inherited, so a
    // failure here is otherwise diagnosed blind. Setting `RUST_WORKER_LOG_OUT=<path>` keeps it:
    // it is how the cancellation fixture below was sized, and it is the fastest way to see what
    // the worker actually did.
    if (process.env.RUST_WORKER_LOG_OUT) {
      fs.writeFileSync(process.env.RUST_WORKER_LOG_OUT, workerLog());
    }
    // Exactly the buckets `createOwnBucket` made, and nothing else.
    for (const bucket of ownedBuckets) {
      await removeOwnBucket(bucket).catch(() => undefined);
    }
    s3.destroy();
    if (stagingRoot) fs.rmSync(stagingRoot, { recursive: true, force: true });
    if (fixtureRoot) fs.rmSync(fixtureRoot, { recursive: true, force: true });
  });

  // =========================================================================================
  // 1. The whole contract, end to end
  // =========================================================================================

  describe('a successful ingestion', () => {
    let artifact: BuildDatasetArtifactResult;
    let manifest: DatasetManifest;
    /** What the *server* held as the pending Activity's heartbeat while it was running. */
    let observedHeartbeat: unknown;
    let observedIdentity = '';
    let history: History;

    before(async () => {
      const handle = await client.workflow.start('GenomicIngestionWorkflow', {
        taskQueue: CONTROL_PLANE_TASK_QUEUE,
        workflowId: `ingest-${DATASET_ID}`,
        args: [{ datasetId: DATASET_ID, datasetKey: 'demo-small' }],
      });

      // Heartbeat details only exist on the server while the Activity is pending, so they are
      // sampled concurrently with the run rather than looked for afterwards.
      let watching = true;
      const watcher = (async () => {
        while (watching) {
          try {
            const pending = (await handle.describe()).raw.pendingActivities?.[0];
            if (pending?.heartbeatDetails?.payloads?.length) {
              observedHeartbeat = decodeJsonPayload(pending.heartbeatDetails.payloads[0]);
              observedIdentity = pending.lastWorkerIdentity ?? '';
              return;
            }
          } catch {
            // The execution may close between the describe and the read; the assertions below
            // are what decide whether anything was missed.
          }
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      })();

      manifest = await handle.result();
      watching = false;
      await watcher;
      history = await handle.fetchHistory();
      artifact = artifacts.get(DATASET_ID)!;
    });

    it('schedules the production activity type on the rust task queue', () => {
      const scheduled = (history.events ?? []).filter((e) => e.activityTaskScheduledEventAttributes);
      const rust = scheduled.filter(
        (e) => e.activityTaskScheduledEventAttributes?.activityType?.name === RUST_ACTIVITY_TYPE,
      );
      assert.equal(rust.length, 1, 'exactly one buildDatasetArtifact should be scheduled');

      const attributes = rust[0].activityTaskScheduledEventAttributes!;
      assert.equal(attributes.taskQueue?.name, RUST_INGESTION_TASK_QUEUE);
      assert.deepEqual(
        decodeJsonPayload(attributes.input?.payloads?.[0]),
        activityInputs.get(DATASET_ID),
        'the Rust worker must receive the camelCase contract payload verbatim',
      );
    });

    it('is served by the rust worker identity', () => {
      const identities = (history.events ?? [])
        .filter(
          (e) =>
            e.activityTaskStartedEventAttributes !== undefined ||
            e.activityTaskCompletedEventAttributes !== undefined,
        )
        .map(
          (e) =>
            e.activityTaskStartedEventAttributes?.identity ??
            e.activityTaskCompletedEventAttributes?.identity ??
            '',
        )
        .filter((identity) => identity.startsWith(WORKER_IDENTITY_PREFIX));

      assert.ok(
        identities.length > 0,
        `expected an activity served by "${WORKER_IDENTITY_PREFIX}…", history had none`,
      );
      assert.ok(
        observedIdentity.startsWith(WORKER_IDENTITY_PREFIX),
        `the pending activity's worker identity should be the Rust worker, got "${observedIdentity}"`,
      );
    });

    it('heartbeats the frozen payload through every phase, in the contract order', () => {
      // What the server held while the activity was running. This is the *only* server-side
      // heartbeat observation available: Core throttles heartbeats to ~0.8 × `heartbeatTimeout`
      // before they reach the server, and the whole activity finishes well inside one 15-second
      // window, so `observedHeartbeat` can only ever be the first heartbeat of the run.
      // Everything below about the *sequence* therefore comes from the worker's own stdout — the
      // log line is emitted from the single call site that calls `record_heartbeat`, never from
      // a second code path, but it is a worker observation and not a server one.
      const pending = IngestionHeartbeatSchema.parse(observedHeartbeat);
      assert.ok(pending.processedBytes >= 0);

      // The keepalive re-sends the last observation verbatim while a long uninterruptible stage
      // runs, so consecutive identical payloads are collapsed — they are one observation,
      // repeated.
      const rendered = [...workerLog().matchAll(/heartbeat=(\{.*?\})/g)].map((match) => match[1]!);
      const logged = rendered
        .filter((payload, index) => index === 0 || payload !== rendered[index - 1])
        .map((payload) => IngestionHeartbeatSchema.parse(JSON.parse(payload)));
      assert.ok(logged.length >= 6, `expected heartbeats for every phase, got ${logged.length}`);
      assert.equal(logged[0].phase, 'DOWNLOADING_SOURCE');
      assert.equal(logged.at(-1)!.phase, 'FINALIZING');
      assert.deepEqual(
        [...new Set(logged.map((beat) => beat.phase))].sort(),
        [...PHASE_ORDER].sort(),
        'every contract phase must be reported',
      );

      // The regression a set comparison cannot see: the processor's own last event used to be
      // published as FINALIZING, so the run reached the terminal phase, regressed to
      // UPLOADING_PARTITION, and reached it again.
      assertPhasesNeverRegress(
        logged.map((beat) => beat.phase),
        'the successful run',
      );
      assert.equal(
        logged.filter((beat) => beat.phase === 'FINALIZING').length,
        1,
        'FINALIZING is terminal and published exactly once',
      );

      // The counters describe cumulative work, so a consumer polling heartbeats is entitled to
      // read them as progress — the same entitlement `assertPhasesNeverRegress` protects for the
      // phase. The regression this catches: a stage that builds its event from
      // `ProgressEvent::phase(...)`, which zeroes every counter, does not leave the published
      // picture alone — the projection *assigns* these fields, so it resets them. The export did
      // exactly that, and the published sequence went true totals → zero for the whole export
      // phase → true totals again. A set comparison of phases cannot see it, and neither can a
      // Rust-side test that stops at the `ProgressEvent`.
      for (const field of ['processedBytes', 'processedVariants', 'completedFiles'] as const) {
        assert.ok(
          logged.every((beat, index) => index === 0 || beat[field] >= logged[index - 1]![field]),
          `${field} must never regress across the published heartbeats, got ` +
            JSON.stringify(logged.map((beat) => ({ phase: beat.phase, [field]: beat[field] }))),
        );
      }

      const uploading = logged.filter((beat) => beat.phase === 'UPLOADING_PARTITION');
      assert.equal(uploading.length, CHROMOSOMES.length, 'one heartbeat per uploaded partition');
      assert.deepEqual(
        uploading.map((beat) => beat.currentPartition),
        [...CHROMOSOMES].sort(),
        'partitions are uploaded in the canonical byte-wise order',
      );
      assert.ok(
        uploading.every((beat, index) => index === 0 || beat.uploadedBytes > uploading[index - 1]!.uploadedBytes),
        'uploadedBytes must accumulate',
      );
      assert.equal(logged.at(-1)!.processedVariants, EXPECTED_VARIANTS);
    });

    it('returns the complete inventory with per-file checksums and statistics', () => {
      assert.equal(artifact.attemptPrefix, ATTEMPT_PREFIX);
      assert.equal(artifact.variantCount, EXPECTED_VARIANTS);
      assert.equal(artifact.rejectedRecordCount, MALFORMED_RECORDS);
      assert.equal(artifact.referenceBuild, 'GRCh38');
      assert.match(artifact.processorVersion, /^rust-ingestion-worker\/\d+\.\d+\.\d+$/);

      // Canonical `(chrom, relativePath)` order, byte-wise — not numeric.
      assert.deepEqual(
        artifact.parquetObjects.map((object) => object.chrom),
        [...CHROMOSOMES].sort(),
      );

      for (const object of artifact.parquetObjects) {
        assert.equal(object.bucket, ARTIFACT_BUCKET);
        assert.equal(
          object.key,
          `${ATTEMPT_PREFIX}${VARIANTS_SEGMENT}chrom=${object.chrom}/part-000.parquet`,
        );
        assert.match(object.checksumSha256, /^[0-9a-f]{64}$/);
        assert.ok(object.byteSize > 0, `${object.key} must declare its size`);
        assert.ok(object.etag.length > 0);
        assert.equal(object.versionId, null, 'the dev bucket is unversioned');
        assert.equal(object.rowCount, VARIANTS_PER_CHROMOSOME);
        assert.equal(object.minPos, firstPos(0));
        assert.equal(object.maxPos, lastPos);
      }

      assert.equal(
        artifact.parquetObjects.reduce((total, object) => total + object.rowCount, 0),
        artifact.variantCount,
      );
      // Distinct content per partition: a mapping bug that published one file under every key
      // would otherwise pass every check above.
      assert.equal(
        new Set(artifact.parquetObjects.map((object) => object.checksumSha256)).size,
        CHROMOSOMES.length,
      );
    });

    it('agrees with the typescript dataset checksum', () => {
      assert.match(artifact.datasetChecksumSha256, /^[0-9a-f]{64}$/);
      assert.equal(
        computeDatasetChecksumSha256(artifact.attemptPrefix, artifact.parquetObjects),
        artifact.datasetChecksumSha256,
        'the Rust checksum must reproduce under the TypeScript canonicalisation',
      );
      // The full production validation ran inside `publishDataset`; the manifest it produced is
      // the proof it passed.
      assert.equal(manifest.datasetChecksumSha256, artifact.datasetChecksumSha256);
      assert.equal(manifest.attemptPrefix, ATTEMPT_PREFIX);
      assert.deepEqual(manifest.parquetObjects, artifact.parquetObjects);
    });

    it('publishes objects that satisfy the typescript verifier, and no manifest', async () => {
      for (const object of artifact.parquetObjects) {
        const facts = await head(object.bucket, object.key);
        assert.equal(facts.contentLength, object.byteSize, `${object.key} SIZE_MISMATCH`);
        assert.equal(facts.etag, object.etag, `${object.key} ETAG_MISMATCH`);
        assert.equal(
          facts.checksumSha256,
          object.checksumSha256,
          `${object.key} CHECKSUM_METADATA_MISMATCH`,
        );
      }

      const written = await listKeys(ARTIFACT_BUCKET);
      assert.deepEqual(
        written,
        artifact.parquetObjects.map((object) => object.key).sort(),
        'the activity must write exactly its inventory — no manifest, no stray objects',
      );
      for (const key of written) {
        assert.ok(
          key.startsWith(`${ATTEMPT_PREFIX}${VARIANTS_SEGMENT}`),
          `'${key}' escaped the attempt prefix`,
        );
      }
    });

    it('leaves no local staging state behind', () => {
      assert.deepEqual(
        fs.readdirSync(stagingRoot),
        [],
        'the attempt workspace must be removed once the activity completes',
      );
    });
  });

  // =========================================================================================
  // 2. A retry runs under its own prefix and its own workspace
  // =========================================================================================

  describe('a retried ingestion', () => {
    let artifact: BuildDatasetArtifactResult;
    let history: History;
    const observedAttempts: number[] = [];
    /** The attempt-1 failure as the *server* reported it on the pending activity. */
    let observedFailureType = '';

    before(async () => {
      /*
       * How attempt 1 is made to fail, and why this way.
       *
       * The failure has to be *retryable* under the frozen taxonomy, has to be induced from
       * outside the worker (a test-only branch inside the production Activity would prove less
       * than nothing), and has to be repairable between attempts.
       *
       * Withholding the artifact bucket does all three. Attempt 1 downloads, builds locally, and
       * then fails its first `PutObject` with `NoSuchBucket`; `object_store::upload_error` maps
       * a 404 on the publish path onto `ObjectStoreUnavailable`, which the taxonomy classifies
       * as retryable, so Temporal schedules attempt 2. Mutating the *source* object cannot be
       * used for this: a changed or missing source is `SourceObjectChanged`, which is
       * deliberately non-retryable and would end the workflow at attempt 1.
       *
       * The repair window is not the 1-second retry backoff — it is that plus the whole of
       * attempt 2's local build, which is seconds. And losing the race is not silent: attempt 3
       * would publish under `attempt-3/` and the assertions below would fail.
       */
      const handle = await client.workflow.start('GenomicIngestionWorkflow', {
        taskQueue: CONTROL_PLANE_TASK_QUEUE,
        workflowId: `ingest-${RETRY_DATASET_ID}`,
        args: [{ datasetId: RETRY_DATASET_ID, datasetKey: 'demo-small' }],
      });

      let repaired = false;
      let watching = true;
      const watcher = (async () => {
        while (watching) {
          try {
            const pending = (await handle.describe()).raw.pendingActivities?.[0];
            const attempt = pending?.attempt ?? 0;
            if (attempt > 0) observedAttempts.push(attempt);
            const failureType = pending?.lastFailure?.applicationFailureInfo?.type;
            if (failureType) observedFailureType = failureType;
            if (!repaired && attempt >= 2) {
              // Flip the flag only once the bucket genuinely exists. Setting it first and
              // letting `createOwnBucket` fail underneath the surrounding `catch` would leave
              // `repaired` permanently `true` with no bucket ever created — the watcher would
              // never retry, and the workflow would keep failing until the 45-minute
              // `scheduleToCloseTimeout` instead of the few seconds this test expects.
              await createOwnBucket(RETRY_ARTIFACT_BUCKET);
              repaired = true;
            }
          } catch {
            // The execution may close between the describe and the read — or `createOwnBucket`
            // above may fail (e.g. a transient MinIO hiccup); either way `repaired` is left
            // `false` and the next tick tries again.
          }
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      })();

      // Bounded well under the 45-minute `scheduleToCloseTimeout`: if the repair above never
      // lands, or attempt 2 never completes, this scenario must fail fast rather than hang the
      // suite.
      await withTimeout(handle.result(), 120_000, 'the retried workflow to complete');
      watching = false;
      await watcher;
      history = await handle.fetchHistory();
      artifact = artifacts.get(RETRY_DATASET_ID)!;
      assert.ok(repaired, 'the test never observed attempt 2 starting');
    });

    it('fails attempt 1 with a retryable failure and completes on attempt 2', () => {
      // `rustActivityEvents` asserts there is exactly one scheduled event: a retry is the same
      // scheduled activity run again, not a second one.
      const own = rustActivityEvents(history);

      // Server-side retries do not write an `ActivityTaskFailed` per attempt — only the *last*
      // `ActivityTaskStarted` reaches history, carrying the attempt number it ran under and the
      // failure that caused the previous one.
      const started = own
        .map((e) => e.activityTaskStartedEventAttributes)
        .filter((attributes) => attributes !== undefined && attributes !== null);
      assert.equal(started.length, 1, 'one rust activity execution reached history');
      assert.equal(
        started[0]!.attempt,
        2,
        'the activity that produced the result must be the second attempt',
      );
      assert.equal(
        started[0]!.lastFailure?.applicationFailureInfo?.type,
        'ObjectStoreUnavailable',
        'the induced failure must be the retryable object-store one',
      );
      assert.notEqual(
        started[0]!.lastFailure?.applicationFailureInfo?.nonRetryable,
        true,
        'a non-retryable failure would have ended the workflow at attempt 1',
      );

      const completed = own.filter((e) => e.activityTaskCompletedEventAttributes);
      assert.equal(completed.length, 1, 'the retried rust activity must have completed');

      // The same two facts, observed live off the pending activity rather than off history.
      assert.ok(
        observedAttempts.includes(2),
        `the server must have reported a second attempt, saw ${JSON.stringify([...new Set(observedAttempts)])}`,
      );
      assert.equal(observedFailureType, 'ObjectStoreUnavailable');
    });

    it('publishes attempt 2 under its own prefix, with nothing under attempt 1', async () => {
      assert.equal(
        artifact.attemptPrefix,
        `${RETRY_ALLOWED_PREFIX}attempt-2/`,
        'the published prefix must be the retry attempt, not the first',
      );
      assert.notEqual(artifact.attemptPrefix, `${RETRY_ALLOWED_PREFIX}attempt-1/`);
      assert.equal(artifact.variantCount, RETRY_EXPECTED_VARIANTS);

      const written = await listKeys(RETRY_ARTIFACT_BUCKET);
      assert.ok(written.length > 0, 'attempt 2 must have published its inventory');
      assert.deepEqual(
        written,
        artifact.parquetObjects.map((object) => object.key).sort(),
        'exactly the inventory attempt 2 declared — no leftovers from attempt 1',
      );
      for (const key of written) {
        assert.ok(
          key.startsWith(`${RETRY_ALLOWED_PREFIX}attempt-2/`),
          `'${key}' was not written under attempt 2's own prefix`,
        );
      }
      assert.equal(
        written.filter((key) => key.startsWith(`${RETRY_ALLOWED_PREFIX}attempt-1/`)).length,
        0,
        'no retry may append to a previous attempt\'s prefix',
      );
    });

    it('gives each attempt its own local workspace, and removes both', () => {
      const workspaces = workspacesFor(RETRY_DATASET_ID);
      assert.equal(workspaces.length, 2, `expected two attempt workspaces, got ${JSON.stringify(workspaces)}`);
      assert.notEqual(
        workspaces[0],
        workspaces[1],
        'attempt 2 must not reuse attempt 1\'s staging database or export directory',
      );
      assert.ok(workspaces[0]!.endsWith('-attempt-1'), workspaces[0]);
      assert.ok(workspaces[1]!.endsWith('-attempt-2'), workspaces[1]);

      assert.deepEqual(
        fs.readdirSync(stagingRoot),
        [],
        'both attempts must have removed their workspaces',
      );
    });

    it('still agrees with the typescript dataset checksum', () => {
      assert.equal(
        computeDatasetChecksumSha256(artifact.attemptPrefix, artifact.parquetObjects),
        artifact.datasetChecksumSha256,
        'the checksum is content identity and must not depend on which attempt produced it',
      );
    });
  });

  // =========================================================================================
  // 3. Cancelling a running activity across the language boundary
  // =========================================================================================

  describe('a cancelled ingestion', () => {
    let history: History;
    let workflowStatus = '';
    let resultError: unknown;
    let sourceBefore: HeadFacts;

    before(async () => {
      await createOwnBucket(CANCEL_ARTIFACT_BUCKET);
      await s3.send(
        new PutObjectCommand({
          Bucket: CANCEL_ARTIFACT_BUCKET,
          Key: CANARY_KEY,
          Body: Buffer.from(CANARY_BODY, 'utf8'),
        }),
      );
      sourceBefore = await seedSource(CANCEL_SOURCE_KEY, CANCEL_VARIANTS_PER_CHROMOSOME, false, true);
      activityInputs.set(
        CANCEL_DATASET_ID,
        artifactInputFor(
          CANCEL_DATASET_ID,
          CANCEL_ARTIFACT_BUCKET,
          CANCEL_SOURCE_KEY,
          sourceBefore,
        ),
      );

      const handle = await client.workflow.start('GenomicIngestionWorkflow', {
        taskQueue: CONTROL_PLANE_TASK_QUEUE,
        workflowId: `ingest-${CANCEL_DATASET_ID}`,
        args: [{ datasetId: CANCEL_DATASET_ID, datasetKey: 'demo-small' }],
      });

      // Cancel only once the Rust Activity is demonstrably in flight — the server has to be
      // holding a heartbeat from it — so the cancellation is delivered to a running attempt.
      await waitFor(
        'the rust activity to start heartbeating',
        async () => {
          const pending = (await handle.describe()).raw.pendingActivities?.[0];
          return Boolean(pending?.heartbeatDetails?.payloads?.length);
        },
        120_000,
      );
      await handle.cancel();

      // Bounded well under the 45-minute `scheduleToCloseTimeout`: this is exactly finding 4's
      // regression guard — a Rust side that never produces `ActivityTaskCanceled` must fail this
      // scenario in seconds, not hang the suite.
      resultError = await withTimeout(
        handle.result().then(
          (value) => new Error(`the cancelled workflow returned ${JSON.stringify(value)}`),
          (error: unknown) => error,
        ),
        120_000,
        'the cancelled workflow to close',
      );
      workflowStatus = (await handle.describe()).status.name;
      history = await handle.fetchHistory();
    });

    it('records an ActivityTaskCanceled in server history, not a failure or a timeout', () => {
      const own = rustActivityEvents(history);
      const outcome = own.map((event) => event.eventType).join(', ');
      const count = (predicate: (event: HistoryEvent) => unknown): number =>
        own.filter((event) => predicate(event)).length;

      assert.equal(
        count((e) => e.activityTaskCancelRequestedEventAttributes),
        1,
        'WAIT_CANCELLATION_COMPLETED must have asked the activity to cancel',
      );
      assert.equal(
        count((e) => e.activityTaskCanceledEventAttributes),
        1,
        `the Rust activity must answer with ActivityTaskCanceled; it produced ${outcome}`,
      );
      assert.equal(
        count((e) => e.activityTaskFailedEventAttributes),
        0,
        `a cancellation must not be recorded as a failure; it produced ${outcome}`,
      );
      assert.equal(
        count((e) => e.activityTaskTimedOutEventAttributes),
        0,
        `a cancellation must not be left to time out; it produced ${outcome}`,
      );
      assert.equal(
        count((e) => e.activityTaskCompletedEventAttributes),
        0,
        `the activity must not have run to completion — the fixture would be too small; it produced ${outcome}`,
      );

      // The Workflow blocks on the cancellation response, so it can only close once the Rust
      // side has produced one. Closing as CANCELLED is that having happened.
      assert.equal(workflowStatus, 'CANCELLED', `the workflow closed as ${workflowStatus}`);
      assert.ok(resultError instanceof Error, 'a cancelled workflow must not resolve');
    });

    it('removes the local workspace it created', () => {
      assert.equal(
        workspacesFor(CANCEL_DATASET_ID).length,
        1,
        'exactly one attempt should have run',
      );
      assert.deepEqual(
        fs.readdirSync(stagingRoot),
        [],
        'the cancelled attempt must remove its own staging database and export directory',
      );
    });

    it('deletes nothing in the object store', async () => {
      // The sentinel this run seeded is untouched: the Activity's cleanup is local-only.
      const canary = await head(CANCEL_ARTIFACT_BUCKET, CANARY_KEY);
      assert.equal(canary.contentLength, Buffer.byteLength(CANARY_BODY));

      // The pinned source is exactly the object the workflow was scheduled against.
      const sourceAfter = await head(SOURCE_BUCKET, CANCEL_SOURCE_KEY);
      assert.equal(sourceAfter.etag, sourceBefore.etag);
      assert.equal(sourceAfter.contentLength, sourceBefore.contentLength);

      // Anything the attempt did upload before it stopped stays orphaned rather than deleted,
      // and cannot escape its own prefix. No manifest exists, so no query path can reach it.
      const written = await listKeys(CANCEL_ARTIFACT_BUCKET);
      for (const key of written) {
        if (key === CANARY_KEY) continue;
        assert.ok(
          key.startsWith(`${CANCEL_ALLOWED_PREFIX}attempt-1/${VARIANTS_SEGMENT}`),
          `'${key}' escaped the cancelled attempt's prefix`,
        );
      }
      assert.equal(
        written.filter((key) => key.endsWith('manifest.json')).length,
        0,
        'no manifest may exist for a cancelled ingestion',
      );
      assert.equal(artifacts.has(CANCEL_DATASET_ID), false, 'publishDataset must never have run');
    });
  });
});
