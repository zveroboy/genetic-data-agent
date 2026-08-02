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
 * The two control-plane Activities are the only substitutes: `inspectDatasetSource` is replaced
 * by a stub that pins the per-run source object this test seeded, and `publishDataset` runs the
 * production validation but does not write a manifest — no manifest is written anywhere in this
 * test, which is also the assertion that the Rust Activity does not write one either.
 *
 * Requires a running MinIO (`docker compose up -d minio`) and the `temporal` CLI. Every S3
 * bucket, local directory and server process it uses is created by this run, under a per-run
 * name, and removed again in `after`.
 */
import assert from 'node:assert/strict';
import { execFile, spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
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

const S3_ENDPOINT = process.env.S3_ENDPOINT ?? 'http://localhost:9000';
const S3_ACCESS_KEY = process.env.S3_ACCESS_KEY ?? 'admin';
const S3_SECRET_KEY = process.env.S3_SECRET_KEY ?? 'password123';

/**
 * Per-run names. This test runs against the developer's shared MinIO, so it must never touch a
 * bucket it did not create; `after` removes exactly these two and nothing else.
 */
const RUN_ID = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const SOURCE_BUCKET = `rust-ingest-src-${RUN_ID}`;
const ARTIFACT_BUCKET = `rust-ingest-art-${RUN_ID}`;
const SOURCE_KEY = 'samples/demo_user.vcf';

const DATASET_ID = `ds-e2e-${RUN_ID.replace(/[^a-z0-9]/gi, '')}`;
const ARTIFACT_VERSION = 'iv-e2e-1';
const ALLOWED_PREFIX = allowedPrefixFor(DATASET_ID, ARTIFACT_VERSION);
const ATTEMPT_PREFIX = `${ALLOWED_PREFIX}attempt-1/`;

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

/** Empties and removes one of the two buckets this run created. Never called on any other. */
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

function syntheticVcf(): string {
  const lines = [
    '##fileformat=VCFv4.2',
    '##source=TemporalRustActivityIntegrationTest',
    '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tDEMO_USER',
  ];
  for (const chrom of CHROMOSOMES) {
    for (let index = 0; index < VARIANTS_PER_CHROMOSOME; index += 1) {
      const pos = firstPos(index);
      lines.push(
        `${chrom}\t${pos}\trs${chrom}_${index}\tA\tG\t99\tPASS\tGENE=SYN\tGT\t${
          index % 3 === 0 ? '0/1' : '1/1'
        }`,
      );
    }
  }
  // Rejected, not fatal: too few columns, an unparseable position, an unknown contig.
  lines.push('1\t123\trs_bad_columns');
  lines.push('1\tnot-a-position\trs_bad_pos\tA\tG\t99\tPASS\tGENE=SYN\tGT\t0/1');
  lines.push('chrUn_gl000220\t500\trs_bad_contig\tA\tG\t99\tPASS\tGENE=SYN\tGT\t0/1');
  return `${lines.join('\n')}\n`;
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

function decodeJsonPayload(payload: { data?: Uint8Array | null } | null | undefined): unknown {
  if (!payload?.data) return undefined;
  return JSON.parse(Buffer.from(payload.data).toString('utf8'));
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

  /** The input the stub `inspectDatasetSource` hands to the Rust Activity. */
  let activityInput: BuildDatasetArtifactInput;
  /** The Rust Activity's result, captured by the stub `publishDataset`. */
  let artifact: BuildDatasetArtifactResult;
  let manifest: DatasetManifest;
  /** What the *server* held as the pending Activity's heartbeat while it was running. */
  let observedHeartbeat: unknown;
  let observedIdentity = '';
  let history: Awaited<ReturnType<WorkflowHandle['fetchHistory']>>;

  before(async () => {
    // --- MinIO: two buckets owned by this run, seeded with one synthetic source object ----
    await execFileAsync('docker', ['compose', 'up', '-d', 'minio'], { cwd: REPO_ROOT });
    await waitFor('minio', async () => (await fetch(`${S3_ENDPOINT}/minio/health/live`)).ok);
    await s3.send(new CreateBucketCommand({ Bucket: SOURCE_BUCKET }));
    await s3.send(new CreateBucketCommand({ Bucket: ARTIFACT_BUCKET }));

    const body = Buffer.from(syntheticVcf(), 'utf8');
    await s3.send(
      new PutObjectCommand({ Bucket: SOURCE_BUCKET, Key: SOURCE_KEY, Body: body, ContentType: 'text/plain' }),
    );
    const source = await head(SOURCE_BUCKET, SOURCE_KEY);

    activityInput = BuildDatasetArtifactInputSchema.parse({
      contractVersion: CONTRACT_VERSION,
      datasetId: DATASET_ID,
      datasetKey: 'demo-small',
      source: {
        bucket: SOURCE_BUCKET,
        key: SOURCE_KEY,
        etag: source.etag,
        versionId: null,
        contentLength: source.contentLength,
      },
      reference: { build: 'GRCh38', version: 'demo-clinvar-grch38-v1' },
      target: {
        bucket: ARTIFACT_BUCKET,
        artifactVersion: ARTIFACT_VERSION,
        allowedPrefix: ALLOWED_PREFIX,
      },
    });

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
      rustWorkerLog.join('').includes('[rust-ingestion-worker] ready'),
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
        async inspectDatasetSource(): Promise<BuildDatasetArtifactInput> {
          return activityInput;
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
          artifact = parsed;
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

    // --- One production workflow execution, watched while it runs ---------------------------
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

    for (const bucket of [SOURCE_BUCKET, ARTIFACT_BUCKET]) {
      await removeOwnBucket(bucket).catch(() => undefined);
    }
    s3.destroy();
    if (stagingRoot) fs.rmSync(stagingRoot, { recursive: true, force: true });
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
      activityInput,
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

  it('heartbeats the frozen payload through every phase', () => {
    // What the server held while the activity was running.
    const pending = IngestionHeartbeatSchema.parse(observedHeartbeat);
    assert.ok(pending.processedBytes >= 0);

    // The complete, ordered account: heartbeats are throttled before they reach the server, so
    // the worker's own log of every `record_heartbeat` call is what shows all six phases. The
    // keepalive re-sends the last observation verbatim while a long uninterruptible stage runs,
    // so consecutive identical payloads are collapsed — they are one observation, repeated.
    const workerLog = rustWorkerLog.join('').replace(/\u001b\[[0-9;]*m/g, '');
    const rendered = [...workerLog.matchAll(/heartbeat=(\{.*?\})/g)].map((match) => match[1]!);
    const logged = rendered
      .filter((payload, index) => index === 0 || payload !== rendered[index - 1])
      .map((payload) => IngestionHeartbeatSchema.parse(JSON.parse(payload)));
    assert.ok(logged.length >= 6, `expected heartbeats for every phase, got ${logged.length}`);
    assert.equal(logged[0].phase, 'DOWNLOADING_SOURCE');
    assert.equal(logged.at(-1)!.phase, 'FINALIZING');
    assert.deepEqual(
      [...new Set(logged.map((beat) => beat.phase))].sort(),
      [
        'DOWNLOADING_SOURCE',
        'EXPORTING_PARQUET',
        'FINALIZING',
        'PARSING',
        'UPLOADING_PARTITION',
        'WRITING_DUCKDB',
      ],
      'every contract phase must be reported',
    );

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
