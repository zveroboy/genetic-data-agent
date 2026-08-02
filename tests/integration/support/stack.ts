/**
 * The pieces every cross-language end-to-end test needs, in one place.
 *
 * Each test file brings up its **own** Temporal dev server on a free port and its **own**
 * per-run S3 buckets, and shuts both down again. That isolation is deliberate:
 * `docker compose up` also runs a `ts-control-worker` and a `rust-ingestion-worker` against the
 * compose Temporal, and those containers carry the *production* seeded catalog. A test that
 * shared their namespace would have its Workflow tasks answered by a Worker that knows nothing
 * about the sources the test seeded.
 *
 * Nothing here deletes anything it did not create. Buckets are minted with a per-run suffix and
 * tracked in `OwnedBuckets`; the MinIO container itself is started, never removed.
 */
import assert from 'node:assert/strict';
import { execFile, spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import zlib from 'node:zlib';

import {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectsCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { Client, Connection } from '@temporalio/client';
import { NativeConnection, Worker } from '@temporalio/worker';
import type { Hono } from 'hono';

import {
  type ControlPlaneActivities,
  createControlPlaneActivities,
  type DatasetSourceCatalog,
} from '../../../ts-api-agent/src/application/control-plane-activities.ts';
import { CONTROL_PLANE_TASK_QUEUE } from '../../../ts-api-agent/src/application/workflows.ts';
import type { DatasetCatalogEntry, DatasetKey } from '../../../ts-api-agent/src/domain/datasets.ts';
import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../../ts-api-agent/src/domain/datasets.ts';
import { nodeListener } from '../../../ts-api-agent/src/http/node-listener.ts';
import type { ObjectStore } from '../../../ts-api-agent/src/infrastructure/object-store/object-store.ts';
import { S3ObjectStore } from '../../../ts-api-agent/src/infrastructure/object-store/s3-object-store.ts';

const execFileAsync = promisify(execFile);

export const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');

/**
 * `cargo` is installed through rustup and is not on a login shell's PATH on every machine. The
 * directory to prepend is not guessable in general — it depends on how rustup was installed —
 * so it is read from `CARGO_BIN_DIR` when set (`export CARGO_BIN_DIR="$(rustup which cargo |
 * xargs dirname)"` is the portable way to get it, and is what `GUIDE.md` recommends) and falls
 * back to this machine's Homebrew layout only when that variable is absent. Prepending is
 * harmless even when `cargo` is already reachable another way.
 */
const CARGO_BIN_DIR = process.env.CARGO_BIN_DIR ?? '/opt/homebrew/opt/rustup/bin';
export const CARGO_PATH = `${CARGO_BIN_DIR}:${process.env.PATH ?? ''}`;

export const S3_ENDPOINT = process.env.S3_ENDPOINT ?? 'http://localhost:9000';
export const S3_ACCESS_KEY = process.env.S3_ACCESS_KEY ?? 'admin';
export const S3_SECRET_KEY = process.env.S3_SECRET_KEY ?? 'password123';
export const S3_REGION = process.env.S3_REGION ?? 'us-east-1';

export const RUST_WORKER_READY_LINE = '[rust-ingestion-worker] ready';

/** A short, filesystem- and bucket-safe token unique to one test process. */
export function newRunId(): string {
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
}

export async function freePort(): Promise<number> {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address() as net.AddressInfo;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
}

export async function waitFor(
  label: string,
  predicate: () => Promise<boolean>,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`timed out waiting for ${label}: ${String(lastError)}`);
}

/**
 * Races a promise against a bound.
 *
 * `handle.result()` has no timeout of its own, so a Workflow that never completes — the exact
 * regression these suites exist to catch — would otherwise hang until the runner's own timeout.
 */
export async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const bound = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () => reject(new Error(`timed out after ${timeoutMs}ms waiting for ${label}`)),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([promise, bound]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

// ---------------------------------------------------------------------------------------------
// MinIO and S3
// ---------------------------------------------------------------------------------------------

export function newS3Client(): S3Client {
  return new S3Client({
    endpoint: S3_ENDPOINT,
    region: S3_REGION,
    forcePathStyle: true,
    credentials: { accessKeyId: S3_ACCESS_KEY, secretAccessKey: S3_SECRET_KEY },
  });
}

export function newObjectStore(): S3ObjectStore {
  return new S3ObjectStore({
    endpoint: S3_ENDPOINT,
    region: S3_REGION,
    accessKeyId: S3_ACCESS_KEY,
    secretAccessKey: S3_SECRET_KEY,
    forcePathStyle: true,
  });
}

/** Brings up the compose MinIO service and waits for its health endpoint. */
export async function startMinio(): Promise<void> {
  await execFileAsync('docker', ['compose', 'up', '-d', 'minio'], { cwd: REPO_ROOT });
  await waitFor('minio', async () => (await fetch(`${S3_ENDPOINT}/minio/health/live`)).ok);
}

/** The canonical cross-language ETag form: the header value with its quotes removed. */
export function canonicalEtag(raw: string | undefined): string {
  const value = raw ?? '';
  return value.startsWith('"') && value.endsWith('"') ? value.slice(1, -1) : value;
}

export interface HeadFacts {
  readonly contentLength: number;
  readonly etag: string;
  readonly checksumSha256: string | undefined;
  readonly lastModified: number;
}

export async function headObject(
  s3: S3Client,
  bucket: string,
  key: string,
): Promise<HeadFacts> {
  const response = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
  return {
    contentLength: Number(response.ContentLength),
    etag: canonicalEtag(response.ETag),
    checksumSha256: response.Metadata?.sha256,
    lastModified: response.LastModified?.getTime() ?? 0,
  };
}

export async function listKeys(s3: S3Client, bucket: string): Promise<string[]> {
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

/**
 * Buckets one test run created, and the only buckets it will ever remove.
 *
 * `removeAll` is the single teardown entry point; nothing in these suites deletes a bucket,
 * prefix or object it did not itself create in the same run.
 */
export class OwnedBuckets {
  private readonly names: string[] = [];
  private readonly s3: S3Client;

  // Node runs TypeScript in strip-only mode: a parameter property would be erased to nothing.
  constructor(s3: S3Client) {
    this.s3 = s3;
  }

  async create(name: string): Promise<string> {
    await this.s3.send(new CreateBucketCommand({ Bucket: name }));
    this.names.push(name);
    return name;
  }

  async removeAll(): Promise<void> {
    for (const bucket of this.names) {
      try {
        const keys = await listKeys(this.s3, bucket);
        for (let index = 0; index < keys.length; index += 1000) {
          await this.s3.send(
            new DeleteObjectsCommand({
              Bucket: bucket,
              Delete: { Objects: keys.slice(index, index + 1000).map((Key) => ({ Key })) },
            }),
          );
        }
        await this.s3.send(new DeleteBucketCommand({ Bucket: bucket }));
      } catch {
        // Teardown is best effort; a bucket that is already gone is not a failure.
      }
    }
    this.names.length = 0;
  }
}

export interface VcfRecord {
  readonly chrom: string;
  readonly pos: number;
  readonly rsid: string;
  readonly ref: string;
  readonly alt: string;
  readonly gt: string;
}

/** One synthetic chromosome's worth of rows, generated rather than materialised up front. */
export interface VcfPartitionSpec {
  readonly chrom: string;
  readonly count: number;
  /** Position of row `index`. */
  readonly pos: (index: number) => number;
  /** Optional per-row override, used to plant a known clinical target. */
  readonly override?: (index: number) => Partial<VcfRecord> | undefined;
}

/**
 * Writes a synthetic VCF, streaming so a several-hundred-thousand-row fixture never has to be
 * held in memory as one string.
 *
 * Rows are emitted in ascending `pos` per chromosome. The ingestion path sorts anyway — that is
 * the point of `sortOrder` — but a fixture that is already sorted would not prove much, so
 * `descending` writes each partition backwards to force the producer to do the ordering.
 */
export async function writeSyntheticVcf(
  destination: string,
  partitions: readonly VcfPartitionSpec[],
  options: { compress?: boolean; malformedRecords?: boolean; descending?: boolean } = {},
): Promise<void> {
  const file = fs.createWriteStream(destination);
  const stream = options.compress === true ? zlib.createGzip({ level: 1 }) : file;
  if (options.compress === true) stream.pipe(file);
  const write = async (text: string): Promise<void> => {
    if (!stream.write(text)) await once(stream, 'drain');
  };

  await write(
    '##fileformat=VCFv4.2\n' +
      '##source=CrossLanguageIngestionIntegrationTest\n' +
      '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tDEMO_USER\n',
  );

  for (const partition of partitions) {
    let chunk = '';
    for (let step = 0; step < partition.count; step += 1) {
      const index = options.descending === true ? partition.count - 1 - step : step;
      const base: VcfRecord = {
        chrom: partition.chrom,
        pos: partition.pos(index),
        rsid: `rs${partition.chrom}_${index}`,
        ref: 'A',
        alt: 'G',
        gt: index % 3 === 0 ? '0/1' : '1/1',
      };
      const record = { ...base, ...(partition.override?.(index) ?? {}) };
      chunk +=
        `${record.chrom}\t${record.pos}\t${record.rsid}\t${record.ref}\t${record.alt}\t` +
        `99\tPASS\tGENE=SYN\tGT\t${record.gt}\n`;
      if (chunk.length >= 1 << 20) {
        await write(chunk);
        chunk = '';
      }
    }
    if (chunk.length > 0) await write(chunk);
  }

  if (options.malformedRecords === true) {
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

/** Uploads a local file as a source object and returns the identity the contract pins. */
export async function putSourceObject(
  s3: S3Client,
  bucket: string,
  key: string,
  localPath: string,
): Promise<HeadFacts> {
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: fs.createReadStream(localPath),
      ContentLength: fs.statSync(localPath).size,
      ContentType: 'application/octet-stream',
    }),
  );
  return headObject(s3, bucket, key);
}

/**
 * A catalog entry the test process seeds itself.
 *
 * The key must still be one of the two allowlisted `DatasetKey`s — the wire schema is
 * `z.enum(DATASET_KEYS)` and rejects anything else — so a test pins an existing key to a source
 * object it uploaded, rather than inventing a third key the contract would refuse.
 */
export function testCatalogEntry(
  key: DatasetKey,
  bucket: string,
  objectKey: string,
): DatasetCatalogEntry {
  return Object.freeze({
    key,
    displayName: `Test source for ${key}`,
    description: 'Seeded by an integration test, inside the test process only.',
    source: Object.freeze({ bucket, key: objectKey }),
    expectedReferenceBuild: REFERENCE_BUILD,
    referenceVersion: REFERENCE_VERSION,
  });
}

/** An in-process catalog carrying exactly the entries a test seeded. */
export function testCatalog(
  entries: Readonly<Partial<Record<DatasetKey, DatasetCatalogEntry>>>,
): DatasetSourceCatalog {
  return {
    get(requestedKey: string): DatasetCatalogEntry {
      const entry = entries[requestedKey as DatasetKey];
      if (entry === undefined) {
        throw new Error(`this test seeded no source for dataset key '${requestedKey}'`);
      }
      return entry;
    },
  };
}

// ---------------------------------------------------------------------------------------------
// Temporal
// ---------------------------------------------------------------------------------------------

export interface TemporalDevServer {
  readonly address: string;
  readonly client: Client;
  stop(): Promise<void>;
}

/** Starts `temporal server start-dev` headless on a free port, with its own in-memory state. */
export async function startTemporalDevServer(): Promise<TemporalDevServer> {
  const port = await freePort();
  const address = `127.0.0.1:${port}`;
  const process_ = spawn(
    'temporal',
    [
      'server',
      'start-dev',
      '--ip',
      '127.0.0.1',
      '--port',
      String(port),
      '--headless',
      '--log-level',
      'error',
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );

  await waitFor('temporal dev server', async () => {
    const probe = await Connection.connect({ address, connectTimeout: 1000 });
    await probe.workflowService.getSystemInfo({ namespace: 'default' });
    await probe.close();
    return true;
  });

  const connection = await Connection.connect({ address });
  const client = new Client({ connection, namespace: 'default' });

  return {
    address,
    client,
    async stop() {
      await connection.close().catch(() => undefined);
      process_.kill('SIGINT');
      await once(process_, 'exit').catch(() => undefined);
    },
  };
}

// ---------------------------------------------------------------------------------------------
// The two Workers
// ---------------------------------------------------------------------------------------------

export interface RustWorker {
  /** Everything the worker has written to stdout/stderr, colour escapes stripped. */
  log(): string;
  stop(): Promise<void>;
}

/** Compiles `temporal_worker` once. Cheap after the first call in a session. */
export async function buildRustWorker(): Promise<void> {
  await execFileAsync(
    'cargo',
    ['build', '--manifest-path', 'rust-ingestion-worker/Cargo.toml', '--bin', 'temporal_worker'],
    { cwd: REPO_ROOT, env: { ...process.env, PATH: CARGO_PATH }, maxBuffer: 32 * 1024 * 1024 },
  );
}

/**
 * Starts the real Rust data-plane Worker against a Temporal address and a staging root the
 * caller owns.
 *
 * Nothing about this is a stand-in: it is `target/debug/temporal_worker`, the same binary the
 * `rust-ingestion-worker` image runs, polling the activity-only `genomic-ingestion-rust` queue.
 */
export async function startRustWorker(options: {
  readonly address: string;
  readonly stagingRoot: string;
  readonly readyTimeoutMs?: number;
  /** Overrides the S3 endpoint, for a suite that brought up its own MinIO. */
  readonly s3Endpoint?: string;
}): Promise<RustWorker> {
  const captured: string[] = [];
  const log = (): string => captured.join('').replace(/\[[0-9;]*m/g, '');

  const child: ChildProcess = spawn(path.join(REPO_ROOT, 'target/debug/temporal_worker'), [], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      TEMPORAL_ADDRESS: options.address,
      TEMPORAL_NAMESPACE: 'default',
      S3_ENDPOINT: options.s3Endpoint ?? S3_ENDPOINT,
      S3_ACCESS_KEY,
      S3_SECRET_KEY,
      S3_FORCE_PATH_STYLE: 'true',
      INGESTION_STAGING_ROOT: options.stagingRoot,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout?.setEncoding('utf8');
  child.stderr?.setEncoding('utf8');
  child.stdout?.on('data', (chunk: string) => captured.push(chunk));
  child.stderr?.on('data', (chunk: string) => captured.push(chunk));

  await waitFor(
    'rust ingestion worker',
    async () => log().includes(RUST_WORKER_READY_LINE),
    options.readyTimeoutMs ?? 60_000,
  );

  return {
    log,
    async stop() {
      child.kill('SIGINT');
      await once(child, 'exit').catch(() => undefined);
    },
  };
}

export interface ControlPlaneWorker {
  stop(): Promise<void>;
}

/**
 * Runs the production TypeScript control plane: the real `GenomicIngestionWorkflow` and the real
 * `inspectDatasetSource`/`publishDataset` activities.
 *
 * `buildDatasetArtifact` is deliberately not registered here — it exists only in Rust, on its own
 * queue, and is scheduled by name.
 */
export async function startControlPlaneWorker(options: {
  readonly address: string;
  readonly objectStore: ObjectStore;
  readonly artifactBucket: string;
  readonly catalog: DatasetSourceCatalog;
  readonly artifactVersion?: string;
}): Promise<ControlPlaneWorker> {
  const activities: ControlPlaneActivities = createControlPlaneActivities({
    objectStore: options.objectStore,
    artifactBucket: options.artifactBucket,
    catalog: options.catalog,
    ...(options.artifactVersion === undefined ? {} : { artifactVersion: options.artifactVersion }),
  });

  const connection = await NativeConnection.connect({ address: options.address });
  const worker = await Worker.create({
    connection,
    namespace: 'default',
    taskQueue: CONTROL_PLANE_TASK_QUEUE,
    workflowsPath: new URL(
      '../../../ts-api-agent/src/application/workflows.ts',
      import.meta.url,
    ).pathname,
    activities: activities as unknown as Record<string, unknown>,
  });
  const running = worker.run();

  return {
    async stop() {
      worker.shutdown();
      await running.catch(() => undefined);
      await connection.close().catch(() => undefined);
    },
  };
}

// ---------------------------------------------------------------------------------------------
// The HTTP API
// ---------------------------------------------------------------------------------------------

export interface RunningApi {
  readonly baseUrl: string;
  stop(): Promise<void>;
}

/** Listens the production Hono app on a free loopback port. */
export async function startApi(app: Hono): Promise<RunningApi> {
  const server = http.createServer(nodeListener(app));
  const port = await freePort();
  server.listen(port, '127.0.0.1');
  await once(server, 'listening');
  return {
    baseUrl: `http://127.0.0.1:${port}`,
    async stop() {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

export interface JsonResponse<T = any> {
  readonly status: number;
  readonly body: T;
}

export async function postJson<T = any>(
  url: string,
  body: unknown,
): Promise<JsonResponse<T>> {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as T };
}

export async function getJson<T = any>(url: string): Promise<JsonResponse<T>> {
  const response = await fetch(url);
  return { status: response.status, body: (await response.json()) as T };
}

/**
 * Polls `GET /api/ingestions/:workflowId` until the run leaves the in-flight states.
 *
 * The API forwards the Workflow's own query verbatim, so this observes real progress; it never
 * infers one. Returns the terminal progress payload.
 */
export async function waitForIngestion(
  baseUrl: string,
  workflowId: string,
  timeoutMs: number,
): Promise<{ state: string; message: string }> {
  const deadline = Date.now() + timeoutMs;
  let last: { state: string; message: string } = { state: 'UNKNOWN', message: '' };
  while (Date.now() < deadline) {
    const { status, body } = await getJson(`${baseUrl}/api/ingestions/${workflowId}`);
    if (status === 200) {
      last = body;
      if (body.state === 'COMPLETED' || body.state === 'FAILED') return last;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `ingestion '${workflowId}' did not reach a terminal state within ${timeoutMs}ms; ` +
      `last observed ${JSON.stringify(last)}`,
  );
}

/**
 * Asserts that the environment carries no LLM provider credentials.
 *
 * `askBioinformaticsAgent` routes to a real provider the moment one is configured, which would
 * make an end-to-end assertion depend on a remote model's wording. Cleared rather than mocked:
 * the deterministic local path these tests exercise is the same production code path a
 * deployment without a provider key takes.
 */
export function clearLlmProviderKeys(): void {
  delete process.env.CEREBRAS_API_KEY;
  delete process.env.ANTHROPIC_API_KEY;
  assert.equal(process.env.CEREBRAS_API_KEY, undefined);
  assert.equal(process.env.ANTHROPIC_API_KEY, undefined);
}
