/**
 * A cold `ts-api` container, on a Docker network with **no route off the host**, answers `/ask`.
 *
 * This is the claim the serving path's extension policy exists to support. The session factory
 * forces `autoinstall_known_extensions` and `autoload_known_extensions` off and does a bare
 * `LOAD httpfs`, and the image preinstalls a matching `httpfs` at build time — so a container
 * that has never served a request, and cannot reach `extensions.duckdb.org` or anything else on
 * the Internet, must still be able to read remote Parquet out of MinIO and answer a question.
 * If the extension were fetched lazily, this test is where that would show up.
 *
 * Shape of the run:
 *
 *   * an `internal: true` Docker network is created for this run;
 *   * MinIO is attached to it (and to the default bridge, so the host can seed it);
 *   * a real ingestion is performed from the host — Temporal dev server, the real Rust worker,
 *     the real control plane — publishing a real dataset into that MinIO;
 *   * the `ts-api` image is started attached **only** to the internal network;
 *   * the request is issued from *inside* that container, so nothing about the answer depends on
 *     a route the container does not have.
 *
 * Every container, network and bucket named below is created by this run and removed by it.
 */
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';
import { promisify } from 'node:util';

import { S3Client } from '@aws-sdk/client-s3';

import { manifestKeyFor } from '../../ts-api-agent/src/application/control-plane-activities.ts';
import {
  type DatasetManifest,
  DatasetManifestSchema,
} from '../../ts-api-agent/src/application/ingestion-contracts.ts';
import { S3ObjectStore } from '../../ts-api-agent/src/infrastructure/object-store/s3-object-store.ts';
import {
  type ControlPlaneWorker,
  OwnedBuckets,
  REPO_ROOT,
  type RustWorker,
  type TemporalDevServer,
  buildRustWorker,
  freePort,
  newRunId,
  putSourceObject,
  startControlPlaneWorker,
  startRustWorker,
  startTemporalDevServer,
  testCatalog,
  testCatalogEntry,
  waitFor,
  withTimeout,
} from './support/stack.ts';

const execFileAsync = promisify(execFile);

const RUN_ID = newRunId();
const NETWORK = `genomic-offline-${RUN_ID}`;
const MINIO_CONTAINER = `genomic-offline-minio-${RUN_ID}`;
const API_CONTAINER = `genomic-offline-api-${RUN_ID}`;
const IMAGE_TAG = `genomic-ts-api:offline-${RUN_ID}`;
const SOURCE_BUCKET = `offline-src-${RUN_ID}`;
const ARTIFACT_BUCKET = `offline-art-${RUN_ID}`;
const MINIO_USER = 'admin';
const MINIO_PASSWORD = 'password123';

const DATASET_KEY = 'demo-small';
const SOURCE_KEY = 'samples/demo_user.vcf';

// Bounds every `docker` invocation below with `execFile`'s own `timeout`/`killSignal`, which
// actually terminates the child process — unlike `withTimeout` (see `support/stack.ts`), which
// only races a promise and leaves whatever it wraps still running. A `docker build` is the case
// that matters most: without this, a wedged build would keep consuming CPU/disk in the
// background for the rest of the run (and beyond) even after the test that started it had timed
// out and moved on.
const DEFAULT_DOCKER_TIMEOUT_MS = 60_000;

async function docker(
  args: string[],
  options: { allowFailure?: boolean; timeoutMs?: number } = {},
): Promise<string> {
  try {
    const { stdout } = await execFileAsync('docker', args, {
      maxBuffer: 64 * 1024 * 1024,
      timeout: options.timeoutMs ?? DEFAULT_DOCKER_TIMEOUT_MS,
      killSignal: 'SIGKILL',
    });
    return stdout;
  } catch (error) {
    if (options.allowFailure === true) return '';
    throw error;
  }
}

/**
 * Runs a snippet of Node inside the API container and returns its stdout.
 *
 * Bounded by default: a wedged container must not block this suite until the runner's own
 * one-hour `--test-timeout`. 30s is generous for everything called here (a health probe, a DNS
 * lookup, one `/ask`), and the timeout kills the `docker exec` client-side rather than hanging
 * indefinitely on a container that stopped responding.
 */
async function inContainer(script: string, timeoutMs = 30_000): Promise<string> {
  return docker(['exec', API_CONTAINER, 'node', '-e', script], { timeoutMs });
}

describe('a cold, network-isolated ts-api container', () => {
  let temporal: TemporalDevServer;
  let rustWorker: RustWorker;
  let controlPlane: ControlPlaneWorker;
  let objectStore: S3ObjectStore;
  let s3: S3Client;
  let buckets: OwnedBuckets;
  let workDir = '';
  let stagingRoot = '';
  let minioPort = 0;
  let datasetId = '';
  let manifest: DatasetManifest;

  before(async () => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'offline-e2e-'));
    stagingRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'offline-staging-'));

    // --- the image under test, and an isolated network with no way off the host --------------
    // Absolute `-f`: the suite runs from the `ts-api-agent` workspace directory, and a
    // Dockerfile path is resolved against the caller's cwd, not against the build context.
    // `timeoutMs` here — not `withTimeout` — is what actually kills a wedged build instead of
    // merely giving up on waiting for it.
    await docker(
      ['build', '-f', path.join(REPO_ROOT, 'ts-api-agent/Dockerfile'), '-t', IMAGE_TAG, REPO_ROOT],
      { timeoutMs: 20 * 60_000 },
    );
    await docker(['network', 'create', '--internal', NETWORK]);

    // --- MinIO: on the internal network as `minio`, and reachable from the host to be seeded --
    minioPort = await freePort();
    await docker([
      'run', '-d',
      '--name', MINIO_CONTAINER,
      '-p', `127.0.0.1:${minioPort}:9000`,
      '-e', `MINIO_ROOT_USER=${MINIO_USER}`,
      '-e', `MINIO_ROOT_PASSWORD=${MINIO_PASSWORD}`,
      // Pinned to match `docker-compose.yml`'s `minio` service — see the comment there for how
      // to move this pin.
      'minio/minio:RELEASE.2025-09-07T16-13-09Z',
      'server', '/data',
    ]);
    await docker(['network', 'connect', '--alias', 'minio', NETWORK, MINIO_CONTAINER]);
    const endpoint = `http://127.0.0.1:${minioPort}`;
    await waitFor('the run-owned minio', async () =>
      (await fetch(`${endpoint}/minio/health/live`)).ok,
    );

    s3 = new S3Client({
      endpoint,
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: { accessKeyId: MINIO_USER, secretAccessKey: MINIO_PASSWORD },
    });
    buckets = new OwnedBuckets(s3);
    await buckets.create(SOURCE_BUCKET);
    await buckets.create(ARTIFACT_BUCKET);
    await putSourceObject(
      s3,
      SOURCE_BUCKET,
      SOURCE_KEY,
      path.join(REPO_ROOT, 'tests/fixtures/demo_user.vcf'),
    );

    // --- a real ingestion into that MinIO, driven from the host ------------------------------
    temporal = await startTemporalDevServer();
    await buildRustWorker();
    rustWorker = await startRustWorker({
      address: temporal.address,
      stagingRoot,
      // The Rust worker runs on the host, so it reaches this run's MinIO on its published port
      // rather than through the internal network the container is confined to.
      s3Endpoint: endpoint,
    });

    objectStore = new S3ObjectStore({
      endpoint,
      region: 'us-east-1',
      accessKeyId: MINIO_USER,
      secretAccessKey: MINIO_PASSWORD,
      forcePathStyle: true,
    });
    controlPlane = await startControlPlaneWorker({
      address: temporal.address,
      objectStore,
      artifactBucket: ARTIFACT_BUCKET,
      catalog: testCatalog({
        [DATASET_KEY]: testCatalogEntry(DATASET_KEY, SOURCE_BUCKET, SOURCE_KEY),
      }),
    });

    datasetId = `offline-${RUN_ID}`;
    manifest = DatasetManifestSchema.parse(
      await withTimeout(
        temporal.client.workflow.execute('GenomicIngestionWorkflow', {
          taskQueue: 'genomic-control-plane',
          workflowId: `genomic-ingestion-${datasetId}`,
          args: [{ datasetId, datasetKey: DATASET_KEY }],
        }),
        5 * 60_000,
        'the offline-fixture ingestion',
      ),
    );

    // --- the container, attached to the internal network and nothing else --------------------
    await docker([
      'run', '-d',
      '--name', API_CONTAINER,
      '--network', NETWORK,
      '-e', 'S3_ENDPOINT=http://minio:9000',
      '-e', `S3_ACCESS_KEY=${MINIO_USER}`,
      '-e', `S3_SECRET_KEY=${MINIO_PASSWORD}`,
      '-e', 'S3_FORCE_PATH_STYLE=true',
      '-e', `S3_ARTIFACT_BUCKET=${ARTIFACT_BUCKET}`,
      '-e', 'DUCKDB_ALLOW_EXTENSION_INSTALL=false',
      // Nothing points at a Temporal server: `/ask` must not need the orchestrator, and an
      // unreachable one must not stop a published dataset being read.
      IMAGE_TAG,
    ]);
    await waitFor(
      'the isolated api container',
      async () => (await inContainer(
        "fetch('http://127.0.0.1:3000/health').then(r=>r.text()).then(t=>console.log(t))",
      )).includes('"ok"'),
      120_000,
    );
  });

  after(async () => {
    // Diagnostics before teardown: without them a failure above leaves nothing to read.
    if (process.env.OFFLINE_CONTAINER_LOG_OUT) {
      fs.writeFileSync(
        process.env.OFFLINE_CONTAINER_LOG_OUT,
        await docker(['logs', API_CONTAINER], { allowFailure: true }),
      );
    }
    await controlPlane?.stop();
    await rustWorker?.stop();
    await temporal?.stop();
    objectStore?.destroy();
    await buckets?.removeAll();
    s3?.destroy();
    // Exactly the containers, network and image this run created.
    for (const container of [API_CONTAINER, MINIO_CONTAINER]) {
      await docker(['rm', '-f', container], { allowFailure: true });
    }
    await docker(['network', 'rm', NETWORK], { allowFailure: true });
    await docker(['image', 'rm', IMAGE_TAG], { allowFailure: true });
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
    if (stagingRoot) fs.rmSync(stagingRoot, { recursive: true, force: true });
  });

  it('genuinely has no route off the host', async () => {
    // Two probes, because "no Internet" has two failure modes and only one of them is DNS.
    const dns = await inContainer(`
      import('node:dns').then(({ promises }) => promises.resolve4('extensions.duckdb.org'))
        .then((a) => console.log('RESOLVED ' + a.join(',')))
        .catch((e) => console.log('FAILED ' + (e.code ?? e.message)));
    `);
    const http = await inContainer(`
      const c = new AbortController();
      setTimeout(() => c.abort(), 4000);
      fetch('http://1.1.1.1/', { signal: c.signal })
        .then((r) => console.log('REACHED ' + r.status))
        .catch((e) => console.log('FAILED ' + (e.cause?.code ?? e.name)));
    `);
    assert.match(dns, /^FAILED/m, `DNS resolved from inside the isolated container: ${dns}`);
    assert.match(http, /^FAILED/m, `an external address was reachable: ${http}`);
  });

  it('loaded httpfs from the image, with autoload and autoinstall off', async () => {
    const output = await inContainer(`
      (async () => {
        const { DuckDBInstance } = await import('@duckdb/node-api');
        const instance = await DuckDBInstance.create(':memory:');
        const connection = await instance.connect();
        await connection.run(\`
          SET autoinstall_known_extensions = false;
          SET autoload_known_extensions = false;
          LOAD httpfs;
        \`);
        const [row] = (await connection.runAndReadAll(
          "SELECT extension_version AS v FROM duckdb_extensions() WHERE extension_name='httpfs' AND loaded"
        )).getRowObjectsJS();
        console.log('HTTPFS ' + row.v);
      })().catch((e) => console.log('FAILED ' + e.message));
    `);
    assert.match(output, /^HTTPFS \w+/m, `httpfs did not load offline: ${output}`);
    // GUIDE.md documents this exact pair (engine v1.5.5, extension 827222f) as what the image
    // preinstalls. A binding bump could leave the extension loading (the assertion above stays
    // green) while pulling a different build than documented; pin the actual value here too, in
    // the one place that runs against the built container rather than the build script.
    assert.match(
      output,
      /^HTTPFS 827222f$/m,
      `httpfs extension version drifted from the pair documented in GUIDE.md: ${output}`,
    );
  });

  it('answers /ask from remote Parquet, from inside the isolated network', async () => {
    const output = await inContainer(`
      fetch('http://127.0.0.1:3000/ask', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ datasetId: ${JSON.stringify(datasetId)}, question: 'Can I drink coffee?' }),
      })
        .then(async (r) => console.log('STATUS ' + r.status + ' ' + (await r.text())))
        .catch((e) => console.log('FAILED ' + e.message));
    `);

    console.log(`\n[offline-container] ${output.trim()}`);
    assert.match(output, /^STATUS 200 /m, `/ask did not succeed offline: ${output}`);

    const body = JSON.parse(output.slice(output.indexOf('{')));
    assert.equal(body.datasetId, datasetId);
    const variant = body.variants.find((entry: any) => entry.rsid === 'rs762551');
    assert.ok(variant, `no rs762551 evidence in ${JSON.stringify(body.variants)}`);
    assert.equal(variant.gene, 'CYP1A2');
    assert.equal(variant.userGenotype, 'A/A');

    // The provenance must name the dataset that was actually ingested — the container read the
    // real published manifest, not something baked into the image.
    assert.equal(body.provenance.datasetChecksumSha256, manifest.datasetChecksumSha256);
    assert.equal(body.provenance.referenceVersion, manifest.referenceVersion);
    const chrom15 = manifest.parquetObjects.find((object) => object.chrom === '15')!;
    assert.deepEqual(body.provenance.filesScanned, [`s3://${ARTIFACT_BUCKET}/${chrom15.key}`]);
  });

  it('reports the traffic it caused, from the engine, in its own logs', async () => {
    const logs = await docker(['logs', API_CONTAINER]);
    const line = logs
      .split('\n')
      .reverse()
      .find((entry) => entry.includes('[serving-metrics]'));
    assert.ok(line, `no serving-metrics record in the container log:\n${logs.slice(-2000)}`);

    const metrics = JSON.parse(line.slice(line.indexOf('{')));
    assert.equal(metrics.datasetId, datasetId);
    assert.equal(metrics.datasetChecksumSha256, manifest.datasetChecksumSha256);
    assert.equal(metrics.referenceVersion, manifest.referenceVersion);
    assert.equal(metrics.selectedFileCount, 1);
    assert.ok(metrics.s3RequestCount > 0, 'the engine must report the requests it made');
    assert.ok(metrics.bytesRead > 0, 'the engine must report the bytes it read');
    assert.ok(metrics.queryLatencyMs >= 0);
    console.log(`[offline-container] serving metrics: ${JSON.stringify(metrics)}`);
  });
});
