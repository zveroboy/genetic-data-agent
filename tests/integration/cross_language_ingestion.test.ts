/**
 * The vertical slice, end to end, with nothing stubbed.
 *
 * `POST /api/ingestions` on the real HTTP surface starts the real `GenomicIngestionWorkflow`;
 * the real TypeScript control plane resolves the seeded source and later publishes; the real
 * `target/debug/temporal_worker` serves `buildDatasetArtifact` on `genomic-ingestion-rust`
 * against real MinIO; and `POST /ask` answers the finished dataset by reading remote Parquet
 * through DuckDB `httpfs`. The only substitution anywhere is *which object* the two allowlisted
 * catalog keys point at — each test run seeds its own source object into its own bucket, so it
 * never writes to, or depends on, anything shared.
 *
 * Two things this proves that no unit test can:
 *
 * 1. **Publication order.** The manifest is written only after every object it declares already
 *    exists. That is checked at the one instant it is decidable — inside `putJsonConditional`,
 *    through an independent S3 client — not inferred from the finished bucket. See
 *    `support/recording-object-store.ts`.
 * 2. **Physical sortedness at volume.** `COPY … PARTITION_BY` silently drops the query's
 *    `ORDER BY` inside partition files, and does so only once a partition spans more than one
 *    chunk. The large-dataset scenario below reads `file_row_number` back out of the published
 *    Parquet and asserts the rows are in `(pos, ref, alt)` order *physically*, which is the only
 *    form of the promise that row-group pruning actually depends on.
 *
 * Requires MinIO (`docker compose up -d minio`), the `temporal` CLI and a Rust toolchain.
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { GetObjectCommand, type S3Client } from '@aws-sdk/client-s3';

import { manifestKeyFor } from '../../ts-api-agent/src/application/control-plane-activities.ts';
import {
  assertValidDatasetManifest,
  computeDatasetChecksumSha256,
} from '../../ts-api-agent/src/application/dataset-checksum.ts';
import {
  ARTIFACT_FORMAT,
  type DatasetManifest,
  DatasetManifestSchema,
  LAYOUT_VERSION,
  PARQUET_SCHEMA_FINGERPRINT,
  SCHEMA_VERSION,
  VARIANTS_SEGMENT,
  allowedPrefixFor,
} from '../../ts-api-agent/src/application/ingestion-contracts.ts';
import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../ts-api-agent/src/domain/datasets.ts';
import { createApp } from '../../ts-api-agent/src/index.ts';
import { askBioinformaticsAgent } from '../../ts-api-agent/src/infrastructure/ai/agent.ts';
import { openClinVarCoordinateResolver } from '../../ts-api-agent/src/infrastructure/database/clinvar-coordinate-resolver.ts';
import { createDuckDbSessionFactory } from '../../ts-api-agent/src/infrastructure/database/duckdb-session-factory.ts';
import { createParquetDatasetResolver } from '../../ts-api-agent/src/infrastructure/database/parquet-dataset-resolver.ts';
import { buildReferenceDatabase } from '../../ts-api-agent/src/infrastructure/database/reference-bootstrap.ts';
import { createTemporalIngestionClient } from '../../ts-api-agent/src/infrastructure/temporal/temporal-ingestion-client.ts';
import { datasetCatalog } from '../../ts-api-agent/src/application/dataset-catalog.ts';
import {
  recordPublicationOrder,
  type PublicationRecorder,
} from './support/recording-object-store.ts';
import {
  type ControlPlaneWorker,
  OwnedBuckets,
  REPO_ROOT,
  type RunningApi,
  S3_ACCESS_KEY,
  S3_ENDPOINT,
  S3_REGION,
  S3_SECRET_KEY,
  type RustWorker,
  type TemporalDevServer,
  buildRustWorker,
  clearLlmProviderKeys,
  getJson,
  headObject,
  listKeys,
  newObjectStore,
  newRunId,
  newS3Client,
  postJson,
  putSourceObject,
  startApi,
  startControlPlaneWorker,
  startMinio,
  startRustWorker,
  startTemporalDevServer,
  testCatalog,
  testCatalogEntry,
  waitForIngestion,
  withTimeout,
  writeSyntheticVcf,
} from './support/stack.ts';

const RUN_ID = newRunId();
const SOURCE_BUCKET = `xlang-src-${RUN_ID}`;
const ARTIFACT_BUCKET = `xlang-art-${RUN_ID}`;
const DEMO_SOURCE_KEY = 'samples/demo_user.vcf';
const LARGE_SOURCE_KEY = 'samples/large_source.vcf.gz';

/** The artifact version `createControlPlaneActivities` pins by default: `v{layoutVersion}`. */
const ARTIFACT_VERSION = `v${LAYOUT_VERSION}`;

/** What `tests/fixtures/demo_user.vcf` contains, byte-wise ordered as the contract requires. */
const DEMO_PARTITIONS = ['12', '15', '16', '2'] as const;
const DEMO_VARIANT_COUNT = 4;

/**
 * The real GIAB NA12878/HG001 benchmark VCF, when the developer has fetched it
 * (`make download-real-data`). It is git-ignored, so a fresh clone will not have it; the
 * large-dataset scenario then falls back to a synthetic source of comparable volume and says so
 * loudly. Both shapes exercise the same assertion — a fixture substitution inside an automated
 * test, which `contracts/ingestion-v1.md` permits and no runtime path may do.
 */
const NA12878_LOCAL = path.join(REPO_ROOT, 'data/na12878_hg001.vcf.gz');

/** Enough rows per partition that a partition spans several row groups (`ROW_GROUP_SIZE` 100k). */
const SYNTHETIC_LARGE_ROWS_PER_PARTITION = 420_000;
const SYNTHETIC_LARGE_PARTITIONS = ['1', '12'] as const;

const INGESTION_TIMEOUT_MS = 15 * 60_000;

interface DuckDbFileSnapshot {
  readonly [file: string]: number;
}

/**
 * Every `.duckdb` file under the repository, recursively, with its size.
 *
 * The serving path holds user variants only in remote Parquet: no per-dataset database is
 * downloaded, built or cached. A recursive snapshot before and after `/ask` is what makes that
 * checkable from outside the process — `duckdb-session-factory.test.ts` asks the live connection
 * the same question from inside.
 */
function duckDbFiles(root: string): DuckDbFileSnapshot {
  const found: Record<string, number> = {};
  const skip = new Set(['node_modules', 'target', '.git', '.codegraph']);
  const walk = (directory: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      if (entry.isDirectory()) {
        if (skip.has(entry.name)) continue;
        walk(path.join(directory, entry.name));
      } else if (entry.name.endsWith('.duckdb') || entry.name.endsWith('.duckdb.wal')) {
        const full = path.join(directory, entry.name);
        found[path.relative(root, full)] = fs.statSync(full).size;
      }
    }
  };
  walk(root);
  return found;
}

describe('cross-language genomic ingestion (API → Temporal → Rust → S3 → /ask)', () => {
  let s3: S3Client;
  let buckets: OwnedBuckets;
  let temporal: TemporalDevServer;
  let rustWorker: RustWorker;
  let controlPlane: ControlPlaneWorker;
  let api: RunningApi;
  let recorder: PublicationRecorder;
  let objectStore: ReturnType<typeof newObjectStore>;
  let probeStore: ReturnType<typeof newObjectStore>;
  let coordinateResolver: Awaited<ReturnType<typeof openClinVarCoordinateResolver>>;
  let workDir = '';
  let stagingRoot = '';
  let usedRealNa12878 = false;
  let ingestionClient: ReturnType<typeof createTemporalIngestionClient>;

  before(async () => {
    clearLlmProviderKeys();
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'xlang-e2e-'));
    stagingRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'xlang-staging-'));

    await startMinio();
    s3 = newS3Client();
    buckets = new OwnedBuckets(s3);
    await buckets.create(SOURCE_BUCKET);
    await buckets.create(ARTIFACT_BUCKET);

    // --- the two seeded sources, both owned by this run -------------------------------------
    await putSourceObject(
      s3,
      SOURCE_BUCKET,
      DEMO_SOURCE_KEY,
      path.join(REPO_ROOT, 'tests/fixtures/demo_user.vcf'),
    );

    usedRealNa12878 = fs.existsSync(NA12878_LOCAL);
    if (usedRealNa12878) {
      await putSourceObject(s3, SOURCE_BUCKET, LARGE_SOURCE_KEY, NA12878_LOCAL);
    } else {
      console.log(
        `\n[cross-language-e2e] ${NA12878_LOCAL} is absent (it is git-ignored; fetch it with ` +
          `'make download-real-data'). Substituting a synthetic ` +
          `${SYNTHETIC_LARGE_ROWS_PER_PARTITION}-row-per-partition source so the sortedness ` +
          `assertion still runs at a volume where COPY … PARTITION_BY was observed to lose order.`,
      );
      const local = path.join(workDir, 'large_source.vcf.gz');
      await writeSyntheticVcf(
        local,
        SYNTHETIC_LARGE_PARTITIONS.map((chrom) => ({
          chrom,
          count: SYNTHETIC_LARGE_ROWS_PER_PARTITION,
          pos: (index: number) => 10_000 + index * 7,
        })),
        // Written descending so the producer, not the fixture, is what puts rows in order.
        { compress: true, descending: true },
      );
      await putSourceObject(s3, SOURCE_BUCKET, LARGE_SOURCE_KEY, local);
      fs.rmSync(local, { force: true });
    }

    // --- Temporal, the Rust data plane, the TypeScript control plane -------------------------
    temporal = await startTemporalDevServer();
    await buildRustWorker();
    rustWorker = await startRustWorker({ address: temporal.address, stagingRoot });

    objectStore = newObjectStore();
    probeStore = newObjectStore();
    recorder = recordPublicationOrder(objectStore, probeStore);

    controlPlane = await startControlPlaneWorker({
      address: temporal.address,
      objectStore: recorder,
      artifactBucket: ARTIFACT_BUCKET,
      catalog: testCatalog({
        'demo-small': testCatalogEntry('demo-small', SOURCE_BUCKET, DEMO_SOURCE_KEY),
        'na12878-full': testCatalogEntry('na12878-full', SOURCE_BUCKET, LARGE_SOURCE_KEY),
      }),
    });

    // --- the production HTTP surface, with production adapters -------------------------------
    const snapshot = await buildReferenceDatabase({
      tsvPath: path.join(REPO_ROOT, 'tests/fixtures/clinvar_coordinates_grch38.tsv'),
      databasePath: path.join(workDir, 'reference.duckdb'),
      referenceVersion: REFERENCE_VERSION,
      referenceBuild: REFERENCE_BUILD,
    });
    coordinateResolver = await openClinVarCoordinateResolver({ databasePath: snapshot.path });

    ingestionClient = createTemporalIngestionClient({ address: temporal.address });
    api = await startApi(
      createApp({
        catalog: datasetCatalog,
        ingestionClient,
        datasetResolver: createParquetDatasetResolver({
          objectStore,
          artifactBucket: ARTIFACT_BUCKET,
        }),
        coordinateResolver,
        sessionFactory: createDuckDbSessionFactory({
          s3: {
            endpoint: new URL(S3_ENDPOINT).host,
            region: S3_REGION,
            accessKeyId: S3_ACCESS_KEY,
            secretAccessKey: S3_SECRET_KEY,
            useSsl: new URL(S3_ENDPOINT).protocol === 'https:',
            urlStyle: 'path',
            scope: `s3://${ARTIFACT_BUCKET}/`,
          },
        }),
        askAgent: (question, options) => askBioinformaticsAgent(question, options),
      }),
    );
  });

  after(async () => {
    await api?.stop();
    await ingestionClient?.close();
    await coordinateResolver?.close();
    await controlPlane?.stop();
    await rustWorker?.stop();
    await temporal?.stop();
    objectStore?.destroy();
    probeStore?.destroy();
    await buckets?.removeAll();
    s3?.destroy();
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
    if (stagingRoot) fs.rmSync(stagingRoot, { recursive: true, force: true });
  });

  // ===========================================================================================
  // The demo dataset: the whole slice, from HTTP request to answered question
  // ===========================================================================================

  describe("ingesting 'demo-small' through the real pipeline", () => {
    let datasetId = '';
    let workflowId = '';
    let manifest: DatasetManifest;
    let duckDbFilesBeforeAsk: DuckDbFileSnapshot;
    let askResponse: any;

    before(async () => {
      const started = await postJson(`${api.baseUrl}/api/ingestions`, {
        datasetKey: 'demo-small',
      });
      assert.equal(started.status, 202, JSON.stringify(started.body));
      datasetId = started.body.datasetId;
      workflowId = started.body.workflowId;

      const terminal = await waitForIngestion(api.baseUrl, workflowId, INGESTION_TIMEOUT_MS);
      assert.equal(
        terminal.state,
        'COMPLETED',
        `ingestion did not complete: ${JSON.stringify(terminal)}\n${rustWorker.log().slice(-4000)}`,
      );

      manifest = DatasetManifestSchema.parse(
        await probeStore.getJson({
          bucket: ARTIFACT_BUCKET,
          key: manifestKeyFor(datasetId),
        }),
      );

      duckDbFilesBeforeAsk = duckDbFiles(REPO_ROOT);
      const asked = await postJson(`${api.baseUrl}/ask`, {
        datasetId,
        question: 'Can I drink coffee?',
      });
      assert.equal(asked.status, 200, JSON.stringify(asked.body));
      askResponse = asked.body;
    });

    it('scheduled the rust activity on its own queue, under the rust worker identity', () => {
      const log = rustWorker.log();
      assert.match(log, /task_queue=genomic-ingestion-rust/);
      assert.match(log, /identity=rust-ingestion-worker@/);
      assert.match(log, new RegExp(`dataset[_ ]?id=${datasetId}|${datasetId}`));
    });

    it('published the manifest only after every object it declares already existed', () => {
      const writes = recorder.manifestWrites.filter((write) =>
        write.key === manifestKeyFor(datasetId),
      );
      assert.equal(writes.length, 1, 'exactly one manifest write is expected for this dataset');
      const write = writes[0]!;

      assert.deepEqual(
        write.declaredKeys,
        manifest.parquetObjects.map((object) => object.key),
        'the observed write must be the manifest under test',
      );
      assert.ok(write.declaredKeys.length > 0, 'a manifest declaring nothing proves nothing');

      const absent = write.presence.filter((entry) => !entry.present);
      assert.deepEqual(
        absent,
        [],
        'every declared Parquet object had to exist at the instant the manifest was written',
      );
      for (const entry of write.presence) {
        const declared = manifest.parquetObjects.find((object) => object.key === entry.key)!;
        assert.equal(entry.etag, declared.etag, `${entry.key} had a different ETag at write time`);
        assert.equal(entry.contentLength, declared.byteSize);
        assert.equal(entry.checksumSha256, declared.checksumSha256);
      }

      // …and the write really was the last thing that happened: the activity HEADed the whole
      // inventory first, so every declared key appears in the operations that preceded it.
      const headedBefore = new Set(
        write.precedingOps
          .filter((operation) => operation.op === 'head' || operation.op === 'headMany')
          .map((operation) => operation.key),
      );
      for (const key of write.declaredKeys) {
        assert.ok(
          headedBefore.has(key),
          `'${key}' was never verified before the manifest claimed it`,
        );
      }
    });

    it('the publication-order check above is capable of failing', async () => {
      // The assertion above is only meaningful if a missing object would have been noticed. Run
      // the same decorator against a probe that reports one declared object absent, and confirm
      // it records the absence rather than waving the write through.
      const written: string[] = [];
      const observer = recordPublicationOrder(
        {
          async head() {
            return null;
          },
          async headMany(locations) {
            return locations.map(() => null);
          },
          async getJson() {
            return null;
          },
          async putJsonConditional(location) {
            written.push(location.key);
            return { outcome: 'created', etag: null, versionId: null };
          },
        },
        {
          async head(location) {
            // Everything exists except the object the manifest is about to declare.
            return location.key.endsWith('.parquet')
              ? null
              : { ...location, etag: 'x', versionId: null, contentLength: 1, checksumSha256: 'x' };
          },
          async headMany(locations) {
            return locations.map(() => null);
          },
          async getJson() {
            return null;
          },
          async putJsonConditional() {
            return { outcome: 'exists' as const };
          },
        },
      );

      await observer.putJsonConditional(
        { bucket: ARTIFACT_BUCKET, key: 'datasets/probe/manifest.json' },
        { parquetObjects: [{ key: 'datasets/probe/versions/v1/attempt-1/variants/chrom=1/part-000.parquet' }] },
      );

      assert.equal(observer.manifestWrites.length, 1);
      assert.deepEqual(
        observer.manifestWrites[0]!.presence.map((entry) => entry.present),
        [false],
        'the decorator must record an object that was absent when the manifest was written',
      );
      assert.deepEqual(written, ['datasets/probe/manifest.json']);
    });

    it('records a dataset checksum the descriptor list reproduces', () => {
      assert.equal(
        manifest.datasetChecksumSha256,
        computeDatasetChecksumSha256(manifest.attemptPrefix, manifest.parquetObjects),
      );
      assert.doesNotThrow(() =>
        assertValidDatasetManifest(manifest, { expectedBucket: ARTIFACT_BUCKET }),
      );
    });

    it('counted the demo fixture variants and rejected nothing', () => {
      assert.ok(manifest.variantCount > 0, 'a dataset with no variants is not an ingestion');
      assert.equal(manifest.variantCount, DEMO_VARIANT_COUNT);
      assert.equal(manifest.rejectedRecordCount, 0);
      assert.equal(
        manifest.variantCount,
        manifest.parquetObjects.reduce((sum, object) => sum + object.rowCount, 0),
      );
    });

    it('laid the artifact out as one Parquet object per chromosome partition', () => {
      assert.deepEqual(
        manifest.parquetObjects.map((object) => object.chrom),
        [...DEMO_PARTITIONS],
      );
      assert.equal(manifest.artifactFormat, ARTIFACT_FORMAT);
      assert.equal(manifest.layoutVersion, LAYOUT_VERSION);
      assert.equal(manifest.schemaVersion, SCHEMA_VERSION);
      assert.equal(manifest.schemaFingerprint, PARQUET_SCHEMA_FINGERPRINT);
      assert.deepEqual(manifest.partitionSpec, ['chrom']);
      assert.deepEqual(manifest.sortOrder, ['chrom', 'pos', 'ref', 'alt']);
      assert.equal(manifest.referenceBuild, REFERENCE_BUILD);
      assert.equal(manifest.referenceVersion, REFERENCE_VERSION);
      assert.equal(manifest.artifactVersion, ARTIFACT_VERSION);

      const allowedPrefix = allowedPrefixFor(datasetId, ARTIFACT_VERSION);
      assert.ok(manifest.attemptPrefix.startsWith(allowedPrefix));
      assert.ok(manifest.attemptPrefix.length > allowedPrefix.length);
      for (const object of manifest.parquetObjects) {
        assert.equal(object.bucket, ARTIFACT_BUCKET);
        assert.equal(
          object.key,
          `${manifest.attemptPrefix}${VARIANTS_SEGMENT}chrom=${object.chrom}/part-000.parquet`,
        );
      }
    });

    it('declares the inventory in canonical (chrom, relativePath) byte order, without repeats', () => {
      const relativePaths = manifest.parquetObjects.map((object) => [
        object.chrom,
        object.key.slice(`${manifest.attemptPrefix}${VARIANTS_SEGMENT}`.length),
      ]);
      const sorted = [...relativePaths].sort((left, right) =>
        Buffer.compare(
          Buffer.from(`${left[0]} ${left[1]}`),
          Buffer.from(`${right[0]} ${right[1]}`),
        ),
      );
      assert.deepEqual(relativePaths, sorted);
      assert.equal(
        new Set(manifest.parquetObjects.map((object) => object.key)).size,
        manifest.parquetObjects.length,
      );
    });

    it('matches every declared ETag, size and checksum against the objects in MinIO', async () => {
      for (const object of manifest.parquetObjects) {
        const head = await headObject(s3, object.bucket, object.key);
        assert.equal(head.etag, object.etag, `${object.key} ETag`);
        assert.equal(head.contentLength, object.byteSize, `${object.key} size`);
        assert.equal(head.checksumSha256, object.checksumSha256, `${object.key} sha256 metadata`);
        const body = await s3.send(
          new GetObjectCommand({ Bucket: object.bucket, Key: object.key }),
        );
        const bytes = Buffer.from(await body.Body!.transformToByteArray());
        assert.equal(
          crypto.createHash('sha256').update(bytes).digest('hex'),
          object.checksumSha256,
          `${object.key} content does not hash to its declared checksum`,
        );
        assert.equal(bytes.byteLength, object.byteSize);
      }
    });

    it('answers /ask from that dataset, with provenance naming exactly what it read', () => {
      assert.equal(askResponse.datasetId, datasetId);
      const variant = askResponse.variants.find((entry: any) => entry.rsid === 'rs762551');
      assert.ok(variant, `no rs762551 evidence in ${JSON.stringify(askResponse.variants)}`);
      assert.equal(variant.gene, 'CYP1A2');
      assert.equal(variant.userGenotype, 'C/C');

      const provenance = askResponse.provenance;
      assert.equal(provenance.datasetId, datasetId);
      assert.equal(provenance.datasetChecksumSha256, manifest.datasetChecksumSha256);
      assert.equal(provenance.referenceVersion, REFERENCE_VERSION);
      assert.equal(provenance.referenceBuild, REFERENCE_BUILD);
      assert.equal(provenance.schemaFingerprint, PARQUET_SCHEMA_FINGERPRINT);

      const chrom15 = manifest.parquetObjects.find((object) => object.chrom === '15')!;
      assert.deepEqual(
        provenance.filesScanned,
        [`s3://${ARTIFACT_BUCKET}/${chrom15.key}`],
        'a chromosome-15 target must read the chromosome-15 object and nothing else',
      );
    });

    it('downloaded no per-dataset .duckdb while answering', () => {
      assert.deepEqual(
        duckDbFiles(REPO_ROOT),
        duckDbFilesBeforeAsk,
        'the serving path must not create, download or cache a local database for a dataset',
      );
      // The only `.duckdb` this test ever creates is the versioned reference snapshot, which is
      // reference data and not user data.
      assert.deepEqual(
        fs.readdirSync(workDir).filter((entry) => entry.endsWith('.duckdb')),
        ['reference.duckdb'],
      );
    });

    it('published nothing outside its own dataset prefix', async () => {
      const keys = await listKeys(s3, ARTIFACT_BUCKET);
      const foreign = keys.filter((key) => !key.startsWith('datasets/'));
      assert.deepEqual(foreign, [], 'the artifact bucket holds published datasets and nothing else');
      const own = keys.filter((key) => key.startsWith(`datasets/${datasetId}/`));
      assert.deepEqual(
        own.sort(),
        [manifestKeyFor(datasetId), ...manifest.parquetObjects.map((object) => object.key)].sort(),
        'the dataset prefix holds exactly the manifest and the declared objects',
      );
    });
  });

  // ===========================================================================================
  // The large dataset: physical sortedness where the defect is actually observable
  // ===========================================================================================

  describe("ingesting 'na12878-full' at volume", () => {
    let datasetId = '';
    let manifest: DatasetManifest;
    let elapsedMs = 0;

    before(async () => {
      const started = await postJson(`${api.baseUrl}/api/ingestions`, {
        datasetKey: 'na12878-full',
      });
      assert.equal(started.status, 202, JSON.stringify(started.body));
      datasetId = started.body.datasetId;

      const startedAt = Date.now();
      const terminal = await withTimeout(
        waitForIngestion(api.baseUrl, started.body.workflowId, INGESTION_TIMEOUT_MS),
        INGESTION_TIMEOUT_MS,
        'the large ingestion',
      );
      elapsedMs = Date.now() - startedAt;
      assert.equal(
        terminal.state,
        'COMPLETED',
        `large ingestion did not complete: ${JSON.stringify(terminal)}\n${rustWorker
          .log()
          .slice(-4000)}`,
      );

      manifest = DatasetManifestSchema.parse(
        await probeStore.getJson({ bucket: ARTIFACT_BUCKET, key: manifestKeyFor(datasetId) }),
      );

      console.log(
        `\n[cross-language-e2e] ${JSON.stringify(
          {
            source: usedRealNa12878 ? 'GIAB NA12878/HG001 (real)' : 'synthetic substitute',
            datasetId,
            elapsedMs,
            variantCount: manifest.variantCount,
            rejectedRecordCount: manifest.rejectedRecordCount,
            partitions: manifest.parquetObjects.map((object) => ({
              chrom: object.chrom,
              rows: object.rowCount,
              bytes: object.byteSize,
            })),
          },
          null,
          2,
        )}`,
      );
    });

    it('produced a large, multi-row-group dataset', () => {
      assert.ok(
        manifest.variantCount >= 400_000,
        `expected a genuinely large ingest, got ${manifest.variantCount} variants — the ` +
          'partition-writer ordering defect is invisible below a few hundred thousand rows',
      );
      assert.ok(
        manifest.parquetObjects.some((object) => object.rowCount > 100_000),
        'at least one partition must exceed ROW_GROUP_SIZE, or nothing spans two row groups',
      );
      assert.deepEqual(
        manifest.parquetObjects.map((object) => object.chrom),
        [...manifest.parquetObjects]
          .map((object) => object.chrom)
          .sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right))),
      );
    });

    it('wrote every partition physically sorted by (pos, ref, alt)', async () => {
      const sessionFactory = createDuckDbSessionFactory({
        s3: {
          endpoint: new URL(S3_ENDPOINT).host,
          region: S3_REGION,
          accessKeyId: S3_ACCESS_KEY,
          secretAccessKey: S3_SECRET_KEY,
          useSsl: new URL(S3_ENDPOINT).protocol === 'https:',
          urlStyle: 'path',
          scope: `s3://${ARTIFACT_BUCKET}/`,
        },
        // A whole-partition scan of a multi-hundred-thousand-row object over HTTP is not a
        // 10-second serving query.
        queryDeadlineMs: 300_000,
      });
      const session = await sessionFactory.open();
      const violations: { chrom: string; count: number; rows: number }[] = [];
      try {
        for (const object of manifest.parquetObjects) {
          // Physical order, not scan order: `file_row_number` is the row's position *in the
          // file*, so `lag(...) OVER (ORDER BY file_row_number)` compares each row with the one
          // physically before it. Ordering by `pos` here instead would make the query sort the
          // data itself and assert nothing at all.
          // `violations` is a `FILTER`, not an outer `WHERE`: an outer `WHERE` would restrict
          // *both* aggregates to the matching rows, so a well-sorted file (correctly zero
          // violations) would also aggregate `max(file_row_number)` over zero rows — NULL,
          // coerced to 0 below — silently reporting "zero rows examined" on every passing run.
          // `FILTER` keeps `rows` computed over the whole partition regardless of how many rows
          // violate the ordering.
          const [row] = await session.query(`
            SELECT
              count(*) FILTER (
                WHERE prev_pos IS NOT NULL AND (pos, ref, alt) < (prev_pos, prev_ref, prev_alt)
              ) AS violations,
              max(file_row_number) + 1 AS rows
            FROM (
              SELECT file_row_number,
                     pos, ref, alt,
                     lag(pos) OVER (ORDER BY file_row_number) AS prev_pos,
                     lag(ref) OVER (ORDER BY file_row_number) AS prev_ref,
                     lag(alt) OVER (ORDER BY file_row_number) AS prev_alt
              FROM read_parquet(
                ['s3://${object.bucket}/${object.key}'],
                file_row_number = true
              )
            );
          `);
          violations.push({
            chrom: object.chrom,
            count: Number(row?.violations ?? -1),
            // The query's own row count, not `object.rowCount` from the manifest: the manifest
            // says how many rows the producer *wrote*, which proves nothing about how many the
            // readback query actually saw. A partition that reads back as zero rows also reads
            // back as zero violations, so sourcing `rows` from the manifest would let that pass
            // silently as `violations: 0`. Asserted below.
            rows: Number(row?.rows ?? 0),
          });
        }
      } finally {
        await session.close();
      }

      console.log(
        `[cross-language-e2e] physical sortedness: ${JSON.stringify(violations)}`,
      );
      assert.deepEqual(
        violations.filter((entry) => entry.count !== 0),
        [],
        'rows inside a published partition must be physically ordered by (pos, ref, alt); ' +
          'row-group pruning is only correct because they are',
      );
      assert.ok(violations.length > 0, 'the sweep must have examined at least one partition');
      assert.ok(
        violations.every((entry) => entry.rows > 0),
        'every partition must actually read back rows from the readback query itself; a ' +
          `zero-row readback would otherwise pass as zero violations: ${JSON.stringify(violations)}`,
      );
    });

    it('the sortedness check above is capable of failing', async () => {
      // A "0 violations" result is only worth something if a violation would have been counted.
      // The same query, over a file written deliberately out of order, must find them — otherwise
      // the assertion above is a green light wired to nothing.
      const sessionFactory = createDuckDbSessionFactory({
        s3: {
          endpoint: new URL(S3_ENDPOINT).host,
          region: S3_REGION,
          accessKeyId: S3_ACCESS_KEY,
          secretAccessKey: S3_SECRET_KEY,
          useSsl: new URL(S3_ENDPOINT).protocol === 'https:',
          urlStyle: 'path',
          scope: `s3://${ARTIFACT_BUCKET}/`,
        },
        queryDeadlineMs: 60_000,
      });
      const scrambled = path.join(workDir, 'scrambled.parquet');
      const session = await sessionFactory.open();
      try {
        await session.query(`
          COPY (
            SELECT (CASE WHEN i % 7 = 0 THEN 300000 - i ELSE i END)::UINTEGER AS pos,
                   'A' AS ref, 'G' AS alt
            FROM range(0, 200000) t(i)
          ) TO '${scrambled}' (FORMAT PARQUET);
        `);
        const [row] = await session.query(`
          SELECT count(*) AS violations
          FROM (
            SELECT pos, ref, alt,
                   lag(pos) OVER (ORDER BY file_row_number) AS prev_pos,
                   lag(ref) OVER (ORDER BY file_row_number) AS prev_ref,
                   lag(alt) OVER (ORDER BY file_row_number) AS prev_alt
            FROM read_parquet(['${scrambled}'], file_row_number = true)
          )
          WHERE prev_pos IS NOT NULL
            AND (pos, ref, alt) < (prev_pos, prev_ref, prev_alt);
        `);
        assert.ok(
          Number(row?.violations) > 0,
          'the physical-order query found no violations in a deliberately unsorted file',
        );
      } finally {
        await session.close();
        fs.rmSync(scrambled, { force: true });
      }
    });

    it('declares min/max positions that bound the rows actually written', async () => {
      const sessionFactory = createDuckDbSessionFactory({
        s3: {
          endpoint: new URL(S3_ENDPOINT).host,
          region: S3_REGION,
          accessKeyId: S3_ACCESS_KEY,
          secretAccessKey: S3_SECRET_KEY,
          useSsl: new URL(S3_ENDPOINT).protocol === 'https:',
          urlStyle: 'path',
          scope: `s3://${ARTIFACT_BUCKET}/`,
        },
        queryDeadlineMs: 300_000,
      });
      const session = await sessionFactory.open();
      try {
        for (const object of manifest.parquetObjects) {
          const [row] = await session.query(`
            SELECT min(pos) AS lo, max(pos) AS hi, count(*) AS n
            FROM read_parquet(['s3://${object.bucket}/${object.key}']);
          `);
          assert.equal(Number(row?.lo), object.minPos, `${object.key} minPos`);
          assert.equal(Number(row?.hi), object.maxPos, `${object.key} maxPos`);
          assert.equal(Number(row?.n), object.rowCount, `${object.key} rowCount`);
        }
      } finally {
        await session.close();
      }
    });
  });

});
