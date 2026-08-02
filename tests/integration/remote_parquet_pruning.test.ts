/**
 * Partition and row-group pruning, measured on datasets the **real pipeline** produced.
 *
 * Two source VCFs carrying chromosomes 1, 12, 15 and X are seeded into this run's own bucket and
 * ingested through `POST /api/ingestions` → `GenomicIngestionWorkflow` → the Rust
 * `buildDatasetArtifact` Activity. Nothing here hand-writes a Parquet file or hand-assembles a
 * manifest: the objects under test are the ones the producer wrote, with the producer's own
 * `ROW_GROUP_SIZE`, compression and sort order.
 *
 * Two things are then proven that a unit test cannot:
 *
 * 1. **Pruning, by request accounting.** Every S3 request DuckDB issues is routed through an
 *    instrumented proxy that forwards verbatim to MinIO (Host preserved, so SigV4 still
 *    validates) and records method, decoded path, Range header and response byte count. For a
 *    chromosome-12 target, not one request may touch the chromosome-1 or chromosome-15 objects,
 *    and the chromosome-12 object must not be downloaded whole.
 * 2. **Isolation.** Two datasets carrying opposing genotypes answer differently, and neither
 *    repository can see the other's objects — selection comes from each dataset's own manifest,
 *    never from a prefix listing or a glob.
 *
 * The control-plane `ObjectStore` talks to MinIO directly, so the proxy counters describe the
 * query engine's traffic alone.
 */
import assert from 'node:assert/strict';
import { once } from 'node:events';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import type { S3Client } from '@aws-sdk/client-s3';

import { manifestKeyFor } from '../../ts-api-agent/src/application/control-plane-activities.ts';
import { datasetCatalog } from '../../ts-api-agent/src/application/dataset-catalog.ts';
import {
  type DatasetManifest,
  DatasetManifestSchema,
} from '../../ts-api-agent/src/application/ingestion-contracts.ts';
import type { DatasetKey } from '../../ts-api-agent/src/domain/datasets.ts';
import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../ts-api-agent/src/domain/datasets.ts';
import { createApp } from '../../ts-api-agent/src/index.ts';
import { askBioinformaticsAgent } from '../../ts-api-agent/src/infrastructure/ai/agent.ts';
import { openClinVarCoordinateResolver } from '../../ts-api-agent/src/infrastructure/database/clinvar-coordinate-resolver.ts';
import { createDuckDbSessionFactory } from '../../ts-api-agent/src/infrastructure/database/duckdb-session-factory.ts';
import {
  HIVE_PARTITION_READ_OPTIONS,
  type GenotypeRepository,
  createGenotypeRepositoryFactory,
} from '../../ts-api-agent/src/infrastructure/database/duckdb.ts';
import { createParquetDatasetResolver } from '../../ts-api-agent/src/infrastructure/database/parquet-dataset-resolver.ts';
import { buildReferenceDatabase } from '../../ts-api-agent/src/infrastructure/database/reference-bootstrap.ts';
import { createTemporalIngestionClient } from '../../ts-api-agent/src/infrastructure/temporal/temporal-ingestion-client.ts';
import {
  type ControlPlaneWorker,
  OwnedBuckets,
  REPO_ROOT,
  type RunningApi,
  type RustWorker,
  S3_ACCESS_KEY,
  S3_ENDPOINT,
  S3_REGION,
  S3_SECRET_KEY,
  type TemporalDevServer,
  buildRustWorker,
  clearLlmProviderKeys,
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
  writeSyntheticVcf,
} from './support/stack.ts';

const RUN_ID = newRunId();
const SOURCE_BUCKET = `pruning-src-${RUN_ID}`;
const ARTIFACT_BUCKET = `pruning-art-${RUN_ID}`;
const INGESTION_TIMEOUT_MS = 10 * 60_000;

/** `rust-ingestion-worker/src/artifact/mod.rs::ROW_GROUP_SIZE`. */
const PRODUCER_ROW_GROUP_SIZE = 100_000;

/** SLCO1B1 rs4149056, GRCh38 chr12:21178615 T>C — the chromosome-12 pruning target. */
const CHROM12_TARGET_POS = 21_178_615;
const CHROM12_ROWS = 300_000;
/** Row index of the target once the producer has sorted the partition: row group 1 of 3. */
const CHROM12_TARGET_ROW = 150_000;
const CHROM12_POS_STEP = 100;
const CHROM12_POS_BASE = CHROM12_TARGET_POS - CHROM12_TARGET_ROW * CHROM12_POS_STEP;

const CHROM1_ROWS = 100_000;

/** CYP1A2 rs762551, GRCh38 chr15:74749576 A>C — the isolation target. */
const CHROM15_TARGET_POS = 74_749_576;

/** G6PD rs1050828, GRCh38 chrX:154536002 C>T — the non-numeric partition target. */
const CHROMX_TARGET_POS = 154_536_002;

interface DatasetSpec {
  readonly label: string;
  readonly datasetKey: DatasetKey;
  readonly sourceKey: string;
  /** Raw VCF genotype of rs762551 in this dataset. */
  readonly rs762551Genotype: string;
  /** Raw VCF genotype of rs4149056 in this dataset. */
  readonly rs4149056Genotype: string;
}

const DATASETS: readonly DatasetSpec[] = [
  {
    label: 'pruning-a',
    datasetKey: 'demo-small',
    sourceKey: 'samples/pruning_a.vcf.gz',
    rs762551Genotype: '0/1',
    rs4149056Genotype: '0/1',
  },
  {
    label: 'pruning-b',
    datasetKey: 'na12878-full',
    sourceKey: 'samples/pruning_b.vcf.gz',
    rs762551Genotype: '1/1',
    rs4149056Genotype: '1/1',
  },
];

interface ProxyRequest {
  method: string;
  path: string;
  rangeStart?: number;
  rangeEnd?: number;
  status: number;
  bytes: number;
}

/** Counting HTTP proxy in front of MinIO; only DuckDB is pointed at it. */
class MinioProxy {
  readonly requests: ProxyRequest[] = [];
  private server?: http.Server;
  port = 0;

  async start(upstream: URL): Promise<void> {
    this.server = http.createServer((req, res) => {
      const range = /bytes=(\d+)-(\d+)?/.exec(req.headers.range ?? '');
      const record: ProxyRequest = {
        method: req.method ?? '',
        path: decodeURIComponent(req.url ?? ''),
        rangeStart: range ? Number(range[1]) : undefined,
        rangeEnd: range?.[2] ? Number(range[2]) : undefined,
        status: 0,
        bytes: 0,
      };
      this.requests.push(record);

      const upstreamReq = http.request(
        {
          host: upstream.hostname,
          port: upstream.port,
          method: req.method,
          path: req.url,
          headers: req.headers, // Host preserved so MinIO's SigV4 check still matches.
        },
        (upstreamRes) => {
          record.status = upstreamRes.statusCode ?? 0;
          res.writeHead(upstreamRes.statusCode ?? 502, upstreamRes.headers);
          upstreamRes.on('data', (chunk: Buffer) => {
            record.bytes += chunk.length;
          });
          upstreamRes.pipe(res);
        },
      );
      upstreamReq.on('error', () => res.destroy());
      req.pipe(upstreamReq);
    });
    this.server.listen(0, '127.0.0.1');
    await once(this.server, 'listening');
    this.port = (this.server.address() as net.AddressInfo).port;
  }

  reset(): void {
    this.requests.length = 0;
  }

  async stop(): Promise<void> {
    if (!this.server) return;
    await new Promise<void>((resolve) => this.server!.close(() => resolve()));
  }
}

interface Span {
  start: number;
  end: number;
}

function overlap(a: Span, b: Span): number {
  return Math.max(0, Math.min(a.end, b.end) - Math.max(a.start, b.start));
}

function requestSpan(request: ProxyRequest, objectEnd: number): Span {
  return {
    start: request.rangeStart ?? 0,
    end: request.rangeEnd !== undefined ? request.rangeEnd + 1 : objectEnd,
  };
}

/**
 * Every `.duckdb` file directly under the repository root, as a sorted list.
 *
 * Deliberately non-recursive: it only catches a stray file dropped at the root, which is what
 * this repository's stale `data_*.duckdb` files look like. It is not a substitute for the real
 * guard against a per-dataset local database — that is `duckdb_databases()` in
 * `duckdb-session-factory.test.ts`, which asks the live connection directly rather than
 * inspecting the filesystem.
 */
function duckDbFilesInRepoRoot(): string[] {
  return fs
    .readdirSync(REPO_ROOT)
    .filter((entry) => entry.endsWith('.duckdb'))
    .sort();
}

describe('remote parquet serving (dataset isolation and partition pruning)', () => {
  const proxy = new MinioProxy();
  let s3: S3Client;
  let buckets: OwnedBuckets;
  let temporal: TemporalDevServer;
  let rustWorker: RustWorker;
  let controlPlane: ControlPlaneWorker;
  let api: RunningApi;
  let objectStore: ReturnType<typeof newObjectStore>;
  let coordinateResolver: Awaited<ReturnType<typeof openClinVarCoordinateResolver>>;
  let ingestionClient: ReturnType<typeof createTemporalIngestionClient>;
  let workDir = '';
  let stagingRoot = '';
  let bindingVersion = '';
  let httpfsVersion = '';

  const datasetIds = new Map<string, string>();
  const manifests = new Map<string, DatasetManifest>();
  const repositories = new Map<string, GenotypeRepository>();
  /** Byte spans of the chromosome-12 row groups, per dataset label. */
  const rowGroups = new Map<string, (Span & { id: number })[]>();
  let duckDbFilesBefore: string[] = [];
  /** Points DuckDB — and only DuckDB — at the counting proxy. */
  let sessionFactory: ReturnType<typeof createDuckDbSessionFactory>;

  /** Writes one dataset's source VCF: chromosomes 1, 12, 15 and X, with the planted targets. */
  async function writeSource(spec: DatasetSpec, destination: string): Promise<void> {
    await writeSyntheticVcf(
      destination,
      [
        {
          chrom: '1',
          count: CHROM1_ROWS,
          pos: (index) => 10_000_000 + index * 100,
        },
        {
          chrom: '12',
          count: CHROM12_ROWS,
          pos: (index) => CHROM12_POS_BASE + index * CHROM12_POS_STEP,
          override: (index) =>
            index === CHROM12_TARGET_ROW
              ? { rsid: 'rs4149056', ref: 'T', alt: 'C', gt: spec.rs4149056Genotype }
              : undefined,
        },
        {
          chrom: '15',
          count: 200,
          pos: (index) => CHROM15_TARGET_POS + index * 1000,
          override: (index) =>
            index === 0
              ? { rsid: 'rs762551', ref: 'A', alt: 'C', gt: spec.rs762551Genotype }
              : { ref: 'C', alt: 'T', gt: '0/0' },
        },
        {
          // A non-numeric partition value in the same dataset as the autosomes: this is the
          // dataset shape contracts/ingestion-v1.md singles out, where a narrow scan and a
          // whole-dataset scan disagree about `chrom`'s type unless autocast is disabled.
          chrom: 'X',
          count: 200,
          pos: (index) => CHROMX_TARGET_POS + index * 1000,
          override: (index) =>
            index === 0
              ? { rsid: 'rs1050828', ref: 'C', alt: 'T', gt: '0/1' }
              : { ref: 'A', alt: 'G', gt: '0/0' },
        },
      ],
      // Descending, so the producer is what puts the rows in `(pos, ref, alt)` order — the
      // row-group geometry this test measures is only meaningful if the sort really happened.
      { compress: true, descending: true },
    );
  }

  before(async () => {
    clearLlmProviderKeys();
    duckDbFilesBefore = duckDbFilesInRepoRoot();
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-parquet-pruning-'));
    stagingRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-parquet-staging-'));
    bindingVersion = JSON.parse(
      fs.readFileSync(path.join(REPO_ROOT, 'node_modules/@duckdb/node-api/package.json'), 'utf8'),
    ).version;

    await startMinio();
    s3 = newS3Client();
    buckets = new OwnedBuckets(s3);
    await buckets.create(SOURCE_BUCKET);
    await buckets.create(ARTIFACT_BUCKET);

    for (const spec of DATASETS) {
      const local = path.join(workDir, `${spec.label}.vcf.gz`);
      await writeSource(spec, local);
      await putSourceObject(s3, SOURCE_BUCKET, spec.sourceKey, local);
      fs.rmSync(local, { force: true });
    }

    temporal = await startTemporalDevServer();
    await buildRustWorker();
    rustWorker = await startRustWorker({ address: temporal.address, stagingRoot });

    objectStore = newObjectStore();
    controlPlane = await startControlPlaneWorker({
      address: temporal.address,
      objectStore,
      artifactBucket: ARTIFACT_BUCKET,
      catalog: testCatalog(
        Object.fromEntries(
          DATASETS.map((spec) => [
            spec.datasetKey,
            testCatalogEntry(spec.datasetKey, SOURCE_BUCKET, spec.sourceKey),
          ]),
        ),
      ),
    });

    const snapshot = await buildReferenceDatabase({
      tsvPath: path.join(REPO_ROOT, 'tests/fixtures/clinvar_coordinates_grch38.tsv'),
      databasePath: path.join(workDir, 'reference.duckdb'),
      referenceVersion: REFERENCE_VERSION,
      referenceBuild: REFERENCE_BUILD,
    });
    coordinateResolver = await openClinVarCoordinateResolver({ databasePath: snapshot.path });
    ingestionClient = createTemporalIngestionClient({ address: temporal.address });

    // The proxy sits between DuckDB and MinIO. Everything else — the control plane, the
    // manifest resolver, this test's own S3 client — talks to MinIO directly, so the counters
    // below describe the query engine's traffic and nothing else.
    await proxy.start(new URL(S3_ENDPOINT));
    sessionFactory = createDuckDbSessionFactory({
      s3: {
        endpoint: `127.0.0.1:${proxy.port}`,
        region: S3_REGION,
        accessKeyId: S3_ACCESS_KEY,
        secretAccessKey: S3_SECRET_KEY,
        useSsl: false,
        urlStyle: 'path',
        scope: `s3://${ARTIFACT_BUCKET}/`,
      },
      queryDeadlineMs: 120_000,
    });

    api = await startApi(
      createApp({
        catalog: datasetCatalog,
        ingestionClient,
        datasetResolver: createParquetDatasetResolver({
          objectStore,
          artifactBucket: ARTIFACT_BUCKET,
        }),
        coordinateResolver,
        sessionFactory,
        askAgent: (question, options) => askBioinformaticsAgent(question, options),
      }),
    );

    for (const spec of DATASETS) {
      const started = await postJson(`${api.baseUrl}/api/ingestions`, {
        datasetKey: spec.datasetKey,
      });
      assert.equal(started.status, 202, JSON.stringify(started.body));
      const terminal = await waitForIngestion(
        api.baseUrl,
        started.body.workflowId,
        INGESTION_TIMEOUT_MS,
      );
      assert.equal(
        terminal.state,
        'COMPLETED',
        `${spec.label} did not complete: ${JSON.stringify(terminal)}\n${rustWorker
          .log()
          .slice(-4000)}`,
      );
      datasetIds.set(spec.label, started.body.datasetId);
      manifests.set(
        spec.label,
        DatasetManifestSchema.parse(
          await objectStore.getJson({
            bucket: ARTIFACT_BUCKET,
            key: manifestKeyFor(started.body.datasetId),
          }),
        ),
      );
    }

    const factory = createGenotypeRepositoryFactory({
      datasetResolver: createParquetDatasetResolver({
        objectStore,
        artifactBucket: ARTIFACT_BUCKET,
      }),
      coordinateResolver,
      sessionFactory,
    });
    for (const spec of DATASETS) {
      repositories.set(spec.label, await factory.open(datasetIds.get(spec.label)!));
    }

    // Row-group geometry, read out of the *published* object rather than assumed. Done through a
    // direct session (not the proxy) so it does not pollute the request counters.
    const direct = createDuckDbSessionFactory({
      s3: {
        endpoint: new URL(S3_ENDPOINT).host,
        region: S3_REGION,
        accessKeyId: S3_ACCESS_KEY,
        secretAccessKey: S3_SECRET_KEY,
        useSsl: new URL(S3_ENDPOINT).protocol === 'https:',
        urlStyle: 'path',
        scope: `s3://${ARTIFACT_BUCKET}/`,
      },
      queryDeadlineMs: 120_000,
    });
    const probe = await direct.open();
    try {
      httpfsVersion = String(
        (
          await probe.query(
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'httpfs' AND loaded;",
          )
        )[0]?.extension_version,
      );
      for (const spec of DATASETS) {
        const chrom12 = manifests
          .get(spec.label)!
          .parquetObjects.find((object) => object.chrom === '12')!;
        rowGroups.set(
          spec.label,
          (
            await probe.query(`
              -- total_compressed_size covers the dictionary page too, so a chunk's end must be
              -- measured from its first page, not from data_page_offset.
              SELECT row_group_id,
                     min(coalesce(dictionary_page_offset, data_page_offset)) AS rg_start,
                     max(coalesce(dictionary_page_offset, data_page_offset) + total_compressed_size)
                       AS rg_end
              FROM parquet_metadata('s3://${chrom12.bucket}/${chrom12.key}')
              GROUP BY row_group_id ORDER BY row_group_id;
            `)
          ).map((row) => ({
            id: Number(row.row_group_id),
            start: Number(row.rg_start),
            end: Number(row.rg_end),
          })),
        );
      }
    } finally {
      await probe.close();
    }
  });

  after(async () => {
    await api?.stop();
    await ingestionClient?.close();
    await coordinateResolver?.close();
    await controlPlane?.stop();
    await rustWorker?.stop();
    await temporal?.stop();
    await proxy.stop();
    objectStore?.destroy();
    await buckets?.removeAll();
    s3?.destroy();
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
    if (stagingRoot) fs.rmSync(stagingRoot, { recursive: true, force: true });
  });

  it("ingested both datasets with the producer's own multi-row-group geometry", () => {
    for (const spec of DATASETS) {
      const manifest = manifests.get(spec.label)!;
      assert.deepEqual(
        manifest.parquetObjects.map((object) => object.chrom),
        ['1', '12', '15', 'X'],
        `${spec.label} must publish exactly the four seeded partitions, byte-wise ordered`,
      );
      const chrom12 = manifest.parquetObjects.find((object) => object.chrom === '12')!;
      assert.equal(chrom12.rowCount, CHROM12_ROWS);
      assert.equal(
        rowGroups.get(spec.label)!.length,
        CHROM12_ROWS / PRODUCER_ROW_GROUP_SIZE,
        'the chromosome-12 object must span three row groups for the pruning claim to mean anything',
      );
    }
  });

  it('answers the same question differently from two published datasets', async () => {
    const [first, second] = DATASETS;
    const a = await repositories.get(first!.label)!.synthesizeVariant('CYP1A2');
    const b = await repositories.get(second!.label)!.synthesizeVariant('CYP1A2');

    assert.deepEqual(
      a.variants.map((variant) => [variant.rsid, variant.userGenotype]),
      [['rs762551', 'A/C']],
    );
    assert.deepEqual(
      b.variants.map((variant) => [variant.rsid, variant.userGenotype]),
      [['rs762551', 'C/C']],
    );
    assert.notEqual(
      a.provenance.datasetChecksumSha256,
      b.provenance.datasetChecksumSha256,
      'two different datasets must not report the same content checksum',
    );
    assert.equal(a.provenance.datasetId, datasetIds.get(first!.label));
    assert.equal(b.provenance.datasetId, datasetIds.get(second!.label));
    for (const uri of a.provenance.filesScanned) {
      assert.ok(
        uri.includes(`/datasets/${datasetIds.get(first!.label)}/`),
        `dataset ${first!.label} scanned an object outside its own prefix: ${uri}`,
      );
    }
  });

  it('reads only the chromosome-12 object for a chromosome-12 target', async () => {
    const spec = DATASETS[0]!;
    const manifest = manifests.get(spec.label)!;
    const chrom12 = manifest.parquetObjects.find((object) => object.chrom === '12')!;
    const chrom1 = manifest.parquetObjects.find((object) => object.chrom === '1')!;
    const size = chrom12.byteSize;
    const groups = rowGroups.get(spec.label)!;

    proxy.reset();
    const startedAt = performance.now();
    const result = await repositories.get(spec.label)!.synthesizeVariant('rs4149056');
    const queryLatencyMs = Math.round(performance.now() - startedAt);

    const gets = proxy.requests.filter((request) => request.method === 'GET');
    /**
     * DuckDB's httpfs locates the footer with a single speculative tail GET whose span
     * terminates at the object's real last byte (`end === size`) — it does not know the true
     * footer size in advance, so it guesses a fixed-size window and reads to EOF. That GET is
     * structurally identifiable by this property alone, independent of any row-group geometry:
     * no ordinary row-group data read ever needs to run all the way to the physical end of the
     * file, only the footer probe does.
     */
    const isEofTerminatingGet = (request: ProxyRequest): boolean =>
      requestSpan(request, size).end === size;
    // Row-group attribution counts only non-footer-probe GETs. Excluding the footer probe
    // outright — rather than clipping its span to a computed footer offset and hoping the
    // remainder is small — is what lets the last row group (immediately adjacent to the footer)
    // keep the same strict-zero assertion as row group 0 below.
    const bytesFromRowGroup = (id: number) =>
      gets
        .filter((request) => !isEofTerminatingGet(request))
        .reduce((sum, request) => sum + overlap(requestSpan(request, size), groups[id]!), 0);
    const totalBytes = proxy.requests.reduce((sum, request) => sum + request.bytes, 0);

    console.log(
      `\n[remote-parquet-pruning] ${JSON.stringify(
        {
          duckdbBinding: `@duckdb/node-api@${bindingVersion}`,
          httpfsVersion,
          datasetId: result.provenance.datasetId,
          filesSelected: result.provenance.filesScanned,
          inventorySize: manifest.parquetObjects.length,
          chrom12ObjectBytes: size,
          chrom12RowCount: chrom12.rowCount,
          chrom1ObjectBytes: chrom1.byteSize,
          chrom1RowCount: chrom1.rowCount,
          s3Requests: proxy.requests.length,
          bytesRead: totalBytes,
          bytesReadRatio: Number((totalBytes / size).toFixed(4)),
          queryLatencyMs,
          dataBytesPerRowGroup: groups.map((group) => bytesFromRowGroup(group.id)),
          requests: proxy.requests,
        },
        null,
        2,
      )}`,
    );

    assert.deepEqual(
      result.variants.map((variant) => [variant.rsid, variant.gene, variant.userGenotype]),
      [['rs4149056', 'SLCO1B1', 'T/C']],
    );
    assert.deepEqual(result.provenance.filesScanned, [`s3://${ARTIFACT_BUCKET}/${chrom12.key}`]);
    assert.deepEqual(
      proxy.requests.filter((request) => request.path.includes('/chrom=1/')),
      [],
      'no request may touch the chromosome-1 object',
    );
    assert.deepEqual(
      proxy.requests.filter((request) => request.path.includes('/chrom=15/')),
      [],
      'no request may touch the chromosome-15 object',
    );
    assert.deepEqual(
      proxy.requests.filter((request) => !request.path.includes(chrom12.key)),
      [],
      'every request must address the single selected chromosome-12 object',
    );
    assert.ok(
      proxy.requests.every(
        (request) => request.method === 'HEAD' || request.rangeStart !== undefined,
      ),
      'a full-object GET would download the dataset; only HEADs and ranged GETs are allowed',
    );
    assert.ok(
      totalBytes < size * 0.5,
      `read ${totalBytes} of ${size} bytes (${((totalBytes / size) * 100).toFixed(1)}%); a targeted read must not approach a full download`,
    );
    assert.equal(bytesFromRowGroup(0), 0, 'no data may be read from non-matching row group 0');

    // The last row group is immediately adjacent to the footer. httpfs locates the footer with a
    // single fixed-size speculative tail GET rather than a preliminary round trip to learn the
    // true footer size first — the same "guess and hope it's enough" technique essentially every
    // remote Parquet reader uses. For a compact object, that guess window is larger than the true
    // footer + column/offset-index trailer, so the same GET also incidentally pulls in the tail
    // end of the last row group's column data: bytes that are never used to answer the query but
    // are genuinely on the wire regardless of pruning. That is structural — a property of the
    // footer-probe size relative to this object's layout, not a pruning regression. Because that
    // GET is excluded from row-group attribution entirely (identified by `end === size`, not
    // tolerated as a fraction of the row group's size), the last row group gets the same
    // strict-zero assertion as row group 0.
    const footerAdjacentRowGroup = groups[groups.length - 1]!;
    assert.equal(
      bytesFromRowGroup(footerAdjacentRowGroup.id),
      0,
      `no data may be attributed to row group ${footerAdjacentRowGroup.id} (adjacent to the footer) ` +
        'once the footer-probe GET is excluded from row-group accounting',
    );

    // The exclusion above only holds if the footer probe itself stays small — otherwise a
    // regression could hide a whole row-group read inside "one GET that happens to end at EOF"
    // and it would simply vanish from every row-group total instead of being caught. Bound it
    // directly: FOOTER_PROBE_MAX_BYTES is 4x the 16 KiB probe observed across every real-MinIO
    // run of this test (headroom for an httpfs version bump) and still far tighter than one
    // column chunk of this object, so a single-column-chunk read smuggled in under "ends at EOF"
    // cannot pass.
    const FOOTER_PROBE_MAX_BYTES = 64 * 1024;
    const eofTerminatingGets = gets.filter((request) => isEofTerminatingGet(request));
    for (const request of eofTerminatingGets) {
      assert.ok(
        request.bytes <= FOOTER_PROBE_MAX_BYTES,
        `an EOF-terminating GET read ${request.bytes} bytes, exceeding the ` +
          `${FOOTER_PROBE_MAX_BYTES}-byte footer-probe bound — too large to be the footer probe alone`,
      );
    }

    // The per-request bound above only stops one large probe; it does nothing against several
    // small ones. Two or three EOF-terminating GETs, each individually under
    // FOOTER_PROBE_MAX_BYTES, are all excluded from `bytesFromRowGroup` by construction and would
    // sail past the per-request check individually — while together reading deep into the last
    // row group with every assertion above still green. httpfs locates the footer with exactly
    // one speculative tail read per query, so assert that invariant directly instead of only
    // bounding each probe's individual size.
    assert.equal(
      eofTerminatingGets.length,
      1,
      `expected exactly one EOF-terminating GET (httpfs's single speculative footer probe), got ` +
        `${eofTerminatingGets.length}: ${JSON.stringify(eofTerminatingGets)} — an unbounded count ` +
        `of individually-small EOF-terminating GETs could otherwise smuggle a meaningful slice of ` +
        `a row group past both the per-request size bound and the row-group exclusion`,
    );

    assert.ok(bytesFromRowGroup(1) > 0, 'the matching row group must actually be read');
  });

  it('answers an X-chromosome target, and reads chrom back as a string', async () => {
    const spec = DATASETS[0]!;
    const manifest = manifests.get(spec.label)!;
    const chromX = manifest.parquetObjects.find((object) => object.chrom === 'X')!;

    proxy.reset();
    const result = await repositories.get(spec.label)!.synthesizeVariant('G6PD');

    assert.deepEqual(
      result.variants.map((variant) => [variant.rsid, variant.gene, variant.userGenotype]),
      [['rs1050828', 'G6PD', 'C/T']],
    );
    assert.deepEqual(result.provenance.filesScanned, [`s3://${ARTIFACT_BUCKET}/${chromX.key}`]);
    assert.deepEqual(
      proxy.requests.filter((request) => !request.path.includes(chromX.key)),
      [],
      'an X-chromosome target must not touch any autosome object',
    );

    // The regression this pins: with `hive_partitioning = true` alone, DuckDB infers the Hive
    // column's type from the partitions a scan touched, so `chrom` comes back as a number for
    // an autosome-only scan of this very dataset — and on the Node binding a BIGINT `chrom`
    // arrives as a JS BigInt that fails `=== '12'` and throws inside JSON.stringify. Both
    // scans below go through the exact option set the repository uses.
    const session = await sessionFactory.open();
    try {
      for (const object of [chromX, manifest.parquetObjects.find((o) => o.chrom === '12')!]) {
        const [row] = await session.query(`
          SELECT DISTINCT chrom
          FROM read_parquet(['s3://${ARTIFACT_BUCKET}/${object.key}'], ${HIVE_PARTITION_READ_OPTIONS})
          LIMIT 1;
        `);
        assert.equal(
          typeof row?.chrom,
          'string',
          `chrom must arrive as a string for the chrom=${object.chrom} partition`,
        );
        assert.equal(row?.chrom, object.chrom);
        assert.doesNotThrow(() => JSON.stringify(row));
      }
    } finally {
      await session.close();
    }
  });

  it('cannot be pointed at another dataset by asking for one that was never published', async () => {
    const factory = createGenotypeRepositoryFactory({
      datasetResolver: createParquetDatasetResolver({
        objectStore,
        artifactBucket: ARTIFACT_BUCKET,
      }),
      coordinateResolver,
      sessionFactory,
    });

    proxy.reset();
    await assert.rejects(() => factory.open(`pruning-c-${RUN_ID}`), /DatasetNotPublished/);
    assert.deepEqual(proxy.requests, [], 'an unpublished dataset must cost the query engine nothing');
  });

  it('leaves the repository root free of stray .duckdb files (a filesystem spot check, not the isolation guard)', () => {
    // Non-recursive by construction: this only catches a `.duckdb` dropped at the repository
    // root, the shape of this repo's pre-existing stale `data_*.duckdb` files. It cannot see a
    // file nested under a subdirectory, so it is not proof that no per-dataset database was
    // created anywhere. That proof is `duckdb_databases()` in `duckdb-session-factory.test.ts`,
    // which asks the live connection directly rather than inspecting the filesystem.
    assert.deepEqual(
      duckDbFilesInRepoRoot(),
      duckDbFilesBefore,
      'the serving path must not create or cache a local .duckdb at the repository root',
    );
  });
});
