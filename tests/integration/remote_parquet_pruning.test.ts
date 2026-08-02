/**
 * Serving-path integration test: two published datasets in MinIO, queried through the real
 * manifest resolver, the real versioned ClinVar reference and real remote DuckDB sessions.
 *
 * Two things are proven here that a unit test cannot:
 *
 * 1. **Isolation.** Two datasets carrying opposing `rs762551` genotypes answer differently,
 *    and neither repository can see the other's objects — selection comes from each dataset's
 *    own manifest, never from a prefix listing or a glob.
 * 2. **Pruning, by request accounting.** For a chromosome-12 target, every S3 request DuckDB
 *    issues is routed through an instrumented proxy that forwards verbatim to MinIO (Host
 *    preserved, so SigV4 still validates) and records method, decoded path, Range header and
 *    response byte count. Not one request may touch the chromosome-1 or chromosome-15 objects,
 *    and the chromosome-12 object must not be downloaded whole.
 *
 * The control-plane `ObjectStore` talks to MinIO directly, so the proxy counters describe the
 * query engine's traffic alone.
 */
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import { DuckDBInstance } from '@duckdb/node-api';

import { computeDatasetChecksumSha256 } from '../../ts-api-agent/src/application/dataset-checksum.ts';
import { assertValidDatasetManifest } from '../../ts-api-agent/src/application/dataset-checksum.ts';
import {
  ARTIFACT_FORMAT,
  type DatasetManifest,
  DatasetManifestSchema,
  LAYOUT_VERSION,
  PARQUET_SCHEMA_FINGERPRINT,
  type ParquetObject,
  SCHEMA_VERSION,
} from '../../ts-api-agent/src/application/ingestion-contracts.ts';
import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../ts-api-agent/src/domain/datasets.ts';
import { openClinVarCoordinateResolver } from '../../ts-api-agent/src/infrastructure/database/clinvar-coordinate-resolver.ts';
import { createDuckDbSessionFactory } from '../../ts-api-agent/src/infrastructure/database/duckdb-session-factory.ts';
import {
  HIVE_PARTITION_READ_OPTIONS,
  type GenotypeRepository,
  createGenotypeRepositoryFactory,
} from '../../ts-api-agent/src/infrastructure/database/duckdb.ts';
import { createParquetDatasetResolver } from '../../ts-api-agent/src/infrastructure/database/parquet-dataset-resolver.ts';
import { buildReferenceDatabase } from '../../ts-api-agent/src/infrastructure/database/reference-bootstrap.ts';
import { S3ObjectStore } from '../../ts-api-agent/src/infrastructure/object-store/s3-object-store.ts';

const execFileAsync = promisify(execFile);

const REPO_ROOT = path.resolve(import.meta.dirname, '../..');
const S3_ENDPOINT = process.env.S3_ENDPOINT ?? 'http://localhost:9000';
const S3_ACCESS_KEY = process.env.S3_ACCESS_KEY ?? 'admin';
const S3_SECRET_KEY = process.env.S3_SECRET_KEY ?? 'password123';
const ARTIFACT_BUCKET = 'genomic-artifacts';
const ARTIFACT_VERSION = 'v1';

const RUN_ID = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

/** SLCO1B1 rs4149056, GRCh38 chr12:21178615 T>C — the chromosome-12 pruning target. */
const CHROM12_TARGET_POS = 21_178_615;
const CHROM12_ROWS = 300_000;
const CHROM12_ROW_GROUP_SIZE = 100_000;
/** Row index of the target, placed in row group 1 of 3. */
const CHROM12_TARGET_ROW = 150_000;
const CHROM12_POS_BASE = CHROM12_TARGET_POS - CHROM12_TARGET_ROW * 100;

/** CYP1A2 rs762551, GRCh38 chr15:74749576 A>C — the isolation target. */
const CHROM15_TARGET_POS = 74_749_576;

/** G6PD rs1050828, GRCh38 chrX:154536002 C>T — the non-numeric partition target. */
const CHROMX_TARGET_POS = 154_536_002;

interface DatasetSpec {
  readonly datasetId: string;
  /** Raw VCF genotype of rs762551 in this dataset. */
  readonly rs762551Genotype: string;
  /** Raw VCF genotype of rs4149056 in this dataset. */
  readonly rs4149056Genotype: string;
}

const DATASETS: readonly DatasetSpec[] = [
  { datasetId: `pruning-a-${RUN_ID}`, rs762551Genotype: '0/1', rs4149056Genotype: '0/1' },
  { datasetId: `pruning-b-${RUN_ID}`, rs762551Genotype: '1/1', rs4149056Genotype: '1/1' },
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

async function awsS3(args: string[]): Promise<string> {
  const { stdout } = await execFileAsync('aws', ['--endpoint-url', S3_ENDPOINT, ...args], {
    env: {
      ...process.env,
      AWS_ACCESS_KEY_ID: S3_ACCESS_KEY,
      AWS_SECRET_ACCESS_KEY: S3_SECRET_KEY,
      AWS_DEFAULT_REGION: 'us-east-1',
      AWS_EC2_METADATA_DISABLED: 'true',
    },
  });
  return stdout;
}

async function waitFor(label: string, predicate: () => Promise<boolean>, timeoutMs = 60_000) {
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

interface LocalPartition {
  readonly chrom: string;
  readonly relativePath: string;
  readonly localPath: string;
  readonly rowCount: number;
  readonly minPos: number;
  readonly maxPos: number;
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
  let workDir: string;
  let objectStore: S3ObjectStore;
  let bindingVersion: string;
  let httpfsVersion = '';
  const repositories = new Map<string, GenotypeRepository>();
  const manifests = new Map<string, DatasetManifest>();
  /** Byte size of each published object, keyed by S3 key. */
  const objectSize = new Map<string, number>();
  /** Byte spans of the chromosome-12 row groups, per dataset. */
  const rowGroups = new Map<string, (Span & { id: number })[]>();
  let duckDbFilesBefore: string[] = [];
  let coordinateResolver: Awaited<ReturnType<typeof openClinVarCoordinateResolver>>;
  /** Points DuckDB — and only DuckDB — at the counting proxy. */
  let sessionFactory: ReturnType<typeof createDuckDbSessionFactory>;

  /** Builds one dataset's Parquet partitions locally, matching the frozen physical schema. */
  async function buildPartitions(spec: DatasetSpec, outputDir: string): Promise<LocalPartition[]> {
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    const partitions: LocalPartition[] = [];
    try {
      const sources: { chrom: string; select: string }[] = [
        {
          chrom: '1',
          select: `
            SELECT (10000000 + i * 100)::UINTEGER AS pos,
                   'rs1' || i AS rsid, 'A' AS ref, 'G' AS alt,
                   CASE WHEN i % 3 = 0 THEN '0/1' ELSE '1/1' END AS gt_raw
            FROM range(0, 100000) t(i)`,
        },
        {
          chrom: '12',
          select: `
            SELECT (${CHROM12_POS_BASE} + i * 100)::UINTEGER AS pos,
                   CASE WHEN i = ${CHROM12_TARGET_ROW} THEN 'rs4149056' ELSE 'rs12' || i END AS rsid,
                   CASE WHEN i = ${CHROM12_TARGET_ROW} THEN 'T' ELSE 'A' END AS ref,
                   CASE WHEN i = ${CHROM12_TARGET_ROW} THEN 'C' ELSE 'G' END AS alt,
                   CASE WHEN i = ${CHROM12_TARGET_ROW} THEN '${spec.rs4149056Genotype}'
                        WHEN i % 3 = 0 THEN '0/1' ELSE '1/1' END AS gt_raw
            FROM range(0, ${CHROM12_ROWS}) t(i)`,
        },
        {
          chrom: '15',
          select: `
            SELECT (${CHROM15_TARGET_POS} + i * 1000)::UINTEGER AS pos,
                   CASE WHEN i = 0 THEN 'rs762551' ELSE 'rs15' || i END AS rsid,
                   CASE WHEN i = 0 THEN 'A' ELSE 'C' END AS ref,
                   CASE WHEN i = 0 THEN 'C' ELSE 'T' END AS alt,
                   CASE WHEN i = 0 THEN '${spec.rs762551Genotype}' ELSE '0/0' END AS gt_raw
            FROM range(0, 200) t(i)`,
        },
        {
          // A non-numeric partition value in the same dataset as the autosomes: this is the
          // dataset shape contracts/ingestion-v1.md singles out, where a narrow scan and a
          // whole-dataset scan disagree about `chrom`'s type unless autocast is disabled.
          chrom: 'X',
          select: `
            SELECT (${CHROMX_TARGET_POS} + i * 1000)::UINTEGER AS pos,
                   CASE WHEN i = 0 THEN 'rs1050828' ELSE 'rsX' || i END AS rsid,
                   CASE WHEN i = 0 THEN 'C' ELSE 'A' END AS ref,
                   CASE WHEN i = 0 THEN 'T' ELSE 'G' END AS alt,
                   CASE WHEN i = 0 THEN '0/1' ELSE '0/0' END AS gt_raw
            FROM range(0, 200) t(i)`,
        },
      ];

      for (const source of sources) {
        const relativePath = `chrom=${source.chrom}/part-000.parquet`;
        const localPath = path.join(outputDir, relativePath);
        fs.mkdirSync(path.dirname(localPath), { recursive: true });
        await connection.run(`
          COPY (SELECT pos, rsid, ref, alt, gt_raw FROM (${source.select}) ORDER BY pos, ref, alt)
          TO '${localPath}'
          (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ${CHROM12_ROW_GROUP_SIZE});
        `);
        const [stats] = (
          await connection.runAndReadAll(
            `SELECT count(*) AS n, min(pos) AS lo, max(pos) AS hi FROM read_parquet('${localPath}');`,
          )
        ).getRowObjects();
        partitions.push({
          chrom: source.chrom,
          relativePath,
          localPath,
          rowCount: Number(stats!.n),
          minPos: Number(stats!.lo),
          maxPos: Number(stats!.hi),
        });
      }

      const chrom12 = partitions.find((partition) => partition.chrom === '12')!;
      rowGroups.set(
        spec.datasetId,
        (
          await connection.runAndReadAll(`
            -- total_compressed_size covers the dictionary page too, so a chunk's end must be
            -- measured from its first page, not from data_page_offset.
            SELECT row_group_id,
                   min(coalesce(dictionary_page_offset, data_page_offset)) AS rg_start,
                   max(coalesce(dictionary_page_offset, data_page_offset) + total_compressed_size)
                     AS rg_end
            FROM parquet_metadata('${chrom12.localPath}')
            GROUP BY row_group_id ORDER BY row_group_id;
          `)
        )
          .getRowObjects()
          .map((row) => ({
            id: Number(row.row_group_id),
            start: Number(row.rg_start),
            end: Number(row.rg_end),
          })),
      );
    } finally {
      connection.disconnectSync();
      instance.closeSync();
    }
    return partitions;
  }

  /** Uploads the partitions and publishes the manifest last, exactly as the control plane does. */
  async function publish(spec: DatasetSpec, partitions: readonly LocalPartition[]): Promise<void> {
    const attemptPrefix = `datasets/${spec.datasetId}/versions/${ARTIFACT_VERSION}/attempt-1/`;
    const descriptors: ParquetObject[] = [];

    for (const partition of [...partitions].sort((left, right) =>
      Buffer.compare(Buffer.from(left.chrom), Buffer.from(right.chrom)),
    )) {
      const key = `${attemptPrefix}variants/${partition.relativePath}`;
      const body = fs.readFileSync(partition.localPath);
      const checksumSha256 = (await import('node:crypto'))
        .createHash('sha256')
        .update(body)
        .digest('hex');
      await awsS3([
        's3',
        'cp',
        partition.localPath,
        `s3://${ARTIFACT_BUCKET}/${key}`,
        '--metadata',
        `sha256=${checksumSha256}`,
      ]);
      const head = await objectStore.head({ bucket: ARTIFACT_BUCKET, key });
      assert.ok(head?.etag, `uploaded object '${key}' reported no ETag`);
      objectSize.set(key, body.byteLength);
      descriptors.push({
        bucket: ARTIFACT_BUCKET,
        key,
        etag: head.etag,
        versionId: head.versionId,
        chrom: partition.chrom,
        checksumSha256,
        byteSize: body.byteLength,
        rowCount: partition.rowCount,
        minPos: partition.minPos,
        maxPos: partition.maxPos,
      });
    }

    const manifest = DatasetManifestSchema.parse({
      datasetId: spec.datasetId,
      artifactFormat: ARTIFACT_FORMAT,
      layoutVersion: LAYOUT_VERSION,
      schemaVersion: SCHEMA_VERSION,
      schemaFingerprint: PARQUET_SCHEMA_FINGERPRINT,
      artifactVersion: ARTIFACT_VERSION,
      referenceBuild: REFERENCE_BUILD,
      referenceVersion: REFERENCE_VERSION,
      attemptPrefix,
      datasetChecksumSha256: computeDatasetChecksumSha256(attemptPrefix, descriptors),
      variantCount: descriptors.reduce((sum, object) => sum + object.rowCount, 0),
      rejectedRecordCount: 0,
      processorVersion: 'integration-test/1.0.0',
      partitionSpec: ['chrom'],
      sortOrder: ['chrom', 'pos', 'ref', 'alt'],
      parquetObjects: descriptors,
    });
    assertValidDatasetManifest(manifest, { expectedBucket: ARTIFACT_BUCKET });

    const written = await objectStore.putJsonConditional(
      { bucket: ARTIFACT_BUCKET, key: `datasets/${spec.datasetId}/manifest.json` },
      manifest,
    );
    assert.equal(written.outcome, 'created');
    manifests.set(spec.datasetId, manifest);
  }

  before(async () => {
    duckDbFilesBefore = duckDbFilesInRepoRoot();
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-parquet-pruning-'));
    bindingVersion = JSON.parse(
      fs.readFileSync(path.join(REPO_ROOT, 'node_modules/@duckdb/node-api/package.json'), 'utf8'),
    ).version;

    await execFileAsync('docker', ['compose', 'up', '-d', 'minio'], { cwd: REPO_ROOT });
    await waitFor('minio', async () => (await fetch(`${S3_ENDPOINT}/minio/health/live`)).ok);
    await awsS3(['s3', 'mb', `s3://${ARTIFACT_BUCKET}`]).catch(() => '');

    objectStore = new S3ObjectStore({
      endpoint: S3_ENDPOINT,
      region: 'us-east-1',
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
      forcePathStyle: true,
    });

    for (const spec of DATASETS) {
      const outputDir = path.join(workDir, spec.datasetId);
      await publish(spec, await buildPartitions(spec, outputDir));
    }

    await proxy.start(new URL(S3_ENDPOINT));

    const snapshot = await buildReferenceDatabase({
      tsvPath: path.join(REPO_ROOT, 'tests/fixtures/clinvar_coordinates_grch38.tsv'),
      databasePath: path.join(workDir, 'reference.duckdb'),
      referenceVersion: REFERENCE_VERSION,
      referenceBuild: REFERENCE_BUILD,
    });
    coordinateResolver = await openClinVarCoordinateResolver({ databasePath: snapshot.path });

    sessionFactory = createDuckDbSessionFactory({
      s3: {
        endpoint: `127.0.0.1:${proxy.port}`,
        region: 'us-east-1',
        accessKeyId: S3_ACCESS_KEY,
        secretAccessKey: S3_SECRET_KEY,
        useSsl: false,
        urlStyle: 'path',
        scope: `s3://${ARTIFACT_BUCKET}/`,
      },
    });

    const factory = createGenotypeRepositoryFactory({
      datasetResolver: createParquetDatasetResolver({ objectStore, artifactBucket: ARTIFACT_BUCKET }),
      coordinateResolver,
      sessionFactory,
    });

    for (const spec of DATASETS) {
      repositories.set(spec.datasetId, await factory.open(spec.datasetId));
    }

    const probe = await sessionFactory.open();
    httpfsVersion = String(
      (
        await probe.query(
          "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'httpfs' AND loaded;",
        )
      )[0]?.extension_version,
    );
    await probe.close();
  });

  after(async () => {
    await coordinateResolver?.close();
    await proxy.stop();
    objectStore?.destroy();
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
    for (const spec of DATASETS) {
      await awsS3(['s3', 'rm', `s3://${ARTIFACT_BUCKET}/datasets/${spec.datasetId}`, '--recursive'])
        .catch(() => '');
    }
  });

  it('answers the same question differently from two published datasets', async () => {
    const [first, second] = DATASETS;
    const a = await repositories.get(first!.datasetId)!.synthesizeVariant('CYP1A2');
    const b = await repositories.get(second!.datasetId)!.synthesizeVariant('CYP1A2');

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
    assert.equal(a.provenance.datasetId, first!.datasetId);
    assert.equal(b.provenance.datasetId, second!.datasetId);
    for (const uri of a.provenance.filesScanned) {
      assert.ok(
        uri.includes(`/datasets/${first!.datasetId}/`),
        `dataset ${first!.datasetId} scanned an object outside its own prefix: ${uri}`,
      );
    }
  });

  it('reads only the chromosome-12 object for a chromosome-12 target', async () => {
    const spec = DATASETS[0]!;
    const manifest = manifests.get(spec.datasetId)!;
    const chrom12 = manifest.parquetObjects.find((object) => object.chrom === '12')!;
    const chrom1 = manifest.parquetObjects.find((object) => object.chrom === '1')!;
    const size = chrom12.byteSize;
    const groups = rowGroups.get(spec.datasetId)!;

    proxy.reset();
    const result = await repositories.get(spec.datasetId)!.synthesizeVariant('rs4149056');

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
    // remainder is small — is what lets row group 2 (immediately adjacent to the footer) keep
    // the same strict-zero assertion as row group 0 below: the footer probe's incidental
    // overlap with row group 2's tail bytes (explained at FOOTER_PROBE_MAX_BYTES below) is no
    // longer counted as row-group traffic at all, instead of being tolerated as a percentage of
    // an unrelated quantity.
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
          datasetId: spec.datasetId,
          filesSelected: result.provenance.filesScanned,
          inventorySize: manifest.parquetObjects.length,
          chrom12ObjectBytes: size,
          chrom1ObjectBytes: chrom1.byteSize,
          s3Requests: proxy.requests.length,
          bytesRead: totalBytes,
          bytesReadRatio: Number((totalBytes / size).toFixed(4)),
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

    // Row group 2 is the last one in the file, immediately adjacent to the footer. httpfs
    // locates the footer with a single fixed-size speculative tail GET (observed above: exactly
    // 16 KiB, ending at the file's last byte) rather than a preliminary round trip to learn the
    // true footer size first — the same "guess and hope it's enough" technique essentially every
    // remote Parquet reader uses. For an object this compact, that guess window is larger than
    // the true footer + column/offset-index trailer (about 1.7 KiB here), so the same GET also
    // incidentally pulls in the tail end of row group 2's own column data: bytes that are never
    // used to answer the query (only row group 1 is joined against) but are genuinely on the
    // wire regardless of pruning. That is structural — a property of the footer-probe size
    // relative to this object's layout, not a pruning regression. Because that GET is now
    // excluded from row-group attribution entirely (identified by `end === size`, not tolerated
    // as a fraction of row group 2's size), row group 2 gets the same strict-zero assertion as
    // row group 0: nothing attributable to actual row-group scanning may come from it.
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
    // directly: FOOTER_PROBE_MAX_BYTES is 4x the observed 16 KiB probe (headroom for an httpfs
    // version bump) and still far tighter than one column chunk of this object (~73.6 KB on
    // average across its 5 physical columns, from row group 1's 368,187 bytes / 5), so a
    // single-column-chunk read smuggled in under "ends at EOF" cannot pass.
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
    // FOOTER_PROBE_MAX_BYTES, are all excluded from `bytesFromRowGroup` by construction (Finding
    // 1, fix pass 2) and would sail past the per-request check individually — while together
    // reading deep into row group 2 (any one 64 KiB tail read already reaches back
    // FOOTER_PROBE_MAX_BYTES bytes from EOF, well inside its 356,546-byte span) with every
    // assertion above still green. httpfs locates the footer with exactly one speculative tail
    // read per query — observed as exactly 1 across every real-MinIO run of this test to date
    // (fix passes 1 and 2, and this pass) — so assert that invariant directly instead of only
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
    const manifest = manifests.get(spec.datasetId)!;
    const chromX = manifest.parquetObjects.find((object) => object.chrom === 'X')!;

    proxy.reset();
    const result = await repositories.get(spec.datasetId)!.synthesizeVariant('G6PD');

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
      datasetResolver: createParquetDatasetResolver({ objectStore, artifactBucket: ARTIFACT_BUCKET }),
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
