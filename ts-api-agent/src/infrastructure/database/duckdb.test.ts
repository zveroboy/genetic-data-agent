/**
 * Dataset-scoped genotype repository tests.
 *
 * The repository is the only thing in the system that turns a question ("CYP1A2?") into a
 * remote read of somebody's genome, so these tests are mostly about what it declines to do:
 * it will not open on a dataset with no manifest, will not query a reference snapshot that
 * disagrees with the manifest, will not widen the scan when a target cannot be resolved or is
 * outside every declared position range, and will not leave a session open when a query fails.
 *
 * The DuckDB session is faked here so the exact SQL reaching the engine can be asserted —
 * the explicit file list, the mandatory `hive_types_autocast = 0`, the literal partition value
 * and the parameterized positions. That the same SQL really prunes S3 traffic is proven
 * against MinIO in `tests/integration/remote_parquet_pruning.test.ts`.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  computeDatasetChecksumSha256,
  sha256Hex,
} from '../../application/dataset-checksum.ts';
import {
  type DatasetManifest,
  DatasetManifestSchema,
  type ParquetObject,
} from '../../application/ingestion-contracts.ts';
import {
  type ClinVarCoordinateResolver,
  TargetNotResolvableError,
  type VariantTarget,
} from './clinvar-coordinate-resolver.ts';
import {
  type ConditionalPutOutcome,
  type ObjectHead,
  type ObjectLocation,
  type ObjectStore,
  headManyBounded,
} from '../object-store/object-store.ts';
import type { SynthesizedVariant } from '../../domain/types.ts';
import type { DuckDbSession, DuckDbSessionFactory } from './duckdb-session-factory.ts';
import {
  DatasetNotPublishedError,
  TargetNotPresentError,
  createParquetDatasetResolver,
} from './parquet-dataset-resolver.ts';
import {
  ReferenceSnapshotMismatchError,
  RemoteDatasetUnavailableError,
  createGenotypeRepositoryFactory,
} from './duckdb.ts';

const ARTIFACT_BUCKET = 'genomic-artifacts';
const DATASET_ID = 'ds-serving-001';
const ARTIFACT_VERSION = 'iv-test-001';
const ATTEMPT_PREFIX = `datasets/${DATASET_ID}/versions/${ARTIFACT_VERSION}/attempt-1/`;

const goldenManifest = DatasetManifestSchema.parse(
  JSON.parse(
    readFileSync(
      fileURLToPath(new URL('../../../../contracts/fixtures/dataset-manifest.json', import.meta.url)),
      'utf8',
    ),
  ),
);

function parquetObject(chrom: string, part: number, minPos: number, maxPos: number): ParquetObject {
  const relativePath = `chrom=${chrom}/part-${String(part).padStart(3, '0')}.parquet`;
  const key = `${ATTEMPT_PREFIX}variants/${relativePath}`;
  return {
    bucket: ARTIFACT_BUCKET,
    key,
    etag: `etag-${key}`,
    versionId: null,
    chrom,
    checksumSha256: sha256Hex(relativePath),
    byteSize: 8192 + part,
    rowCount: 1000,
    minPos,
    maxPos,
  };
}

const CHROM_1 = parquetObject('1', 0, 10_000, 249_000_000);
const CHROM_12 = parquetObject('12', 0, 20_000_000, 60_000_000);
const CHROM_15 = parquetObject('15', 0, 70_000_000, 90_000_000);
const INVENTORY = [CHROM_1, CHROM_12, CHROM_15];

function manifest(overrides: Partial<DatasetManifest> = {}): DatasetManifest {
  return DatasetManifestSchema.parse({
    ...structuredClone(goldenManifest),
    datasetId: DATASET_ID,
    artifactVersion: ARTIFACT_VERSION,
    attemptPrefix: ATTEMPT_PREFIX,
    parquetObjects: INVENTORY,
    datasetChecksumSha256: computeDatasetChecksumSha256(ATTEMPT_PREFIX, INVENTORY),
    ...overrides,
  });
}

class FakeObjectStore implements ObjectStore {
  readonly objects = new Map<string, { body: string; head: ObjectHead }>();
  readonly requests: string[] = [];

  seedManifest(value: DatasetManifest): void {
    const key = `datasets/${value.datasetId}/manifest.json`;
    const body = JSON.stringify(value);
    this.objects.set(`${ARTIFACT_BUCKET}/${key}`, {
      body,
      head: {
        bucket: ARTIFACT_BUCKET,
        key,
        etag: 'manifest-etag',
        versionId: null,
        contentLength: Buffer.byteLength(body, 'utf8'),
        checksumSha256: sha256Hex(body),
      },
    });
    for (const object of value.parquetObjects) {
      this.objects.set(`${object.bucket}/${object.key}`, {
        body: '',
        head: {
          bucket: object.bucket,
          key: object.key,
          etag: object.etag,
          versionId: object.versionId,
          contentLength: object.byteSize,
          checksumSha256: object.checksumSha256,
        },
      });
    }
  }

  async head(location: ObjectLocation): Promise<ObjectHead | null> {
    this.requests.push(`HEAD ${location.bucket}/${location.key}`);
    return this.objects.get(`${location.bucket}/${location.key}`)?.head ?? null;
  }

  async headMany(locations: readonly ObjectLocation[]): Promise<readonly (ObjectHead | null)[]> {
    return headManyBounded((location) => this.head(location), locations, 4);
  }

  async getJson(location: ObjectLocation): Promise<unknown> {
    this.requests.push(`GET ${location.bucket}/${location.key}`);
    const stored = this.objects.get(`${location.bucket}/${location.key}`);
    return stored === undefined ? null : JSON.parse(stored.body);
  }

  async putJsonConditional(): Promise<ConditionalPutOutcome> {
    throw new Error('the serving path must never write');
  }
}

interface RecordedQuery {
  readonly sql: string;
  readonly values: readonly (string | number | null)[];
}

class FakeSessionFactory implements DuckDbSessionFactory {
  readonly queries: RecordedQuery[] = [];
  openCount = 0;
  closeCount = 0;
  rows: Record<string, unknown>[] = [];
  failWith: Error | null = null;

  async open(): Promise<DuckDbSession> {
    this.openCount += 1;
    const owner = this;
    return {
      async query(sql, values = []) {
        owner.queries.push({ sql, values: [...values] });
        if (owner.failWith !== null) throw owner.failWith;
        return owner.rows;
      },
      async close() {
        owner.closeCount += 1;
      },
    };
  }
}

function variantTarget(overrides: Partial<VariantTarget> = {}): VariantTarget {
  return {
    referenceBuild: 'GRCh38',
    referenceVersion: 'demo-clinvar-grch38-v1',
    chrom: '12',
    pos: 21_178_615,
    ref: 'T',
    alt: 'C',
    rsid: 'rs4149056',
    gene: 'SLCO1B1',
    phenotype: 'Statins myopathy risk',
    clinicalSignificance: 'Risk Factor',
    evidenceNote: 'Intermediate OATP1B1 function.',
    ...overrides,
  };
}

class FakeCoordinateResolver implements ClinVarCoordinateResolver {
  readonly calls: { targetId: string; referenceBuild: string }[] = [];
  readonly referenceVersion: string;
  readonly referenceBuild: string;
  readonly #targets: readonly VariantTarget[];

  constructor(
    referenceVersion = 'demo-clinvar-grch38-v1',
    referenceBuild = 'GRCh38',
    targets: readonly VariantTarget[] = [variantTarget()],
  ) {
    this.referenceVersion = referenceVersion;
    this.referenceBuild = referenceBuild;
    this.#targets = targets;
  }

  async resolve(targetId: string, referenceBuild: string): Promise<readonly VariantTarget[]> {
    this.calls.push({ targetId, referenceBuild });
    if (this.#targets.length === 0) {
      throw new TargetNotResolvableError(targetId, this.referenceVersion);
    }
    return this.#targets;
  }

  async close(): Promise<void> {}
}

interface Harness {
  readonly store: FakeObjectStore;
  readonly sessions: FakeSessionFactory;
  readonly coordinates: FakeCoordinateResolver;
  readonly factory: ReturnType<typeof createGenotypeRepositoryFactory>;
}

function harness(options: {
  manifest?: DatasetManifest | null;
  coordinates?: FakeCoordinateResolver;
  rows?: Record<string, unknown>[];
} = {}): Harness {
  const store = new FakeObjectStore();
  if (options.manifest !== null) store.seedManifest(options.manifest ?? manifest());
  const sessions = new FakeSessionFactory();
  sessions.rows = options.rows ?? [];
  const coordinates = options.coordinates ?? new FakeCoordinateResolver();
  const factory = createGenotypeRepositoryFactory({
    datasetResolver: createParquetDatasetResolver({
      objectStore: store,
      artifactBucket: ARTIFACT_BUCKET,
    }),
    coordinateResolver: coordinates,
    sessionFactory: sessions,
  });
  return { store, sessions, coordinates, factory };
}

/** One row as DuckDB returns it: the physical, snake_case column names. */
const PARQUET_ROW = {
  rsid: 'rs4149056',
  gene: 'SLCO1B1',
  user_genotype: 'T/C',
  phenotype: 'Statins myopathy risk',
  clinical_significance: 'Risk Factor',
  evidence_note: 'Intermediate OATP1B1 function.',
};

/** The same row as it leaves the repository: a camelCase wire payload. */
const SYNTHESIZED_VARIANT: SynthesizedVariant = {
  rsid: 'rs4149056',
  gene: 'SLCO1B1',
  userGenotype: 'T/C',
  phenotype: 'Statins myopathy risk',
  clinicalSignificance: 'Risk Factor',
  evidenceNote: 'Intermediate OATP1B1 function.',
};

describe('genotype repository', () => {
  it('refuses to open a dataset with no published manifest, opening no session', async () => {
    const { factory, sessions } = harness({ manifest: null });

    await assert.rejects(() => factory.open(DATASET_ID), DatasetNotPublishedError);
    assert.equal(sessions.openCount, 0, 'an unpublished dataset must never reach DuckDB');
  });

  it('refuses a reference snapshot that disagrees with the manifest', async () => {
    for (const mismatched of [
      new FakeCoordinateResolver('demo-clinvar-grch38-v2', 'GRCh38'),
      new FakeCoordinateResolver('demo-clinvar-grch38-v1', 'GRCh37'),
    ]) {
      const { factory, sessions } = harness({ coordinates: mismatched });

      await assert.rejects(() => factory.open(DATASET_ID), ReferenceSnapshotMismatchError);
      assert.equal(sessions.openCount, 0);
    }
  });

  it('builds an explicit, pruned file list with the mandatory hive options', async () => {
    const { factory, sessions } = harness({ rows: [PARQUET_ROW] });
    const repository = await factory.open(DATASET_ID);

    await repository.synthesizeVariant('SLCO1B1');

    assert.equal(sessions.queries.length, 1);
    const { sql, values } = sessions.queries[0]!;

    assert.ok(
      sql.includes(`'s3://${ARTIFACT_BUCKET}/${CHROM_12.key}'`),
      `chromosome-12 file missing from the scan list:\n${sql}`,
    );
    assert.ok(!sql.includes(CHROM_1.key), 'the chromosome-1 file must not be in the scan list');
    assert.ok(!sql.includes(CHROM_15.key), 'the chromosome-15 file must not be in the scan list');
    // Frozen by contracts/ingestion-v1.md#reading-the-dataset: bare `hive_partitioning = true`
    // lets DuckDB infer `chrom` as BIGINT from an autosome-only scan, which is exactly what
    // candidate pruning produces.
    assert.match(sql, /hive_partitioning\s*=\s*true/);
    assert.match(sql, /hive_types_autocast\s*=\s*0/);
    assert.match(sql, /chrom\s*=\s*'12'/, 'the partition value must be a literal above the scan');
    assert.ok(values.includes(21_178_615), `position predicate must be parameterized: ${values}`);
  });

  it('never emits a wildcard, a glob or a caller-supplied path', async () => {
    const { factory, sessions } = harness({ rows: [PARQUET_ROW] });
    const repository = await factory.open(DATASET_ID);

    await repository.synthesizeVariant('SLCO1B1');

    const { sql } = sessions.queries[0]!;
    assert.ok(!sql.includes('*'), `the scan list must be explicit, not a glob:\n${sql}`);
    assert.ok(!/\?\s*\.parquet/.test(sql));
    for (const uri of sql.matchAll(/'s3:\/\/[^']+'/g)) {
      assert.ok(
        INVENTORY.some((object) => uri[0] === `'s3://${object.bucket}/${object.key}'`),
        `${uri[0]} is not a validated manifest descriptor`,
      );
    }
  });

  it('groups candidates per chromosome so each scan carries its own literal partition value', async () => {
    const coordinates = new FakeCoordinateResolver('demo-clinvar-grch38-v1', 'GRCh38', [
      variantTarget(),
      variantTarget({ chrom: '15', pos: 74_749_576, ref: 'A', alt: 'C', rsid: 'rs762551', gene: 'CYP1A2' }),
    ]);
    const { factory, sessions } = harness({ coordinates, rows: [PARQUET_ROW] });
    const repository = await factory.open(DATASET_ID);

    const result = await repository.synthesizeVariant('multi');

    assert.equal(sessions.queries.length, 2);
    assert.match(sessions.queries[0]!.sql, /chrom\s*=\s*'12'/);
    assert.match(sessions.queries[1]!.sql, /chrom\s*=\s*'15'/);
    assert.deepEqual(result.provenance.filesScanned, [
      `s3://${ARTIFACT_BUCKET}/${CHROM_12.key}`,
      `s3://${ARTIFACT_BUCKET}/${CHROM_15.key}`,
    ]);
    assert.equal(sessions.openCount, 1, 'one request opens one session, not one per chromosome');
    assert.equal(sessions.closeCount, 1);
  });

  it('returns provenance naming the dataset content, reference snapshot and files scanned', async () => {
    const { factory } = harness({ rows: [PARQUET_ROW] });
    const repository = await factory.open(DATASET_ID);

    const result = await repository.synthesizeVariant('SLCO1B1');

    // The SQL row's physical column names become the wire payload's camelCase field names;
    // `toSynthesizedVariant` is the only place that translation happens.
    assert.deepEqual(result.variants, [SYNTHESIZED_VARIANT]);
    assert.deepEqual(result.provenance, {
      datasetId: DATASET_ID,
      datasetChecksumSha256: computeDatasetChecksumSha256(ATTEMPT_PREFIX, INVENTORY),
      referenceBuild: 'GRCh38',
      referenceVersion: 'demo-clinvar-grch38-v1',
      filesScanned: [`s3://${ARTIFACT_BUCKET}/${CHROM_12.key}`],
      targetsResolved: 1,
    });
    assert.doesNotThrow(() => JSON.stringify(result), 'the result must be JSON serializable');
  });

  it('propagates an unresolvable target without opening a session or heading an object', async () => {
    const coordinates = new FakeCoordinateResolver('demo-clinvar-grch38-v1', 'GRCh38', []);
    const { factory, sessions, store } = harness({ coordinates });
    const repository = await factory.open(DATASET_ID);
    store.requests.length = 0;

    await assert.rejects(() => repository.synthesizeVariant('NOT_A_GENE'), TargetNotResolvableError);

    assert.equal(sessions.openCount, 0, 'an unresolved target must never open a DuckDB session');
    assert.deepEqual(
      store.requests,
      [],
      'an unresolved target must not fall back to heading — let alone scanning — every Parquet file',
    );
  });

  it('reports a target outside every declared position range as TargetNotPresent', async () => {
    const coordinates = new FakeCoordinateResolver('demo-clinvar-grch38-v1', 'GRCh38', [
      variantTarget({ pos: 1 }),
    ]);
    const { factory, sessions } = harness({ coordinates });
    const repository = await factory.open(DATASET_ID);

    await assert.rejects(() => repository.synthesizeVariant('SLCO1B1'), TargetNotPresentError);
    assert.equal(sessions.openCount, 0, 'an absent target must not broaden into a scan');
  });

  it('verifies every candidate object before the scan and heads nothing else', async () => {
    const { factory, store } = harness({ rows: [PARQUET_ROW] });
    const repository = await factory.open(DATASET_ID);
    store.requests.length = 0;

    await repository.synthesizeVariant('SLCO1B1');

    assert.deepEqual(store.requests, [`HEAD ${ARTIFACT_BUCKET}/${CHROM_12.key}`]);
  });

  it('closes the session even when the query fails', async () => {
    const { factory, sessions } = harness();
    sessions.failWith = new Error('engine exploded');
    const repository = await factory.open(DATASET_ID);

    await assert.rejects(() => repository.synthesizeVariant('SLCO1B1'), /engine exploded/);
    assert.equal(sessions.closeCount, 1, 'the session must be closed in a finally');
  });

  it('maps a mid-scan object-store IO fault onto RemoteDatasetUnavailable, not a raw DuckDB error', async () => {
    const { factory, sessions } = harness();
    sessions.failWith = new Error(
      "IO Error: Connection error for HTTP HEAD to 'http://127.0.0.1:1/genomic-artifacts/...'",
    );
    const repository = await factory.open(DATASET_ID);

    const error = await repository.synthesizeVariant('SLCO1B1').then(
      () => null,
      (thrown: unknown) => thrown as Error,
    );

    assert.ok(error instanceof RemoteDatasetUnavailableError, `unexpected error: ${error}`);
    assert.equal(error.name, 'RemoteDatasetUnavailable');
    assert.equal(error.datasetId, DATASET_ID);
    assert.equal(error.cause, sessions.failWith);
    assert.equal(sessions.closeCount, 1, 'the session must still be closed in a finally');
  });

  it('leaves a genuine SQL fault distinct from an object-store outage', async () => {
    const { factory, sessions } = harness();
    // DuckDB's own exception categories: a Binder/Parser/Catalog fault means the query itself
    // is wrong, not that the object store failed. It must never be reported as the same error
    // as a transport/IO fault.
    sessions.failWith = new Error("Binder Error: column 'gt_raw' not found");
    const repository = await factory.open(DATASET_ID);

    const error = await repository.synthesizeVariant('SLCO1B1').then(
      () => null,
      (thrown: unknown) => thrown as Error,
    );

    assert.equal(error, sessions.failWith, 'a SQL fault must propagate unchanged, not be wrapped');
    assert.ok(!(error instanceof RemoteDatasetUnavailableError));
  });

  it('exposes no global user-data repository', async () => {
    const module: Record<string, unknown> = await import('./duckdb.ts');

    for (const [name, value] of Object.entries(module)) {
      assert.ok(
        typeof value !== 'object' || value === null || !('synthesizeVariant' in value),
        `'${name}' is an importable pre-built genotype repository; the serving path must open one per dataset`,
      );
    }
    assert.ok(!('duckDbRepository' in module));
    assert.ok(!('DuckDbRepository' in module));
  });

  it('does not import a filesystem fixture path or duckdb-async', () => {
    const source = readFileSync(fileURLToPath(new URL('./duckdb.ts', import.meta.url)), 'utf8');

    assert.ok(!source.includes('duckdb-async'), 'duckdb-async is broken on Node 25 and must be gone');
    assert.ok(!source.includes('demo_user.vcf'), 'no runtime path may fall back to a fixture');
    assert.ok(!source.includes('annotations_mock'), 'no runtime path may fall back to a fixture');
    assert.ok(!source.includes('genomic_data.duckdb'), 'no global user-data database may remain');
    assert.ok(!/catch\s*(\([^)]*\))?\s*\{\s*\}/.test(source), 'no exception may be swallowed');
  });
});
