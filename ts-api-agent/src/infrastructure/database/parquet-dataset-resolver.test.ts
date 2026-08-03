/**
 * Manifest resolution and candidate-file selection tests.
 *
 * The resolver is the gate between "a dataset id arrived from the outside" and "these exact
 * immutable S3 objects may be handed to DuckDB". Everything here is about what it refuses:
 * an unpublished dataset, an oversized or malformed manifest, an inventory that is too large,
 * out of canonical order, duplicated, in the wrong bucket, under another dataset's prefix, or
 * whose partition directories contradict their descriptors.
 *
 * The store is an in-memory fake that behaves like S3 for the two operations the resolver
 * uses: it reports byte-accurate content lengths from the stored body and records every
 * request. That recording is what makes "an unknown dataset costs zero Parquet requests"
 * an assertion about observable behaviour rather than about a mock's call log shape.
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
  ContractValidationError,
  type DatasetManifest,
  DatasetManifestSchema,
  type ParquetObject,
} from '../../application/ingestion-contracts.ts';
import {
  CHECKSUM_METADATA_KEY,
  type ConditionalPutOutcome,
  type ObjectHead,
  type ObjectLocation,
  type ObjectStore,
  headManyBounded,
} from '../object-store/object-store.ts';
import { ObjectVerificationError } from '../../application/object-identity.ts';
import {
  DatasetNotPublishedError,
  DatasetResolutionError,
  MAX_DATASET_PARQUET_OBJECTS,
  MAX_MANIFEST_BYTES,
  TargetNotPresentError,
  createParquetDatasetResolver,
  selectCandidateObjects,
} from './parquet-dataset-resolver.ts';

const ARTIFACT_BUCKET = 'genomic-artifacts';
const DATASET_ID = 'ds-test-001';
const ARTIFACT_VERSION = 'iv-test-001';
const ATTEMPT_PREFIX = `datasets/${DATASET_ID}/versions/${ARTIFACT_VERSION}/attempt-1/`;

function readManifestFixture(): DatasetManifest {
  const url = new URL('../../../../contracts/fixtures/dataset-manifest.json', import.meta.url);
  return DatasetManifestSchema.parse(JSON.parse(readFileSync(fileURLToPath(url), 'utf8')));
}

const goldenManifest = readManifestFixture();

const clone = <T>(value: T): T => structuredClone(value);

interface StoredObject {
  readonly body: string;
  readonly etag: string;
  readonly versionId: string | null;
  readonly checksumSha256: string | null;
}

/**
 * In-memory `ObjectStore`. Content length is the byte length of the stored body, so the
 * resolver's manifest size guard is exercised against a real measurement rather than a number
 * the test made up. `headMany` delegates to the production scheduler.
 */
class FakeObjectStore implements ObjectStore {
  readonly objects = new Map<string, StoredObject>();
  /** Every request, in issue order, as `VERB bucket/key`. */
  readonly requests: string[] = [];

  static id(location: ObjectLocation): string {
    return `${location.bucket}/${location.key}`;
  }

  put(location: ObjectLocation, body: string, overrides: Partial<StoredObject> = {}): void {
    this.objects.set(FakeObjectStore.id(location), {
      body,
      etag: `etag-${FakeObjectStore.id(location)}`,
      versionId: null,
      checksumSha256: sha256Hex(body),
      ...overrides,
    });
  }

  putJson(location: ObjectLocation, value: unknown, overrides: Partial<StoredObject> = {}): void {
    this.put(location, JSON.stringify(value), overrides);
  }

  /** Requests whose key ends in `.parquet`; the count that must stay at zero. */
  parquetRequests(): string[] {
    return this.requests.filter((entry) => entry.endsWith('.parquet'));
  }

  async head(location: ObjectLocation): Promise<ObjectHead | null> {
    this.requests.push(`HEAD ${FakeObjectStore.id(location)}`);
    const stored = this.objects.get(FakeObjectStore.id(location));
    if (stored === undefined) return null;
    return {
      bucket: location.bucket,
      key: location.key,
      etag: stored.etag,
      versionId: stored.versionId,
      contentLength: Buffer.byteLength(stored.body, 'utf8'),
      checksumSha256: stored.checksumSha256,
    };
  }

  async headMany(locations: readonly ObjectLocation[]): Promise<readonly (ObjectHead | null)[]> {
    return headManyBounded((location) => this.head(location), locations, 4);
  }

  async getJson(location: ObjectLocation): Promise<unknown> {
    this.requests.push(`GET ${FakeObjectStore.id(location)}`);
    const stored = this.objects.get(FakeObjectStore.id(location));
    return stored === undefined ? null : JSON.parse(stored.body);
  }

  async putJsonConditional(): Promise<ConditionalPutOutcome> {
    throw new Error('the serving path must never write');
  }
}

/** A descriptor for `chrom=<chrom>/part-NNN.parquet` under the standard attempt prefix. */
function parquetObject(
  chrom: string,
  part: number,
  stats: { minPos: number; maxPos: number },
  overrides: Partial<ParquetObject> = {},
): ParquetObject {
  const relativePath = `chrom=${chrom}/part-${String(part).padStart(3, '0')}.parquet`;
  const key = `${ATTEMPT_PREFIX}variants/${relativePath}`;
  return {
    bucket: ARTIFACT_BUCKET,
    key,
    etag: `etag-${ARTIFACT_BUCKET}/${key}`,
    versionId: null,
    chrom,
    checksumSha256: sha256Hex(relativePath),
    byteSize: 4096 + part,
    rowCount: 100,
    minPos: stats.minPos,
    maxPos: stats.maxPos,
    ...overrides,
  };
}

/** Builds a manifest whose declared checksum really is the checksum of its inventory. */
function manifestOf(
  objects: readonly ParquetObject[],
  overrides: Partial<DatasetManifest> = {},
): DatasetManifest {
  const base = clone(goldenManifest);
  return DatasetManifestSchema.parse({
    ...base,
    datasetId: DATASET_ID,
    artifactVersion: ARTIFACT_VERSION,
    attemptPrefix: ATTEMPT_PREFIX,
    parquetObjects: objects,
    datasetChecksumSha256: computeDatasetChecksumSha256(ATTEMPT_PREFIX, objects),
    ...overrides,
  });
}

const CHROM_1 = parquetObject('1', 0, { minPos: 10_000, maxPos: 249_000_000 });
const CHROM_12 = parquetObject('12', 0, { minPos: 20_000_000, maxPos: 133_000_000 });
const CHROM_12_TAIL = parquetObject('12', 1, { minPos: 133_000_001, maxPos: 133_200_000 });
/** Canonical (chrom, relativePath) order is byte-wise, so '1' sorts before '12'. */
const INVENTORY = [CHROM_1, CHROM_12, CHROM_12_TAIL];

interface Harness {
  readonly store: FakeObjectStore;
  readonly resolver: ReturnType<typeof createParquetDatasetResolver>;
}

function harness(manifest?: DatasetManifest | string): Harness {
  const store = new FakeObjectStore();
  if (manifest !== undefined) {
    const location = { bucket: ARTIFACT_BUCKET, key: `datasets/${DATASET_ID}/manifest.json` };
    if (typeof manifest === 'string') {
      store.put(location, manifest);
    } else {
      store.putJson(location, manifest);
    }
    for (const object of manifest === undefined || typeof manifest === 'string'
      ? []
      : manifest.parquetObjects) {
      store.put({ bucket: object.bucket, key: object.key }, 'x'.repeat(object.byteSize), {
        etag: object.etag,
        versionId: object.versionId,
        checksumSha256: object.checksumSha256,
      });
    }
  }
  return {
    store,
    resolver: createParquetDatasetResolver({ objectStore: store, artifactBucket: ARTIFACT_BUCKET }),
  };
}

async function rejection(promise: Promise<unknown>): Promise<Error> {
  try {
    await promise;
  } catch (error) {
    return error as Error;
  }
  throw new assert.AssertionError({ message: 'expected the promise to reject' });
}

describe('parquet dataset resolver', () => {
  it('resolves a published manifest and reproduces its dataset checksum from the inventory', async () => {
    const manifest = manifestOf(INVENTORY);
    const { resolver } = harness(manifest);

    const dataset = await resolver.resolve(DATASET_ID);

    assert.equal(dataset.datasetId, DATASET_ID);
    assert.equal(dataset.bucket, ARTIFACT_BUCKET);
    assert.equal(dataset.referenceBuild, 'GRCh38');
    assert.equal(dataset.referenceVersion, 'demo-clinvar-grch38-v3');
    assert.equal(
      dataset.datasetChecksumSha256,
      computeDatasetChecksumSha256(ATTEMPT_PREFIX, INVENTORY),
      'the resolver must accept only a manifest whose inventory reproduces its own checksum',
    );
    assert.deepEqual(
      dataset.parquetObjects.map((object) => object.key),
      INVENTORY.map((object) => object.key),
    );
  });

  it('reads the manifest of the requested dataset and nothing else', async () => {
    const { store, resolver } = harness(manifestOf(INVENTORY));

    await resolver.resolve(DATASET_ID);

    assert.deepEqual(store.requests, [
      `HEAD ${ARTIFACT_BUCKET}/datasets/${DATASET_ID}/manifest.json`,
      `GET ${ARTIFACT_BUCKET}/datasets/${DATASET_ID}/manifest.json`,
    ]);
  });

  it('reports an unpublished dataset without issuing a single Parquet request', async () => {
    const { store, resolver } = harness();

    const error = await rejection(resolver.resolve('ds-never-published'));

    assert.ok(error instanceof DatasetNotPublishedError, `unexpected error: ${error}`);
    assert.deepEqual(store.parquetRequests(), [], 'an unknown dataset must cost zero Parquet requests');
    assert.deepEqual(store.requests, [
      `HEAD ${ARTIFACT_BUCKET}/datasets/ds-never-published/manifest.json`,
    ]);
  });

  it('rejects a dataset id that is not a single safe path segment before touching the store', async () => {
    const { store, resolver } = harness(manifestOf(INVENTORY));

    for (const unsafe of ['../ds-test-001', 'ds/../../etc', 'ds-test-001/manifest.json', '']) {
      const error = await rejection(resolver.resolve(unsafe));
      assert.ok(error instanceof DatasetResolutionError, `unexpected error: ${error}`);
      assert.equal(error.code, 'DATASET_ID_UNSAFE');
    }
    assert.deepEqual(store.requests, [], 'an unsafe dataset id must never reach the store');
  });

  it('refuses a manifest object larger than 1 MiB before parsing it', async () => {
    const oversized = JSON.stringify({ pad: 'a'.repeat(MAX_MANIFEST_BYTES) });
    assert.ok(Buffer.byteLength(oversized, 'utf8') > MAX_MANIFEST_BYTES);
    const { store, resolver } = harness(oversized);

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof DatasetResolutionError, `unexpected error: ${error}`);
    assert.equal(error.code, 'MANIFEST_TOO_LARGE');
    assert.deepEqual(
      store.requests,
      [`HEAD ${ARTIFACT_BUCKET}/datasets/${DATASET_ID}/manifest.json`],
      'an oversized manifest must be rejected on the HEAD, never buffered',
    );
  });

  it('rejects a manifest that is not a valid dataset manifest', async () => {
    const { resolver } = harness('{"datasetId":"ds-test-001"}');

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof DatasetResolutionError, `unexpected error: ${error}`);
    assert.equal(error.code, 'MANIFEST_MALFORMED');
  });

  it('rejects unknown layout and schema versions', async () => {
    for (const [field, value] of [
      ['layoutVersion', 2],
      ['schemaVersion', 2],
      ['artifactFormat', 'delta-lake'],
    ] as const) {
      const manifest = { ...clone(manifestOf(INVENTORY)), [field]: value };
      const { resolver } = harness(JSON.stringify(manifest));

      const error = await rejection(resolver.resolve(DATASET_ID));

      assert.ok(error instanceof DatasetResolutionError, `unexpected error for ${field}: ${error}`);
      assert.equal(error.code, 'MANIFEST_MALFORMED');
      assert.match(error.message, new RegExp(field));
    }
  });

  it('rejects a manifest whose schema fingerprint is not the frozen Parquet schema', async () => {
    const manifest = manifestOf(INVENTORY, { schemaFingerprint: sha256Hex('some other schema') });
    const { resolver } = harness(manifest);

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof ContractValidationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'SCHEMA_FINGERPRINT_MISMATCH');
  });

  it('rejects a manifest that claims a different dataset id than the one requested', async () => {
    const store = new FakeObjectStore();
    // A well-formed, internally consistent manifest for `ds-other` parked at `ds-test-001`'s key.
    const other = DatasetManifestSchema.parse({
      ...clone(goldenManifest),
      datasetId: 'ds-other',
      artifactVersion: ARTIFACT_VERSION,
      attemptPrefix: `datasets/ds-other/versions/${ARTIFACT_VERSION}/attempt-1/`,
      parquetObjects: [
        {
          ...clone(CHROM_1),
          key: `datasets/ds-other/versions/${ARTIFACT_VERSION}/attempt-1/variants/chrom=1/part-000.parquet`,
        },
      ],
      datasetChecksumSha256: computeDatasetChecksumSha256(
        `datasets/ds-other/versions/${ARTIFACT_VERSION}/attempt-1/`,
        [
          {
            ...clone(CHROM_1),
            key: `datasets/ds-other/versions/${ARTIFACT_VERSION}/attempt-1/variants/chrom=1/part-000.parquet`,
          },
        ],
      ),
    });
    store.putJson({ bucket: ARTIFACT_BUCKET, key: `datasets/${DATASET_ID}/manifest.json` }, other);
    const resolver = createParquetDatasetResolver({
      objectStore: store,
      artifactBucket: ARTIFACT_BUCKET,
    });

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof DatasetResolutionError, `unexpected error: ${error}`);
    assert.equal(error.code, 'MANIFEST_DATASET_ID_MISMATCH');
  });

  it(`refuses an inventory of more than ${MAX_DATASET_PARQUET_OBJECTS} Parquet objects`, async () => {
    const many = Array.from({ length: MAX_DATASET_PARQUET_OBJECTS + 1 }, (_unused, index) =>
      parquetObject('1', index, { minPos: index * 1000, maxPos: index * 1000 + 999 }),
    );
    const { store, resolver } = harness(manifestOf(many));

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof DatasetResolutionError, `unexpected error: ${error}`);
    assert.equal(error.code, 'TOO_MANY_PARQUET_OBJECTS');
    assert.deepEqual(store.parquetRequests(), [], 'an oversized inventory must cost zero Parquet requests');
  });

  it(`accepts an inventory of exactly ${MAX_DATASET_PARQUET_OBJECTS} Parquet objects`, async () => {
    const many = Array.from({ length: MAX_DATASET_PARQUET_OBJECTS }, (_unused, index) =>
      parquetObject('1', index, { minPos: index * 1000, maxPos: index * 1000 + 999 }),
    );
    const { resolver } = harness(manifestOf(many));

    const dataset = await resolver.resolve(DATASET_ID);

    assert.equal(dataset.parquetObjects.length, MAX_DATASET_PARQUET_OBJECTS);
  });

  it('rejects descriptors that are not in canonical (chrom, relativePath) order', async () => {
    const manifest = manifestOf([CHROM_12, CHROM_1, CHROM_12_TAIL]);
    const { resolver } = harness(manifest);

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof ContractValidationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'NONCANONICAL_ORDER');
  });

  it('rejects a duplicated descriptor', async () => {
    const manifest = manifestOf([CHROM_1, CHROM_12, CHROM_12]);
    const { resolver } = harness(manifest);

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof ContractValidationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'DUPLICATE_KEY');
  });

  it('rejects a descriptor in a bucket other than the configured artifact bucket', async () => {
    const manifest = manifestOf([{ ...clone(CHROM_1), bucket: 'attacker-bucket' }, CHROM_12]);
    const { resolver } = harness(manifest);

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof ContractValidationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'BUCKET_MISMATCH');
  });

  it("rejects a descriptor whose key sits outside this dataset's immutable prefix", async () => {
    const stolen: ParquetObject = {
      ...clone(CHROM_1),
      key: `datasets/ds-someone-else/versions/${ARTIFACT_VERSION}/attempt-1/variants/chrom=1/part-000.parquet`,
    };
    const manifest = {
      ...clone(manifestOf([CHROM_1, CHROM_12])),
      parquetObjects: [stolen, clone(CHROM_12)],
    };
    const { resolver } = harness(JSON.stringify(manifest));

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof ContractValidationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'KEY_OUTSIDE_ALLOWED_PREFIX');
  });

  it('rejects a partition value that disagrees with its chrom= directory', async () => {
    // The descriptor claims chromosome 7 while sitting in `chrom=1/`: a query that trusted the
    // descriptor would read chromosome-1 data and label it chromosome 7.
    const manifest = {
      ...clone(manifestOf([CHROM_1, CHROM_12])),
      parquetObjects: [{ ...clone(CHROM_1), chrom: '7' }, clone(CHROM_12)],
    };
    const { resolver } = harness(JSON.stringify(manifest));

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof ContractValidationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'PARTITION_MISMATCH');
  });

  it('rejects a partition value outside the canonical chromosome domain', async () => {
    // '23' is a legal path segment and agrees with its own `chrom=23/` directory, so nothing
    // in the wire schema or the inventory invariants catches it — but it is not a contig this
    // producer emits, and the value reaches SQL as a literal.
    const manifest = manifestOf([parquetObject('23', 0, { minPos: 1, maxPos: 2 })]);
    const { resolver } = harness(manifest);

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof DatasetResolutionError, `unexpected error: ${error}`);
    assert.equal(error.code, 'UNSUPPORTED_PARTITION_VALUE');
  });

  it('accepts the non-numeric contigs the producer normalises to', async () => {
    const objects = ['MT', 'X', 'Y'].map((chrom, index) =>
      parquetObject(chrom, index, { minPos: 1, maxPos: 1000 }),
    );
    const { resolver } = harness(manifestOf(objects));

    const dataset = await resolver.resolve(DATASET_ID);

    assert.deepEqual(dataset.parquetObjects.map((object) => object.chrom), ['MT', 'X', 'Y']);
  });

  it('rejects a manifest whose declared checksum the inventory does not reproduce', async () => {
    const manifest = manifestOf(INVENTORY, { datasetChecksumSha256: sha256Hex('not the inventory') });
    const { resolver } = harness(manifest);

    const error = await rejection(resolver.resolve(DATASET_ID));

    assert.ok(error instanceof ContractValidationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'DATASET_CHECKSUM_MISMATCH');
  });
});

describe('candidate file selection', () => {
  const dataset = {
    datasetId: DATASET_ID,
    bucket: ARTIFACT_BUCKET,
    parquetObjects: INVENTORY,
  };

  it('selects only files whose partition and position range can contain the target', () => {
    const selected = selectCandidateObjects(dataset, [{ chrom: '12', pos: 21_178_615 }]);

    assert.deepEqual(
      selected.map((object) => object.key),
      [CHROM_12.key],
      'the chromosome-1 file and the out-of-range chromosome-12 tail must not be selected',
    );
  });

  it('selects every file of the right chromosome whose range covers one of the targets', () => {
    const selected = selectCandidateObjects(dataset, [
      { chrom: '12', pos: 21_178_615 },
      { chrom: '12', pos: 133_150_000 },
    ]);

    assert.deepEqual(selected.map((object) => object.key), [CHROM_12.key, CHROM_12_TAIL.key]);
  });

  it('preserves canonical inventory order and never repeats a file', () => {
    const selected = selectCandidateObjects(dataset, [
      { chrom: '12', pos: 30_000_000 },
      { chrom: '12', pos: 21_178_615 },
      { chrom: '1', pos: 20_000 },
    ]);

    assert.deepEqual(selected.map((object) => object.key), [CHROM_1.key, CHROM_12.key]);
  });

  it('returns nothing rather than broadening the scan when no file can contain the target', () => {
    assert.deepEqual(selectCandidateObjects(dataset, [{ chrom: '12', pos: 1 }]), []);
    assert.deepEqual(selectCandidateObjects(dataset, [{ chrom: 'X', pos: 21_178_615 }]), []);
    assert.deepEqual(selectCandidateObjects(dataset, []), []);
  });
});

describe('candidate object verification', () => {
  async function resolveAndVerify(mutate: (store: FakeObjectStore) => void): Promise<Error> {
    const { store, resolver } = harness(manifestOf(INVENTORY));
    const dataset = await resolver.resolve(DATASET_ID);
    mutate(store);
    return rejection(resolver.verifyCandidates(dataset, [CHROM_12]));
  }

  it('accepts candidates whose stored identity still matches the manifest', async () => {
    const { store, resolver } = harness(manifestOf(INVENTORY));
    const dataset = await resolver.resolve(DATASET_ID);
    store.requests.length = 0;

    const uris = await resolver.verifyCandidates(dataset, [CHROM_12]);

    assert.deepEqual(uris, [`s3://${ARTIFACT_BUCKET}/${CHROM_12.key}`]);
    assert.deepEqual(
      store.requests,
      [`HEAD ${ARTIFACT_BUCKET}/${CHROM_12.key}`],
      'verification heads exactly the candidate files, never the whole inventory',
    );
  });

  it('refuses to query an object that has disappeared', async () => {
    const error = await resolveAndVerify((store) =>
      store.objects.delete(`${ARTIFACT_BUCKET}/${CHROM_12.key}`),
    );
    assert.ok(error instanceof ObjectVerificationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'OBJECT_MISSING');
  });

  it('refuses to query an object whose ETag drifted from the manifest', async () => {
    const error = await resolveAndVerify((store) => {
      const id = `${ARTIFACT_BUCKET}/${CHROM_12.key}`;
      store.objects.set(id, { ...store.objects.get(id)!, etag: 'replaced' });
    });
    assert.ok(error instanceof ObjectVerificationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'ETAG_MISMATCH');
    // Serving verifies against the published manifest, and says so — the same shared verifier
    // publication uses, told which document made the claim it is contradicting.
    assert.match(error.message, /the manifest declares/);
  });

  it('refuses to query an object whose size drifted from the manifest', async () => {
    const error = await resolveAndVerify((store) => {
      const id = `${ARTIFACT_BUCKET}/${CHROM_12.key}`;
      store.objects.set(id, { ...store.objects.get(id)!, body: 'truncated' });
    });
    assert.ok(error instanceof ObjectVerificationError, `unexpected error: ${error}`);
    assert.equal(error.code, 'SIZE_MISMATCH');
  });

  it('refuses to query an object with no or mismatched content checksum metadata', async () => {
    const missing = await resolveAndVerify((store) => {
      const id = `${ARTIFACT_BUCKET}/${CHROM_12.key}`;
      store.objects.set(id, { ...store.objects.get(id)!, checksumSha256: null });
    });
    assert.ok(missing instanceof ObjectVerificationError, `unexpected error: ${missing}`);
    assert.equal(missing.code, 'CHECKSUM_METADATA_MISSING');

    const wrong = await resolveAndVerify((store) => {
      const id = `${ARTIFACT_BUCKET}/${CHROM_12.key}`;
      store.objects.set(id, { ...store.objects.get(id)!, checksumSha256: sha256Hex('other') });
    });
    assert.ok(wrong instanceof ObjectVerificationError, `unexpected error: ${wrong}`);
    assert.equal(wrong.code, 'CHECKSUM_METADATA_MISMATCH');
  });

  it('rejects an empty candidate list as TargetNotPresent rather than broadening the scan', async () => {
    const { store, resolver } = harness(manifestOf(INVENTORY));
    const dataset = await resolver.resolve(DATASET_ID);
    store.requests.length = 0;

    const error = await rejection(resolver.verifyCandidates(dataset, []));

    assert.ok(error instanceof TargetNotPresentError, `unexpected error: ${error}`);
    assert.deepEqual(store.requests, [], 'an absent target must cost no further requests');
  });

  it('refuses a candidate that is not part of the resolved inventory', async () => {
    const { resolver } = harness(manifestOf(INVENTORY));
    const dataset = await resolver.resolve(DATASET_ID);
    const forged = parquetObject('12', 9, { minPos: 1, maxPos: 2 });

    const error = await rejection(resolver.verifyCandidates(dataset, [forged]));

    assert.ok(error instanceof DatasetResolutionError, `unexpected error: ${error}`);
    assert.equal(error.code, 'CANDIDATE_NOT_IN_MANIFEST');
  });
});

// The metadata key is part of the frozen storage convention the verifier depends on.
assert.equal(CHECKSUM_METADATA_KEY, 'sha256');
