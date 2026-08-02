/**
 * Control-plane activity tests.
 *
 * Both activities are exercised end to end against an in-memory `ObjectStore` fake that
 * behaves like the real thing: it stores bytes, hands back byte-accurate content lengths,
 * round-trips JSON through `JSON.stringify`/`JSON.parse`, refuses to overwrite an existing
 * object on a conditional put, and records every operation it is asked to perform. The
 * assertions are about observable store state and operation order, not about which methods a
 * mock saw.
 *
 * The golden fixtures under `contracts/fixtures/` are the payload shapes; nothing here
 * restates the frozen wire schema.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { UnknownDatasetKeyError } from './dataset-catalog.ts';
import {
  computeDatasetChecksumSha256,
  sha256Hex,
} from './dataset-checksum.ts';
import {
  ARTIFACT_FORMAT,
  BuildDatasetArtifactInputSchema,
  BuildDatasetArtifactResultSchema,
  type BuildDatasetArtifactResult,
  ContractValidationError,
  LAYOUT_VERSION,
  PARQUET_SCHEMA_FINGERPRINT,
  type ParquetObject,
  SCHEMA_VERSION,
  allowedPrefixFor,
} from './ingestion-contracts.ts';
import {
  CHECKSUM_METADATA_KEY,
  ConditionalWriteIndeterminateError,
  type ConditionalPutOutcome,
  type ObjectHead,
  type ObjectLocation,
  type ObjectStore,
  headManyBounded,
} from '../infrastructure/object-store/object-store.ts';
import {
  DEFAULT_ARTIFACT_VERSION,
  DatasetObjectVerificationError,
  DatasetPublicationConflict,
  DatasetSourceUnavailableError,
  createControlPlaneActivities,
  manifestKeyFor,
} from './control-plane-activities.ts';

const ARTIFACT_BUCKET = 'genomic-artifacts';
const SOURCE_BUCKET = 'genomic-data';

function readFixture(name: string): unknown {
  const url = new URL(`../../../contracts/fixtures/${name}`, import.meta.url);
  return JSON.parse(readFileSync(fileURLToPath(url), 'utf8'));
}

const inputFixture = readFixture('build-dataset-artifact.input.json');
const resultFixture = readFixture('build-dataset-artifact.result.json');
const manifestFixture = readFixture('dataset-manifest.json');

const goldenInput = BuildDatasetArtifactInputSchema.parse(inputFixture);
const goldenResult = BuildDatasetArtifactResultSchema.parse(resultFixture);

const clone = <T>(value: T): T => structuredClone(value);

/** Yields to the event loop so overlapping fake requests are actually observable. */
const tick = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

interface StoredObject {
  body: string;
  etag: string | null;
  versionId: string | null;
  metadata: Record<string, string>;
}

interface SeedOptions {
  readonly etag?: string | null;
  readonly versionId?: string | null;
  readonly checksumSha256?: string | null;
}

/**
 * In-memory object store. `headMany` delegates to the production scheduler in
 * `object-store.ts`, so the ordering and concurrency assertions below exercise the real
 * scheduling code; only the transport is faked.
 */
class FakeObjectStore implements ObjectStore {
  readonly objects = new Map<string, StoredObject>();
  /** Every operation, in completion-independent issue order, as `VERB bucket/key`. */
  readonly operations: string[] = [];
  /** Locations whose `head` raises, standing in for a transient store failure. */
  readonly unavailable = new Set<string>();
  /**
   * Locations whose `putJsonConditional` raises `ConditionalWriteIndeterminateError`, standing
   * in for S3's HTTP 409 `ConditionalRequestConflict` — a concurrent conditional write racing
   * this one, distinct from the ordinary "already exists" (412) case below.
   */
  readonly indeterminateOnPut = new Set<string>();
  maxInFlightHeads = 0;

  #inFlightHeads = 0;
  #putCount = 0;

  static id(location: ObjectLocation): string {
    return `${location.bucket}/${location.key}`;
  }

  seed(location: ObjectLocation, body: string, options: SeedOptions = {}): void {
    const checksum = options.checksumSha256 === undefined ? sha256Hex(body) : options.checksumSha256;
    this.objects.set(FakeObjectStore.id(location), {
      body,
      etag: options.etag === undefined ? `etag-${FakeObjectStore.id(location)}` : options.etag,
      versionId: options.versionId ?? null,
      metadata: checksum === null ? {} : { [CHECKSUM_METADATA_KEY]: checksum },
    });
  }

  operationsMatching(verb: string): string[] {
    return this.operations.filter((entry) => entry.startsWith(`${verb} `));
  }

  async head(location: ObjectLocation): Promise<ObjectHead | null> {
    const id = FakeObjectStore.id(location);
    this.operations.push(`HEAD ${id}`);
    this.#inFlightHeads += 1;
    this.maxInFlightHeads = Math.max(this.maxInFlightHeads, this.#inFlightHeads);
    try {
      await tick();
      if (this.unavailable.has(id)) {
        throw new Error(`object store unavailable for '${id}'`);
      }
      const stored = this.objects.get(id);
      if (stored === undefined) {
        return null;
      }
      return {
        bucket: location.bucket,
        key: location.key,
        etag: stored.etag,
        versionId: stored.versionId,
        contentLength: Buffer.byteLength(stored.body, 'utf8'),
        checksumSha256: stored.metadata[CHECKSUM_METADATA_KEY] ?? null,
      };
    } finally {
      this.#inFlightHeads -= 1;
    }
  }

  async headMany(
    locations: readonly ObjectLocation[],
    options: { readonly concurrency?: number } = {},
  ): Promise<readonly (ObjectHead | null)[]> {
    return headManyBounded((location) => this.head(location), locations, options.concurrency);
  }

  async getJson(location: ObjectLocation): Promise<unknown> {
    const id = FakeObjectStore.id(location);
    this.operations.push(`GET ${id}`);
    await tick();
    const stored = this.objects.get(id);
    return stored === undefined ? null : JSON.parse(stored.body);
  }

  async putJsonConditional(
    location: ObjectLocation,
    value: unknown,
  ): Promise<ConditionalPutOutcome> {
    const id = FakeObjectStore.id(location);
    this.operations.push(`PUT ${id}`);
    await tick();
    if (this.indeterminateOnPut.has(id)) {
      throw new ConditionalWriteIndeterminateError(location);
    }
    if (this.objects.has(id)) {
      return { outcome: 'exists' };
    }
    this.#putCount += 1;
    const etag = `etag-put-${this.#putCount}`;
    this.objects.set(id, {
      body: JSON.stringify(value),
      etag,
      versionId: null,
      metadata: {},
    });
    return { outcome: 'created', etag, versionId: null };
  }
}

function activitiesFor(
  store: FakeObjectStore,
  overrides: { readonly artifactVersion?: string; readonly headConcurrency?: number } = {},
): ReturnType<typeof createControlPlaneActivities> {
  return createControlPlaneActivities({
    objectStore: store,
    artifactBucket: ARTIFACT_BUCKET,
    artifactVersion: overrides.artifactVersion ?? goldenInput.target.artifactVersion,
    ...(overrides.headConcurrency === undefined
      ? {}
      : { headConcurrency: overrides.headConcurrency }),
  });
}

/** Seeds the fake with exactly the objects the golden result declares. */
function seedGoldenInventory(store: FakeObjectStore): void {
  for (const object of goldenResult.parquetObjects) {
    store.seed({ bucket: object.bucket, key: object.key }, 'p'.repeat(object.byteSize), {
      etag: object.etag,
      versionId: object.versionId,
      checksumSha256: object.checksumSha256,
    });
  }
}

const goldenManifestLocation: ObjectLocation = {
  bucket: goldenInput.target.bucket,
  key: manifestKeyFor(goldenInput.datasetId),
};

describe('createControlPlaneActivities never registers buildDatasetArtifact', () => {
  it('exposes exactly inspectDatasetSource and publishDataset, in that order', () => {
    // Brief Step 5: buildDatasetArtifact must never exist in TypeScript — it is Rust-only,
    // scheduled by name on `genomic-ingestion-rust` and never implemented or registered here.
    // This pins the control-plane activity map so a future edit that adds a TypeScript
    // buildDatasetArtifact (or renames/drops one of the two real activities) fails a test
    // instead of silently passing all 144 others.
    const activities = activitiesFor(new FakeObjectStore());
    assert.deepStrictEqual(Object.keys(activities), ['inspectDatasetSource', 'publishDataset']);
  });
});

describe('inspectDatasetSource resolves immutable source identity through the catalog', () => {
  function storeWithSource(options: SeedOptions = {}): FakeObjectStore {
    const store = new FakeObjectStore();
    store.seed({ bucket: SOURCE_BUCKET, key: 'samples/demo_user.vcf' }, 'x'.repeat(1024), {
      etag: 'fixture-etag',
      ...options,
    });
    return store;
  }

  it('reproduces the golden activity input from the catalog entry and the object head', async () => {
    const store = storeWithSource();
    const { inspectDatasetSource } = activitiesFor(store);

    const input = await inspectDatasetSource(goldenInput.datasetId, 'demo-small');

    assert.deepEqual(input, inputFixture);
    assert.deepEqual(store.operations, [`HEAD ${SOURCE_BUCKET}/samples/demo_user.vcf`]);
  });

  it('records the ETag, the nullable version ID and the content length', async () => {
    const store = storeWithSource({ versionId: 'source-version-7' });
    const { inspectDatasetSource } = activitiesFor(store);

    const input = await inspectDatasetSource(goldenInput.datasetId, 'demo-small');

    assert.equal(input.source.etag, 'fixture-etag');
    assert.equal(input.source.versionId, 'source-version-7');
    assert.equal(input.source.contentLength, 1024);
  });

  it('keeps a null version ID for an unversioned bucket', async () => {
    const store = storeWithSource();
    const { inspectDatasetSource } = activitiesFor(store);

    assert.equal((await inspectDatasetSource(goldenInput.datasetId, 'demo-small')).source.versionId, null);
  });

  it('resolves na12878-full to its own catalog key', async () => {
    const store = new FakeObjectStore();
    store.seed({ bucket: SOURCE_BUCKET, key: 'samples/na12878_hg001.vcf.gz' }, 'gz', {
      etag: 'na12878-etag',
    });
    const { inspectDatasetSource } = activitiesFor(store);

    const input = await inspectDatasetSource('ds-na12878', 'na12878-full');

    assert.equal(input.datasetKey, 'na12878-full');
    assert.deepEqual(
      { bucket: input.source.bucket, key: input.source.key },
      { bucket: SOURCE_BUCKET, key: 'samples/na12878_hg001.vcf.gz' },
    );
  });

  it('fails when the object head carries no ETag', async () => {
    const store = storeWithSource({ etag: null });
    const { inspectDatasetSource } = activitiesFor(store);

    await assert.rejects(
      () => inspectDatasetSource(goldenInput.datasetId, 'demo-small'),
      DatasetSourceUnavailableError,
    );
  });

  it('fails when the source object does not exist', async () => {
    const store = new FakeObjectStore();
    const { inspectDatasetSource } = activitiesFor(store);

    await assert.rejects(
      () => inspectDatasetSource(goldenInput.datasetId, 'demo-small'),
      DatasetSourceUnavailableError,
    );
  });

  it('cannot be pointed at a URL, an alternate bucket or a path by its caller', async () => {
    const store = storeWithSource();
    store.seed({ bucket: 'attacker-bucket', key: 'samples/demo_user.vcf' }, 'evil', {
      etag: 'attacker-etag',
    });
    const { inspectDatasetSource } = activitiesFor(store);

    for (const injected of [
      's3://attacker-bucket/samples/demo_user.vcf',
      'https://attacker.example/file.vcf',
      '../../etc/passwd',
      '/etc/passwd',
      'attacker-bucket/samples/demo_user.vcf',
      'demo-small ',
      '',
    ]) {
      await assert.rejects(
        () => inspectDatasetSource(goldenInput.datasetId, injected),
        UnknownDatasetKeyError,
        `'${injected}' must not resolve to a source object`,
      );
    }

    assert.deepEqual(store.operations, [], 'a rejected key must never reach the object store');
  });

  it('derives the writable artifact prefix rather than accepting one', async () => {
    const store = storeWithSource();
    const { inspectDatasetSource } = activitiesFor(store, { artifactVersion: DEFAULT_ARTIFACT_VERSION });

    const input = await inspectDatasetSource('ds-derived', 'demo-small');

    assert.equal(input.target.bucket, ARTIFACT_BUCKET);
    assert.equal(input.target.artifactVersion, DEFAULT_ARTIFACT_VERSION);
    assert.equal(
      input.target.allowedPrefix,
      allowedPrefixFor('ds-derived', DEFAULT_ARTIFACT_VERSION),
    );
  });
});

describe('publishDataset verifies the inventory and writes the manifest last', () => {
  it('publishes exactly the golden manifest', async () => {
    const store = new FakeObjectStore();
    seedGoldenInventory(store);
    const { publishDataset } = activitiesFor(store);

    const manifest = await publishDataset(goldenInput, goldenResult);

    assert.deepEqual(manifest, manifestFixture);
    assert.deepEqual(await store.getJson(goldenManifestLocation), manifestFixture);
  });

  it('binds the manifest to the frozen layout, schema and fingerprint', async () => {
    const store = new FakeObjectStore();
    seedGoldenInventory(store);
    const { publishDataset } = activitiesFor(store);

    const manifest = await publishDataset(goldenInput, goldenResult);

    assert.equal(manifest.artifactFormat, ARTIFACT_FORMAT);
    assert.equal(manifest.layoutVersion, LAYOUT_VERSION);
    assert.equal(manifest.schemaVersion, SCHEMA_VERSION);
    assert.equal(manifest.schemaFingerprint, PARQUET_SCHEMA_FINGERPRINT);
    assert.equal(manifest.referenceVersion, goldenInput.reference.version);
    assert.deepEqual(manifest.parquetObjects, goldenResult.parquetObjects);
  });

  it('heads every declared object before the manifest put, and copies no Parquet payload', async () => {
    const store = new FakeObjectStore();
    seedGoldenInventory(store);
    const { publishDataset } = activitiesFor(store);

    await publishDataset(goldenInput, goldenResult);

    const manifestId = FakeObjectStore.id(goldenManifestLocation);
    assert.deepEqual(store.operations, [
      ...goldenResult.parquetObjects.map(
        (object) => `HEAD ${FakeObjectStore.id({ bucket: object.bucket, key: object.key })}`,
      ),
      `PUT ${manifestId}`,
    ]);
    assert.deepEqual(store.operationsMatching('PUT'), [`PUT ${manifestId}`]);
    assert.equal(
      store.operations.some(
        (entry) => entry.includes('variants/') && !entry.startsWith('HEAD '),
      ),
      false,
      'the payload must never be read back or rewritten',
    );
  });

  it('heads objects in canonical manifest order with bounded concurrency', async () => {
    const store = new FakeObjectStore();
    const { input, result } = wideInventory('ds-wide', goldenInput.target.artifactVersion);
    seedInventory(store, result);
    const { publishDataset } = activitiesFor(store, { headConcurrency: 3 });

    await publishDataset(input, result);

    assert.equal(result.parquetObjects.length, 8);
    assert.deepEqual(
      store.operationsMatching('HEAD'),
      result.parquetObjects.map(
        (object) => `HEAD ${FakeObjectStore.id({ bucket: object.bucket, key: object.key })}`,
      ),
    );
    assert.equal(store.maxInFlightHeads, 3, 'verification must be bounded, and actually parallel');
  });

  it('is idempotent: a second identical publish does not change the manifest identity', async () => {
    const store = new FakeObjectStore();
    seedGoldenInventory(store);
    const { publishDataset } = activitiesFor(store);

    const first = await publishDataset(goldenInput, goldenResult);
    const afterFirst = await store.head(goldenManifestLocation);
    const second = await publishDataset(goldenInput, goldenResult);
    const afterSecond = await store.head(goldenManifestLocation);

    assert.deepEqual(second, first);
    assert.deepEqual(afterSecond, afterFirst);
    assert.equal(store.operationsMatching('PUT').length, 2, 'both attempts stay conditional');
    assert.deepEqual(await store.getJson(goldenManifestLocation), manifestFixture);
  });

  it('propagates ConditionalWriteIndeterminateError untouched instead of treating a 409-shaped conflict as `exists`', async () => {
    const store = new FakeObjectStore();
    seedGoldenInventory(store);
    store.indeterminateOnPut.add(FakeObjectStore.id(goldenManifestLocation));
    const { publishDataset } = activitiesFor(store);

    await assert.rejects(
      () => publishDataset(goldenInput, goldenResult),
      ConditionalWriteIndeterminateError,
    );
    // The whole point of not treating a 409 as `exists`: publishDataset must not race a
    // concurrent writer with an immediate read-back. It must propagate the error and stop,
    // never issuing the GET that only follows the `exists` branch.
    assert.deepEqual(store.operationsMatching('GET'), []);
  });

  it('raises DatasetPublicationConflict when a different manifest is already published', async () => {
    const store = new FakeObjectStore();
    seedGoldenInventory(store);
    const foreign = clone(manifestFixture) as Record<string, unknown>;
    foreign.variantCount = 999;
    store.seed(goldenManifestLocation, JSON.stringify(foreign), { etag: 'etag-existing' });
    const { publishDataset } = activitiesFor(store);

    await assert.rejects(
      () => publishDataset(goldenInput, goldenResult),
      DatasetPublicationConflict,
    );
    assert.deepEqual(await store.getJson(goldenManifestLocation), foreign);
  });

  it('raises DatasetPublicationConflict when the published manifest is not a valid manifest', async () => {
    const store = new FakeObjectStore();
    seedGoldenInventory(store);
    store.seed(goldenManifestLocation, JSON.stringify({ corrupt: true }), { etag: 'etag-existing' });
    const { publishDataset } = activitiesFor(store);

    await assert.rejects(
      () => publishDataset(goldenInput, goldenResult),
      DatasetPublicationConflict,
    );
  });
});

describe('publishDataset writes no manifest when verification fails', () => {
  /**
   * Runs a publish that must fail and asserts the dataset stayed unqueryable: nothing was
   * written at the manifest key at all.
   */
  async function expectNoPublication(
    prepare: (store: FakeObjectStore) => {
      readonly input: typeof goldenInput;
      readonly result: BuildDatasetArtifactResult;
    },
    expected: NonNullable<Parameters<typeof assert.rejects>[1]>,
  ): Promise<FakeObjectStore> {
    const store = new FakeObjectStore();
    const { input, result } = prepare(store);
    const { publishDataset } = activitiesFor(store);

    await assert.rejects(() => publishDataset(input, result), expected);

    assert.deepEqual(store.operationsMatching('PUT'), [], 'no manifest may be written');
    assert.equal(await store.getJson(goldenManifestLocation), null);
    return store;
  }

  function goldenPair(store: FakeObjectStore): {
    input: typeof goldenInput;
    result: BuildDatasetArtifactResult;
  } {
    seedGoldenInventory(store);
    return { input: goldenInput, result: clone(goldenResult) };
  }

  it('when a declared object is missing', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      store.objects.delete(
        FakeObjectStore.id({
          bucket: pair.result.parquetObjects[1]!.bucket,
          key: pair.result.parquetObjects[1]!.key,
        }),
      );
      return pair;
    }, DatasetObjectVerificationError);
  });

  it('when a HEAD request fails outright', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      store.unavailable.add(
        FakeObjectStore.id({
          bucket: pair.result.parquetObjects[0]!.bucket,
          key: pair.result.parquetObjects[0]!.key,
        }),
      );
      return pair;
    }, /object store unavailable for/);
  });

  it('when the stored object carries no ETag (reaches ETAG_MISSING, not ETAG_MISMATCH)', async () => {
    const store = new FakeObjectStore();
    const pair = goldenPair(store);
    const object = pair.result.parquetObjects[0]!;
    store.seed({ bucket: object.bucket, key: object.key }, 'p'.repeat(object.byteSize), {
      etag: null,
      versionId: object.versionId,
      checksumSha256: object.checksumSha256,
    });
    const { publishDataset } = activitiesFor(store);

    await assert.rejects(
      () => publishDataset(pair.input, pair.result),
      (error: unknown) => {
        assert.ok(error instanceof DatasetObjectVerificationError);
        assert.equal(error.code, 'ETAG_MISSING');
        return true;
      },
    );
    assert.deepEqual(store.operationsMatching('PUT'), [], 'no manifest may be written');
    assert.equal(await store.getJson(goldenManifestLocation), null);
  });

  it('when a declared ETag does not match the stored object', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      pair.result.parquetObjects[0]!.etag = 'an-etag-from-another-upload';
      return pair;
    }, DatasetObjectVerificationError);
  });

  it('when a declared version ID does not match the stored object', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      pair.result.parquetObjects[1]!.versionId = 'some-other-version';
      return pair;
    }, DatasetObjectVerificationError);
  });

  it('when the stored size contradicts the declared byte size', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      const object = pair.result.parquetObjects[0]!;
      store.seed({ bucket: object.bucket, key: object.key }, 'p'.repeat(object.byteSize - 1), {
        etag: object.etag,
        versionId: object.versionId,
        checksumSha256: object.checksumSha256,
      });
      return pair;
    }, DatasetObjectVerificationError);
  });

  it('when the stored object carries no checksum metadata', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      const object = pair.result.parquetObjects[0]!;
      store.seed({ bucket: object.bucket, key: object.key }, 'p'.repeat(object.byteSize), {
        etag: object.etag,
        versionId: object.versionId,
        checksumSha256: null,
      });
      return pair;
    }, DatasetObjectVerificationError);
  });

  it('when the checksum metadata contradicts the declared content checksum', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      const object = pair.result.parquetObjects[0]!;
      store.seed({ bucket: object.bucket, key: object.key }, 'p'.repeat(object.byteSize), {
        etag: object.etag,
        versionId: object.versionId,
        checksumSha256: 'f'.repeat(64),
      });
      return pair;
    }, DatasetObjectVerificationError);
  });

  it('when the attempt prefix escapes the dataset version prefix', async () => {
    const store = await expectNoPublication((fake) => {
      const pair = goldenPair(fake);
      pair.result.attemptPrefix = 'datasets/ds-other/versions/iv-test-001/attempt-1/';
      return pair;
    }, ContractValidationError);
    assert.deepEqual(store.operationsMatching('HEAD'), [], 'a bad prefix is rejected before any HEAD');
  });

  it('when an object key sits outside the attempt prefix', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      pair.result.parquetObjects[0]!.key = 'datasets/ds-test-001/manifest.json';
      return pair;
    }, ContractValidationError);
  });

  it('when the inventory is not in canonical order', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      pair.result.parquetObjects.reverse();
      return pair;
    }, ContractValidationError);
  });

  it('when the declared dataset checksum does not reproduce', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      pair.result.datasetChecksumSha256 = '0'.repeat(64);
      return pair;
    }, ContractValidationError);
  });

  it('when the inventory is empty', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      pair.result.parquetObjects = [];
      return pair;
    }, ContractValidationError);
  });

  it('when the result was produced against another reference build', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      pair.result.referenceBuild = 'GRCh37';
      return pair;
    }, ContractValidationError);
  });

  it('when the input targets a bucket this worker does not publish to', async () => {
    const store = new FakeObjectStore();
    seedGoldenInventory(store);
    const { publishDataset } = createControlPlaneActivities({
      objectStore: store,
      artifactBucket: 'some-other-artifact-bucket',
      artifactVersion: goldenInput.target.artifactVersion,
    });

    await assert.rejects(() => publishDataset(goldenInput, goldenResult), ContractValidationError);
    assert.deepEqual(store.operations, [], 'nothing may reach the store');
  });

  it('when the input targets an artifact version this worker does not publish', async () => {
    const store = new FakeObjectStore();
    seedGoldenInventory(store);
    const { publishDataset } = createControlPlaneActivities({
      objectStore: store,
      artifactBucket: ARTIFACT_BUCKET,
      artifactVersion: `${goldenInput.target.artifactVersion}-other`,
    });

    await assert.rejects(() => publishDataset(goldenInput, goldenResult), ContractValidationError);
    assert.deepEqual(store.operations, [], 'nothing may reach the store');
  });

  it('when the inventory lives in a bucket the input does not target', async () => {
    await expectNoPublication((store) => {
      const pair = goldenPair(store);
      for (const object of pair.result.parquetObjects) {
        object.bucket = 'attacker-bucket';
        store.seed({ bucket: object.bucket, key: object.key }, 'p'.repeat(object.byteSize), {
          etag: object.etag,
          versionId: object.versionId,
          checksumSha256: object.checksumSha256,
        });
      }
      return pair;
    }, ContractValidationError);
  });
});

/** Eight partitions whose chromosome names are already in byte-wise canonical order. */
const WIDE_CHROMS = ['1', '10', '11', '12', '2', '3', 'X', 'Y'] as const;

function wideInventory(
  datasetId: string,
  artifactVersion: string,
): { input: typeof goldenInput; result: BuildDatasetArtifactResult } {
  const allowedPrefix = allowedPrefixFor(datasetId, artifactVersion);
  const attemptPrefix = `${allowedPrefix}attempt-2/`;
  const parquetObjects: ParquetObject[] = WIDE_CHROMS.map((chrom, index) => {
    const body = `parquet-body-${chrom}`;
    return {
      bucket: ARTIFACT_BUCKET,
      key: `${attemptPrefix}variants/chrom=${chrom}/part-000.parquet`,
      etag: `etag-${chrom}`,
      versionId: index % 2 === 0 ? null : `version-${chrom}`,
      chrom,
      checksumSha256: sha256Hex(body),
      byteSize: Buffer.byteLength(body, 'utf8'),
      rowCount: 100 + index,
      minPos: 1_000 + index,
      maxPos: 2_000 + index,
    };
  });

  return {
    input: BuildDatasetArtifactInputSchema.parse({
      ...clone(inputFixture as object),
      datasetId,
      target: { bucket: ARTIFACT_BUCKET, artifactVersion, allowedPrefix },
    }),
    result: BuildDatasetArtifactResultSchema.parse({
      ...clone(resultFixture as object),
      attemptPrefix,
      datasetChecksumSha256: computeDatasetChecksumSha256(attemptPrefix, parquetObjects),
      parquetObjects,
    }),
  };
}

function seedInventory(store: FakeObjectStore, result: BuildDatasetArtifactResult): void {
  for (const object of result.parquetObjects) {
    const body = `parquet-body-${object.chrom}`;
    store.seed({ bucket: object.bucket, key: object.key }, body, {
      etag: object.etag,
      versionId: object.versionId,
      checksumSha256: object.checksumSha256,
    });
  }
}
