/**
 * Contract freeze tests.
 *
 * These assert real serialization behaviour against the golden fixtures under
 * `contracts/fixtures/`, which are read verbatim by both TypeScript and Rust. They must
 * not restate the schema inline: if a wire name changes, the fixture parse fails here and
 * the equivalent Rust test in `rust-ingestion-worker/src/contracts.rs` fails too.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  ARTIFACT_FORMAT,
  BuildDatasetArtifactInputSchema,
  BuildDatasetArtifactResultSchema,
  CONTRACT_VERSION,
  ContractValidationError,
  DatasetManifestSchema,
  IngestionHeartbeatSchema,
  LAYOUT_VERSION,
  PARQUET_SCHEMA_COLUMNS,
  PARQUET_SCHEMA_FINGERPRINT,
  PARTITION_SPEC,
  SCHEMA_VERSION,
  SORT_ORDER,
  VARIANTS_SEGMENT,
} from './ingestion-contracts.ts';
import {
  assertValidArtifactResult,
  assertValidDatasetManifest,
  computeDatasetChecksumSha256,
  sha256Hex,
} from './dataset-checksum.ts';
import { UnknownDatasetKeyError, datasetCatalog } from './dataset-catalog.ts';

function readFixture(name: string): unknown {
  const url = new URL(`../../../contracts/fixtures/${name}`, import.meta.url);
  return JSON.parse(readFileSync(fileURLToPath(url), 'utf8'));
}

const inputFixture = readFixture('build-dataset-artifact.input.json');
const resultFixture = readFixture('build-dataset-artifact.result.json');
const manifestFixture = readFixture('dataset-manifest.json');

/** Exactly the heartbeat payload frozen for the Rust activity. */
const heartbeatFixture = {
  phase: 'PARSING',
  processedBytes: 4096,
  processedVariants: 2500,
  currentPartition: '12',
  completedFiles: 3,
  uploadedBytes: 1048576,
};

const clone = <T>(value: T): T => structuredClone(value);

describe('golden fixtures round-trip through the frozen schemas', () => {
  it('parses the build input without dropping or renaming a field', () => {
    const parsed = BuildDatasetArtifactInputSchema.parse(inputFixture);
    assert.deepEqual(parsed, inputFixture);
    assert.equal(parsed.contractVersion, CONTRACT_VERSION);
    assert.equal(parsed.datasetKey, 'demo-small');
    assert.equal(parsed.source.versionId, null);
  });

  it('parses the build result without dropping or renaming a field', () => {
    const parsed = BuildDatasetArtifactResultSchema.parse(resultFixture);
    assert.deepEqual(parsed, resultFixture);
    assert.equal(parsed.parquetObjects.length, 2);
    assert.equal(parsed.parquetObjects[0]!.versionId, null);
    assert.equal(parsed.parquetObjects[1]!.versionId, 'fixture-version-chrom-12');
  });

  it('parses the manifest without dropping or renaming a field', () => {
    const parsed = DatasetManifestSchema.parse(manifestFixture);
    assert.deepEqual(parsed, manifestFixture);
    assert.equal(parsed.artifactFormat, ARTIFACT_FORMAT);
    assert.equal(parsed.layoutVersion, LAYOUT_VERSION);
    assert.equal(parsed.schemaVersion, SCHEMA_VERSION);
    assert.equal(parsed.schemaFingerprint, PARQUET_SCHEMA_FINGERPRINT);
    assert.deepEqual(parsed.partitionSpec, PARTITION_SPEC);
    assert.deepEqual(parsed.sortOrder, SORT_ORDER);
  });

  it('publishes the build result inventory unchanged into the manifest', () => {
    const result = BuildDatasetArtifactResultSchema.parse(resultFixture);
    const manifest = DatasetManifestSchema.parse(manifestFixture);
    assert.deepEqual(manifest.parquetObjects, result.parquetObjects);
    assert.equal(manifest.attemptPrefix, result.attemptPrefix);
    assert.equal(manifest.datasetChecksumSha256, result.datasetChecksumSha256);
    assert.equal(manifest.variantCount, result.variantCount);
    assert.equal(manifest.rejectedRecordCount, result.rejectedRecordCount);
    assert.equal(manifest.referenceBuild, result.referenceBuild);
    assert.equal(manifest.processorVersion, result.processorVersion);
  });

  it('parses the ingestion heartbeat and allows a null partition', () => {
    assert.deepEqual(IngestionHeartbeatSchema.parse(heartbeatFixture), heartbeatFixture);
    const downloading = { ...heartbeatFixture, phase: 'DOWNLOADING_SOURCE', currentPartition: null };
    assert.deepEqual(IngestionHeartbeatSchema.parse(downloading), downloading);
  });
});

describe('the wire schemas are closed', () => {
  it('rejects an unknown field on the input', () => {
    const extra = { ...clone(inputFixture as object), localPath: '/tmp/evil.vcf' };
    assert.equal(BuildDatasetArtifactInputSchema.safeParse(extra).success, false);
  });

  it('rejects an unknown field on a nested source object', () => {
    const extra = clone(inputFixture) as { source: Record<string, unknown> };
    extra.source.url = 'https://attacker.example/file.vcf';
    assert.equal(BuildDatasetArtifactInputSchema.safeParse(extra).success, false);
  });

  it('rejects an unknown field on a Parquet object descriptor', () => {
    const extra = clone(resultFixture) as { parquetObjects: Record<string, unknown>[] };
    extra.parquetObjects[0]!.uri = 's3://genomic-artifacts/anything.parquet';
    assert.equal(BuildDatasetArtifactResultSchema.safeParse(extra).success, false);
  });

  it('rejects an unknown field on the manifest', () => {
    const extra = { ...clone(manifestFixture as object), publishedAt: '2026-08-02T00:00:00Z' };
    assert.equal(DatasetManifestSchema.safeParse(extra).success, false);
  });

  it('rejects an unknown field on the heartbeat', () => {
    assert.equal(
      IngestionHeartbeatSchema.safeParse({ ...heartbeatFixture, stackTrace: 'boom' }).success,
      false,
    );
  });

  it('rejects an unknown contract version', () => {
    const bumped = { ...clone(inputFixture as object), contractVersion: 2 };
    assert.equal(BuildDatasetArtifactInputSchema.safeParse(bumped).success, false);
  });

  it('rejects an unknown layout version on the manifest', () => {
    const bumped = { ...clone(manifestFixture as object), layoutVersion: 2 };
    assert.equal(DatasetManifestSchema.safeParse(bumped).success, false);
  });

  it('rejects an unknown dataset key', () => {
    for (const datasetKey of ['na12878', 's3://attacker/file.vcf', '../../etc/passwd', '']) {
      const rejected = { ...clone(inputFixture as object), datasetKey };
      assert.equal(BuildDatasetArtifactInputSchema.safeParse(rejected).success, false);
    }
  });

  it('rejects an unknown heartbeat phase', () => {
    assert.equal(
      IngestionHeartbeatSchema.safeParse({ ...heartbeatFixture, phase: 'DELETING' }).success,
      false,
    );
  });
});

describe('the dataset content checksum is derived from relative descriptors', () => {
  const result = BuildDatasetArtifactResultSchema.parse(resultFixture);

  it('reproduces the checksum recorded in the golden result', () => {
    assert.equal(
      computeDatasetChecksumSha256(result.attemptPrefix, result.parquetObjects),
      result.datasetChecksumSha256,
    );
  });

  it('is independent of the attempt prefix', () => {
    const otherPrefix = 'datasets/ds-test-001/versions/iv-test-001/attempt-7/';
    const rehomed = result.parquetObjects.map((object) => ({
      ...object,
      key: otherPrefix + object.key.slice(result.attemptPrefix.length),
      etag: 'a-different-upload-etag',
      versionId: null,
    }));
    assert.equal(
      computeDatasetChecksumSha256(otherPrefix, rehomed),
      result.datasetChecksumSha256,
    );
  });

  it('is independent of the order the descriptors are listed in', () => {
    assert.equal(
      computeDatasetChecksumSha256(result.attemptPrefix, [...result.parquetObjects].reverse()),
      result.datasetChecksumSha256,
    );
  });

  it('changes when file content changes', () => {
    const tampered = clone(result.parquetObjects);
    tampered[0]!.checksumSha256 = 'f'.repeat(64);
    assert.notEqual(
      computeDatasetChecksumSha256(result.attemptPrefix, tampered),
      result.datasetChecksumSha256,
    );
  });
});

describe('artifact result inventory validation', () => {
  const input = BuildDatasetArtifactInputSchema.parse(inputFixture);
  const result = BuildDatasetArtifactResultSchema.parse(resultFixture);

  function expectRejection(mutate: (draft: typeof result) => void, code: string): void {
    const draft = clone(result);
    mutate(draft);
    assert.throws(
      () => assertValidArtifactResult(input, draft),
      (error: unknown) => {
        assert.ok(error instanceof ContractValidationError);
        assert.equal(error.code, code);
        return true;
      },
    );
  }

  it('accepts the golden input/result pair', () => {
    assert.doesNotThrow(() => assertValidArtifactResult(input, result));
  });

  it('accepts the mandated attempt-{n}/variants/chrom=<value>/ layout', () => {
    for (const object of result.parquetObjects) {
      assert.match(
        object.key,
        /^datasets\/[^/]+\/versions\/[^/]+\/attempt-\d+\/variants\/chrom=[^/]+\/part-\d{3}\.parquet$/,
      );
    }
    assert.doesNotThrow(() => assertValidArtifactResult(input, result));
  });

  it('rejects a key that omits the variants/ segment', () => {
    expectRejection((draft) => {
      for (const object of draft.parquetObjects) {
        object.key = `${draft.attemptPrefix}chrom=${object.chrom}/part-000.parquet`;
      }
    }, 'KEY_OUTSIDE_ALLOWED_PREFIX');
  });

  it('rejects an empty inventory', () => {
    expectRejection((draft) => {
      draft.parquetObjects = [];
    }, 'EMPTY_INVENTORY');
  });

  it('rejects an attempt prefix outside the allowed version prefix', () => {
    expectRejection((draft) => {
      draft.attemptPrefix = 'datasets/ds-other/versions/iv-test-001/attempt-1/';
    }, 'ATTEMPT_PREFIX_OUTSIDE_ALLOWED_PREFIX');
  });

  it('rejects a key outside the attempt prefix', () => {
    expectRejection((draft) => {
      draft.parquetObjects[0]!.key = 'datasets/ds-other/versions/iv-x/chrom=1/part-000.parquet';
    }, 'KEY_OUTSIDE_ALLOWED_PREFIX');
  });

  it('rejects a descriptor in another bucket', () => {
    expectRejection((draft) => {
      draft.parquetObjects[0]!.bucket = 'attacker-bucket';
    }, 'BUCKET_MISMATCH');
  });

  it('rejects duplicate keys', () => {
    expectRejection((draft) => {
      draft.parquetObjects[1] = clone(draft.parquetObjects[0]!);
    }, 'DUPLICATE_KEY');
  });

  it('rejects a noncanonical file ordering', () => {
    expectRejection((draft) => {
      draft.parquetObjects.reverse();
    }, 'NONCANONICAL_ORDER');
  });

  it('rejects a chromosome that contradicts its partition directory', () => {
    expectRejection((draft) => {
      draft.parquetObjects[1]!.chrom = '13';
    }, 'PARTITION_MISMATCH');
  });

  it('rejects a relative path that is not chromosome partitioned', () => {
    expectRejection((draft) => {
      draft.parquetObjects[0]!.key = `${draft.attemptPrefix}${VARIANTS_SEGMENT}part-000.parquet`;
    }, 'PARTITION_MISMATCH');
  });

  it('rejects an allowedPrefix that is not derived from the dataset and artifact version', () => {
    for (const allowedPrefix of ['datasets/', 'datasets/ds-test-001/', 'datasets/ds-other/versions/iv-test-001/']) {
      const widened = clone(input);
      (widened.target as { allowedPrefix: string }).allowedPrefix = allowedPrefix;
      assert.throws(
        () => assertValidArtifactResult(widened, result),
        (error: unknown) => {
          assert.ok(error instanceof ContractValidationError);
          assert.equal(error.code, 'ALLOWED_PREFIX_MISMATCH');
          return true;
        },
      );
    }
  });

  it('rejects a dataset checksum that does not match the descriptor list', () => {
    expectRejection((draft) => {
      draft.datasetChecksumSha256 = '0'.repeat(64);
    }, 'DATASET_CHECKSUM_MISMATCH');
  });

  it('rejects a result whose reference build contradicts the requested reference', () => {
    expectRejection((draft) => {
      draft.referenceBuild = 'GRCh37';
    }, 'REFERENCE_BUILD_MISMATCH');
  });
});

describe('published manifest validation', () => {
  const manifest = DatasetManifestSchema.parse(manifestFixture);

  function expectRejection(mutate: (draft: typeof manifest) => void, code: string): void {
    const draft = clone(manifest);
    mutate(draft);
    assert.throws(
      () => assertValidDatasetManifest(draft),
      (error: unknown) => {
        assert.ok(error instanceof ContractValidationError);
        assert.equal(error.code, code);
        return true;
      },
    );
  }

  it('accepts the golden manifest', () => {
    assert.doesNotThrow(() => assertValidDatasetManifest(manifest));
  });

  it('derives the allowed prefix from its own dataset and artifact version', () => {
    expectRejection((draft) => {
      draft.datasetId = 'ds-someone-else';
    }, 'ATTEMPT_PREFIX_OUTSIDE_ALLOWED_PREFIX');
  });

  it('rejects a dataset checksum that does not match the descriptor list', () => {
    expectRejection((draft) => {
      draft.datasetChecksumSha256 = '0'.repeat(64);
    }, 'DATASET_CHECKSUM_MISMATCH');
  });

  it('rejects a schema fingerprint that is not the frozen Parquet schema', () => {
    expectRejection((draft) => {
      draft.schemaFingerprint = '0'.repeat(64);
    }, 'SCHEMA_FINGERPRINT_MISMATCH');
  });

  it('rejects an inventory published outside the expected artifact bucket', () => {
    assert.throws(
      () => assertValidDatasetManifest(manifest, { expectedBucket: 'some-other-bucket' }),
      (error: unknown) => {
        assert.ok(error instanceof ContractValidationError);
        assert.equal(error.code, 'BUCKET_MISMATCH');
        return true;
      },
    );
  });
});

describe('the schema module is importable from Temporal workflow code', () => {
  const moduleDir = fileURLToPath(new URL('.', import.meta.url));

  /**
   * Every module `ingestion-contracts.ts` reaches transitively through relative imports.
   * A Node built-in anywhere in that graph would be pulled into the workflow bundle, where
   * it cannot be resolved.
   */
  function localImportGraph(entry: string): Map<string, string> {
    const sources = new Map<string, string>();
    const pending = [entry];
    while (pending.length > 0) {
      const specifier = pending.pop()!;
      if (sources.has(specifier)) continue;
      const source = readFileSync(join(moduleDir, specifier), 'utf8');
      sources.set(specifier, source);
      for (const match of source.matchAll(/from\s+'(\.[^']+)'/g)) {
        pending.push(match[1]!);
      }
    }
    return sources;
  }

  it('pulls in no node: built-in, directly or transitively', () => {
    const graph = localImportGraph('./ingestion-contracts.ts');
    assert.ok(graph.size >= 2, 'expected the domain module to be part of the graph');
    for (const [specifier, source] of graph) {
      assert.equal(
        /from\s+'node:[^']+'|require\(\s*'node:/.test(source),
        false,
        `${specifier} imports a node: built-in; workflow code cannot bundle it`,
      );
    }
  });

  it('keeps the checksum helpers in a module workflow code does not import', () => {
    const checksumModule = readFileSync(join(moduleDir, 'dataset-checksum.ts'), 'utf8');
    assert.match(checksumModule, /from 'node:crypto'/);
    // Prose in the module header may name `node:crypto`; an import or a call may not.
    assert.doesNotMatch(
      readFileSync(join(moduleDir, 'ingestion-contracts.ts'), 'utf8'),
      /from 'node:crypto'|createHash\(/,
    );
  });

  it('pins the schema fingerprint to the digest it claims to be', () => {
    assert.equal(sha256Hex(PARQUET_SCHEMA_COLUMNS), PARQUET_SCHEMA_FINGERPRINT);
  });
});

describe('seeded dataset catalog', () => {
  it('maps demo-small to a fixed S3 identity', () => {
    assert.deepEqual(datasetCatalog.get('demo-small').source, {
      bucket: 'genomic-data',
      key: 'samples/demo_user.vcf',
    });
  });

  it('maps na12878-full to a fixed S3 identity', () => {
    assert.deepEqual(datasetCatalog.get('na12878-full').source, {
      bucket: 'genomic-data',
      key: 'samples/na12878_hg001.vcf.gz',
    });
  });

  it('rejects anything that is not a seeded key', () => {
    assert.throws(() => datasetCatalog.get('s3://attacker/file.vcf'), UnknownDatasetKeyError);
    assert.throws(() => datasetCatalog.get('https://attacker/file.vcf'), UnknownDatasetKeyError);
    assert.throws(() => datasetCatalog.get('../../tests/fixtures/demo_user.vcf'), UnknownDatasetKeyError);
    assert.throws(() => datasetCatalog.get('/etc/passwd'), UnknownDatasetKeyError);
    assert.throws(() => datasetCatalog.get('demo-small '), UnknownDatasetKeyError);
    assert.throws(() => datasetCatalog.get(''), UnknownDatasetKeyError);
  });

  it('lists exactly the two seeded datasets with their pinned reference', () => {
    const entries = datasetCatalog.list();
    assert.deepEqual(
      entries.map((entry) => entry.key),
      ['demo-small', 'na12878-full'],
    );
    for (const entry of entries) {
      assert.equal(entry.expectedReferenceBuild, 'GRCh38');
      assert.equal(entry.referenceVersion, 'demo-clinvar-grch38-v2');
      assert.ok(entry.displayName.length > 0);
    }
  });

  it('keeps the display name separate from the S3 identity', () => {
    const entry = datasetCatalog.get('demo-small');
    assert.deepEqual(Object.keys(entry.source), ['bucket', 'key']);
    assert.notEqual(entry.displayName, entry.source.key);
  });

  it('cannot be mutated by a caller', () => {
    const entry = datasetCatalog.get('demo-small');
    assert.throws(() => {
      (entry.source as { bucket: string }).bucket = 'attacker-bucket';
    }, TypeError);
    assert.equal(datasetCatalog.get('demo-small').source.bucket, 'genomic-data');
  });
});
