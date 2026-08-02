/**
 * Frozen `ingestion-v1` wire schemas.
 *
 * Every payload that crosses the TypeScript/Rust boundary is defined here and mirrored by
 * `rust-ingestion-worker/src/contracts.rs`. Rules:
 *
 * - JSON-compatible primitives only, camelCase field names. No dates, BigInt, buffers,
 *   class instances or serialized language-native errors.
 * - Wire objects are `.strict()`: an unknown field is a contract violation, not a warning.
 * - The dataset content checksum is derived from *relative* descriptors, so it is stable
 *   across activity attempts and S3 prefixes.
 *
 * Workflow code must import from this module with `import type` only: the checksum helper
 * uses `node:crypto`, which is unavailable inside the Temporal workflow sandbox.
 */
import { createHash } from 'node:crypto';

import { z } from 'zod';

import { DATASET_KEYS } from '../domain/datasets.ts';

/** Wire contract version carried by every activity input. */
export const CONTRACT_VERSION = 1;

/** Physical artifact layout version (prefix shape, partition directories). */
export const LAYOUT_VERSION = 1;

/** Logical Parquet column set version. */
export const SCHEMA_VERSION = 1;

export const ARTIFACT_FORMAT = 'parquet-dataset';

/** Parquet is partitioned by chromosome. */
export const PARTITION_SPEC = ['chrom'] as const;

/** Rows are globally ordered by these columns; `chrom` comes from the partition directory. */
export const SORT_ORDER = ['chrom', 'pos', 'ref', 'alt'] as const;

/**
 * Canonical description of the physical Parquet file schema. `chrom` is encoded by the
 * `chrom=<value>` directory and is not a physical column.
 */
export const PARQUET_SCHEMA_COLUMNS =
  'pos:UINTEGER:NOT NULL;rsid:VARCHAR:NULL;ref:VARCHAR:NOT NULL;alt:VARCHAR:NOT NULL;gt_raw:VARCHAR:NOT NULL';

/** SHA-256 of `PARQUET_SCHEMA_COLUMNS`; recorded in every published manifest. */
export const PARQUET_SCHEMA_FINGERPRINT = sha256Hex(PARQUET_SCHEMA_COLUMNS);

/** Activity progress phases, in the order the Rust worker reports them. */
export const INGESTION_PHASES = [
  'DOWNLOADING_SOURCE',
  'PARSING',
  'WRITING_DUCKDB',
  'EXPORTING_PARQUET',
  'UPLOADING_PARTITION',
  'FINALIZING',
] as const;

function sha256Hex(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

const sha256HexSchema = z.string().regex(/^[0-9a-f]{64}$/, 'expected a lowercase SHA-256 hex digest');

/** A single S3 path segment: no separators, no `=` (reserved for partition directories). */
const pathSegmentSchema = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._-]*$/, 'unsafe path segment');

/** A key prefix ending in `/`, built only from safe segments. */
const keyPrefixSchema = z
  .string()
  .regex(/^([A-Za-z0-9][A-Za-z0-9._-]*\/)+$/, 'unsafe object key prefix');

/** A full object key; `=` is allowed because partition directories use `chrom=<value>`. */
const objectKeySchema = z
  .string()
  .regex(/^([A-Za-z0-9][A-Za-z0-9._=-]*\/)*[A-Za-z0-9][A-Za-z0-9._=-]*$/, 'unsafe object key');

const bucketSchema = z.string().regex(/^[a-z0-9][a-z0-9.-]{2,62}$/, 'invalid bucket name');

const nonNegativeInt = z.number().int().nonnegative();

/** Fields shared by every S3 object reference on the wire. */
const s3ObjectRefShape = {
  bucket: bucketSchema,
  key: objectKeySchema,
  etag: z.string().min(1),
  versionId: z.string().min(1).nullable(),
};

export const DatasetKeySchema = z.enum(DATASET_KEYS);

export const S3ObjectRefSchema = z.object(s3ObjectRefShape).strict();

export const SourceObjectSchema = z
  .object({ ...s3ObjectRefShape, contentLength: nonNegativeInt })
  .strict();

export const ReferenceSelectorSchema = z
  .object({ build: z.string().min(1), version: z.string().min(1) })
  .strict();

export const ArtifactTargetSchema = z
  .object({
    bucket: bucketSchema,
    artifactVersion: pathSegmentSchema,
    allowedPrefix: keyPrefixSchema,
  })
  .strict();

export const BuildDatasetArtifactInputSchema = z
  .object({
    contractVersion: z.literal(CONTRACT_VERSION),
    datasetId: pathSegmentSchema,
    datasetKey: DatasetKeySchema,
    source: SourceObjectSchema,
    reference: ReferenceSelectorSchema,
    target: ArtifactTargetSchema,
  })
  .strict();

/** One published Parquet object plus the statistics used for partition/row-group pruning. */
export const ParquetObjectSchema = z
  .object({
    ...s3ObjectRefShape,
    chrom: z.string().min(1),
    checksumSha256: sha256HexSchema,
    byteSize: nonNegativeInt,
    rowCount: nonNegativeInt,
    minPos: nonNegativeInt,
    maxPos: nonNegativeInt,
  })
  .strict();

/** Fields the activity result and the published manifest share verbatim. */
const artifactInventoryShape = {
  attemptPrefix: keyPrefixSchema,
  datasetChecksumSha256: sha256HexSchema,
  variantCount: nonNegativeInt,
  rejectedRecordCount: nonNegativeInt,
  referenceBuild: z.string().min(1),
  processorVersion: z.string().min(1),
  parquetObjects: z.array(ParquetObjectSchema),
};

export const BuildDatasetArtifactResultSchema = z.object(artifactInventoryShape).strict();

export const DatasetManifestSchema = z
  .object({
    datasetId: pathSegmentSchema,
    artifactFormat: z.literal(ARTIFACT_FORMAT),
    layoutVersion: z.literal(LAYOUT_VERSION),
    schemaVersion: z.literal(SCHEMA_VERSION),
    schemaFingerprint: sha256HexSchema,
    artifactVersion: pathSegmentSchema,
    referenceVersion: z.string().min(1),
    partitionSpec: z.tuple([z.literal('chrom')]),
    sortOrder: z.tuple([
      z.literal('chrom'),
      z.literal('pos'),
      z.literal('ref'),
      z.literal('alt'),
    ]),
    ...artifactInventoryShape,
  })
  .strict();

export const IngestionHeartbeatSchema = z
  .object({
    phase: z.enum(INGESTION_PHASES),
    processedBytes: nonNegativeInt,
    processedVariants: nonNegativeInt,
    currentPartition: z.string().min(1).nullable(),
    completedFiles: nonNegativeInt,
    uploadedBytes: nonNegativeInt,
  })
  .strict();

export type S3ObjectRef = z.infer<typeof S3ObjectRefSchema>;
export type SourceObject = z.infer<typeof SourceObjectSchema>;
export type ReferenceSelector = z.infer<typeof ReferenceSelectorSchema>;
export type ArtifactTarget = z.infer<typeof ArtifactTargetSchema>;
export type BuildDatasetArtifactInput = z.infer<typeof BuildDatasetArtifactInputSchema>;
export type ParquetObject = z.infer<typeof ParquetObjectSchema>;
export type BuildDatasetArtifactResult = z.infer<typeof BuildDatasetArtifactResultSchema>;
export type DatasetManifest = z.infer<typeof DatasetManifestSchema>;
export type IngestionHeartbeat = z.infer<typeof IngestionHeartbeatSchema>;
export type IngestionPhase = z.infer<typeof IngestionHeartbeatSchema>['phase'];

export type ContractValidationCode =
  | 'EMPTY_INVENTORY'
  | 'ATTEMPT_PREFIX_OUTSIDE_ALLOWED_PREFIX'
  | 'KEY_OUTSIDE_ALLOWED_PREFIX'
  | 'BUCKET_MISMATCH'
  | 'DUPLICATE_KEY'
  | 'NONCANONICAL_ORDER'
  | 'PARTITION_MISMATCH'
  | 'DATASET_CHECKSUM_MISMATCH'
  | 'SCHEMA_FINGERPRINT_MISMATCH'
  | 'REFERENCE_BUILD_MISMATCH';

/** Raised when a structurally valid payload violates an inventory or identity invariant. */
export class ContractValidationError extends Error {
  readonly code: ContractValidationCode;

  constructor(code: ContractValidationCode, message: string) {
    super(`${code}: ${message}`);
    this.name = 'ContractValidationError';
    this.code = code;
  }
}

function fail(code: ContractValidationCode, message: string): never {
  throw new ContractValidationError(code, message);
}

/** Byte-wise ascending comparison, matching Rust's `Ord for str`. */
function compareUtf8(left: string, right: string): number {
  return Buffer.compare(Buffer.from(left, 'utf8'), Buffer.from(right, 'utf8'));
}

function relativePathOf(attemptPrefix: string, key: string): string {
  if (!key.startsWith(attemptPrefix)) {
    fail('KEY_OUTSIDE_ALLOWED_PREFIX', `object key '${key}' is not below '${attemptPrefix}'`);
  }
  return key.slice(attemptPrefix.length);
}

/**
 * Canonical descriptor list: relative path, content checksum and statistics, sorted
 * byte-wise by `(chrom, relativePath)`. Deliberately excludes bucket, key prefix, ETag and
 * version ID so the same dataset content yields the same checksum on every attempt.
 */
function canonicalDescriptorBlock(attemptPrefix: string, objects: readonly ParquetObject[]): string {
  return objects
    .map((object) => ({ object, relativePath: relativePathOf(attemptPrefix, object.key) }))
    .sort(
      (left, right) =>
        compareUtf8(left.object.chrom, right.object.chrom) ||
        compareUtf8(left.relativePath, right.relativePath),
    )
    .map(
      ({ object, relativePath }) =>
        [
          object.chrom,
          relativePath,
          object.checksumSha256,
          object.byteSize,
          object.rowCount,
          object.minPos,
          object.maxPos,
        ].join('\t') + '\n',
    )
    .join('');
}

/** Deterministic content checksum of a Parquet dataset. Independent of the attempt prefix. */
export function computeDatasetChecksumSha256(
  attemptPrefix: string,
  objects: readonly ParquetObject[],
): string {
  return sha256Hex(canonicalDescriptorBlock(attemptPrefix, objects));
}

interface InventoryExpectations {
  readonly allowedPrefix: string;
  readonly attemptPrefix: string;
  readonly datasetChecksumSha256: string;
  readonly objects: readonly ParquetObject[];
  readonly expectedBucket?: string;
}

/**
 * Enforces the invariants a Parquet inventory must satisfy before anything is published or
 * queried: single bucket, every key below the attempt prefix below the allowed immutable
 * version prefix, `chrom=<value>` partition agreement, no duplicates, canonical ordering
 * and a reproducible dataset checksum.
 */
export function assertCanonicalArtifactInventory(expectations: InventoryExpectations): void {
  const { allowedPrefix, attemptPrefix, datasetChecksumSha256, objects } = expectations;

  if (objects.length === 0) {
    fail('EMPTY_INVENTORY', 'a published dataset must declare at least one Parquet object');
  }

  if (!attemptPrefix.startsWith(allowedPrefix) || attemptPrefix.length === allowedPrefix.length) {
    fail(
      'ATTEMPT_PREFIX_OUTSIDE_ALLOWED_PREFIX',
      `attempt prefix '${attemptPrefix}' is not below '${allowedPrefix}'`,
    );
  }

  const expectedBucket = expectations.expectedBucket ?? objects[0]!.bucket;
  const relativePaths: string[] = [];

  for (const object of objects) {
    if (object.bucket !== expectedBucket) {
      fail(
        'BUCKET_MISMATCH',
        `object '${object.key}' is in bucket '${object.bucket}', expected '${expectedBucket}'`,
      );
    }

    const relativePath = relativePathOf(attemptPrefix, object.key);
    const partition = /^chrom=([^/]+)\/[^/]+$/.exec(relativePath);
    if (partition === null) {
      fail(
        'PARTITION_MISMATCH',
        `'${relativePath}' is not a 'chrom=<value>/<file>' partition path`,
      );
    }
    if (partition[1] !== object.chrom) {
      fail(
        'PARTITION_MISMATCH',
        `descriptor chrom '${object.chrom}' contradicts partition '${partition[1]}'`,
      );
    }
    relativePaths.push(relativePath);
  }

  const seen = new Set<string>();
  for (const object of objects) {
    if (seen.has(object.key)) {
      fail('DUPLICATE_KEY', `object key '${object.key}' is declared more than once`);
    }
    seen.add(object.key);
  }

  const canonical = objects
    .map((object, index) => ({ key: object.key, chrom: object.chrom, path: relativePaths[index]! }))
    .sort(
      (left, right) =>
        compareUtf8(left.chrom, right.chrom) || compareUtf8(left.path, right.path),
    )
    .map((entry) => entry.key);

  for (const [index, key] of canonical.entries()) {
    if (objects[index]!.key !== key) {
      fail(
        'NONCANONICAL_ORDER',
        `objects must be ordered by (chrom, relativePath); position ${index} should be '${key}'`,
      );
    }
  }

  const computed = computeDatasetChecksumSha256(attemptPrefix, objects);
  if (computed !== datasetChecksumSha256) {
    fail(
      'DATASET_CHECKSUM_MISMATCH',
      `declared '${datasetChecksumSha256}' but the descriptor list hashes to '${computed}'`,
    );
  }
}

/** Validates an activity result against the input that requested it. */
export function assertValidArtifactResult(
  input: BuildDatasetArtifactInput,
  result: BuildDatasetArtifactResult,
): void {
  if (result.referenceBuild !== input.reference.build) {
    fail(
      'REFERENCE_BUILD_MISMATCH',
      `result declares '${result.referenceBuild}' but '${input.reference.build}' was requested`,
    );
  }

  assertCanonicalArtifactInventory({
    allowedPrefix: input.target.allowedPrefix,
    attemptPrefix: result.attemptPrefix,
    datasetChecksumSha256: result.datasetChecksumSha256,
    objects: result.parquetObjects,
    expectedBucket: input.target.bucket,
  });
}

/**
 * Validates a published manifest in isolation. The allowed prefix is derived from the
 * manifest's own `datasetId`/`artifactVersion`, so a manifest cannot claim objects that
 * belong to another dataset or artifact version.
 */
export function assertValidDatasetManifest(
  manifest: DatasetManifest,
  options: { readonly expectedBucket?: string } = {},
): void {
  if (manifest.schemaFingerprint !== PARQUET_SCHEMA_FINGERPRINT) {
    fail(
      'SCHEMA_FINGERPRINT_MISMATCH',
      `manifest declares '${manifest.schemaFingerprint}', expected '${PARQUET_SCHEMA_FINGERPRINT}'`,
    );
  }

  assertCanonicalArtifactInventory({
    allowedPrefix: `datasets/${manifest.datasetId}/versions/${manifest.artifactVersion}/`,
    attemptPrefix: manifest.attemptPrefix,
    datasetChecksumSha256: manifest.datasetChecksumSha256,
    objects: manifest.parquetObjects,
    ...(options.expectedBucket === undefined ? {} : { expectedBucket: options.expectedBucket }),
  });
}
