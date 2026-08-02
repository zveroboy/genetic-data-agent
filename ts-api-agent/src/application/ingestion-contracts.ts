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
 * This module is deliberately free of Node built-ins at both import and evaluation time, so
 * Temporal workflow code may import its constants, schemas and types by value. Everything
 * that needs `node:crypto` lives in `./dataset-checksum.ts`, which workflow code must not
 * import. `ingestion-contracts.test.ts` enforces that boundary.
 */
import { z } from 'zod';

import { DATASET_KEYS } from '../domain/datasets.ts';

/** Wire contract version carried by every activity input. */
export const CONTRACT_VERSION = 1;

/** Physical artifact layout version (prefix shape, partition directories). */
export const LAYOUT_VERSION = 1;

/**
 * Key segment separating an attempt prefix from its partition directories:
 * `{attemptPrefix}variants/{relativePath}`.
 *
 * It is part of the S3 key only. `relativePath` — the unit the dataset checksum is computed
 * from — is `chrom=<value>/part-NNN.parquet` and never carries this segment, so the checksum
 * stays computable from the Rust processor's local Parquet descriptors, which have no S3
 * knowledge. The segment is added by the S3 mapping layer.
 */
export const VARIANTS_SEGMENT = 'variants/';

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

/**
 * SHA-256 of `PARQUET_SCHEMA_COLUMNS`; recorded in every published manifest.
 *
 * Pinned as a literal rather than derived, exactly as Rust pins it in
 * `contracts.rs::PARQUET_SCHEMA_FINGERPRINT`, so this module never needs `node:crypto`.
 * `ingestion-contracts.test.ts` asserts the literal against a freshly computed digest and
 * against the golden manifest fixture, so the three cannot drift apart.
 */
export const PARQUET_SCHEMA_FINGERPRINT =
  '89e4e0a61728e9776376f7550d09426acba14bd486c68a918e66fb11d437d7de';

/** Activity progress phases, in the order the Rust worker reports them. */
export const INGESTION_PHASES = [
  'DOWNLOADING_SOURCE',
  'PARSING',
  'WRITING_DUCKDB',
  'EXPORTING_PARQUET',
  'UPLOADING_PARTITION',
  'FINALIZING',
] as const;

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
  | 'ALLOWED_PREFIX_MISMATCH'
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

/**
 * The single immutable prefix a dataset's artifact version may write under. Derived, never
 * taken from the wire: a caller-supplied `allowedPrefix` such as `datasets/` would otherwise
 * satisfy every containment check.
 */
export function allowedPrefixFor(datasetId: string, artifactVersion: string): string {
  return `datasets/${datasetId}/versions/${artifactVersion}/`;
}

/** The prefix every Parquet object of one attempt sits under. */
export function variantsPrefixFor(attemptPrefix: string): string {
  return `${attemptPrefix}${VARIANTS_SEGMENT}`;
}
