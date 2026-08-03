/**
 * Domain vocabulary for the genomic ingestion pipeline.
 *
 * These names cross the TypeScript/Rust boundary, so they are frozen here and mirrored in
 * `rust-ingestion-worker/src/contracts.rs`. See `contracts/ingestion-v1.md`.
 */

/** The only datasets that may be ingested. Arbitrary uploads, URLs and paths are out of scope. */
export const DATASET_KEYS = ['demo-small', 'na12878-full'] as const;
export type DatasetKey = (typeof DATASET_KEYS)[number];

export function isDatasetKey(value: unknown): value is DatasetKey {
  return typeof value === 'string' && (DATASET_KEYS as readonly string[]).includes(value);
}

/** Reference genome build every seeded dataset is pinned to. */
export const REFERENCE_BUILD = 'GRCh38';

/**
 * Versioned ClinVar snapshot every seeded dataset resolves coordinates against.
 *
 * `v3` is the first machine-selected table: ~14,000 coordinates chosen from ClinVar by
 * classification and review status (`clinvar-source-records.ts`), rather than the 14 rows the
 * featured target list produces. `v2` was that 14-row table, and `v1` a hand-typed one whose
 * alleles were inverted for several variants and whose VKORC1 and TP53 rows carried GRCh37
 * positions.
 *
 * All three are genuinely different datasets and must not share a version string: a dataset
 * ingested against an older one is rejected by `ReferenceSnapshotMismatch` at `/ask` until it is
 * re-ingested, which is the check doing its job. Re-ingestion is the agreed cost of this bump —
 * there is deliberately no compatibility shim, because a shim would mean one version string
 * naming two tables and that is the failure the check exists to catch.
 */
export const REFERENCE_VERSION = 'demo-clinvar-grch38-v3';

/** Immutable S3 identity of a seeded source object. Never derived from API input. */
export interface DatasetSourceObject {
  readonly bucket: string;
  readonly key: string;
}

/** One allowlisted catalog entry. Display metadata is deliberately separate from S3 identity. */
export interface DatasetCatalogEntry {
  readonly key: DatasetKey;
  readonly displayName: string;
  readonly description: string;
  readonly source: DatasetSourceObject;
  readonly expectedReferenceBuild: string;
  readonly referenceVersion: string;
}

/**
 * Observable ingestion states. `RESOLVING -> BUILDING -> VERIFYING_OBJECTS ->
 * PUBLISHING_MANIFEST -> COMPLETED` is the only success path; a dataset is queryable only
 * in `COMPLETED`.
 */
export const INGESTION_STATES = [
  'RESOLVING',
  'BUILDING',
  'VERIFYING_OBJECTS',
  'PUBLISHING_MANIFEST',
  'COMPLETED',
  'FAILED',
] as const;
export type IngestionState = (typeof INGESTION_STATES)[number];

/**
 * Stable failure type names raised by the Rust activity and matched by name in the
 * TypeScript workflow's retry policy. Changing a spelling changes retry behaviour.
 */
export const INGESTION_FAILURE_TYPES = [
  'InvalidVcfFormat',
  'SourceObjectChanged',
  'ObjectStoreUnavailable',
  'ArtifactWriteFailed',
  'ArtifactValidationFailed',
] as const;
export type IngestionFailureType = (typeof INGESTION_FAILURE_TYPES)[number];

/** Failures that are deterministic: retrying the same input cannot succeed. */
export const NON_RETRYABLE_FAILURE_TYPES = [
  'InvalidVcfFormat',
  'SourceObjectChanged',
  'ArtifactValidationFailed',
] as const satisfies readonly IngestionFailureType[];
