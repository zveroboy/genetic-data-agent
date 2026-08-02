/**
 * Manifest resolution and candidate-file selection for the serving path.
 *
 * This module answers exactly one question: *given a dataset id, which immutable S3 objects
 * may a query touch?* Everything downstream — the DuckDB session, the SQL, the returned
 * genotypes — is built from its output, so it is the place where a user-supplied string stops
 * being able to influence what gets read.
 *
 * Three rules make that true:
 *
 * - **The manifest is the only readiness signal.** A prefix without
 *   `datasets/{datasetId}/manifest.json` is not queryable; there is no listing, no glob and no
 *   "try the prefix and see". The `ObjectStore` port deliberately has no `listPrefix`.
 * - **The manifest is validated before any SQL exists.** Layout/schema version, schema
 *   fingerprint, bucket, prefix containment, partition agreement, ordering, duplicates and the
 *   dataset content checksum are all checked (see `contracts/ingestion-v1.md`, "Canonical
 *   inventory invariants"), and the allowed prefix is derived from the manifest's own
 *   `datasetId`/`artifactVersion` rather than read off the wire.
 * - **Selection is explicit.** Candidate objects are chosen in application code from the
 *   validated inventory by partition value and declared `minPos`/`maxPos`, and turned into a
 *   literal URI list. An empty selection is `TargetNotPresent`, never a widened scan.
 */
import { isDeepStrictEqual } from 'node:util';

import { assertValidDatasetManifest } from '../../application/dataset-checksum.ts';
import {
  type DatasetManifest,
  DatasetManifestSchema,
  type ParquetObject,
} from '../../application/ingestion-contracts.ts';
import { manifestKeyFor } from '../../application/control-plane-activities.ts';
import {
  DEFAULT_HEAD_CONCURRENCY,
  type ObjectHead,
  type ObjectStore,
} from '../object-store/object-store.ts';

/**
 * Upper bound on the inventory a single query may consider. A human dataset partitioned by
 * chromosome has at most a couple of dozen objects; anything approaching this bound is a
 * malformed or hostile manifest, not a genome.
 */
export const MAX_DATASET_PARQUET_OBJECTS = 128;

/** A published manifest is a small JSON document; refuse to buffer anything larger. */
export const MAX_MANIFEST_BYTES = 1_048_576;

/**
 * The canonical partition-value domain, identical to the allowlist the Rust producer
 * normalises to. `chrom` is a `VARCHAR` on both sides of the language boundary — `X`, `Y` and
 * `MT` are not numbers — see `contracts/ingestion-v1.md`, "Reading the dataset".
 */
export const CANONICAL_CHROMOSOMES: readonly string[] = Object.freeze([
  ...Array.from({ length: 22 }, (_unused, index) => String(index + 1)),
  'X',
  'Y',
  'MT',
]);

/** A single safe path segment, matching the wire schema's `pathSegmentSchema`. */
const DATASET_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

export type DatasetResolutionCode =
  | 'DATASET_ID_UNSAFE'
  | 'MANIFEST_SIZE_UNKNOWN'
  | 'MANIFEST_TOO_LARGE'
  | 'MANIFEST_MALFORMED'
  | 'MANIFEST_DATASET_ID_MISMATCH'
  | 'TOO_MANY_PARQUET_OBJECTS'
  | 'UNSUPPORTED_PARTITION_VALUE'
  | 'CANDIDATE_NOT_IN_MANIFEST';

/** Raised when a dataset cannot be resolved to a trustworthy, queryable inventory. */
export class DatasetResolutionError extends Error {
  readonly code: DatasetResolutionCode;
  readonly datasetId: string;

  constructor(code: DatasetResolutionCode, datasetId: string, detail: string) {
    super(`${code}: dataset '${datasetId}' ${detail}`);
    this.name = 'DatasetResolutionFailed';
    this.code = code;
    this.datasetId = datasetId;
  }
}

/**
 * Raised when no manifest exists for the requested dataset. Distinct from a *malformed*
 * manifest: this is the ordinary "ingestion has not finished, or never ran" answer, and it is
 * reached after a single `HEAD` — no Parquet object is ever touched.
 */
export class DatasetNotPublishedError extends Error {
  readonly datasetId: string;

  constructor(datasetId: string) {
    super(
      `dataset '${datasetId}' has no published manifest; published Parquet objects become ` +
        'queryable only once a matching manifest exists',
    );
    this.name = 'DatasetNotPublished';
    this.datasetId = datasetId;
  }
}

/**
 * Raised when the resolved coordinates fall outside every declared partition and position
 * range. The dataset simply does not contain the target — widening the scan to "look harder"
 * would read the user's whole genome to prove a negative the manifest already proves.
 */
export class TargetNotPresentError extends Error {
  readonly datasetId: string;

  constructor(datasetId: string, detail: string) {
    super(`dataset '${datasetId}' declares no Parquet object that can contain ${detail}`);
    this.name = 'TargetNotPresent';
    this.datasetId = datasetId;
  }
}

/**
 * Codes mirrored from the control plane's publication-time verification
 * (`ObjectVerificationCode` in `application/control-plane-activities.ts`). Serving re-checks
 * the same identity because a manifest can outlive the objects it describes.
 */
export type ParquetObjectVerificationCode =
  | 'OBJECT_MISSING'
  | 'ETAG_MISSING'
  | 'ETAG_MISMATCH'
  | 'VERSION_ID_MISMATCH'
  | 'SIZE_MISMATCH'
  | 'CHECKSUM_METADATA_MISSING'
  | 'CHECKSUM_METADATA_MISMATCH';

/** Raised when a candidate object no longer matches the identity its manifest declares. */
export class ParquetObjectVerificationError extends Error {
  readonly code: ParquetObjectVerificationCode;
  readonly key: string;

  constructor(code: ParquetObjectVerificationCode, key: string, detail: string) {
    super(`${code}: object '${key}' ${detail}`);
    this.name = 'ParquetObjectVerificationFailed';
    this.code = code;
    this.key = key;
  }
}

/** A validated, queryable dataset. Nothing outside this shape may reach a SQL string. */
export interface ResolvedParquetDataset {
  readonly datasetId: string;
  readonly bucket: string;
  readonly datasetChecksumSha256: string;
  readonly referenceBuild: string;
  readonly referenceVersion: string;
  readonly manifest: DatasetManifest;
  readonly parquetObjects: readonly ParquetObject[];
}

/** The minimum a caller must know to prune: which partition, which position. */
export interface CoordinateSelector {
  readonly chrom: string;
  readonly pos: number;
}

export interface ParquetDatasetResolverConfig {
  readonly objectStore: ObjectStore;
  /** Bucket every published artifact lives in. Server configuration, never request input. */
  readonly artifactBucket: string;
  readonly maxParquetObjects?: number;
  readonly headConcurrency?: number;
}

export interface ParquetDatasetResolver {
  /** Resolves a dataset id to its validated inventory, or throws. */
  resolve(datasetId: string): Promise<ResolvedParquetDataset>;

  /**
   * Re-verifies the candidate objects against the manifest and returns their immutable
   * `s3://` URIs, in canonical inventory order.
   */
  verifyCandidates(
    dataset: Pick<ResolvedParquetDataset, 'datasetId' | 'parquetObjects'>,
    candidates: readonly ParquetObject[],
  ): Promise<readonly string[]>;
}

/**
 * The only place an `s3://` URI is constructed. Both components come from a descriptor that
 * has already passed `assertValidDatasetManifest`, so neither can carry a traversal segment,
 * a wildcard or a quote.
 */
export function parquetObjectUri(object: ParquetObject): string {
  return `s3://${object.bucket}/${object.key}`;
}

/**
 * Files that *can* contain at least one of the targets: the partition value must match
 * exactly and the position must fall inside the object's declared `[minPos, maxPos]`.
 *
 * Order and identity follow the manifest: the result is a subsequence of the canonical
 * inventory with no repeats, so two targets in the same file cost one file.
 */
export function selectCandidateObjects(
  dataset: Pick<ResolvedParquetDataset, 'parquetObjects'>,
  targets: readonly CoordinateSelector[],
): readonly ParquetObject[] {
  if (targets.length === 0) return [];
  return dataset.parquetObjects.filter((object) =>
    targets.some(
      (target) =>
        target.chrom === object.chrom &&
        target.pos >= object.minPos &&
        target.pos <= object.maxPos,
    ),
  );
}

/**
 * Checks one candidate against the identity its descriptor declares, immediately before the
 * scan. Everything the query trusts — ETag, version, size and content checksum — is compared,
 * so an object silently replaced or truncated after publication is refused rather than read.
 *
 * This deliberately repeats the control plane's publication-time check
 * (`verifyPublishedObject`): publication proves the objects were right *then*, and a manifest
 * is long lived.
 */
function verifyCandidateObject(object: ParquetObject, head: ObjectHead | null): void {
  if (head === null) {
    throw new ParquetObjectVerificationError(
      'OBJECT_MISSING',
      object.key,
      'is declared by the manifest but does not exist',
    );
  }
  if (head.etag === null) {
    throw new ParquetObjectVerificationError('ETAG_MISSING', object.key, 'is stored without an ETag');
  }
  if (head.etag !== object.etag) {
    throw new ParquetObjectVerificationError(
      'ETAG_MISMATCH',
      object.key,
      `has ETag '${head.etag}', the manifest declares '${object.etag}'`,
    );
  }
  if (head.versionId !== object.versionId) {
    throw new ParquetObjectVerificationError(
      'VERSION_ID_MISMATCH',
      object.key,
      `has version '${head.versionId}', the manifest declares '${object.versionId}'`,
    );
  }
  if (head.contentLength !== object.byteSize) {
    throw new ParquetObjectVerificationError(
      'SIZE_MISMATCH',
      object.key,
      `is ${head.contentLength} bytes, the manifest declares ${object.byteSize}`,
    );
  }
  if (head.checksumSha256 === null) {
    throw new ParquetObjectVerificationError(
      'CHECKSUM_METADATA_MISSING',
      object.key,
      'carries no SHA-256 content metadata',
    );
  }
  if (head.checksumSha256 !== object.checksumSha256) {
    throw new ParquetObjectVerificationError(
      'CHECKSUM_METADATA_MISMATCH',
      object.key,
      `has content checksum '${head.checksumSha256}', the manifest declares '${object.checksumSha256}'`,
    );
  }
}

export function createParquetDatasetResolver(
  config: ParquetDatasetResolverConfig,
): ParquetDatasetResolver {
  const { objectStore, artifactBucket } = config;
  const maxParquetObjects = config.maxParquetObjects ?? MAX_DATASET_PARQUET_OBJECTS;
  const headConcurrency = config.headConcurrency ?? DEFAULT_HEAD_CONCURRENCY;

  return {
    async resolve(datasetId: string): Promise<ResolvedParquetDataset> {
      // The dataset id becomes an object key, so it is checked against the same single-safe-
      // segment rule the wire schema enforces before it is ever concatenated into one.
      if (!DATASET_ID_PATTERN.test(datasetId)) {
        throw new DatasetResolutionError(
          'DATASET_ID_UNSAFE',
          datasetId,
          'is not a single safe path segment',
        );
      }

      const location = { bucket: artifactBucket, key: manifestKeyFor(datasetId) };

      // HEAD first: it distinguishes "not published" from "published but broken" at the cost of
      // one request, and it bounds the body before `getJson` buffers anything.
      const head = await objectStore.head(location);
      if (head === null) {
        throw new DatasetNotPublishedError(datasetId);
      }
      if (head.contentLength === null) {
        throw new DatasetResolutionError(
          'MANIFEST_SIZE_UNKNOWN',
          datasetId,
          'has a manifest of unknown size; refusing to buffer it',
        );
      }
      if (head.contentLength > MAX_MANIFEST_BYTES) {
        throw new DatasetResolutionError(
          'MANIFEST_TOO_LARGE',
          datasetId,
          `has a ${head.contentLength} byte manifest, above the ${MAX_MANIFEST_BYTES} byte limit`,
        );
      }

      const parsed = DatasetManifestSchema.safeParse(await objectStore.getJson(location));
      if (!parsed.success) {
        throw new DatasetResolutionError(
          'MANIFEST_MALFORMED',
          datasetId,
          `has no valid manifest: ${parsed.error.issues
            .map((issue) => `${issue.path.join('.')} ${issue.message}`)
            .join('; ')}`,
        );
      }
      const manifest = parsed.data;

      // Binds the document to the prefix it was found under. Without this, a manifest for one
      // dataset parked at another's key would validate against its own (self-consistent)
      // inventory and serve somebody else's genome.
      if (manifest.datasetId !== datasetId) {
        throw new DatasetResolutionError(
          'MANIFEST_DATASET_ID_MISMATCH',
          datasetId,
          `is described by a manifest claiming dataset '${manifest.datasetId}'`,
        );
      }

      if (manifest.parquetObjects.length > maxParquetObjects) {
        throw new DatasetResolutionError(
          'TOO_MANY_PARQUET_OBJECTS',
          datasetId,
          `declares ${manifest.parquetObjects.length} Parquet objects, above the ${maxParquetObjects} limit`,
        );
      }

      // Version, fingerprint, bucket, prefix containment, partition agreement, duplicates,
      // canonical ordering and the content checksum. The allowed prefix is derived from the
      // manifest's own identity, never taken from the wire.
      assertValidDatasetManifest(manifest, { expectedBucket: artifactBucket });

      for (const object of manifest.parquetObjects) {
        // The partition value reaches SQL as a literal, so it may only ever be one of the
        // frozen contig names. `assertValidDatasetManifest` proves it matches the directory;
        // this proves the directory itself is a chromosome.
        if (!CANONICAL_CHROMOSOMES.includes(object.chrom)) {
          throw new DatasetResolutionError(
            'UNSUPPORTED_PARTITION_VALUE',
            datasetId,
            `declares partition value '${object.chrom}', which is not a canonical chromosome`,
          );
        }
      }

      return {
        datasetId,
        bucket: artifactBucket,
        datasetChecksumSha256: manifest.datasetChecksumSha256,
        referenceBuild: manifest.referenceBuild,
        referenceVersion: manifest.referenceVersion,
        manifest,
        parquetObjects: manifest.parquetObjects,
      };
    },

    async verifyCandidates(dataset, candidates): Promise<readonly string[]> {
      if (candidates.length === 0) {
        throw new TargetNotPresentError(dataset.datasetId, 'the requested coordinates');
      }

      const byKey = new Map(dataset.parquetObjects.map((object) => [object.key, object]));
      for (const candidate of candidates) {
        // Deep equality, not identity: the descriptor a caller hands back must be *the same
        // declaration* the manifest made, field for field, not merely something with a
        // matching key.
        if (!isDeepStrictEqual(byKey.get(candidate.key), candidate)) {
          throw new DatasetResolutionError(
            'CANDIDATE_NOT_IN_MANIFEST',
            dataset.datasetId,
            `was asked to query '${candidate.key}', which is not one of its validated descriptors`,
          );
        }
      }

      const heads = await objectStore.headMany(
        candidates.map((object) => ({ bucket: object.bucket, key: object.key })),
        { concurrency: headConcurrency },
      );
      for (const [index, candidate] of candidates.entries()) {
        verifyCandidateObject(candidate, heads[index] ?? null);
      }

      return candidates.map(parquetObjectUri);
    },
  };
}
