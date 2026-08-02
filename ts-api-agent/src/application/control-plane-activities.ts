/**
 * Control-plane Temporal Activities, run by the TypeScript Worker on `genomic-control-plane`.
 *
 * `inspectDatasetSource` resolves a seeded catalog key to an immutable S3 source identity.
 * `publishDataset` verifies the inventory the Rust Activity uploaded and then writes
 * `datasets/{datasetId}/manifest.json` **last**.
 *
 * Publication deliberately performs no copy. The Rust Activity already uploaded every
 * Parquet object to its final, attempt-unique, immutable prefix, so publication is a
 * verification pass followed by a single conditional manifest write. The manifest is the only
 * readiness signal: a prefix without one is not queryable, and a failure anywhere below
 * leaves the dataset unqueryable rather than half published.
 *
 * These are Activities, not Workflow code, so Node built-ins and `./dataset-checksum.ts` are
 * fair game here.
 */
import { isDeepStrictEqual } from 'node:util';

import { datasetCatalog } from './dataset-catalog.ts';
import { assertValidArtifactResult, assertValidDatasetManifest } from './dataset-checksum.ts';
import {
  ARTIFACT_FORMAT,
  type BuildDatasetArtifactInput,
  BuildDatasetArtifactInputSchema,
  type BuildDatasetArtifactResult,
  BuildDatasetArtifactResultSchema,
  CONTRACT_VERSION,
  ContractValidationError,
  type DatasetManifest,
  DatasetManifestSchema,
  LAYOUT_VERSION,
  PARQUET_SCHEMA_FINGERPRINT,
  PARTITION_SPEC,
  type ParquetObject,
  SCHEMA_VERSION,
  SORT_ORDER,
  allowedPrefixFor,
} from './ingestion-contracts.ts';
import {
  DEFAULT_HEAD_CONCURRENCY,
  type ObjectHead,
  type ObjectLocation,
  type ObjectStore,
} from '../infrastructure/object-store/object-store.ts';

/**
 * Artifact version used when the Worker does not pin one. A dataset gets a fresh `datasetId`
 * per ingestion request, so the version segment only has to track the physical layout.
 */
export const DEFAULT_ARTIFACT_VERSION = `v${LAYOUT_VERSION}`;

/** The manifest is the readiness signal, and it lives at a derived key. */
export function manifestKeyFor(datasetId: string): string {
  return `datasets/${datasetId}/manifest.json`;
}

/** Raised when a seeded source object cannot be pinned to an immutable identity. */
export class DatasetSourceUnavailableError extends Error {
  readonly bucket: string;
  readonly key: string;

  constructor(location: ObjectLocation, reason: string) {
    super(`source object '${location.bucket}/${location.key}' ${reason}`);
    this.name = 'DatasetSourceUnavailable';
    this.bucket = location.bucket;
    this.key = location.key;
  }
}

export type ObjectVerificationCode =
  | 'OBJECT_MISSING'
  | 'ETAG_MISSING'
  | 'ETAG_MISMATCH'
  | 'VERSION_ID_MISMATCH'
  | 'SIZE_MISMATCH'
  | 'CHECKSUM_METADATA_MISSING'
  | 'CHECKSUM_METADATA_MISMATCH';

/** Raised when a published object does not match the identity the inventory declares. */
export class DatasetObjectVerificationError extends Error {
  readonly code: ObjectVerificationCode;
  readonly key: string;

  constructor(code: ObjectVerificationCode, key: string, detail: string) {
    super(`${code}: object '${key}' ${detail}`);
    this.name = 'DatasetObjectVerificationFailed';
    this.code = code;
    this.key = key;
  }
}

/** Raised when a different manifest is already published for the same dataset. */
export class DatasetPublicationConflict extends Error {
  readonly key: string;

  constructor(key: string, detail: string) {
    super(`a conflicting manifest is already published at '${key}': ${detail}`);
    this.name = 'DatasetPublicationConflict';
    this.key = key;
  }
}

export interface ControlPlaneActivitiesConfig {
  readonly objectStore: ObjectStore;
  /** Bucket every published artifact is written to. Server configuration, never wire input. */
  readonly artifactBucket: string;
  readonly artifactVersion?: string;
  readonly headConcurrency?: number;
}

export interface ControlPlaneActivities {
  inspectDatasetSource(datasetId: string, datasetKey: string): Promise<BuildDatasetArtifactInput>;
  publishDataset(
    input: BuildDatasetArtifactInput,
    result: BuildDatasetArtifactResult,
  ): Promise<DatasetManifest>;
}

/**
 * Checks one published object against the identity its descriptor declares. Every field the
 * later query path trusts — ETag, version, size and content checksum — is compared, so a
 * silently replaced or truncated object cannot reach a manifest.
 */
function verifyPublishedObject(object: ParquetObject, head: ObjectHead | null): void {
  if (head === null) {
    throw new DatasetObjectVerificationError(
      'OBJECT_MISSING',
      object.key,
      'is declared by the inventory but does not exist',
    );
  }
  if (head.etag === null) {
    throw new DatasetObjectVerificationError('ETAG_MISSING', object.key, 'was stored without an ETag');
  }
  if (head.etag !== object.etag) {
    throw new DatasetObjectVerificationError(
      'ETAG_MISMATCH',
      object.key,
      `has ETag '${head.etag}', the inventory declares '${object.etag}'`,
    );
  }
  if (head.versionId !== object.versionId) {
    throw new DatasetObjectVerificationError(
      'VERSION_ID_MISMATCH',
      object.key,
      `has version '${head.versionId}', the inventory declares '${object.versionId}'`,
    );
  }
  if (head.contentLength !== object.byteSize) {
    throw new DatasetObjectVerificationError(
      'SIZE_MISMATCH',
      object.key,
      `is ${head.contentLength} bytes, the inventory declares ${object.byteSize}`,
    );
  }
  if (head.checksumSha256 === null) {
    throw new DatasetObjectVerificationError(
      'CHECKSUM_METADATA_MISSING',
      object.key,
      'carries no SHA-256 content metadata',
    );
  }
  if (head.checksumSha256 !== object.checksumSha256) {
    throw new DatasetObjectVerificationError(
      'CHECKSUM_METADATA_MISMATCH',
      object.key,
      `has content checksum '${head.checksumSha256}', the inventory declares '${object.checksumSha256}'`,
    );
  }
}

/** Assembles the manifest from the validated input/result pair. Nothing is taken on trust. */
function buildManifest(
  input: BuildDatasetArtifactInput,
  result: BuildDatasetArtifactResult,
): DatasetManifest {
  return DatasetManifestSchema.parse({
    datasetId: input.datasetId,
    artifactFormat: ARTIFACT_FORMAT,
    layoutVersion: LAYOUT_VERSION,
    schemaVersion: SCHEMA_VERSION,
    schemaFingerprint: PARQUET_SCHEMA_FINGERPRINT,
    artifactVersion: input.target.artifactVersion,
    referenceVersion: input.reference.version,
    partitionSpec: [...PARTITION_SPEC],
    sortOrder: [...SORT_ORDER],
    attemptPrefix: result.attemptPrefix,
    datasetChecksumSha256: result.datasetChecksumSha256,
    variantCount: result.variantCount,
    rejectedRecordCount: result.rejectedRecordCount,
    referenceBuild: result.referenceBuild,
    processorVersion: result.processorVersion,
    parquetObjects: result.parquetObjects,
  });
}

export function createControlPlaneActivities(
  config: ControlPlaneActivitiesConfig,
): ControlPlaneActivities {
  const { objectStore, artifactBucket } = config;
  const artifactVersion = config.artifactVersion ?? DEFAULT_ARTIFACT_VERSION;
  const headConcurrency = config.headConcurrency ?? DEFAULT_HEAD_CONCURRENCY;

  return {
    /**
     * Resolves a caller-supplied catalog key to the frozen activity input.
     *
     * The caller sends a key and nothing else. Bucket and object key come from the seeded
     * allowlist, and the writable artifact prefix is derived from `datasetId` and the pinned
     * artifact version, so no URL, path or alternate bucket can be injected.
     */
    async inspectDatasetSource(
      datasetId: string,
      datasetKey: string,
    ): Promise<BuildDatasetArtifactInput> {
      const entry = datasetCatalog.get(datasetKey);
      const location: ObjectLocation = { bucket: entry.source.bucket, key: entry.source.key };

      const head = await objectStore.head(location);
      if (head === null) {
        throw new DatasetSourceUnavailableError(location, 'does not exist');
      }
      if (head.etag === null) {
        throw new DatasetSourceUnavailableError(location, 'has no ETag to pin the attempt to');
      }
      if (head.contentLength === null) {
        throw new DatasetSourceUnavailableError(location, 'reports no content length');
      }

      return BuildDatasetArtifactInputSchema.parse({
        contractVersion: CONTRACT_VERSION,
        datasetId,
        datasetKey: entry.key,
        source: {
          bucket: location.bucket,
          key: location.key,
          etag: head.etag,
          versionId: head.versionId,
          contentLength: head.contentLength,
        },
        reference: { build: entry.expectedReferenceBuild, version: entry.referenceVersion },
        target: {
          bucket: artifactBucket,
          artifactVersion,
          allowedPrefix: allowedPrefixFor(datasetId, artifactVersion),
        },
      });
    },

    /**
     * Verifies the complete uploaded inventory and publishes the manifest last.
     *
     * Order is load bearing: parse and validate the inventory, HEAD every declared object in
     * canonical order with bounded concurrency, verify each identity, and only then write the
     * manifest with a conditional put. The Parquet payload is never copied — it is already at
     * its final immutable key.
     */
    async publishDataset(
      input: BuildDatasetArtifactInput,
      result: BuildDatasetArtifactResult,
    ): Promise<DatasetManifest> {
      const parsedInput = BuildDatasetArtifactInputSchema.parse(input);
      const parsedResult = BuildDatasetArtifactResultSchema.parse(result);

      // The artifact bucket is Worker configuration. Even a well-formed input may not
      // redirect publication somewhere this Worker was not configured to write.
      if (parsedInput.target.bucket !== artifactBucket) {
        throw new ContractValidationError(
          'BUCKET_MISMATCH',
          `input targets bucket '${parsedInput.target.bucket}', this worker publishes to '${artifactBucket}'`,
        );
      }

      assertValidArtifactResult(parsedInput, parsedResult);

      const manifest = buildManifest(parsedInput, parsedResult);
      assertValidDatasetManifest(manifest, { expectedBucket: parsedInput.target.bucket });

      const heads = await objectStore.headMany(
        manifest.parquetObjects.map((object) => ({ bucket: object.bucket, key: object.key })),
        { concurrency: headConcurrency },
      );
      for (const [index, object] of manifest.parquetObjects.entries()) {
        verifyPublishedObject(object, heads[index] ?? null);
      }

      const manifestLocation: ObjectLocation = {
        bucket: parsedInput.target.bucket,
        key: manifestKeyFor(manifest.datasetId),
      };
      const written = await objectStore.putJsonConditional(manifestLocation, manifest);
      if (written.outcome === 'created') {
        return manifest;
      }

      // Someone published first — a retry of this very attempt, or a genuine conflict. The
      // existing manifest is authoritative and is never overwritten.
      const published = DatasetManifestSchema.safeParse(await objectStore.getJson(manifestLocation));
      if (!published.success) {
        throw new DatasetPublicationConflict(
          manifestLocation.key,
          'the existing object is not a valid dataset manifest',
        );
      }
      if (!isDeepStrictEqual(published.data, manifest)) {
        throw new DatasetPublicationConflict(
          manifestLocation.key,
          `it describes dataset content '${published.data.datasetChecksumSha256}' from attempt prefix '${published.data.attemptPrefix}'`,
        );
      }
      return published.data;
    },
  };
}
