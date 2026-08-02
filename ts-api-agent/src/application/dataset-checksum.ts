/**
 * Dataset content checksum and the inventory invariants that depend on it.
 *
 * Split out of `./ingestion-contracts.ts` on purpose: this module imports `node:crypto`,
 * which cannot be resolved inside the Temporal workflow sandbox. Workflow code imports
 * `./ingestion-contracts.ts` (constants, schemas, types) and must never import this file;
 * activity and API code imports both.
 */
import { createHash } from 'node:crypto';

import {
  type BuildDatasetArtifactInput,
  type BuildDatasetArtifactResult,
  type ContractValidationCode,
  ContractValidationError,
  type DatasetManifest,
  PARQUET_SCHEMA_FINGERPRINT,
  type ParquetObject,
  allowedPrefixFor,
  variantsPrefixFor,
} from './ingestion-contracts.ts';

export function sha256Hex(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function fail(code: ContractValidationCode, message: string): never {
  throw new ContractValidationError(code, message);
}

/** Byte-wise ascending comparison, matching Rust's `Ord for str`. */
function compareUtf8(left: string, right: string): number {
  return Buffer.compare(Buffer.from(left, 'utf8'), Buffer.from(right, 'utf8'));
}

/**
 * The checksum unit: an object key with `{attemptPrefix}variants/` removed, leaving
 * `chrom=<value>/part-NNN.parquet`. A key that does not sit under exactly that prefix — in
 * particular one missing the `variants/` segment — is rejected rather than reinterpreted.
 */
function relativePathOf(attemptPrefix: string, key: string): string {
  const variantsPrefix = variantsPrefixFor(attemptPrefix);
  if (!key.startsWith(variantsPrefix)) {
    fail('KEY_OUTSIDE_ALLOWED_PREFIX', `object key '${key}' is not below '${variantsPrefix}'`);
  }
  return key.slice(variantsPrefix.length);
}

/**
 * Canonical descriptor list: relative path, content checksum and statistics, sorted
 * byte-wise by `(chrom, relativePath)`. Deliberately excludes bucket, key prefix, ETag and
 * version ID so the same dataset content yields the same checksum on every attempt.
 *
 * Integers are rendered as unpadded base-10, with no sign and no digit separators — the
 * default `Number.prototype.toString()` and Rust `Display` rendering for the non-negative
 * integers these fields hold.
 */
export function canonicalDescriptorBlock(
  attemptPrefix: string,
  objects: readonly ParquetObject[],
): string {
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
 * queried: single bucket, every key below `{attemptPrefix}variants/` below the allowed
 * immutable version prefix, `chrom=<value>` partition agreement, no duplicates, canonical
 * ordering and a reproducible dataset checksum.
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

  // The input's own `allowedPrefix` is never trusted verbatim: a widened value such as
  // `datasets/` would satisfy every containment check below.
  const allowedPrefix = allowedPrefixFor(input.datasetId, input.target.artifactVersion);
  if (input.target.allowedPrefix !== allowedPrefix) {
    fail(
      'ALLOWED_PREFIX_MISMATCH',
      `input declares '${input.target.allowedPrefix}' but '${input.datasetId}'/'${input.target.artifactVersion}' derives '${allowedPrefix}'`,
    );
  }

  assertCanonicalArtifactInventory({
    allowedPrefix,
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
    allowedPrefix: allowedPrefixFor(manifest.datasetId, manifest.artifactVersion),
    attemptPrefix: manifest.attemptPrefix,
    datasetChecksumSha256: manifest.datasetChecksumSha256,
    objects: manifest.parquetObjects,
    ...(options.expectedBucket === undefined ? {} : { expectedBucket: options.expectedBucket }),
  });
}
