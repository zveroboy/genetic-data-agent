/**
 * The single implementation of published-object identity verification.
 *
 * Two paths ask the same question of the same objects, at different times:
 *
 * - **Publication** checks the inventory the Rust Activity uploaded before it writes a
 *   manifest, so a silently replaced or truncated object cannot become queryable.
 * - **Serving** re-checks the candidate objects immediately before a scan, because
 *   publication only proves the objects were right *then* and a manifest is long lived.
 *
 * Both compare exactly the fields the query path later trusts — existence, ETag, version,
 * size and content checksum — against the frozen storage conventions in
 * `contracts/ingestion-v1.md` ("S3 storage conventions"). Those conventions are cross-language
 * and cannot drift per call site, so the checks live here once rather than in each caller: two
 * copies would be two things to keep in step with the contract, and the copy that fell behind
 * would fail open on whichever field it stopped comparing.
 *
 * What legitimately differs between the callers is only which document made the claim, and
 * that shows up in the message text alone (`declaredBy`), never in the comparisons.
 */
import type { ParquetObject } from './ingestion-contracts.ts';
import {
  DEFAULT_HEAD_CONCURRENCY,
  type ObjectHead,
  type ObjectStore,
} from '../infrastructure/object-store/object-store.ts';

/**
 * The code is the contract-visible part of a verification failure — `contracts/ingestion-v1.md`
 * names `ETAG_MISMATCH`, `CHECKSUM_METADATA_MISSING` and `CHECKSUM_METADATA_MISMATCH`
 * normatively as the failures a producer/verifier disagreement produces. The wording around a
 * code is not part of the contract.
 */
export type ObjectVerificationCode =
  | 'OBJECT_MISSING'
  | 'ETAG_MISSING'
  | 'ETAG_MISMATCH'
  | 'VERSION_ID_MISMATCH'
  | 'SIZE_MISMATCH'
  | 'CHECKSUM_METADATA_MISSING'
  | 'CHECKSUM_METADATA_MISMATCH';

/**
 * Which document declared the identity that was contradicted. Publication verifies against the
 * uploaded *inventory*, serving against the published *manifest*, and the message says which so
 * an operator reading a failure knows whether ingestion or the bucket drifted.
 */
export type IdentityDeclaredBy = 'inventory' | 'manifest';

/** Raised when a stored object does not match the identity its descriptor declares. */
export class ObjectVerificationError extends Error {
  readonly code: ObjectVerificationCode;
  readonly key: string;

  constructor(code: ObjectVerificationCode, key: string, detail: string) {
    super(`${code}: object '${key}' ${detail}`);
    this.name = 'ObjectVerificationFailed';
    this.code = code;
    this.key = key;
  }
}

/**
 * Checks one object against the identity its descriptor declares.
 *
 * Order matters for the diagnosis, not for the verdict: a missing ETag is reported as
 * `ETAG_MISSING` rather than as a mismatch against `null`, and the same for checksum metadata.
 */
export function verifyObjectIdentity(
  object: ParquetObject,
  head: ObjectHead | null,
  declaredBy: IdentityDeclaredBy,
): void {
  if (head === null) {
    throw new ObjectVerificationError(
      'OBJECT_MISSING',
      object.key,
      `is declared by the ${declaredBy} but does not exist`,
    );
  }
  if (head.etag === null) {
    throw new ObjectVerificationError('ETAG_MISSING', object.key, 'is stored without an ETag');
  }
  if (head.etag !== object.etag) {
    throw new ObjectVerificationError(
      'ETAG_MISMATCH',
      object.key,
      `has ETag '${head.etag}', the ${declaredBy} declares '${object.etag}'`,
    );
  }
  if (head.versionId !== object.versionId) {
    throw new ObjectVerificationError(
      'VERSION_ID_MISMATCH',
      object.key,
      `has version '${head.versionId}', the ${declaredBy} declares '${object.versionId}'`,
    );
  }
  if (head.contentLength !== object.byteSize) {
    throw new ObjectVerificationError(
      'SIZE_MISMATCH',
      object.key,
      `is ${head.contentLength} bytes, the ${declaredBy} declares ${object.byteSize}`,
    );
  }
  if (head.checksumSha256 === null) {
    throw new ObjectVerificationError(
      'CHECKSUM_METADATA_MISSING',
      object.key,
      'carries no SHA-256 content metadata',
    );
  }
  if (head.checksumSha256 !== object.checksumSha256) {
    throw new ObjectVerificationError(
      'CHECKSUM_METADATA_MISMATCH',
      object.key,
      `has content checksum '${head.checksumSha256}', the ${declaredBy} declares '${object.checksumSha256}'`,
    );
  }
}

export interface VerifyObjectIdentitiesOptions {
  readonly declaredBy: IdentityDeclaredBy;
  readonly concurrency?: number;
}

/**
 * Heads every declared object with bounded concurrency and verifies each identity.
 *
 * `headMany` issues requests in input order and returns one result per input, so a caller that
 * passes a canonically ordered list gets the first failure in canonical order — the same object
 * is blamed on every run, whichever request happened to finish first.
 */
export async function verifyObjectIdentities(
  objectStore: ObjectStore,
  objects: readonly ParquetObject[],
  options: VerifyObjectIdentitiesOptions,
): Promise<void> {
  const heads = await objectStore.headMany(
    objects.map((object) => ({ bucket: object.bucket, key: object.key })),
    { concurrency: options.concurrency ?? DEFAULT_HEAD_CONCURRENCY },
  );
  for (const [index, object] of objects.entries()) {
    verifyObjectIdentity(object, heads[index] ?? null, options.declaredBy);
  }
}
