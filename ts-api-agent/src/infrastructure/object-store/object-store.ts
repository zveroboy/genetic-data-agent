/**
 * The object-store port.
 *
 * Application code depends on this interface, never on an S3 SDK type, so activities can be
 * exercised against an in-memory fake and the adapter stays replaceable.
 *
 * Two conventions cross the TypeScript/Rust boundary and must be mirrored by
 * `rust-ingestion-worker/src/object_store.rs`. Both are frozen, normatively, in
 * `contracts/ingestion-v1.md` ("S3 storage conventions") — that document, not this comment, is
 * the source of truth an implementer on either side should read:
 *
 * - Content checksums travel as the S3 user metadata entry named `CHECKSUM_METADATA_KEY`
 *   (`x-amz-meta-sha256` on the wire), holding a lowercase hex SHA-256 of the object body.
 * - The canonical ETag form is the S3 header value with its surrounding double quotes
 *   removed, so an ETag recorded by an uploader equals the ETag a later `head` reads back.
 *   A multipart ETag's `-N` suffix is left untouched by canonicalization.
 *
 * A location is always a `{ bucket, key }` pair. `s3://` URIs and HTTP URLs are never
 * accepted, parsed or constructed here: an object's bucket comes from the seeded catalog or
 * from validated configuration, never from a caller-supplied string.
 *
 * The port carries only what the control plane actually performs. Bulk transfer stays on the
 * Rust side, which streams the source object and uploads Parquet with its own adapter, so
 * there is deliberately no `downloadToFile` here. Listing is a cleanup/audit concern and must
 * never drive query selection — the manifest inventory does — so there is no `listPrefix`
 * either. Both belong to whichever task first has a caller for them.
 */

/** S3 user metadata entry carrying the lowercase hex SHA-256 of an object's content. */
export const CHECKSUM_METADATA_KEY = 'sha256';

/** Default number of object heads verified at once. */
export const DEFAULT_HEAD_CONCURRENCY = 8;

export interface ObjectLocation {
  readonly bucket: string;
  readonly key: string;
}

/**
 * What an object head reveals. Every field S3 may omit is nullable, so a missing ETag,
 * content length or checksum is a value the caller must handle rather than a silent
 * `undefined`.
 */
export interface ObjectHead {
  readonly bucket: string;
  readonly key: string;
  readonly etag: string | null;
  readonly versionId: string | null;
  readonly contentLength: number | null;
  readonly checksumSha256: string | null;
}

export type ConditionalPutOutcome =
  | { readonly outcome: 'created'; readonly etag: string | null; readonly versionId: string | null }
  | { readonly outcome: 'exists' };

export interface HeadManyOptions {
  readonly concurrency?: number;
}

export interface ObjectStore {
  /** Object identity and metadata, or `null` when the object does not exist. */
  head(location: ObjectLocation): Promise<ObjectHead | null>;

  /**
   * Heads several objects with bounded concurrency, returning one result per input in input
   * order. Requests are issued in input order, so a caller that passes a canonically ordered
   * inventory gets canonically ordered verification.
   */
  headMany(
    locations: readonly ObjectLocation[],
    options?: HeadManyOptions,
  ): Promise<readonly (ObjectHead | null)[]>;

  /** Parsed JSON body, or `null` when the object does not exist. */
  getJson(location: ObjectLocation): Promise<unknown>;

  /**
   * Writes JSON only if the key does not already exist. An existing key is reported as
   * `{ outcome: 'exists' }` and is never overwritten, which is what makes manifest
   * publication safe to retry.
   */
  putJsonConditional(location: ObjectLocation, value: unknown): Promise<ConditionalPutOutcome>;
}

/**
 * Bounded-concurrency scheduler shared by every `ObjectStore` implementation.
 *
 * Work is claimed from a single monotonically advancing cursor, so requests always start in
 * input order and at most `concurrency` of them are ever in flight.
 */
export async function headManyBounded(
  head: (location: ObjectLocation) => Promise<ObjectHead | null>,
  locations: readonly ObjectLocation[],
  concurrency: number = DEFAULT_HEAD_CONCURRENCY,
): Promise<readonly (ObjectHead | null)[]> {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new RangeError(`head concurrency must be a positive integer, got ${concurrency}`);
  }

  const results: (ObjectHead | null)[] = new Array(locations.length).fill(null);
  let cursor = 0;

  async function worker(): Promise<void> {
    while (cursor < locations.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await head(locations[index]!);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, locations.length) }, () => worker()),
  );

  return results;
}
