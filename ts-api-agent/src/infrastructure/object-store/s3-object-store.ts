/**
 * AWS SDK adapter for the `ObjectStore` port, configured for MinIO or S3.
 *
 * Endpoint, region, credentials and path-style mode come from explicit environment
 * variables. The adapter never parses an `s3://` URI and never builds a public HTTP URL from
 * one: it is handed a `{ bucket, key }` pair and lets the SDK construct the request. There is
 * no SQL and no shell here, so there is nothing to escape — bucket and key reach the SDK as
 * structured parameters.
 */
import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';

import {
  CHECKSUM_METADATA_KEY,
  ConditionalWriteIndeterminateError,
  type ConditionalPutOutcome,
  DEFAULT_HEAD_CONCURRENCY,
  type HeadManyOptions,
  type ObjectHead,
  type ObjectLocation,
  type ObjectStore,
  headManyBounded,
} from './object-store.ts';

/** A published manifest is small; refuse to buffer anything that claims otherwise. */
export const MAX_JSON_BYTES = 1_048_576;

/**
 * Defaults for the request budget.
 *
 * An unbounded S3 client is how a single hung socket becomes a hung `/ask`: the SDK's own
 * default is no request timeout at all, so a half-open connection to a wedged endpoint keeps the
 * caller waiting until something upstream gives up. Every value here is a bound rather than a
 * target, and each is overridable per deployment.
 */
export const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
export const DEFAULT_CONNECT_TIMEOUT_MS = 3_000;
export const DEFAULT_MAX_ATTEMPTS = 3;

export interface S3ObjectStoreConfig {
  readonly endpoint: string;
  readonly region: string;
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
  readonly forcePathStyle: boolean;
  /** Per-request wall-clock bound, including the response body. */
  readonly requestTimeoutMs?: number;
  /** Bound on establishing the TCP/TLS connection. */
  readonly connectTimeoutMs?: number;
  /** Total attempts per operation, including the first. */
  readonly maxAttempts?: number;
}

function requireEnv(env: NodeJS.ProcessEnv, names: readonly string[]): Record<string, string> {
  const missing = names.filter((name) => (env[name] ?? '').length === 0);
  if (missing.length > 0) {
    throw new Error(
      `object store configuration is incomplete; set ${missing.join(', ')} explicitly`,
    );
  }
  return Object.fromEntries(names.map((name) => [name, env[name]!]));
}

function parseBoolean(name: string, raw: string | undefined, fallback: boolean): boolean {
  if (raw === undefined || raw.length === 0) return fallback;
  if (raw === 'true') return true;
  if (raw === 'false') return false;
  throw new Error(`${name} must be 'true' or 'false', got '${raw}'`);
}

/**
 * Reads the adapter configuration from the environment.
 *
 * The endpoint must be an absolute `http`/`https` URL. An `s3://` value is rejected rather
 * than rewritten, so a bucket URI can never be turned into a public HTTP endpoint.
 */
export function s3ObjectStoreConfigFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): S3ObjectStoreConfig {
  const required = requireEnv(env, ['S3_ENDPOINT', 'S3_ACCESS_KEY', 'S3_SECRET_KEY']);

  const endpoint = required.S3_ENDPOINT!;
  let protocol: string;
  try {
    protocol = new URL(endpoint).protocol;
  } catch {
    throw new Error(`S3_ENDPOINT must be an absolute URL, got '${endpoint}'`);
  }
  if (protocol !== 'http:' && protocol !== 'https:') {
    throw new Error(`S3_ENDPOINT must be an http(s) URL, got '${endpoint}'`);
  }

  return {
    endpoint,
    region: env.S3_REGION ?? 'us-east-1',
    accessKeyId: required.S3_ACCESS_KEY!,
    secretAccessKey: required.S3_SECRET_KEY!,
    forcePathStyle: parseBoolean('S3_FORCE_PATH_STYLE', env.S3_FORCE_PATH_STYLE, true),
    requestTimeoutMs: positiveIntEnv(env, 'S3_REQUEST_TIMEOUT_MS', DEFAULT_REQUEST_TIMEOUT_MS),
    connectTimeoutMs: positiveIntEnv(env, 'S3_CONNECT_TIMEOUT_MS', DEFAULT_CONNECT_TIMEOUT_MS),
    maxAttempts: positiveIntEnv(env, 'S3_MAX_ATTEMPTS', DEFAULT_MAX_ATTEMPTS),
  };
}

/**
 * A positive integer from the environment, or the default.
 *
 * A malformed value is an error, not a silent fallback: "the timeout you configured is not the
 * one in force" is the kind of thing nobody notices until an outage.
 */
function positiveIntEnv(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name];
  if (raw === undefined || raw.length === 0) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got '${raw}'`);
  }
  return value;
}

/**
 * S3 returns ETags quoted; the canonical cross-language form drops the quotes and nothing
 * else, so a multipart ETag's `-N` suffix survives untouched. See
 * `contracts/ingestion-v1.md` ("S3 storage conventions") for the normative statement this
 * implements.
 */
export function canonicalEtag(raw: string | undefined): string | null {
  if (raw === undefined || raw.length === 0) return null;
  return raw.length >= 2 && raw.startsWith('"') && raw.endsWith('"') ? raw.slice(1, -1) : raw;
}

function statusOf(error: unknown): number | undefined {
  return (error as { $metadata?: { httpStatusCode?: number } } | null)?.$metadata?.httpStatusCode;
}

/**
 * Exported for unit testing (see `s3-object-store.test.ts`); not part of the adapter's public
 * surface otherwise.
 */
export function isNotFound(error: unknown): boolean {
  const name = (error as { name?: string } | null)?.name;
  return statusOf(error) === 404 || name === 'NotFound' || name === 'NoSuchKey';
}

/**
 * S3's ordinary lost `If-None-Match: *` race: the key is confirmed present. Distinct from
 * {@link isConditionalWriteIndeterminate} (409) — see that function's doc comment for why the
 * two must never be merged. Exported for unit testing.
 */
export function isPreconditionFailed(error: unknown): boolean {
  return (
    statusOf(error) === 412 || (error as { name?: string } | null)?.name === 'PreconditionFailed'
  );
}

/**
 * S3 returns HTTP 409 `ConditionalRequestConflict` when a *different* conditional write to the
 * same key is in flight at the same time. AWS documents this as "a conflicting conditional
 * operation is in progress against this resource" — it means the outcome is indeterminate and
 * the request should be retried, not that the object is now present. That is a different claim
 * from 412 `PreconditionFailed` (the ordinary lost race, where presence is confirmed), so this
 * predicate is intentionally disjoint from {@link isPreconditionFailed}: `putJsonConditional`
 * must not report `{ outcome: 'exists' }` for a 409, because a caller that immediately reads the
 * key back (as `publishDataset` does) can race the still-in-flight writer and observe a missing
 * or stale body, turning a retryable race into what looks like a permanent conflict.
 *
 * Exported for unit testing.
 */
export function isConditionalWriteIndeterminate(error: unknown): boolean {
  return (
    statusOf(error) === 409 ||
    (error as { name?: string } | null)?.name === 'ConditionalRequestConflict'
  );
}

export class S3ObjectStore implements ObjectStore {
  readonly #client: S3Client;

  constructor(config: S3ObjectStoreConfig) {
    this.#client = new S3Client({
      endpoint: config.endpoint,
      region: config.region,
      forcePathStyle: config.forcePathStyle,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
      maxAttempts: config.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
      // Bounded rather than left to the SDK's "wait forever" default; see the constants above.
      requestHandler: {
        requestTimeout: config.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
        connectionTimeout: config.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
      },
    });
  }

  static fromEnv(env: NodeJS.ProcessEnv = process.env): S3ObjectStore {
    return new S3ObjectStore(s3ObjectStoreConfigFromEnv(env));
  }

  destroy(): void {
    this.#client.destroy();
  }

  async head(location: ObjectLocation): Promise<ObjectHead | null> {
    let response;
    try {
      response = await this.#client.send(
        new HeadObjectCommand({ Bucket: location.bucket, Key: location.key }),
      );
    } catch (error) {
      if (isNotFound(error)) return null;
      throw error;
    }

    const checksum = response.Metadata?.[CHECKSUM_METADATA_KEY];
    return {
      bucket: location.bucket,
      key: location.key,
      etag: canonicalEtag(response.ETag),
      versionId: response.VersionId ?? null,
      contentLength: response.ContentLength ?? null,
      checksumSha256: checksum === undefined ? null : checksum.toLowerCase(),
    };
  }

  async headMany(
    locations: readonly ObjectLocation[],
    options: HeadManyOptions = {},
  ): Promise<readonly (ObjectHead | null)[]> {
    return headManyBounded(
      (location) => this.head(location),
      locations,
      options.concurrency ?? DEFAULT_HEAD_CONCURRENCY,
    );
  }

  async getJson(location: ObjectLocation): Promise<unknown> {
    let response;
    try {
      response = await this.#client.send(
        new GetObjectCommand({ Bucket: location.bucket, Key: location.key }),
      );
    } catch (error) {
      if (isNotFound(error)) return null;
      throw error;
    }

    // A response with no Content-Length must fail rather than silently skip the size guard:
    // `transformToString` below would otherwise buffer an unbounded body.
    if (response.ContentLength === undefined) {
      throw new Error(
        `'${location.bucket}/${location.key}' returned no Content-Length; refusing to buffer a body of unknown size`,
      );
    }
    if (response.ContentLength > MAX_JSON_BYTES) {
      throw new Error(
        `'${location.bucket}/${location.key}' is ${response.ContentLength} bytes, above the ${MAX_JSON_BYTES} byte JSON limit`,
      );
    }
    if (response.Body === undefined) {
      throw new Error(`'${location.bucket}/${location.key}' returned no body`);
    }
    return JSON.parse(await response.Body.transformToString('utf8'));
  }

  /**
   * `If-None-Match: *` makes the write succeed only when the key is absent, so concurrent or
   * retried publications cannot overwrite an existing manifest.
   *
   * A failed conditional put is mapped by HTTP status, and the two statuses S3 documents for it
   * are handled differently on purpose (see `isPreconditionFailed` /
   * `isConditionalWriteIndeterminate` above): 412 confirms the key is present and resolves to
   * `{ outcome: 'exists' }`; 409 means a concurrent write raced this one and the outcome is
   * unknown, so it is rejected with `ConditionalWriteIndeterminateError` instead of being
   * reported as `exists` — the caller (not this adapter) decides whether/how to retry.
   */
  async putJsonConditional(
    location: ObjectLocation,
    value: unknown,
  ): Promise<ConditionalPutOutcome> {
    try {
      const response = await this.#client.send(
        new PutObjectCommand({
          Bucket: location.bucket,
          Key: location.key,
          Body: JSON.stringify(value),
          ContentType: 'application/json',
          IfNoneMatch: '*',
        }),
      );
      return {
        outcome: 'created',
        etag: canonicalEtag(response.ETag),
        versionId: response.VersionId ?? null,
      };
    } catch (error) {
      if (isPreconditionFailed(error)) return { outcome: 'exists' };
      if (isConditionalWriteIndeterminate(error)) {
        throw new ConditionalWriteIndeterminateError(location);
      }
      throw error;
    }
  }
}
