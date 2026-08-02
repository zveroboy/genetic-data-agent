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

export interface S3ObjectStoreConfig {
  readonly endpoint: string;
  readonly region: string;
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
  readonly forcePathStyle: boolean;
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
  };
}

/** S3 returns ETags quoted; the canonical cross-language form drops the quotes. */
function canonicalEtag(raw: string | undefined): string | null {
  if (raw === undefined || raw.length === 0) return null;
  return raw.length >= 2 && raw.startsWith('"') && raw.endsWith('"') ? raw.slice(1, -1) : raw;
}

function statusOf(error: unknown): number | undefined {
  return (error as { $metadata?: { httpStatusCode?: number } } | null)?.$metadata?.httpStatusCode;
}

function isNotFound(error: unknown): boolean {
  const name = (error as { name?: string } | null)?.name;
  return statusOf(error) === 404 || name === 'NotFound' || name === 'NoSuchKey';
}

function isPreconditionFailed(error: unknown): boolean {
  return (
    statusOf(error) === 412 || (error as { name?: string } | null)?.name === 'PreconditionFailed'
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

    if ((response.ContentLength ?? 0) > MAX_JSON_BYTES) {
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
      throw error;
    }
  }
}
