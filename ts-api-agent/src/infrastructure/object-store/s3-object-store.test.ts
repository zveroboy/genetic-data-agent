/**
 * Unit tests for the pure, synchronous parts of the S3 adapter: environment configuration and
 * ETag canonicalization. Neither needs MinIO or the AWS SDK — `s3ObjectStoreConfigFromEnv`
 * already takes the env object as a parameter, and `canonicalEtag` is a pure string function.
 *
 * These regression-protect the two things `s3-object-store.ts` uniquely owns:
 *
 * - the global constraint "never form public HTTP URLs from `s3://` strings" (an `s3://`
 *   endpoint must be rejected, never rewritten to `http(s)://`);
 * - the canonical (unquoted) ETag form, one of the two cross-language conventions frozen in
 *   `contracts/ingestion-v1.md` ("S3 storage conventions").
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { canonicalEtag, s3ObjectStoreConfigFromEnv } from './s3-object-store.ts';

/** Every variable `s3ObjectStoreConfigFromEnv` requires, as a valid baseline. */
const BASE_ENV: NodeJS.ProcessEnv = {
  S3_ENDPOINT: 'http://localhost:9000',
  S3_ACCESS_KEY: 'minioadmin',
  S3_SECRET_KEY: 'minioadmin-secret',
};

describe('s3ObjectStoreConfigFromEnv', () => {
  it('accepts a valid http endpoint and fills in the documented defaults', () => {
    const config = s3ObjectStoreConfigFromEnv(BASE_ENV);

    assert.equal(config.endpoint, 'http://localhost:9000');
    assert.equal(config.region, 'us-east-1');
    assert.equal(config.accessKeyId, 'minioadmin');
    assert.equal(config.secretAccessKey, 'minioadmin-secret');
    assert.equal(config.forcePathStyle, true);
  });

  it('accepts a valid https endpoint', () => {
    const config = s3ObjectStoreConfigFromEnv({ ...BASE_ENV, S3_ENDPOINT: 'https://s3.example.com' });
    assert.equal(config.endpoint, 'https://s3.example.com');
  });

  it('rejects an s3:// endpoint rather than rewriting it to http(s)', () => {
    assert.throws(
      () => s3ObjectStoreConfigFromEnv({ ...BASE_ENV, S3_ENDPOINT: 's3://genomic-artifacts' }),
      /S3_ENDPOINT must be an http\(s\) URL/,
    );
  });

  it('rejects a non-URL endpoint', () => {
    assert.throws(
      () => s3ObjectStoreConfigFromEnv({ ...BASE_ENV, S3_ENDPOINT: 'not-a-url' }),
      /S3_ENDPOINT must be an absolute URL/,
    );
  });

  it('names the missing required variable in the error', () => {
    const { S3_ACCESS_KEY: _omit, ...rest } = BASE_ENV;
    assert.throws(() => s3ObjectStoreConfigFromEnv(rest), /S3_ACCESS_KEY/);
  });

  it('names every missing required variable when more than one is absent', () => {
    assert.throws(
      () => s3ObjectStoreConfigFromEnv({ S3_ENDPOINT: 'http://localhost:9000' }),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /S3_ACCESS_KEY/);
        assert.match(error.message, /S3_SECRET_KEY/);
        return true;
      },
    );
  });

  it('parses S3_FORCE_PATH_STYLE=true strictly', () => {
    assert.equal(
      s3ObjectStoreConfigFromEnv({ ...BASE_ENV, S3_FORCE_PATH_STYLE: 'true' }).forcePathStyle,
      true,
    );
  });

  it('parses S3_FORCE_PATH_STYLE=false strictly', () => {
    assert.equal(
      s3ObjectStoreConfigFromEnv({ ...BASE_ENV, S3_FORCE_PATH_STYLE: 'false' }).forcePathStyle,
      false,
    );
  });

  it('defaults S3_FORCE_PATH_STYLE to true when unset', () => {
    assert.equal(s3ObjectStoreConfigFromEnv(BASE_ENV).forcePathStyle, true);
  });

  it('rejects an S3_FORCE_PATH_STYLE value that is not exactly "true" or "false"', () => {
    assert.throws(
      () => s3ObjectStoreConfigFromEnv({ ...BASE_ENV, S3_FORCE_PATH_STYLE: 'yes' }),
      /S3_FORCE_PATH_STYLE must be 'true' or 'false', got 'yes'/,
    );
  });

  it('rejects case variants of the boolean rather than coercing them', () => {
    assert.throws(
      () => s3ObjectStoreConfigFromEnv({ ...BASE_ENV, S3_FORCE_PATH_STYLE: 'True' }),
      /S3_FORCE_PATH_STYLE must be 'true' or 'false'/,
    );
  });

  it('defaults S3_REGION to us-east-1 and honours an explicit override', () => {
    assert.equal(s3ObjectStoreConfigFromEnv(BASE_ENV).region, 'us-east-1');
    assert.equal(
      s3ObjectStoreConfigFromEnv({ ...BASE_ENV, S3_REGION: 'eu-central-1' }).region,
      'eu-central-1',
    );
  });
});

describe('canonicalEtag', () => {
  it('strips surrounding double quotes from a quoted ETag', () => {
    assert.equal(canonicalEtag('"d41d8cd98f00b204e9800998ecf8427e"'), 'd41d8cd98f00b204e9800998ecf8427e');
  });

  it('leaves an already-unquoted ETag untouched', () => {
    assert.equal(canonicalEtag('d41d8cd98f00b204e9800998ecf8427e'), 'd41d8cd98f00b204e9800998ecf8427e');
  });

  it('returns null for an empty ETag', () => {
    assert.equal(canonicalEtag(''), null);
  });

  it('returns null for an undefined ETag', () => {
    assert.equal(canonicalEtag(undefined), null);
  });

  it('strips only the quotes from a multipart ETag, leaving the -N suffix intact', () => {
    assert.equal(
      canonicalEtag('"d41d8cd98f00b204e9800998ecf8427e-3"'),
      'd41d8cd98f00b204e9800998ecf8427e-3',
    );
  });

  it('leaves an already-unquoted multipart ETag untouched, suffix and all', () => {
    assert.equal(
      canonicalEtag('d41d8cd98f00b204e9800998ecf8427e-3'),
      'd41d8cd98f00b204e9800998ecf8427e-3',
    );
  });

  it('does not strip a single stray quote character (too short to be a quoted pair)', () => {
    assert.equal(canonicalEtag('"'), '"');
  });
});
