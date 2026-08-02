/**
 * Constrained DuckDB session tests.
 *
 * A serving session is a short-lived, in-memory, resource-capped DuckDB with scoped S3
 * credentials attached for exactly as long as one request needs them. These tests pin the
 * three properties that make that claim true rather than aspirational: the limits are really
 * set on the connection, the query deadline really interrupts a running query, and the
 * credentials really disappear when the session is torn down.
 *
 * No S3 endpoint is contacted here — the credentials are configured but never used, which is
 * why these run without MinIO. Real remote reads are covered by
 * `tests/integration/remote_parquet_pruning.test.ts`.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DuckDBInstance } from '@duckdb/node-api';

import {
  DEFAULT_QUERY_DEADLINE_MS,
  DuckDbSessionClosedError,
  type DuckDbS3SessionConfig,
  QueryDeadlineExceededError,
  SESSION_MEMORY_LIMIT,
  SESSION_SECRET_NAME,
  SESSION_THREADS,
  configureSession,
  createDuckDbSessionFactory,
  duckDbS3SessionConfigFromEnv,
  teardownSession,
} from './duckdb-session-factory.ts';

const S3_CONFIG: DuckDbS3SessionConfig = {
  endpoint: '127.0.0.1:9',
  region: 'us-east-1',
  accessKeyId: 'test-access-key',
  secretAccessKey: 'test-secret-key',
  useSsl: false,
  urlStyle: 'path',
  scope: 's3://genomic-artifacts/',
};

function factory(overrides: { queryDeadlineMs?: number } = {}) {
  return createDuckDbSessionFactory({ s3: S3_CONFIG, ...overrides });
}

describe('duckdb session factory', () => {
  it('applies the contracted memory, thread and metadata-cache limits', async () => {
    const session = await factory().open();
    try {
      const [settings] = await session.query(`
        SELECT current_setting('memory_limit') AS memory_limit,
               current_setting('threads') AS threads,
               current_setting('enable_http_metadata_cache') AS metadata_cache;
      `);

      // `SET memory_limit = '512MB'` is the contracted statement, and DuckDB reads `MB` as
      // 10^6 bytes — 512 MB is 488.2 MiB, which is what it reports back. Asserted verbatim so
      // a future edit to the unit suffix cannot pass unnoticed.
      assert.equal(SESSION_MEMORY_LIMIT, '512MB');
      assert.equal(settings?.memory_limit, '488.2 MiB');
      assert.equal(String(settings?.threads), String(SESSION_THREADS));
      assert.equal(settings?.metadata_cache, true);
    } finally {
      await session.close();
    }
  });

  it('runs entirely in memory: no session ever attaches a database file', async () => {
    const session = await factory().open();
    try {
      const databases = await session.query(
        'SELECT database_name, path FROM duckdb_databases() WHERE NOT internal;',
      );

      assert.deepEqual(
        databases.map((row) => row.path).filter((path) => path !== null && path !== ''),
        [],
        'a serving session must never open or create a per-dataset .duckdb file',
      );
    } finally {
      await session.close();
    }
  });

  it('loads httpfs so remote Parquet is readable', async () => {
    const session = await factory().open();
    try {
      const [httpfs] = await session.query(`
        SELECT extension_version FROM duckdb_extensions()
        WHERE extension_name = 'httpfs' AND loaded;
      `);

      assert.ok(httpfs !== undefined, 'httpfs must be loaded before the first remote read');
      assert.equal(typeof httpfs.extension_version, 'string');
    } finally {
      await session.close();
    }
  });

  it('attaches S3 credentials scoped to the artifact prefix', async () => {
    const session = await factory().open();
    try {
      const secrets = await session.query('SELECT name, type, scope FROM duckdb_secrets();');

      assert.equal(secrets.length, 1);
      assert.equal(secrets[0]?.name, SESSION_SECRET_NAME);
      assert.equal(secrets[0]?.type, 's3');
      assert.deepEqual(secrets[0]?.scope, ['s3://genomic-artifacts/']);
    } finally {
      await session.close();
    }
  });

  it('drops the credentials on teardown', async () => {
    // Driven at the connection level so the *drop* is observable: once a session is closed its
    // instance is gone and nothing could be asked about it any more.
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    try {
      await configureSession(connection, { s3: S3_CONFIG });
      const before = await connection.runAndReadAll('SELECT count(*) AS n FROM duckdb_secrets();');
      assert.equal(Number(before.getRowObjects()[0]?.n), 1);

      await teardownSession(connection);

      const after = await connection.runAndReadAll('SELECT count(*) AS n FROM duckdb_secrets();');
      assert.equal(Number(after.getRowObjects()[0]?.n), 0, 'the S3 secret must not outlive the session');
    } finally {
      connection.disconnectSync();
      instance.closeSync();
    }
  });

  it('drops the credentials even when the query threw', async () => {
    const session = await factory().open();
    let closed = false;
    try {
      await assert.rejects(() => session.query('SELECT * FROM a_table_that_does_not_exist;'));
    } finally {
      await session.close();
      closed = true;
    }
    assert.ok(closed);
    await assert.rejects(() => session.query('SELECT 1;'), DuckDbSessionClosedError);
  });

  it('defaults to a ten-second query deadline', () => {
    assert.equal(DEFAULT_QUERY_DEADLINE_MS, 10_000);
  });

  it('interrupts a query that outlives its deadline instead of waiting for it', async () => {
    const session = await factory({ queryDeadlineMs: 250 }).open();
    const started = Date.now();
    try {
      const error = await session
        .query('SELECT count(*) AS n FROM range(0, 200000000000) t(i) WHERE i % 7 = 3;')
        .then(
          () => null,
          (thrown: unknown) => thrown as Error,
        );

      assert.ok(error instanceof QueryDeadlineExceededError, `unexpected error: ${error}`);
      assert.equal(error.name, 'QueryDeadlineExceeded');
      assert.equal(error.deadlineMs, 250);
      assert.ok(
        Date.now() - started < 10_000,
        'the deadline must cancel the running query, not merely time out around it',
      );
    } finally {
      await session.close();
    }
  });

  it('stays usable for further queries once a fast one has completed', async () => {
    const session = await factory({ queryDeadlineMs: 5_000 }).open();
    try {
      assert.deepEqual(await session.query('SELECT 1 AS one;'), [{ one: 1 }]);
      assert.deepEqual(await session.query('SELECT 2 AS two;'), [{ two: 2 }]);
    } finally {
      await session.close();
    }
  });

  it('returns JSON-compatible primitives, never BigInt', async () => {
    const session = await factory().open();
    try {
      const [row] = await session.query(
        "SELECT count(*) AS n, 'x' AS s, NULL AS nothing FROM range(0, 3);",
      );

      assert.deepEqual(row, { n: 3, s: 'x', nothing: null });
      assert.equal(typeof row?.n, 'number');
      assert.doesNotThrow(() => JSON.stringify(row));
    } finally {
      await session.close();
    }
  });

  it('closes idempotently', async () => {
    const session = await factory().open();
    await session.close();
    await session.close();
  });
});

describe('duckdb session configuration from the environment', () => {
  const env = {
    S3_ENDPOINT: 'http://localhost:9000',
    S3_ACCESS_KEY: 'admin',
    S3_SECRET_KEY: 'password123',
  };

  it('derives a DuckDB endpoint and TLS mode from the configured S3 URL', () => {
    const config = duckDbS3SessionConfigFromEnv(env, 'genomic-artifacts');

    assert.equal(config.endpoint, 'localhost:9000');
    assert.equal(config.useSsl, false);
    assert.equal(config.urlStyle, 'path');
    assert.equal(config.scope, 's3://genomic-artifacts/');
    assert.equal(config.region, 'us-east-1');
  });

  it('keeps TLS on for an https endpoint', () => {
    const config = duckDbS3SessionConfigFromEnv(
      { ...env, S3_ENDPOINT: 'https://s3.eu-west-1.amazonaws.com', S3_REGION: 'eu-west-1' },
      'genomic-artifacts',
    );

    assert.equal(config.endpoint, 's3.eu-west-1.amazonaws.com');
    assert.equal(config.useSsl, true);
    assert.equal(config.region, 'eu-west-1');
  });

  it('refuses an incomplete or non-http configuration', () => {
    assert.throws(() => duckDbS3SessionConfigFromEnv({}, 'genomic-artifacts'), /S3_ENDPOINT/);
    assert.throws(
      () =>
        duckDbS3SessionConfigFromEnv(
          { ...env, S3_ENDPOINT: 's3://genomic-artifacts' },
          'genomic-artifacts',
        ),
      /http/,
    );
  });

  it('refuses a scope bucket that is not a plain bucket name', () => {
    assert.throws(() => duckDbS3SessionConfigFromEnv(env, 's3://genomic-artifacts'), /bucket/);
    assert.throws(() => duckDbS3SessionConfigFromEnv(env, 'bucket/prefix'), /bucket/);
  });
});
