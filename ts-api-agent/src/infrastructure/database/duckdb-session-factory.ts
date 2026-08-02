/**
 * Short-lived, resource-capped, in-memory DuckDB sessions for remote Parquet reads.
 *
 * A serving session is deliberately disposable. It opens `:memory:`, loads `httpfs`, attaches
 * S3 credentials scoped to the artifact bucket, answers one request and disappears. Nothing is
 * persisted, nothing is cached between requests, and no database file is ever created — the
 * user's variants stay in S3 and are read through ranged HTTP requests.
 *
 * Three guarantees are worth stating explicitly, because each of them is a failure mode that
 * would otherwise be invisible:
 *
 * - **Bounded resources.** `memory_limit` and `threads` are set per session, so one pathological
 *   query cannot take the API process down with it.
 * - **A real deadline.** A query that outlives its deadline is *interrupted* through the
 *   binding's cancellation API, not merely abandoned by a promise race. An abandoned query
 *   keeps burning CPU and S3 bandwidth for as long as it feels like.
 * - **Credentials that do not outlive the request.** The S3 secret is dropped and the
 *   connection closed in a `finally`, on every path.
 *
 * The extension is expected to be present already: `LOAD httpfs` is attempted first and
 * `INSTALL` is only reached when a caller explicitly opts in, so a runtime image with a
 * preinstalled extension never needs Internet access on the first `/ask`.
 */
import { DuckDBInstance } from '@duckdb/node-api';
import type { DuckDBConnection } from '@duckdb/node-api';

/** Wall-clock budget for a single serving query. */
export const DEFAULT_QUERY_DEADLINE_MS = 10_000;

export const SESSION_MEMORY_LIMIT = '512MB';
export const SESSION_THREADS = 4;

/** Name of the temporary, in-memory S3 secret each session creates and drops. */
export const SESSION_SECRET_NAME = 'genomic_dataset_s3';

/** Values a serving query may bind. Positions and alleles; never a path or an identifier. */
export type DuckDbParam = string | number | null;

export interface DuckDbS3SessionConfig {
  /** `host[:port]`, without a scheme — the form DuckDB's `httpfs` expects. */
  readonly endpoint: string;
  readonly region: string;
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
  readonly useSsl: boolean;
  readonly urlStyle: 'path' | 'vhost';
  /** Prefix the credentials are valid for, e.g. `s3://genomic-artifacts/`. */
  readonly scope: string;
}

export interface DuckDbSessionConfig {
  readonly s3: DuckDbS3SessionConfig;
  readonly queryDeadlineMs?: number;
  /**
   * Allows `INSTALL httpfs` when the extension is not already present. Off by default: a
   * production image ships the extension, and an implicit download on the first user request
   * is a network dependency in the serving path.
   */
  readonly allowExtensionInstall?: boolean;
}

export interface DuckDbSession {
  query(sql: string, values?: readonly DuckDbParam[]): Promise<Record<string, unknown>[]>;
  close(): Promise<void>;
}

export interface DuckDbSessionFactory {
  open(): Promise<DuckDbSession>;
}

/** Raised when a query is cancelled for exceeding the session's deadline. */
export class QueryDeadlineExceededError extends Error {
  readonly deadlineMs: number;

  constructor(deadlineMs: number) {
    super(`query exceeded its ${deadlineMs} ms deadline and was interrupted`);
    this.name = 'QueryDeadlineExceeded';
    this.deadlineMs = deadlineMs;
  }
}

/** Raised when a caller keeps a session handle past `close()`. */
export class DuckDbSessionClosedError extends Error {
  constructor() {
    super('this DuckDB session has been closed; open a new one per request');
    this.name = 'DuckDbSessionClosed';
  }
}

/** Raised when `httpfs` is neither preinstalled nor installable under the current policy. */
export class HttpfsExtensionUnavailableError extends Error {
  constructor(detail: string) {
    super(
      `the httpfs extension could not be loaded (${detail}); the runtime image must ship a ` +
        'compatible preinstalled extension, or set allowExtensionInstall for local development',
    );
    this.name = 'HttpfsExtensionUnavailable';
  }
}

/** Single-quoted SQL string literal. Only ever applied to configuration, never to user input. */
function sqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

/**
 * Reads the session's S3 configuration from the same environment variables the object-store
 * adapter uses, so the control plane and the query engine can never end up pointed at
 * different endpoints.
 */
export function duckDbS3SessionConfigFromEnv(
  env: NodeJS.ProcessEnv,
  scopeBucket: string,
): DuckDbS3SessionConfig {
  if (!/^[a-z0-9][a-z0-9.-]{2,62}$/.test(scopeBucket)) {
    throw new Error(`the credential scope must be a plain bucket name, got '${scopeBucket}'`);
  }

  const missing = ['S3_ENDPOINT', 'S3_ACCESS_KEY', 'S3_SECRET_KEY'].filter(
    (name) => (env[name] ?? '').length === 0,
  );
  if (missing.length > 0) {
    throw new Error(`DuckDB S3 configuration is incomplete; set ${missing.join(', ')} explicitly`);
  }

  const raw = env.S3_ENDPOINT!;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`S3_ENDPOINT must be an absolute URL, got '${raw}'`);
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`S3_ENDPOINT must be an http(s) URL, got '${raw}'`);
  }

  return {
    endpoint: url.host,
    region: env.S3_REGION ?? 'us-east-1',
    accessKeyId: env.S3_ACCESS_KEY!,
    secretAccessKey: env.S3_SECRET_KEY!,
    useSsl: url.protocol === 'https:',
    urlStyle: 'path',
    scope: `s3://${scopeBucket}/`,
  };
}

async function loadHttpfs(
  connection: DuckDBConnection,
  allowExtensionInstall: boolean,
): Promise<void> {
  try {
    await connection.run('LOAD httpfs;');
    return;
  } catch (error) {
    if (!allowExtensionInstall) {
      throw new HttpfsExtensionUnavailableError((error as Error).message);
    }
  }
  try {
    await connection.run('INSTALL httpfs; LOAD httpfs;');
  } catch (error) {
    throw new HttpfsExtensionUnavailableError((error as Error).message);
  }
}

/**
 * Applies the session limits and attaches the scoped S3 credentials.
 *
 * Exported so the teardown path can be exercised directly: once a session's instance is
 * closed there is nothing left to ask whether the secret really went away.
 */
export async function configureSession(
  connection: DuckDBConnection,
  config: DuckDbSessionConfig,
): Promise<void> {
  await loadHttpfs(connection, config.allowExtensionInstall ?? false);

  await connection.run(`
    SET memory_limit = '${SESSION_MEMORY_LIMIT}';
    SET threads = ${SESSION_THREADS};
    SET enable_http_metadata_cache = true;
  `);

  const { s3 } = config;
  // A scoped secret rather than global `s3_*` settings: the credentials are only offered for
  // URLs below the artifact prefix, so a stray URI elsewhere fails to authenticate instead of
  // quietly succeeding.
  await connection.run(`
    CREATE SECRET ${SESSION_SECRET_NAME} (
      TYPE S3,
      KEY_ID ${sqlString(s3.accessKeyId)},
      SECRET ${sqlString(s3.secretAccessKey)},
      REGION ${sqlString(s3.region)},
      ENDPOINT ${sqlString(s3.endpoint)},
      URL_STYLE ${sqlString(s3.urlStyle)},
      USE_SSL ${s3.useSsl ? 'true' : 'false'},
      SCOPE ${sqlString(s3.scope)}
    );
  `);
}

/** Drops the session's credentials. Safe to call when they were never created. */
export async function teardownSession(connection: DuckDBConnection): Promise<void> {
  await connection.run(`DROP SECRET IF EXISTS ${SESSION_SECRET_NAME};`);
}

/**
 * DuckDB hands back 64-bit integers as `BigInt` and composite values as wrapper objects. A
 * `BigInt` breaks `JSON.stringify` and every `===` against a plain number, so it is converted
 * here rather than leaking into a wire payload.
 */
function toJsonCompatible(value: unknown): unknown {
  if (typeof value === 'bigint') {
    return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value)
      : value.toString();
  }
  if (Array.isArray(value)) return value.map(toJsonCompatible);
  return value;
}

export function createDuckDbSessionFactory(config: DuckDbSessionConfig): DuckDbSessionFactory {
  const deadlineMs = config.queryDeadlineMs ?? DEFAULT_QUERY_DEADLINE_MS;

  return {
    async open(): Promise<DuckDbSession> {
      const instance = await DuckDBInstance.create(':memory:');
      let connection: DuckDBConnection;
      try {
        connection = await instance.connect();
      } catch (error) {
        instance.closeSync();
        throw error;
      }

      try {
        await configureSession(connection, config);
      } catch (error) {
        // Configuration failed mid-way; the secret may or may not exist. Drop it regardless
        // before tearing the session down, then surface the original failure.
        await teardownSession(connection).catch(() => undefined);
        connection.disconnectSync();
        instance.closeSync();
        throw error;
      }

      let closed = false;

      return {
        async query(sql: string, values: readonly DuckDbParam[] = []) {
          if (closed) throw new DuckDbSessionClosedError();

          let interrupted = false;
          const timer = setTimeout(() => {
            interrupted = true;
            connection.interrupt();
          }, deadlineMs);

          try {
            const reader = await connection.runAndReadAll(sql, [...values]);
            return reader.getRowObjectsJS().map((row) => {
              const converted: Record<string, unknown> = {};
              for (const [column, value] of Object.entries(row)) {
                converted[column] = toJsonCompatible(value);
              }
              return converted;
            });
          } catch (error) {
            if (interrupted) throw new QueryDeadlineExceededError(deadlineMs);
            throw error;
          } finally {
            clearTimeout(timer);
          }
        },

        async close() {
          if (closed) return;
          closed = true;
          try {
            await teardownSession(connection);
          } catch (error) {
            // Reported, not rethrown: `close()` runs in a `finally`, and a failed DROP would
            // mask whatever error is actually being handled — typically a deadline interrupt
            // that left the connection unable to run statements. The credentials are in-memory
            // and the instance is destroyed on the next line, so they do not survive either
            // way; what would be lost is the real diagnosis.
            console.warn(
              `[duckdb-session] could not drop ${SESSION_SECRET_NAME} before closing: ${(error as Error).message}`,
            );
          } finally {
            connection.disconnectSync();
            instance.closeSync();
          }
        },
      };
    },
  };
}
