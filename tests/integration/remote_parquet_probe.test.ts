/**
 * Feasibility gate 2: an in-memory Node/DuckDB session can query an explicit remote S3
 * Parquet URI (authenticated MinIO, path style, `httpfs`) with `chrom`/`pos` predicates
 * without downloading the whole object and without touching an unrelated chromosome
 * partition.
 *
 * Request accounting: every DuckDB S3 call is routed through an instrumented HTTP proxy
 * that forwards verbatim to MinIO (Host header preserved, so SigV4 still validates) and
 * records method, decoded path, Range header and response body byte count. Each query
 * gets a fresh in-memory DuckDB so nothing is served from a warm metadata cache.
 */
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';
import { promisify } from 'node:util';

import { DuckDBInstance } from '@duckdb/node-api';

const execFileAsync = promisify(execFile);

const S3_ENDPOINT = process.env.S3_ENDPOINT ?? 'http://localhost:9000';
const S3_ACCESS_KEY = process.env.S3_ACCESS_KEY ?? 'admin';
const S3_SECRET_KEY = process.env.S3_SECRET_KEY ?? 'password123';
// Per-run bucket name: this test runs against a shared MinIO instance (the developer's own
// docker-compose service), so it must never guess at / clobber a pre-existing "probe" bucket.
// Each run gets its own bucket and cleans it up in `after`.
const BUCKET = `probe-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const CHROM12_KEY = 'variants/chrom=12/part-000.parquet';
const CHROM1_KEY = 'variants/chrom=1/part-000.parquet';
const CHROM12_URI = `s3://${BUCKET}/${CHROM12_KEY}`;
const CHROM1_URI = `s3://${BUCKET}/${CHROM1_KEY}`;

const ROWS = 300_000;
const ROW_GROUP_SIZE = 100_000;
/** Row index of the target variant, placed in row group 1 of 3. */
const TARGET_ROW = 150_000;
const TARGET_POS = 21_178_615;
/** `pos` is ordered with a step of 100 so `pos[TARGET_ROW] === TARGET_POS`. */
const POS_BASE = TARGET_POS - TARGET_ROW * 100;

interface DuckDb {
  run(sql: string): Promise<unknown>;
  all(sql: string): Promise<Record<string, unknown>[]>;
  close(): void;
}

async function openInMemoryDuckDb(): Promise<DuckDb> {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  return {
    run: (sql) => connection.run(sql),
    all: async (sql) => (await connection.runAndReadAll(sql)).getRowObjects(),
    close: () => connection.disconnectSync(),
  };
}

interface ProxyRequest {
  method: string;
  path: string;
  rangeStart?: number;
  rangeEnd?: number;
  status: number;
  bytes: number;
}

class MinioProxy {
  readonly requests: ProxyRequest[] = [];
  private server?: http.Server;
  port = 0;

  async start(upstream: URL): Promise<void> {
    this.server = http.createServer((req, res) => {
      const range = /bytes=(\d+)-(\d+)?/.exec(req.headers.range ?? '');
      const record: ProxyRequest = {
        method: req.method ?? '',
        path: decodeURIComponent(req.url ?? ''),
        rangeStart: range ? Number(range[1]) : undefined,
        rangeEnd: range?.[2] ? Number(range[2]) : undefined,
        status: 0,
        bytes: 0,
      };
      this.requests.push(record);

      const upstreamReq = http.request(
        {
          host: upstream.hostname,
          port: upstream.port,
          method: req.method,
          path: req.url,
          headers: req.headers, // Host preserved so MinIO's SigV4 check still matches.
        },
        (upstreamRes) => {
          record.status = upstreamRes.statusCode ?? 0;
          res.writeHead(upstreamRes.statusCode ?? 502, upstreamRes.headers);
          upstreamRes.on('data', (chunk: Buffer) => {
            record.bytes += chunk.length;
          });
          upstreamRes.pipe(res);
        },
      );
      upstreamReq.on('error', () => res.destroy());
      req.pipe(upstreamReq);
    });
    this.server.listen(0, '127.0.0.1');
    await once(this.server, 'listening');
    this.port = (this.server.address() as net.AddressInfo).port;
  }

  reset(): void {
    this.requests.length = 0;
  }

  async stop(): Promise<void> {
    if (!this.server) return;
    await new Promise<void>((resolve) => this.server!.close(() => resolve()));
  }
}

interface Span {
  start: number;
  end: number;
}

function overlap(a: Span, b: Span): number {
  return Math.max(0, Math.min(a.end, b.end) - Math.max(a.start, b.start));
}

function requestSpan(request: ProxyRequest, objectEnd: number): Span {
  const start = request.rangeStart ?? 0;
  return { start, end: request.rangeEnd !== undefined ? request.rangeEnd + 1 : objectEnd };
}

/** Byte offset where the Parquet footer (thrift metadata) begins. */
function footerStart(file: string): number {
  const size = fs.statSync(file).size;
  const tail = Buffer.alloc(8);
  const fd = fs.openSync(file, 'r');
  fs.readSync(fd, tail, 0, 8, size - 8);
  fs.closeSync(fd);
  assert.equal(tail.subarray(4).toString('latin1'), 'PAR1', 'not a parquet file');
  return size - 8 - tail.readUInt32LE(0);
}

async function awsS3(args: string[]): Promise<void> {
  await execFileAsync('aws', ['--endpoint-url', S3_ENDPOINT, ...args], {
    env: {
      ...process.env,
      AWS_ACCESS_KEY_ID: S3_ACCESS_KEY,
      AWS_SECRET_ACCESS_KEY: S3_SECRET_KEY,
      AWS_DEFAULT_REGION: 'us-east-1',
      AWS_EC2_METADATA_DISABLED: 'true',
    },
  });
}

async function waitFor(label: string, predicate: () => Promise<boolean>, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch (err) {
      lastError = err;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`timed out waiting for ${label}: ${String(lastError)}`);
}

describe('remote parquet probe (targeted S3 read feasibility gate)', () => {
  const proxy = new MinioProxy();
  let workDir: string;
  let bindingVersion: string;
  let httpfsVersion: string;
  const objectSize: Record<string, number> = {};
  const objectFooterStart: Record<string, number> = {};
  /** Byte span of every row group of the chromosome-12 object. */
  let rowGroups: (Span & { id: number })[] = [];

  /** Fresh in-memory DuckDB pointed at MinIO through the counting proxy. */
  async function openConfiguredDuckDb(): Promise<DuckDb> {
    const db = await openInMemoryDuckDb();
    await db.run('INSTALL httpfs; LOAD httpfs;');
    await db.run(`
      SET s3_endpoint='127.0.0.1:${proxy.port}';
      SET s3_url_style='path';
      SET s3_use_ssl=false;
      SET s3_region='us-east-1';
      SET s3_access_key_id='${S3_ACCESS_KEY}';
      SET s3_secret_access_key='${S3_SECRET_KEY}';
    `);
    return db;
  }

  before(async () => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'remote-parquet-probe-'));
    bindingVersion = JSON.parse(
      fs.readFileSync(
        path.resolve(import.meta.dirname, '../../node_modules/@duckdb/node-api/package.json'),
        'utf8',
      ),
    ).version;

    await execFileAsync('docker', ['compose', 'up', '-d', 'minio'], {
      cwd: path.resolve(import.meta.dirname, '../..'),
    });
    await waitFor('minio', async () => (await fetch(`${S3_ENDPOINT}/minio/health/live`)).ok);
    await awsS3(['s3', 'mb', `s3://${BUCKET}`]);

    // Build both chromosome objects locally: ZSTD, three ordered 100k-row row groups.
    const builder = await openInMemoryDuckDb();
    for (const [chrom, base, key] of [
      ['12', POS_BASE, CHROM12_KEY],
      ['1', 900_000_000, CHROM1_KEY],
    ] as const) {
      const local = path.join(workDir, `chrom-${chrom}.parquet`);
      await builder.run(`
        COPY (
          SELECT 'rs' || i AS rsid,
                 (${base} + i * 100)::BIGINT AS pos,
                 CASE WHEN i % 3 = 0 THEN '0/1' ELSE '1/1' END AS gt_raw
          FROM range(0, ${ROWS}) t(i)
          ORDER BY pos
        ) TO '${local}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ${ROW_GROUP_SIZE});
      `);
      objectSize[key] = fs.statSync(local).size;
      objectFooterStart[key] = footerStart(local);
      await awsS3(['s3', 'cp', local, `s3://${BUCKET}/${key}`]);
    }

    rowGroups = (
      await builder.all(`
        -- total_compressed_size covers the dictionary page too, so a chunk's end must be
        -- measured from its first page, not from data_page_offset.
        SELECT row_group_id,
               min(coalesce(dictionary_page_offset, data_page_offset)) AS rg_start,
               max(coalesce(dictionary_page_offset, data_page_offset) + total_compressed_size)
                 AS rg_end
        FROM parquet_metadata('${path.join(workDir, 'chrom-12.parquet')}')
        GROUP BY row_group_id ORDER BY row_group_id;
      `)
    ).map((r) => ({
      id: Number(r.row_group_id),
      start: Number(r.rg_start),
      end: Number(r.rg_end),
    }));
    assert.equal(rowGroups.length, 3, 'chromosome-12 object must have three row groups');
    builder.close();

    await proxy.start(new URL(S3_ENDPOINT));

    const probe = await openConfiguredDuckDb();
    httpfsVersion = String(
      (
        await probe.all(
          "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='httpfs' AND loaded",
        )
      )[0]?.extension_version,
    );
    probe.close();
  });

  after(async () => {
    await proxy.stop();
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
    // Clean up only the bucket this run created — never a pre-existing one.
    await awsS3(['s3', 'rb', `s3://${BUCKET}`, '--force']).catch(() => undefined);
  });

  it('reads only the matching row group of the explicitly selected chromosome-12 object', async () => {
    const db = await openConfiguredDuckDb();
    try {
    proxy.reset();

    const rows = await db.all(`
      SELECT rsid, gt_raw
      FROM read_parquet(
        ['${CHROM12_URI}'],
        hive_partitioning = true,
        -- Mandatory per contracts/ingestion-v1.md#reading-the-dataset: bare
        -- hive_partitioning = true infers this all-numeric-chromosome fixture's \`chrom\`
        -- column as BIGINT, and \`chrom = 'X'\` on that inferred type raises a DuckDB
        -- Conversion Error (and, on the Node binding, a BIGINT \`chrom\` breaks \`=== '12'\`
        -- comparisons and JSON.stringify). Do not simplify this back to bare
        -- hive_partitioning — it happens to pass here only because '12' is castable to BIGINT.
        hive_types_autocast = 0
      )
      WHERE chrom = '12' AND pos = ${TARGET_POS};
    `);

    const size = objectSize[CHROM12_KEY];
    const footerSpan: Span = { start: objectFooterStart[CHROM12_KEY], end: size };
    /** DuckDB's footer probe is a single small tail window, not an arbitrary large range. */
    const FOOTER_WINDOW_BYTES = 64 * 1024;

    // Every request against the measured object must be a HEAD or a *ranged* GET. A plain
    // full-object GET would otherwise slip past the footer/data classification below (it
    // wouldn't be counted as either) while still downloading the whole object.
    for (const r of proxy.requests) {
      assert.ok(
        r.method === 'HEAD' || r.rangeStart !== undefined,
        `expected only HEAD or ranged GET requests against the measured object, got unranged ${r.method} ${r.path}`,
      );
    }

    // Classify every GET (ranged or not — requestSpan falls back to the full object span for
    // an unranged request, so nothing is silently dropped from both buckets).
    const gets = proxy.requests.filter((r) => r.method === 'GET');
    // A request that reaches into the thrift footer is a metadata read; DuckDB fetches a
    // tail window that can incidentally include the last bytes of the final row group.
    const footerReads = gets.filter((r) => overlap(requestSpan(r, size), footerSpan) > 0);
    const dataReads = gets.filter((r) => overlap(requestSpan(r, size), footerSpan) === 0);
    const bytesFromRowGroup = (reads: ProxyRequest[], id: number) =>
      reads.reduce((sum, r) => sum + overlap(requestSpan(r, size), rowGroups[id]), 0);
    const totalBytes = proxy.requests.reduce((sum, r) => sum + r.bytes, 0);

    const summary = {
      duckdbBinding: `@duckdb/node-api@${bindingVersion}`,
      httpfsVersion,
      objectSizes: objectSize,
      selectedFiles: [...new Set(proxy.requests.map((r) => r.path))],
      requests: proxy.requests,
      footerReads,
      dataReads,
      totalBytes,
      rowGroups,
      dataBytesPerRowGroup: [0, 1, 2].map((id) => bytesFromRowGroup(dataReads, id)),
      footerReadBytesPerRowGroup: [0, 1, 2].map((id) => bytesFromRowGroup(footerReads, id)),
    };
    console.log(`\n[remote-parquet-gate] targeted ${JSON.stringify(summary, null, 2)}`);

    assert.deepEqual(rows, [{ rsid: `rs${TARGET_ROW}`, gt_raw: '0/1' }]);
    assert.deepEqual(
      proxy.requests.filter((r) => r.path.includes('/chrom=1/')),
      [],
      'no request may touch the unrelated chromosome-1 object',
    );
    assert.ok(footerReads.length > 0, 'expected at least one footer/metadata read');
    assert.ok(dataReads.length > 0, 'expected at least one column-chunk data read');
    assert.ok(
      footerReads.every((r) => requestSpan(r, size).start >= size - FOOTER_WINDOW_BYTES),
      'every footer read must start within the last 64 KiB of the object — a read that starts ' +
        'earlier is not a footer probe, it is a large range that happens to also overlap the ' +
        'footer, and must not be let through the footer/data classification uncounted',
    );
    assert.ok(
      // Observed on this fixture: 202,652 / 567,222 = 35.7%. 45% leaves comfortable margin
      // for run-to-run byte-count jitter while still tripping on a fetch-strategy regression
      // (e.g. downloading a whole row group's worth of extra data, or the whole object).
      totalBytes < size * 0.45,
      `read ${totalBytes} of ${size} bytes (${((totalBytes / size) * 100).toFixed(1)}%); expected close to the observed ~35.7% ratio`,
    );
    assert.equal(
      bytesFromRowGroup(dataReads, 0),
      0,
      'no data may be read from non-matching row group 0',
    );
    assert.equal(
      bytesFromRowGroup(dataReads, 2),
      0,
      'no data may be read from non-matching row group 2',
    );
    assert.ok(
      bytesFromRowGroup(dataReads, 1) > 0,
      'column chunks must be read from the matching row group 1',
    );
    } finally {
      db.close();
    }
  });

  it('captures the EXPLAIN ANALYZE profile of the targeted remote read', async () => {
    const db = await openConfiguredDuckDb();
    try {
    proxy.reset();

    const profile = (
      await db.all(`
        EXPLAIN ANALYZE
        SELECT rsid, gt_raw
        FROM read_parquet(
          ['${CHROM12_URI}'],
          hive_partitioning = true,
          -- Mandatory per contracts/ingestion-v1.md#reading-the-dataset — see the comment on
          -- the first probe query above for why bare hive_partitioning must not be used.
          hive_types_autocast = 0
        )
        WHERE chrom = '12' AND pos = ${TARGET_POS};
      `)
    )
      .map((r) => String(r.explain_value))
      .join('\n');

    console.log(`\n[remote-parquet-gate] EXPLAIN ANALYZE\n${profile}`);
    assert.match(profile, /PARQUET_SCAN|READ_PARQUET/i);
    // The report leans on these specific numbers from the profile as corroboration of the
    // proxy-measured counts — assert them, not just that the plan mentions Parquet at all.
    assert.match(
      profile,
      /Total Files Read:\s*1\b/,
      'expected the profile to confirm exactly one file was read',
    );
    assert.match(
      profile,
      new RegExp(`pos\\s*=\\s*${TARGET_POS}`),
      'expected the profile to show the pos predicate as a pushed-down filter',
    );
    const getCount = proxy.requests.filter((r) => r.method === 'GET').length;
    assert.ok(
      getCount > 0 && getCount <= 6,
      `expected a small, bounded number of GET requests for a targeted read, got ${getCount}`,
    );
    assert.deepEqual(
      proxy.requests.filter((r) => r.path.includes('/chrom=1/')),
      [],
      'profiling the targeted read must not touch chromosome 1 either',
    );
    } finally {
      db.close();
    }
  });

  it('documents the extra cost of passing an unpruned file list', async () => {
    const db = await openConfiguredDuckDb();
    try {
    proxy.reset();

    const rows = await db.all(`
      SELECT rsid, gt_raw
      FROM read_parquet(
        ['${CHROM1_URI}', '${CHROM12_URI}'],
        hive_partitioning = true,
        -- Mandatory per contracts/ingestion-v1.md#reading-the-dataset — see the comment on
        -- the first probe query above for why bare hive_partitioning must not be used.
        hive_types_autocast = 0
      )
      WHERE chrom = '12' AND pos = ${TARGET_POS};
    `);
    assert.deepEqual(rows, [{ rsid: `rs${TARGET_ROW}`, gt_raw: '0/1' }]);

    const chrom1Requests = proxy.requests.filter((r) => r.path.includes('/chrom=1/'));
    const chrom1Bytes = chrom1Requests.reduce((sum, r) => sum + r.bytes, 0);

    console.log(
      `\n[remote-parquet-gate] unpruned-file-list ${JSON.stringify(
        {
          totalBytes: proxy.requests.reduce((sum, r) => sum + r.bytes, 0),
          chrom1RequestCount: chrom1Requests.length,
          chrom1Bytes,
          requests: proxy.requests,
        },
        null,
        2,
      )}`,
    );

    // This is the point of the comparison: an unpruned list makes DuckDB talk to the
    // irrelevant partition, so application-side manifest pruning stays mandatory.
    assert.ok(
      chrom1Requests.length > 0,
      'an unpruned file list is expected to cost requests against chromosome 1',
    );
    } finally {
      db.close();
    }
  });
});
