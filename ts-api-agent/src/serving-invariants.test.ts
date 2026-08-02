/**
 * A source-level sweep of the invariants the serving path is *supposed* to have, asserted
 * against the tree rather than against a running system.
 *
 * Every other test here proves a behaviour on one code path. These assertions prove the absence
 * of a code path — that nowhere in production does a second, quieter way to answer a question
 * exist. That is not something a behavioural test can show: a fixture fallback, a wildcard scan
 * or a cached local database only fires on the day the real path fails, which is the day nobody
 * is watching. So they are checked the only way absence can be: by reading every production
 * source file and refusing the shapes outright.
 *
 * "Production" here means everything under `ts-api-agent/src` that is not a test or a test
 * helper, plus the Rust worker's sources. Scripts under `scripts/` are operator tools, not
 * request-path code, and are excluded — except where noted.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const TS_SOURCE_ROOT = path.join(REPO_ROOT, 'ts-api-agent/src');
const RUST_SOURCE_ROOT = path.join(REPO_ROOT, 'rust-ingestion-worker/src');

interface SourceFile {
  readonly relativePath: string;
  readonly text: string;
}

function collect(root: string, extension: string, exclude: (file: string) => boolean): SourceFile[] {
  const files: SourceFile[] = [];
  const walk = (directory: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const full = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (entry.name.endsWith(extension) && !exclude(full)) {
        files.push({
          relativePath: path.relative(REPO_ROOT, full),
          text: fs.readFileSync(full, 'utf8'),
        });
      }
    }
  };
  walk(root);
  return files;
}

/**
 * TypeScript request-path code.
 *
 * `*.test.ts` and `workflow-test-harness.ts` are tests and test scaffolding. `test_e2e.ts` is a
 * developer smoke script, not something the server imports; it is excluded from the *shape*
 * checks below but is still covered by the fixture-fallback check, because a script that
 * answered from a fixture would still be a lie told to whoever ran it.
 */
const PRODUCTION_TS = collect(
  TS_SOURCE_ROOT,
  '.ts',
  (file) =>
    file.endsWith('.test.ts') ||
    file.endsWith('workflow-test-harness.ts') ||
    file.endsWith('test_e2e.ts'),
);

const ALL_TS = collect(TS_SOURCE_ROOT, '.ts', (file) => file.endsWith('.test.ts'));

const PRODUCTION_RUST = collect(RUST_SOURCE_ROOT, '.rs', () => false);

/** Strips line and block comments, so a prohibited shape *described* in prose does not match. */
function code(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*(\/\/|\*).*$/gm, '');
}

function offenders(
  files: readonly SourceFile[],
  predicate: (source: string, file: SourceFile) => boolean,
): string[] {
  return files.filter((file) => predicate(code(file.text), file)).map((file) => file.relativePath);
}

describe('serving-path invariants (source sweep)', () => {
  it('has production code to sweep at all', () => {
    // A refactor that moved or renamed the source roots would otherwise turn every assertion
    // below into a vacuous pass over an empty list.
    assert.ok(PRODUCTION_TS.length >= 20, `only ${PRODUCTION_TS.length} TypeScript sources found`);
    assert.ok(PRODUCTION_RUST.length >= 5, `only ${PRODUCTION_RUST.length} Rust sources found`);
  });

  it('opens no DuckDB database but :memory:', () => {
    // The two allowed on-disk databases are the *reference* snapshot — versioned ClinVar
    // coordinates derived from a file in this repository, never user data — and nothing else.
    const REFERENCE_MODULES = [
      'ts-api-agent/src/infrastructure/database/reference-bootstrap.ts',
      'ts-api-agent/src/infrastructure/database/clinvar-coordinate-resolver.ts',
    ];
    const bad = offenders(
      PRODUCTION_TS.filter((file) => !REFERENCE_MODULES.includes(file.relativePath)),
      (source) => /DuckDBInstance\.create\(\s*(?!['"]:memory:['"])/.test(source),
    );
    assert.deepEqual(
      bad,
      [],
      'a serving session is in-memory; a file-backed database would be a local copy of user data',
    );
  });

  it('downloads or caches no per-dataset database', () => {
    const bad = offenders(
      PRODUCTION_TS,
      (source, file) =>
        /\.duckdb/.test(source) &&
        !file.relativePath.includes('reference-bootstrap') &&
        !file.relativePath.includes('clinvar-coordinate-resolver'),
    );
    assert.deepEqual(bad, [], 'no production module may name a `.duckdb` path for a dataset');
  });

  it('never scans a wildcard, a prefix or a glob', () => {
    const bad = offenders(
      PRODUCTION_TS,
      (source) => /read_parquet\s*\(\s*['"`][^'"`]*\*/.test(source) || /\*\*\/\*\.parquet/.test(source),
    );
    assert.deepEqual(
      bad,
      [],
      'candidate objects come from the validated manifest inventory, never from a glob',
    );
  });

  it('always pairs hive_partitioning with hive_types_autocast', () => {
    // Frozen normatively in `contracts/ingestion-v1.md` ("Reading the dataset"): with autocast
    // left on, DuckDB infers `chrom`'s type from the partitions a scan happened to touch, so the
    // same dataset yields VARCHAR for a whole-dataset scan and BIGINT for a pruned one.
    const bad: string[] = [];
    for (const file of [...ALL_TS, ...PRODUCTION_RUST]) {
      for (const match of code(file.text).matchAll(/hive_partitioning\s*=\s*true/g)) {
        const window = code(file.text).slice(match.index, match.index + 200);
        if (!/hive_types_autocast|hive_types\s*=/.test(window)) {
          bad.push(`${file.relativePath}:${match.index}`);
        }
      }
    }
    assert.deepEqual(bad, [], 'bare `hive_partitioning = true` is prohibited by the contract');
  });

  it('builds an s3:// URI in exactly one place', () => {
    const bad = offenders(
      PRODUCTION_TS,
      (source, file) =>
        /['"`]s3:\/\//.test(source) &&
        !file.relativePath.endsWith('parquet-dataset-resolver.ts') &&
        !file.relativePath.endsWith('duckdb-session-factory.ts'),
    );
    assert.deepEqual(
      bad,
      [],
      "only `parquetObjectUri` builds an object URI, and only the session factory's credential " +
        'scope names a bucket prefix',
    );
  });

  it('accepts no caller-supplied bucket, key or URI on the request path', () => {
    const index = ALL_TS.find((file) => file.relativePath.endsWith('src/index.ts'))!;
    // Both request bodies are closed lists of field names; anything else is a 400 before it can
    // be sanitised downstream.
    assert.match(code(index.text), /readClosedJsonObject\(c, \['datasetKey'\]\)/);
    assert.match(code(index.text), /readClosedJsonObject\(c, \['datasetId', 'question'\]\)/);
    for (const forbidden of ['body.bucket', 'body.key', 'body.uri', 'body.path', 'body.manifest']) {
      assert.ok(
        !code(index.text).includes(forbidden),
        `the HTTP surface must not read '${forbidden}' from a request`,
      );
    }
  });

  it('turns an unresolvable or absent target into a refusal, not a wider scan', () => {
    const resolver = PRODUCTION_TS.find((file) =>
      file.relativePath.endsWith('parquet-dataset-resolver.ts'),
    )!;
    assert.match(
      code(resolver.text),
      /if \(candidates\.length === 0\) \{\s*throw new TargetNotPresentError/,
      'an empty candidate set must throw before any scan is built',
    );
    const repository = PRODUCTION_TS.find((file) =>
      file.relativePath.endsWith('database/duckdb.ts'),
    )!;
    assert.ok(
      !/read_parquet[\s\S]{0,200}dataset\.parquetObjects/.test(code(repository.text)),
      'the whole inventory must never reach a scan; only selected candidates do',
    );
  });

  it('falls back to no fixture at run time', () => {
    // `reference-bootstrap.ts` names the committed coordinate TSV, which is reference data and
    // the declared source of `demo-clinvar-grch38-v1` — not a stand-in for absent user data.
    const bad = offenders(
      ALL_TS,
      (source, file) =>
        /tests\/fixtures/.test(source) && !file.relativePath.endsWith('reference-bootstrap.ts'),
    );
    assert.deepEqual(bad, [], 'no runtime path may read a fixture');

    const withFallbackWording = offenders(ALL_TS, (source) =>
      /fallback[\s\S]{0,40}(fixture|demo_user|sample)/i.test(source),
    );
    assert.deepEqual(withFallbackWording, []);
  });

  it('launches no subprocess, and never mentions cargo', () => {
    const bad = offenders(
      ALL_TS,
      (source) => /child_process|execFile\(|spawn\(|execSync/.test(source) || /\bcargo\b/.test(source),
    );
    assert.deepEqual(
      bad,
      [],
      'the Rust worker is reached by scheduling a Temporal Activity on its own task queue, ' +
        'never by launching a binary',
    );
  });

  it('registers buildDatasetArtifact nowhere in TypeScript', () => {
    const bad = offenders(
      PRODUCTION_TS,
      (source, file) =>
        /buildDatasetArtifact\s*[:(]/.test(source) &&
        !file.relativePath.endsWith('application/workflows.ts'),
    );
    assert.deepEqual(
      bad,
      [],
      'the activity is declared for the workflow proxy and implemented only in Rust',
    );
  });

  it('never installs a DuckDB extension without an explicit opt-in', () => {
    const factory = PRODUCTION_TS.find((file) =>
      file.relativePath.endsWith('duckdb-session-factory.ts'),
    )!;
    const source = code(factory.text);
    assert.match(source, /SET autoinstall_known_extensions = \$\{allowExtensionInstall/);
    assert.match(source, /SET autoload_known_extensions = \$\{allowExtensionInstall/);
    assert.match(source, /if \(!allowExtensionInstall\) \{\s*throw new HttpfsExtensionUnavailableError/);
  });

  it('emits every required metrics field on the serving path', () => {
    const repository = PRODUCTION_TS.find((file) =>
      file.relativePath.endsWith('database/duckdb.ts'),
    )!;
    const source = code(repository.text);
    const metrics = /\[serving-metrics\][\s\S]{0,900}?\}\)\}`/.exec(source);
    assert.ok(metrics, 'the serving path must emit a structured metrics record');
    for (const field of [
      'datasetId',
      'datasetChecksumSha256',
      'referenceVersion',
      'selectedFileCount',
      's3RequestCount',
      'bytesRead',
      'queryLatencyMs',
    ]) {
      assert.match(metrics[0], new RegExp(`\\b${field}\\b`), `metrics record is missing '${field}'`);
    }
    // …and nothing clinical. A metrics stream is not a place to accumulate a genotype profile.
    for (const forbidden of ['targetId', 'rsid', 'gene', 'genotype', 'variants']) {
      assert.doesNotMatch(
        metrics[0],
        new RegExp(`\\b${forbidden}\\b`),
        `metrics record must not carry '${forbidden}'`,
      );
    }
  });

  it('bounds every S3 client and every serving query', () => {
    const store = PRODUCTION_TS.find((file) => file.relativePath.endsWith('s3-object-store.ts'))!;
    assert.match(code(store.text), /requestTimeout:/);
    assert.match(code(store.text), /connectionTimeout:/);
    assert.match(code(store.text), /maxAttempts:/);

    const factory = PRODUCTION_TS.find((file) =>
      file.relativePath.endsWith('duckdb-session-factory.ts'),
    )!;
    assert.match(code(factory.text), /SET memory_limit = '\$\{SESSION_MEMORY_LIMIT\}'/);
    assert.match(code(factory.text), /connection\.interrupt\(\)/);

    const repository = PRODUCTION_TS.find((file) =>
      file.relativePath.endsWith('database/duckdb.ts'),
    )!;
    assert.match(code(repository.text), /LIMIT \$\{MAX_VARIANT_ROWS\}/);
  });

  it('spawns no process from the Rust worker either', () => {
    const bad = offenders(PRODUCTION_RUST, (source) => /std::process::Command|Command::new/.test(source));
    assert.deepEqual(bad, [], 'the Rust worker does its own work; it shells out to nothing');
  });
});
