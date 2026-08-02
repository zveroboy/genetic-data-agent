/**
 * Builds the versioned ClinVar coordinate snapshot the serving path resolves targets against.
 *
 * The snapshot is a small, read-only DuckDB database derived once from a committed TSV. It is
 * *reference* data — global, versioned and identical for every user — and is deliberately not
 * a user dataset: no genotype is ever written here, and nothing about a person is derivable
 * from it. That is what lets it live in a local file while user variants stay remote.
 *
 * Two properties matter:
 *
 * - **It is labelled.** Every row carries the `reference_version`/`reference_build` it belongs
 *   to, and the build refuses to finish if any row disagrees with the version being declared.
 *   A published dataset manifest names the same version, so a query can prove that the
 *   coordinates it pruned with describe the same genome the Parquet was written against.
 * - **It is built atomically.** The database is assembled under a temporary name and renamed
 *   into place, so a crashed or rejected build leaves no half-populated snapshot for a later
 *   process to open and trust.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { DuckDBInstance } from '@duckdb/node-api';

import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../domain/datasets.ts';

/** Table holding one row per declared reference variant. */
export const REFERENCE_COORDINATES_TABLE = 'clinvar_coordinates';

/** Single-row table recording which snapshot the database *is*. */
export const REFERENCE_SNAPSHOT_TABLE = 'reference_snapshot';

/** Raised when a reference snapshot is missing, unlabelled or internally inconsistent. */
export class ReferenceSnapshotError extends Error {
  readonly databasePath: string;

  constructor(databasePath: string, detail: string) {
    super(`reference snapshot '${databasePath}' ${detail}`);
    this.name = 'ReferenceSnapshotUnavailable';
    this.databasePath = databasePath;
  }
}

/** Identity of a built snapshot. */
export interface ReferenceSnapshot {
  readonly path: string;
  readonly referenceVersion: string;
  readonly referenceBuild: string;
  readonly rowCount: number;
}

export interface BuildReferenceDatabaseOptions {
  /** Tab-separated coordinate snapshot; see `scripts/generate_clinical_benchmark_vcf.ts`. */
  readonly tsvPath: string;
  readonly databasePath: string;
  readonly referenceVersion: string;
  readonly referenceBuild: string;
}

const COLUMN_TYPES = `{
  'reference_version': 'VARCHAR', 'reference_build': 'VARCHAR', 'chrom': 'VARCHAR',
  'pos': 'UINTEGER', 'rsid': 'VARCHAR', 'ref': 'VARCHAR', 'alt': 'VARCHAR',
  'gene': 'VARCHAR', 'phenotype': 'VARCHAR', 'clinical_significance': 'VARCHAR',
  'evidence_note': 'VARCHAR'
}`;

/** Reads the identity a snapshot claims for itself, opening it read-only. */
async function readSnapshot(databasePath: string): Promise<ReferenceSnapshot> {
  let instance;
  try {
    instance = await DuckDBInstance.create(databasePath, { access_mode: 'READ_ONLY' });
  } catch (error) {
    throw new ReferenceSnapshotError(
      databasePath,
      `could not be opened read-only: ${(error as Error).message}`,
    );
  }
  const connection = await instance.connect();
  try {
    const rows = (
      await connection.runAndReadAll(`
        SELECT reference_version, reference_build, row_count FROM ${REFERENCE_SNAPSHOT_TABLE};
      `)
    ).getRowObjects();
    if (rows.length !== 1) {
      throw new ReferenceSnapshotError(
        databasePath,
        `declares ${rows.length} snapshot identities; exactly one is required`,
      );
    }
    const row = rows[0]!;
    return {
      path: databasePath,
      referenceVersion: String(row.reference_version),
      referenceBuild: String(row.reference_build),
      rowCount: Number(row.row_count),
    };
  } catch (error) {
    if (error instanceof ReferenceSnapshotError) throw error;
    throw new ReferenceSnapshotError(
      databasePath,
      `is not a valid coordinate snapshot: ${(error as Error).message}`,
    );
  } finally {
    connection.disconnectSync();
    instance.closeSync();
  }
}

/**
 * Builds the snapshot if it is not already there, and returns its identity either way.
 *
 * Idempotent by design: the serving path may call this at start-up without caring whether a
 * previous run, an image build step or another process got there first.
 */
export async function buildReferenceDatabase(
  options: BuildReferenceDatabaseOptions,
): Promise<ReferenceSnapshot> {
  const { tsvPath, databasePath, referenceVersion, referenceBuild } = options;

  if (fs.existsSync(databasePath)) {
    const existing = await readSnapshot(databasePath);
    if (
      existing.referenceVersion !== referenceVersion ||
      existing.referenceBuild !== referenceBuild
    ) {
      throw new ReferenceSnapshotError(
        databasePath,
        `already holds '${existing.referenceVersion}'/'${existing.referenceBuild}', but ` +
          `'${referenceVersion}'/'${referenceBuild}' was requested`,
      );
    }
    return existing;
  }

  if (!fs.existsSync(tsvPath)) {
    throw new ReferenceSnapshotError(databasePath, `cannot be built: '${tsvPath}' does not exist`);
  }

  fs.mkdirSync(path.dirname(databasePath), { recursive: true });
  // Assembled under a private name and renamed into place, so no reader can ever open a
  // snapshot that is still being written or that failed its own consistency check.
  const stagingPath = `${databasePath}.building-${process.pid}-${Date.now().toString(36)}`;
  fs.rmSync(stagingPath, { force: true });

  const instance = await DuckDBInstance.create(stagingPath);
  const connection = await instance.connect();
  try {
    await connection.run(`
      CREATE TABLE ${REFERENCE_COORDINATES_TABLE} (
        reference_version VARCHAR NOT NULL,
        reference_build   VARCHAR NOT NULL,
        chrom             VARCHAR NOT NULL,
        pos               UINTEGER NOT NULL,
        rsid              VARCHAR,
        ref               VARCHAR NOT NULL,
        alt               VARCHAR NOT NULL,
        gene              VARCHAR NOT NULL,
        phenotype         VARCHAR NOT NULL,
        clinical_significance VARCHAR NOT NULL,
        evidence_note     VARCHAR NOT NULL
      );
    `);

    // The path is a bound parameter, not interpolated: a snapshot path is operator
    // configuration, but nothing in this module needs to know how to quote a filename.
    await connection.run(
      `
        INSERT INTO ${REFERENCE_COORDINATES_TABLE}
        SELECT reference_version, reference_build, chrom, pos, rsid, ref, alt, gene, phenotype,
               clinical_significance, evidence_note
        FROM read_csv(?, delim = '\t', header = true, quote = '', escape = '',
                      columns = ${COLUMN_TYPES});
      `,
      [tsvPath],
    );

    const [audit] = (
      await connection.runAndReadAll(
        `
          SELECT count(*) AS total,
                 count(*) FILTER (
                   WHERE reference_version <> $1 OR reference_build <> $2
                 ) AS mislabelled
          FROM ${REFERENCE_COORDINATES_TABLE};
        `,
        [referenceVersion, referenceBuild],
      )
    ).getRowObjects();

    const total = Number(audit!.total);
    const mislabelled = Number(audit!.mislabelled);
    if (total === 0) {
      throw new ReferenceSnapshotError(databasePath, `would be empty: '${tsvPath}' declares no rows`);
    }
    if (mislabelled > 0) {
      throw new ReferenceSnapshotError(
        databasePath,
        `would mislabel ${mislabelled} of ${total} rows: '${tsvPath}' does not describe ` +
          `'${referenceVersion}'/'${referenceBuild}'`,
      );
    }

    await connection.run(
      `
        CREATE TABLE ${REFERENCE_SNAPSHOT_TABLE} AS
        SELECT $1::VARCHAR AS reference_version,
               $2::VARCHAR AS reference_build,
               $3::BIGINT  AS row_count;
      `,
      [referenceVersion, referenceBuild, total],
    );
  } catch (error) {
    connection.disconnectSync();
    instance.closeSync();
    fs.rmSync(stagingPath, { force: true });
    fs.rmSync(`${stagingPath}.wal`, { force: true });
    throw error;
  }

  connection.disconnectSync();
  instance.closeSync();

  try {
    fs.renameSync(stagingPath, databasePath);
  } catch (error) {
    fs.rmSync(stagingPath, { force: true });
    throw new ReferenceSnapshotError(
      databasePath,
      `could not be published: ${(error as Error).message}`,
    );
  }

  return readSnapshot(databasePath);
}

/**
 * The coordinate snapshot this deployment serves against.
 *
 * The TSV is the demo ClinVar extract that ships with the repository; it is *reference* data,
 * not a fixture standing in for missing user data, and it is the declared source of
 * `REFERENCE_VERSION`. It is derived from the authoritative ClinVar VCF by
 * `scripts/generate_clinvar_reference_tsv.ts`, never edited by hand — see
 * `clinvar-source-records.ts`. Both paths can be overridden so a real deployment can point at a
 * larger snapshot without touching code. The built database lands under `data/reference/`,
 * which is git-ignored: it is derived, not authored.
 */
export function defaultReferenceSnapshotOptions(
  env: NodeJS.ProcessEnv = process.env,
): BuildReferenceDatabaseOptions {
  const repoRoot = fileURLToPath(new URL('../../../../', import.meta.url));
  return {
    tsvPath:
      env.CLINVAR_COORDINATES_TSV ??
      path.join(repoRoot, 'tests/fixtures/clinvar_coordinates_grch38.tsv'),
    databasePath:
      env.CLINVAR_SNAPSHOT_DB ??
      path.join(repoRoot, 'data/reference', `${REFERENCE_VERSION}.duckdb`),
    referenceVersion: REFERENCE_VERSION,
    referenceBuild: REFERENCE_BUILD,
  };
}

/** Opens an already-built snapshot read-only, failing loudly when there is none. */
export async function openReferenceSnapshot(databasePath: string): Promise<ReferenceSnapshot> {
  if (!fs.existsSync(databasePath)) {
    throw new ReferenceSnapshotError(
      databasePath,
      'does not exist; build it from the committed coordinate TSV before serving',
    );
  }
  return readSnapshot(databasePath);
}
