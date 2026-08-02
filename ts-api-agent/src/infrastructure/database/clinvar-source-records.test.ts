/**
 * The committed coordinate table must be exactly what ClinVar says — checked on every run.
 *
 * The defect this closes: `clinvar_coordinates_grch38.tsv` was hand-authored, several rows named
 * the wrong allele pair or a GRCh37 position, and nothing in the repository ever compared it
 * against the source. Because the serving join is strict on `(build, chrom, pos, ref, alt)`,
 * every one of those rows failed *silently* as "no clinical variant data found".
 *
 * ## Why this runs in the unit suite
 *
 * The authoritative source, `data/clinvar.vcf.gz`, is a 193 MB git-ignored download; a test that
 * needs it cannot run in `npm test` on a fresh clone. So the generator also commits
 * `tests/fixtures/clinvar_source_records.vcf` — the ~20 ClinVar records carrying the rsIDs the
 * table declares, copied **verbatim**, rejected candidates included. This test re-derives the
 * whole table from those records and asserts it byte-equals the committed TSV, so a hand-edit of
 * either file fails here, offline, in milliseconds.
 *
 * The extract's own fidelity to the full download is not assumed: it is re-derived from
 * `data/clinvar.vcf.gz` and compared byte-for-byte by
 * `tests/integration/clinvar_reference_source.test.ts`, which announces itself loudly rather
 * than skipping quietly when the download is absent.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, it } from 'node:test';

import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../domain/datasets.ts';
import {
  COORDINATE_TSV_COLUMNS,
  REFERENCE_TARGETS,
  deriveTable,
  parseSourceExtract,
  renderCoordinateTsv,
  renderSourceExtract,
  rsidOf,
  selectCanonicalRecord,
} from './clinvar-source-records.ts';

const repoFile = (relative: string): string =>
  fileURLToPath(new URL(`../../../../${relative}`, import.meta.url));

const COORDINATES_TSV = repoFile('tests/fixtures/clinvar_coordinates_grch38.tsv');
const SOURCE_EXTRACT = repoFile('tests/fixtures/clinvar_source_records.vcf');

const committedTsv = fs.readFileSync(COORDINATES_TSV, 'utf8');
const committedExtract = fs.readFileSync(SOURCE_EXTRACT, 'utf8');
const sourceRecords = parseSourceExtract(committedExtract);

interface TsvRow {
  readonly [column: string]: string;
}

function readCommittedRows(): TsvRow[] {
  const [header, ...body] = committedTsv.trimEnd().split('\n');
  assert.deepEqual(header!.split('\t'), [...COORDINATE_TSV_COLUMNS]);
  return body.map((line) => {
    const cells = line.split('\t');
    assert.equal(cells.length, COORDINATE_TSV_COLUMNS.length, `malformed row: ${line}`);
    return Object.fromEntries(COORDINATE_TSV_COLUMNS.map((column, index) => [column, cells[index]!]));
  });
}

const committedRows = readCommittedRows();

describe('the committed coordinate table is derived from ClinVar, not asserted', () => {
  it('re-derives byte-for-byte from the committed ClinVar source records', () => {
    const { rows, dropped } = deriveTable(
      sourceRecords,
      REFERENCE_TARGETS,
      REFERENCE_VERSION,
      REFERENCE_BUILD,
    );

    assert.deepEqual(
      dropped,
      [],
      'every declared target must resolve; a target the source cannot place belongs out of ' +
        'REFERENCE_TARGETS, not in the table with a guessed coordinate',
    );
    assert.equal(
      renderCoordinateTsv(rows),
      committedTsv,
      'tests/fixtures/clinvar_coordinates_grch38.tsv is stale or was hand-edited; regenerate it ' +
        'with `node scripts/generate_clinvar_reference_tsv.ts`',
    );
  });

  it('places every row exactly where the ClinVar record for its rsID places it', () => {
    assert.equal(committedRows.length, REFERENCE_TARGETS.length);

    for (const row of committedRows) {
      const candidates = sourceRecords.get(row.rsid!);
      assert.ok(candidates && candidates.length > 0, `no ClinVar record for ${row.rsid}`);
      const record = selectCanonicalRecord(candidates);
      assert.ok(record, `no usable ClinVar record for ${row.rsid}`);

      assert.equal(row.chrom, `chr${record.chrom}`, `${row.rsid} chromosome`);
      assert.equal(row.pos, String(record.pos), `${row.rsid} position`);
      assert.equal(row.ref, record.ref, `${row.rsid} reference allele`);
      assert.equal(row.alt, record.alt, `${row.rsid} alternate allele`);
    }
  });

  it('labels every row with the snapshot version and build the deployment serves', () => {
    for (const row of committedRows) {
      assert.equal(row.reference_version, REFERENCE_VERSION, `${row.rsid} reference_version`);
      assert.equal(row.reference_build, REFERENCE_BUILD, `${row.rsid} reference_build`);
    }
  });

  it('covers exactly the declared targets, with no duplicate coordinate', () => {
    assert.deepEqual(
      committedRows.map((row) => row.rsid),
      // Coordinate order, the same ordering the resolver and the manifest inventory use.
      [...REFERENCE_TARGETS]
        .map((target) => target.rsid)
        .sort((left, right) => {
          const find = (rsid: string): TsvRow =>
            committedRows.find((row) => row.rsid === rsid)!;
          const a = find(left);
          const b = find(right);
          return (
            a.chrom!.localeCompare(b.chrom!, 'en') ||
            Number(a.pos) - Number(b.pos) ||
            a.ref!.localeCompare(b.ref!, 'en') ||
            a.alt!.localeCompare(b.alt!, 'en')
          );
        }),
    );

    const coordinates = committedRows.map(
      (row) => `${row.chrom}:${row.pos}:${row.ref}:${row.alt}`,
    );
    assert.equal(new Set(coordinates).size, coordinates.length);
  });

  it('carries no row for an rsID ClinVar does not publish', () => {
    // rs2187668 (HLA-DQA1, celiac susceptibility) was in the hand-written table. ClinVar carries
    // no record for it at all, so it cannot be derived and is not declared. The UI's celiac
    // question therefore has no reference row — a visible gap, not a silent wrong answer.
    assert.ok(!committedTsv.includes('rs2187668'));
    assert.ok(!REFERENCE_TARGETS.some((target) => target.rsid === 'rs2187668'));
  });
});

describe('the ClinVar record a target resolves to is chosen deterministically', () => {
  it('never resolves to a reference-identity record', () => {
    // ClinVar publishes ALT='.' records to carry an assertion about the reference allele. They
    // describe no alteration, so they can never match a user genotype; picking one would look
    // like a resolvable target that always answers "not present".
    const identityRecords = [...sourceRecords.values()]
      .flat()
      .filter((record) => record.alt === '.');
    assert.ok(
      identityRecords.length > 0,
      'the extract must retain rejected candidates, or this rule is untested',
    );
    for (const record of identityRecords) {
      const rsid = rsidOf(record)!;
      const chosen = selectCanonicalRecord(sourceRecords.get(rsid)!)!;
      assert.notEqual(chosen.variationId, record.variationId);
    }
  });

  it('prefers the lowest Variation ID when an rsID has several records', () => {
    const contested = [...sourceRecords.entries()].filter(
      ([, records]) => records.filter((record) => record.alt !== '.').length > 1,
    );
    assert.ok(contested.length > 0, 'the extract must retain a contested rsID, or this is untested');

    for (const [rsid, records] of contested) {
      const chosen = selectCanonicalRecord(records)!;
      const lowest = Math.min(
        ...records.filter((record) => record.alt !== '.').map((record) => record.variationId),
      );
      assert.equal(chosen.variationId, lowest, `${rsid} did not resolve to its earliest record`);
    }
  });

  it('re-renders the committed extract from its own parse, so the file is canonical', () => {
    assert.equal(
      renderSourceExtract(sourceRecords),
      committedExtract,
      'tests/fixtures/clinvar_source_records.vcf is not in the order the generator writes',
    );
  });
});
