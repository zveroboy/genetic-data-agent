/**
 * The committed coordinate table must be exactly what ClinVar says — checked on every run.
 *
 * The defect this closes: `clinvar_coordinates_grch38.tsv` was hand-authored, several rows named
 * the wrong allele pair or a GRCh37 position, and nothing in the repository ever compared it
 * against the source. Because the serving join is strict on `(build, chrom, pos, ref, alt)`,
 * every one of those rows failed *silently* as "no clinical variant data found".
 *
 * ## Two halves, checked two ways
 *
 * The table is a union (see `deriveCoordinateTable`): ~14,000 machine-selected coordinates, plus
 * the featured rows unconditionally.
 *
 * - **The featured half is checked against the source, verbatim.** `data/clinvar.vcf.gz` is a
 *   193 MB git-ignored download, so the generator also commits
 *   `tests/fixtures/clinvar_source_records.vcf` — every ClinVar record carrying a featured rsID,
 *   copied verbatim, rejected candidates included. Re-deriving from it must reproduce the featured
 *   rows exactly, so a hand-edit of either file fails here, offline, in milliseconds.
 * - **The machine half is checked by its rule.** Committing 14,000 source records a second time
 *   would double the repository for no new information, so the selection is pinned against
 *   hand-built records that state each rule as a case — including the two traps: a
 *   `Conflicting_classifications_of_pathogenicity` record must not be read as pathogenic, and a
 *   featured row must survive whatever ClinVar classifies it as.
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
  type ClinVarRecord,
  type CoordinateUniverse,
  FEATURED_TARGETS,
  collectCoordinateUniverse,
  deriveCoordinateTable,
  deriveTable,
  isDrugResponse,
  isPathogenicExpertReviewed,
  parseClinVarLine,
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
  it('re-derives the featured rows byte-for-byte from the committed ClinVar source records', () => {
    const { rows, dropped } = deriveTable(
      sourceRecords,
      FEATURED_TARGETS,
      REFERENCE_VERSION,
      REFERENCE_BUILD,
    );

    assert.deepEqual(
      dropped,
      [],
      'every featured target must resolve; a target the source cannot place belongs out of ' +
        'FEATURED_TARGETS, not in the table with a guessed coordinate',
    );

    // Compared line by line against the committed table rather than as a whole file: the featured
    // rows are 14 of ~14,000, and the property is that each one appears *verbatim*, cells and all.
    const committedLines = new Set(committedTsv.trimEnd().split('\n'));
    const rendered = renderCoordinateTsv(rows).trimEnd().split('\n').slice(1);
    for (const line of rendered) {
      assert.ok(
        committedLines.has(line),
        'a featured row is missing from or differs in tests/fixtures/' +
          `clinvar_coordinates_grch38.tsv; regenerate it with ` +
          `\`node scripts/generate_clinvar_reference_tsv.ts\`:\n  ${line}`,
      );
    }
    assert.equal(rendered.length, FEATURED_TARGETS.length);
  });

  it('keeps every featured target answerable in the machine-selected table', () => {
    // The trap this pins. The machine rule is pathogenicity + expert review plus every
    // `drug_response` record, and most of what the demo answers about is neither: CYP1A2 rs762551
    // is `Likely_benign` and LCT rs4988235 is `association`. A straight replacement would have
    // deleted them, and the failure would have looked like "no clinical variant data found".
    for (const target of FEATURED_TARGETS) {
      const rows = committedRows.filter((row) => row.rsid === target.rsid);
      assert.ok(rows.length > 0, `featured target ${target.rsid} (${target.gene}) has no row`);
      assert.ok(
        rows.some((row) => row.gene === target.gene),
        `${target.rsid} is in the table but not under '${target.gene}', the symbol questions ` +
          'about it resolve',
      );
    }

    // …and the featured row wins its coordinate: rs4988235 is queried as LCT, while ClinVar's
    // GENEINFO for that record says MCM6. A machine row at the same coordinate would spell it
    // MCM6 and the lactose question would stop resolving.
    const lactase = committedRows.filter((row) => row.rsid === 'rs4988235');
    assert.deepEqual(lactase.map((row) => row.gene), ['LCT']);
    assert.match(lactase[0]!.evidence_note!, /ClinVar gene MCM6/);
  });

  it('places every row exactly where the ClinVar record for its rsID places it', () => {
    // Scoped to the featured rsIDs: those are the records the committed extract carries. A
    // coordinate row and the record it came from must agree on all four join fields.
    for (const target of FEATURED_TARGETS) {
      const candidates = sourceRecords.get(target.rsid);
      assert.ok(candidates && candidates.length > 0, `no ClinVar record for ${target.rsid}`);
      const record = selectCanonicalRecord(candidates);
      assert.ok(record, `no usable ClinVar record for ${target.rsid}`);

      const row = committedRows.find(
        (candidate) => candidate.rsid === target.rsid && candidate.gene === target.gene,
      );
      assert.ok(row, `${target.rsid} has no row under ${target.gene}`);
      assert.equal(row.chrom, `chr${record.chrom}`, `${target.rsid} chromosome`);
      assert.equal(row.pos, String(record.pos), `${target.rsid} position`);
      assert.equal(row.ref, record.ref, `${target.rsid} reference allele`);
      assert.equal(row.alt, record.alt, `${target.rsid} alternate allele`);
    }
  });

  it('labels every row with the snapshot version and build the deployment serves', () => {
    for (const row of committedRows) {
      assert.equal(row.reference_version, REFERENCE_VERSION, `${row.rsid} reference_version`);
      assert.equal(row.reference_build, REFERENCE_BUILD, `${row.rsid} reference_build`);
    }
  });

  it('is a machine-selected table, in coordinate order, with no duplicate coordinate', () => {
    // Size is asserted as a range, not a number: the exact count is whichever ClinVar release the
    // table was built from, and pinning it would fail on every refresh for no defect. What must
    // hold is that this is the big table and not the 14-row one somebody regenerated by accident.
    assert.ok(
      committedRows.length > 10_000 && committedRows.length < 20_000,
      `the committed table has ${committedRows.length} rows; that is neither the machine-selected ` +
        'universe nor a deliberate change to the selection rule',
    );

    const ordered = [...committedRows].sort(
      (left, right) =>
        left.chrom!.localeCompare(right.chrom!, 'en') ||
        Number(left.pos) - Number(right.pos) ||
        left.ref!.localeCompare(right.ref!, 'en') ||
        left.alt!.localeCompare(right.alt!, 'en'),
    );
    assert.deepEqual(
      committedRows.map((row) => `${row.chrom}:${row.pos}:${row.ref}:${row.alt}`),
      ordered.map((row) => `${row.chrom}:${row.pos}:${row.ref}:${row.alt}`),
      'the file must be in the coordinate order the resolver and the manifest inventory use',
    );

    const coordinates = committedRows.map(
      (row) => `${row.chrom}:${row.pos}:${row.ref}:${row.alt}`,
    );
    assert.equal(new Set(coordinates).size, coordinates.length);

    // Every row carries an rsID and a gene symbol, because both are selection rules and both are
    // handles the resolver matches a question against.
    for (const row of committedRows) {
      assert.match(row.rsid!, /^rs\d+$/, `'${row.rsid}' is not an rsID`);
      assert.ok(row.gene!.length > 0, `${row.rsid} has no gene symbol`);
      assert.match(row.chrom!, /^chr([1-9]|1\d|2[0-2]|X|Y|MT)$/, `${row.rsid} contig`);
      assert.match(row.ref!, /^[ACGTN]+$/, `${row.rsid} REF`);
      assert.match(row.alt!, /^[ACGTN]+$/, `${row.rsid} ALT`);
    }
  });

  it('carries no row for an rsID ClinVar does not publish', () => {
    // rs2187668 (HLA-DQA1, celiac susceptibility) was in the hand-written table. ClinVar carries
    // no record for it at all, so it cannot be derived and is not declared. The UI's celiac
    // question therefore has no reference row — a visible gap, not a silent wrong answer.
    assert.ok(!committedTsv.includes('rs2187668'));
    assert.ok(!FEATURED_TARGETS.some((target) => target.rsid === 'rs2187668'));
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

/**
 * The union rule, stated as cases over hand-built records.
 *
 * Written as VCF lines rather than object literals because that is what the rule reads: every
 * decision below is made from `CLNSIG`, `CLNREVSTAT`, `RS`, `GENEINFO` and the contig, and a test
 * that constructed parsed records would be free to build a record ClinVar cannot emit.
 */
describe('the coordinate universe is exactly the three declared sets', () => {
  /** One synthetic ClinVar line. `info` is appended verbatim, so a case can omit a field. */
  function line(
    chrom: string,
    pos: number,
    variationId: number,
    ref: string,
    alt: string,
    info: string,
  ): string {
    return [chrom, String(pos), String(variationId), ref, alt, '.', '.', info].join('\t');
  }

  const PATHOGENIC_EXPERT = line(
    '1',
    1000,
    9001,
    'G',
    'A',
    'CLNDN=Long_QT_syndrome;CLNREVSTAT=reviewed_by_expert_panel;CLNSIG=Pathogenic;GENEINFO=KCNQ1:3784;RS=9000001',
  );
  const LIKELY_PATHOGENIC_GUIDELINE = line(
    '2',
    2000,
    9002,
    'C',
    'T',
    'CLNDN=Cystic_fibrosis;CLNREVSTAT=practice_guideline;CLNSIG=Likely_pathogenic;GENEINFO=CFTR:1080;RS=9000002',
  );
  const DRUG_RESPONSE_SINGLE_SUBMITTER = line(
    '3',
    3000,
    9003,
    'A',
    'G',
    'CLNDN=Warfarin_response;CLNREVSTAT=criteria_provided,_single_submitter;CLNSIG=drug_response;GENEINFO=CYP2C9:1559;RS=9000003',
  );
  const DRUG_RESPONSE_AMONG_OTHERS = line(
    '4',
    4000,
    9004,
    'A',
    'G',
    'CLNDN=Tamoxifen_response;CLNREVSTAT=criteria_provided,_single_submitter;CLNSIG=Likely_benign|drug_response|other;GENEINFO=NAT2:10;RS=9000004',
  );
  const CONFLICTING = line(
    '5',
    5000,
    9005,
    'A',
    'G',
    'CLNDN=not_provided;CLNREVSTAT=reviewed_by_expert_panel;CLNSIG=Conflicting_classifications_of_pathogenicity;GENEINFO=BRCA1:672;RS=9000005',
  );
  const PATHOGENIC_SINGLE_SUBMITTER = line(
    '6',
    6000,
    9006,
    'A',
    'G',
    'CLNDN=not_provided;CLNREVSTAT=criteria_provided,_single_submitter;CLNSIG=Pathogenic;GENEINFO=TTN:7273;RS=9000006',
  );
  const BENIGN_EXPERT = line(
    '7',
    7000,
    9007,
    'A',
    'G',
    'CLNDN=not_provided;CLNREVSTAT=reviewed_by_expert_panel;CLNSIG=Benign;GENEINFO=MYH7:4625;RS=9000007',
  );
  const PATHOGENIC_NO_RSID = line(
    '8',
    8000,
    9008,
    'A',
    'G',
    'CLNDN=not_provided;CLNREVSTAT=reviewed_by_expert_panel;CLNSIG=Pathogenic;GENEINFO=PKD1:5310',
  );
  const PATHOGENIC_SCAFFOLD = line(
    'NW_009646201.1',
    9000,
    9009,
    'A',
    'G',
    'CLNDN=not_provided;CLNREVSTAT=reviewed_by_expert_panel;CLNSIG=Pathogenic;GENEINFO=NBPF1:55672;RS=9000009',
  );
  const PATHOGENIC_IDENTITY = line(
    '10',
    10_000,
    9010,
    'A',
    '.',
    'CLNDN=not_provided;CLNREVSTAT=reviewed_by_expert_panel;CLNSIG=Pathogenic;GENEINFO=RYR1:6261;RS=9000010',
  );
  const PATHOGENIC_NO_GENE = line(
    '11',
    11_000,
    9011,
    'A',
    'G',
    'CLNDN=not_provided;CLNREVSTAT=reviewed_by_expert_panel;CLNSIG=Pathogenic;RS=9000011',
  );
  const SAME_COORDINATE_LATER_ACCESSION = line(
    '1',
    1000,
    9999,
    'G',
    'A',
    'CLNDN=Long_QT_syndrome;CLNREVSTAT=reviewed_by_expert_panel;CLNSIG=Pathogenic;GENEINFO=KCNQ1:3784;RS=9000001',
  );

  const ALL_LINES = [
    PATHOGENIC_EXPERT,
    LIKELY_PATHOGENIC_GUIDELINE,
    DRUG_RESPONSE_SINGLE_SUBMITTER,
    DRUG_RESPONSE_AMONG_OTHERS,
    CONFLICTING,
    PATHOGENIC_SINGLE_SUBMITTER,
    BENIGN_EXPERT,
    PATHOGENIC_NO_RSID,
    PATHOGENIC_SCAFFOLD,
    PATHOGENIC_IDENTITY,
    PATHOGENIC_NO_GENE,
    SAME_COORDINATE_LATER_ACCESSION,
  ];

  async function collect(lines: readonly string[]): Promise<CoordinateUniverse> {
    async function* stream(): AsyncGenerator<string> {
      yield '##fileformat=VCFv4.1';
      for (const one of lines) yield one;
    }
    return collectCoordinateUniverse(stream(), []);
  }

  function parse(one: string): ClinVarRecord {
    return parseClinVarLine(one)!;
  }

  it('admits the two sets and nothing else', async () => {
    const universe = await collect(ALL_LINES);
    assert.deepEqual(
      universe.selectedRecords.map((record) => record.variationId).sort((a, b) => a - b),
      [9001, 9002, 9003, 9004],
    );
    // The set counts are *records*, before duplicate coordinates collapse — which is why 3
    // pathogenic records produce 2 pathogenic rows here. Counting coordinates instead would make
    // the reported breakdown unable to explain the difference between the two.
    assert.equal(universe.stats.pathogenicExpertReviewed, 3);
    assert.equal(universe.stats.drugResponse, 2);
  });

  it('does not read a conflicting classification as pathogenic', () => {
    // `Conflicting_classifications_of_pathogenicity` contains a lower-case `pathogenicity`. A
    // case-insensitive match would pull every conflicting record — thousands of them — into a
    // table this system presents as expert-reviewed pathogenic findings.
    assert.equal(isPathogenicExpertReviewed(parse(CONFLICTING)), false);
    assert.equal(isPathogenicExpertReviewed(parse(PATHOGENIC_EXPERT)), true);
    assert.equal(isPathogenicExpertReviewed(parse(LIKELY_PATHOGENIC_GUIDELINE)), true);
  });

  it('requires a panel or a guideline, not a lone submitter', () => {
    assert.equal(isPathogenicExpertReviewed(parse(PATHOGENIC_SINGLE_SUBMITTER)), false);
    assert.equal(isPathogenicExpertReviewed(parse(BENIGN_EXPERT)), false);
  });

  it('takes a drug-response record at any review level, and among other classifications', () => {
    // This is the set the demo is about: pharmacogenomic records are mostly neither pathogenic nor
    // expert-reviewed, so set 1 alone would answer clinical risk and nothing about any drug.
    assert.equal(isDrugResponse(parse(DRUG_RESPONSE_SINGLE_SUBMITTER)), true);
    assert.equal(isDrugResponse(parse(DRUG_RESPONSE_AMONG_OTHERS)), true);
    assert.equal(isDrugResponse(parse(BENIGN_EXPERT)), false);
  });

  it('skips what it cannot place, counting each reason', async () => {
    const universe = await collect(ALL_LINES);
    const { stats } = universe;
    assert.equal(stats.skippedNonCanonicalContig, 1, 'the scaffold record');
    assert.equal(stats.skippedEmptyAllele, 1, "the ALT '.' identity record");
    assert.equal(stats.skippedNoGeneSymbol, 1, 'the record with no GENEINFO');
    assert.ok(stats.withoutRsid >= 1, 'the record with no RS=');
  });

  it('collapses two records at one coordinate onto the earliest accession', async () => {
    const universe = await collect(ALL_LINES);
    assert.equal(universe.stats.duplicateCoordinates, 1);
    const kcnq1 = universe.selectedRecords.filter((record) => record.pos === 1000);
    assert.deepEqual(kcnq1.map((record) => record.variationId), [9001]);
  });

  it('adds the featured rows unconditionally, and lets them win their coordinate', async () => {
    // The rule that keeps the product working: a featured target is in the table whatever ClinVar
    // says about it. `BENIGN_EXPERT` is `Benign` — the machine rule rejects it — and it is here
    // because a target declared it.
    const featured = [{ rsid: 'rs9000007', gene: 'MYH7-AS-FEATURED' }];
    async function* stream(): AsyncGenerator<string> {
      for (const one of ALL_LINES) yield one;
    }
    const universe = await collectCoordinateUniverse(stream(), featured);
    const table = deriveCoordinateTable(universe, featured, REFERENCE_VERSION, REFERENCE_BUILD);

    assert.deepEqual(table.dropped, []);
    assert.equal(table.featuredRowCount, 1);
    assert.equal(table.selectedRowCount, 4);
    const benign = table.rows.filter((row) => row.rsid === 'rs9000007');
    assert.deepEqual(
      benign.map((row) => [row.gene, row.clinical_significance]),
      [['MYH7-AS-FEATURED', 'Benign']],
      'the featured row must be present, under the gene symbol the target declares',
    );

    // …and when the same coordinate is also machine-selected, the featured row is the one kept:
    // it is the only one that can carry a declared gene symbol or a geneNote.
    const alsoSelected = [{ rsid: 'rs9000001', gene: 'KCNQ1-AS-FEATURED' }];
    async function* again(): AsyncGenerator<string> {
      for (const one of ALL_LINES) yield one;
    }
    const second = deriveCoordinateTable(
      await collectCoordinateUniverse(again(), alsoSelected),
      alsoSelected,
      REFERENCE_VERSION,
      REFERENCE_BUILD,
    );
    assert.equal(second.supersededByFeatured, 1);
    assert.deepEqual(
      second.rows.filter((row) => row.pos === 1000).map((row) => row.gene),
      ['KCNQ1-AS-FEATURED'],
    );
  });

  it('reports a featured target the source cannot place instead of dropping it silently', async () => {
    const featured = [{ rsid: 'rs404404', gene: 'NOWHERE' }];
    async function* stream(): AsyncGenerator<string> {
      for (const one of ALL_LINES) yield one;
    }
    const table = deriveCoordinateTable(
      await collectCoordinateUniverse(stream(), featured),
      featured,
      REFERENCE_VERSION,
      REFERENCE_BUILD,
    );
    assert.deepEqual(table.dropped, [
      { rsid: 'rs404404', reason: 'no ClinVar record carries this rsID' },
    ]);
  });
});
