/**
 * Versioned ClinVar coordinate resolution tests.
 *
 * A gene symbol or rsID is not a genomic location. Before any byte of user Parquet is read,
 * the target has to become `(referenceBuild, chrom, pos, ref, alt)` against a *declared*
 * reference snapshot — otherwise the only way to answer "does this person carry CYP1A2
 * rs762551" would be to scan the whole dataset.
 *
 * These tests run against a real DuckDB reference database built from the committed
 * `tests/fixtures/clinvar_coordinates_grch38.tsv` snapshot, so the normalization the resolver
 * performs (`chr12` to `12`, allele case) is exercised on the same rows the serving path uses.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../domain/datasets.ts';
import {
  MAX_TARGETS_PER_QUERY,
  ReferenceBuildMismatchError,
  TargetNotResolvableError,
  TargetResolutionLimitExceededError,
  type ClinVarCoordinateResolver,
  normalizeChromosome,
  openClinVarCoordinateResolver,
} from './clinvar-coordinate-resolver.ts';
import { ReferenceSnapshotError, buildReferenceDatabase } from './reference-bootstrap.ts';

const FIXTURE_TSV = fileURLToPath(
  new URL('../../../../tests/fixtures/clinvar_coordinates_grch38.tsv', import.meta.url),
);

describe('clinvar coordinate resolver', () => {
  let workDir: string;
  let resolver: ClinVarCoordinateResolver;

  before(async () => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'clinvar-reference-'));
    const snapshot = await buildReferenceDatabase({
      tsvPath: FIXTURE_TSV,
      databasePath: path.join(workDir, 'reference.duckdb'),
      referenceVersion: REFERENCE_VERSION,
      referenceBuild: REFERENCE_BUILD,
    });
    assert.equal(snapshot.referenceVersion, REFERENCE_VERSION);
    assert.equal(snapshot.referenceBuild, 'GRCh38');
    assert.ok(snapshot.rowCount > 0, 'the demo reference snapshot must not be empty');
    resolver = await openClinVarCoordinateResolver({ databasePath: snapshot.path });
  });

  after(async () => {
    await resolver?.close();
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
  });

  it('declares the reference snapshot it was opened against', () => {
    assert.equal(resolver.referenceVersion, REFERENCE_VERSION);
    assert.equal(resolver.referenceBuild, 'GRCh38');
  });

  it('resolves a gene symbol to exact GRCh38 coordinates', async () => {
    const targets = await resolver.resolve('CYP1A2', 'GRCh38');

    // Every field below is ClinVar's, not a curator's: VariationID 511079,
    // NC_000015.10:g.74749576C>A, classified Likely benign with no condition named. The
    // hand-written table this replaced had the alleles the other way round, which the strict
    // `(build, chrom, pos, ref, alt)` join turned into a silent "no clinical variant data".
    assert.deepEqual(targets, [
      {
        referenceBuild: 'GRCh38',
        referenceVersion: REFERENCE_VERSION,
        chrom: '15',
        pos: 74749576,
        ref: 'C',
        alt: 'A',
        rsid: 'rs762551',
        gene: 'CYP1A2',
        phenotype: 'No condition named in ClinVar for this variant',
        clinicalSignificance: 'Likely benign',
        evidenceNote:
          'ClinVar VariationID 511079; review status: criteria provided, multiple submitters, ' +
          'no conflicts; gene CYP1A2; 1000 Genomes allele frequency 0.62979.',
      },
    ]);
  });

  it('resolves an rsID to the same coordinates, keeping the rsID as provenance', async () => {
    const [byRsid] = await resolver.resolve('rs4149056', 'GRCh38');
    const [byGene] = await resolver.resolve('SLCO1B1', 'GRCh38');

    assert.ok(byRsid !== undefined && byGene !== undefined);
    assert.equal(byRsid.chrom, '12');
    assert.equal(byRsid.pos, 21178615);
    assert.equal(byRsid.ref, 'T');
    assert.equal(byRsid.alt, 'C');
    assert.equal(byRsid.rsid, 'rs4149056');
    assert.equal(byRsid.gene, 'SLCO1B1');
    // The canonical match key is the coordinate tuple; the rsID is provenance carried alongside.
    assert.deepEqual(
      [byRsid.referenceBuild, byRsid.chrom, byRsid.pos, byRsid.ref, byRsid.alt],
      [byGene.referenceBuild, byGene.chrom, byGene.pos, byGene.ref, byGene.alt],
    );
  });

  it("normalizes the snapshot's chr-prefixed contigs to the partition value domain", async () => {
    const snapshotRows = fs.readFileSync(FIXTURE_TSV, 'utf8').split('\n').slice(1);
    assert.ok(
      snapshotRows.some((line) => line.split('\t')[2] === 'chr12'),
      'the fixture must actually store the chr-prefixed form this test normalizes away',
    );

    const [target] = await resolver.resolve('rs4149056', 'GRCh38');

    assert.equal(target?.chrom, '12', 'chr12 must resolve to the 12 partition value');
  });

  it('normalizes contig spellings to the canonical chromosome domain', () => {
    assert.equal(normalizeChromosome('chr12'), '12');
    assert.equal(normalizeChromosome('12'), '12');
    assert.equal(normalizeChromosome('CHR12'), '12');
    assert.equal(normalizeChromosome('chrX'), 'X');
    assert.equal(normalizeChromosome('chrx'), 'X');
    assert.equal(normalizeChromosome('chrM'), 'MT');
    assert.equal(normalizeChromosome('MT'), 'MT');
    assert.equal(normalizeChromosome('chr23'), null);
    assert.equal(normalizeChromosome('12; DROP TABLE'), null);
    assert.equal(normalizeChromosome(''), null);
  });

  it('resolves a multi-variant gene to every declared target, in coordinate order', async () => {
    const targets = await resolver.resolve('APOE', 'GRCh38');

    assert.deepEqual(
      targets.map((target) => [target.chrom, target.pos, target.rsid]),
      [
        ['19', 44908684, 'rs429358'],
        ['19', 44908822, 'rs7412'],
      ],
    );
  });

  it('matches gene symbols and rsIDs case-insensitively', async () => {
    const upper = await resolver.resolve('RS762551', 'GRCh38');
    const lower = await resolver.resolve('cyp1a2', 'GRCh38');

    assert.equal(upper[0]?.rsid, 'rs762551');
    assert.deepEqual(lower, upper);
  });

  it('refuses a build the snapshot does not describe instead of guessing', async () => {
    const error = await resolver.resolve('CYP1A2', 'GRCh37').then(
      () => null,
      (thrown: unknown) => thrown as Error,
    );

    assert.ok(error instanceof ReferenceBuildMismatchError, `unexpected error: ${error}`);
    assert.equal(error.name, 'ReferenceBuildMismatch');
    assert.equal(error.requestedBuild, 'GRCh37');
    assert.equal(error.snapshotBuild, 'GRCh38');
  });

  it('reports an unknown target as TargetNotResolvable', async () => {
    for (const unknown of ['NOT_A_GENE', 'rs999999999', "'; DROP TABLE clinvar_coordinates; --"]) {
      const error = await resolver.resolve(unknown, 'GRCh38').then(
        () => null,
        (thrown: unknown) => thrown as Error,
      );

      assert.ok(error instanceof TargetNotResolvableError, `unexpected error for ${unknown}: ${error}`);
      assert.equal(error.name, 'TargetNotResolvable');
      assert.equal(error.targetId, unknown);
      assert.equal(error.referenceVersion, REFERENCE_VERSION);
    }
  });

  it('never answers with an empty target list, so no caller can read one as "scan everything"', async () => {
    // The only two outcomes are "at least one coordinate" and "throw". A resolver that could
    // return `[]` would let a downstream candidate-selection step degrade into a full scan.
    const resolved = await resolver.resolve('CYP1A2', 'GRCh38');
    assert.ok(resolved.length > 0);

    await assert.rejects(() => resolver.resolve('NOT_A_GENE', 'GRCh38'), TargetNotResolvableError);
  });

  it('rejects an empty target id without querying the snapshot', async () => {
    await assert.rejects(() => resolver.resolve('   ', 'GRCh38'), TargetNotResolvableError);
  });
});

describe('target resolution limit', () => {
  let workDir: string;
  let resolver: ClinVarCoordinateResolver;

  before(async () => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'clinvar-limit-'));

    // A synthetic gene with one more declared coordinate than `MAX_TARGETS_PER_QUERY` allows,
    // so the truncation-vs-signal behaviour can be pinned without depending on the demo
    // snapshot ever containing a gene this large.
    const header = [
      'reference_version',
      'reference_build',
      'chrom',
      'pos',
      'rsid',
      'ref',
      'alt',
      'gene',
      'phenotype',
      'clinical_significance',
      'evidence_note',
    ].join('\t');
    const geneRows = (gene: string, count: number, chrom: string) =>
      Array.from({ length: count }, (_unused, index) =>
        [
          REFERENCE_VERSION,
          REFERENCE_BUILD,
          chrom,
          String(1_000_000 + index),
          `rs${gene}${index}`,
          'A',
          'G',
          gene,
          'Synthetic phenotype',
          'Uncertain Significance',
          'Synthetic row for the target-resolution-limit test.',
        ].join('\t'),
      );
    // One gene one row over the cap (must signal), one gene at exactly the cap (must not).
    const rows = [
      ...geneRows('MANYVAR', MAX_TARGETS_PER_QUERY + 1, 'chr1'),
      ...geneRows('EXACTLIMIT', MAX_TARGETS_PER_QUERY, 'chr2'),
    ];
    const tsvPath = path.join(workDir, 'overflow_coordinates.tsv');
    fs.writeFileSync(tsvPath, [header, ...rows].join('\n') + '\n');

    const snapshot = await buildReferenceDatabase({
      tsvPath,
      databasePath: path.join(workDir, 'overflow.duckdb'),
      referenceVersion: REFERENCE_VERSION,
      referenceBuild: REFERENCE_BUILD,
    });
    assert.equal(snapshot.rowCount, 2 * MAX_TARGETS_PER_QUERY + 1);
    resolver = await openClinVarCoordinateResolver({ databasePath: snapshot.path });
  });

  after(async () => {
    await resolver?.close();
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
  });

  it('signals overflow instead of silently truncating to the limit', async () => {
    const error = await resolver.resolve('MANYVAR', 'GRCh38').then(
      () => null,
      (thrown: unknown) => thrown as Error,
    );

    assert.ok(error instanceof TargetResolutionLimitExceededError, `unexpected error: ${error}`);
    assert.equal(error.name, 'TargetResolutionLimitExceeded');
    assert.equal(error.targetId, 'MANYVAR');
    assert.equal(error.limit, MAX_TARGETS_PER_QUERY);
    assert.equal(error.referenceVersion, REFERENCE_VERSION);
  });

  it('still resolves a gene at exactly the limit, without signalling overflow', async () => {
    const targets = await resolver.resolve('EXACTLIMIT', 'GRCh38');
    assert.equal(targets.length, MAX_TARGETS_PER_QUERY, 'a gene at exactly the cap must answer in full');
  });
});

describe('target resolution limit is counted after normalisation', () => {
  let workDir: string;
  let resolver: ClinVarCoordinateResolver;

  before(async () => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'clinvar-limit-normalised-'));

    // One raw row more than the cap, but one of those raw rows carries an unplaceable
    // chromosome ('ZZ' does not normalize to anything). The overflow check must be counted
    // after that row is dropped: this gene resolves to exactly MAX_TARGETS_PER_QUERY targets and
    // must answer in full, not be rejected on the pre-filter raw row count.
    const header = [
      'reference_version',
      'reference_build',
      'chrom',
      'pos',
      'rsid',
      'ref',
      'alt',
      'gene',
      'phenotype',
      'clinical_significance',
      'evidence_note',
    ].join('\t');
    const row = (chrom: string, index: number) =>
      [
        REFERENCE_VERSION,
        REFERENCE_BUILD,
        chrom,
        String(1_000_000 + index),
        `rsONEUNPLACEABLE${index}`,
        'A',
        'G',
        'ONEUNPLACEABLE',
        'Synthetic phenotype',
        'Uncertain Significance',
        'Synthetic row for the post-normalisation overflow-count test.',
      ].join('\t');
    const rows = [
      // The unplaceable row sorts first ('ZZ' precedes 'chr3' lexically), so it is guaranteed to
      // be within the fetched LIMIT window regardless of fetch order.
      row('ZZ', 0),
      ...Array.from({ length: MAX_TARGETS_PER_QUERY }, (_unused, index) => row('chr3', index + 1)),
    ];
    const tsvPath = path.join(workDir, 'overflow_normalised.tsv');
    fs.writeFileSync(tsvPath, [header, ...rows].join('\n') + '\n');

    const snapshot = await buildReferenceDatabase({
      tsvPath,
      databasePath: path.join(workDir, 'overflow_normalised.duckdb'),
      referenceVersion: REFERENCE_VERSION,
      referenceBuild: REFERENCE_BUILD,
    });
    assert.equal(snapshot.rowCount, MAX_TARGETS_PER_QUERY + 1);
    resolver = await openClinVarCoordinateResolver({ databasePath: snapshot.path });
  });

  after(async () => {
    await resolver?.close();
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
  });

  it('does not signal overflow when one raw row over the cap is unplaceable', async () => {
    const targets = await resolver.resolve('ONEUNPLACEABLE', 'GRCh38');
    assert.equal(
      targets.length,
      MAX_TARGETS_PER_QUERY,
      'one unplaceable raw row must not count against the cap',
    );
  });
});

describe('reference snapshot bootstrap', () => {
  let workDir: string;

  before(() => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'clinvar-bootstrap-'));
  });

  after(() => {
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
  });

  it('is idempotent: a second build reuses the existing snapshot', async () => {
    const databasePath = path.join(workDir, 'idempotent.duckdb');
    const first = await buildReferenceDatabase({
      tsvPath: FIXTURE_TSV,
      databasePath,
      referenceVersion: REFERENCE_VERSION,
      referenceBuild: REFERENCE_BUILD,
    });
    const before = fs.statSync(databasePath).mtimeMs;

    const second = await buildReferenceDatabase({
      tsvPath: FIXTURE_TSV,
      databasePath,
      referenceVersion: REFERENCE_VERSION,
      referenceBuild: REFERENCE_BUILD,
    });

    assert.deepEqual(second, first);
    assert.equal(fs.statSync(databasePath).mtimeMs, before, 'an existing snapshot must not be rewritten');
  });

  it('refuses to build a snapshot whose rows contradict the declared version', async () => {
    await assert.rejects(
      () =>
        buildReferenceDatabase({
          tsvPath: FIXTURE_TSV,
          databasePath: path.join(workDir, 'mislabelled.duckdb'),
          referenceVersion: 'some-other-snapshot-v9',
          referenceBuild: REFERENCE_BUILD,
        }),
      ReferenceSnapshotError,
    );
    assert.ok(
      !fs.existsSync(path.join(workDir, 'mislabelled.duckdb')),
      'a failed build must leave no partial snapshot behind',
    );
  });

  it('refuses to open a snapshot that does not exist', async () => {
    await assert.rejects(
      () => openClinVarCoordinateResolver({ databasePath: path.join(workDir, 'absent.duckdb') }),
      ReferenceSnapshotError,
    );
  });
});
