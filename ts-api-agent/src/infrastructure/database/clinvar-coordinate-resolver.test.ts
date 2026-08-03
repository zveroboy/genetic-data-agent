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
  type ClinVarCoordinateResolver,
  normalizeChromosome,
  openClinVarCoordinateResolver,
} from './clinvar-coordinate-resolver.ts';
import { FEATURED_TARGETS } from './clinvar-source-records.ts';
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
    const { targets, coordinatesListed } = await resolver.resolve('CYP1A2', 'GRCh38');

    // One coordinate listed, one read: nothing was capped, so nothing about a cap is reported.
    assert.equal(coordinatesListed, 1);
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

  it('publishes the askable surface of the snapshot, without coordinates', async () => {
    // What question routing is derived from. It must be the snapshot's own content — one entry
    // per row, carrying the text the table carries — because a router built from anything else
    // is the hand-kept second copy that went stale in the first place.
    const vocabulary = await resolver.vocabulary();
    const tsvRows = fs.readFileSync(FIXTURE_TSV, 'utf8').trim().split('\n').slice(1);

    assert.equal(vocabulary.length, tsvRows.length);
    assert.deepEqual(
      vocabulary.find((entry) => entry.rsid === 'rs4244285'),
      {
        gene: 'CYP2C19',
        rsid: 'rs4244285',
        phenotype: 'Clopidogrel response; MEPHENYTOIN, POOR METABOLISM OF; PROGUANIL, POOR METABOLISM OF',
        clinicalSignificance: 'Likely benign / other',
      },
    );
    // No chrom, pos, ref or alt: routing decides *which* target, never where it is. Placing it
    // stays the job of `resolve`, which is what enforces the join against user Parquet.
    for (const entry of vocabulary) {
      assert.deepEqual(Object.keys(entry).sort(), [
        'clinicalSignificance',
        'gene',
        'phenotype',
        'rsid',
      ]);
    }
  });

  it('reads the vocabulary once and serves the same frozen list after', async () => {
    const first = await resolver.vocabulary();
    const second = await resolver.vocabulary();
    assert.equal(first, second, 'the snapshot is read-only; re-reading it per question is waste');
    assert.throws(() => (first as { length: number }).length--, TypeError);
  });

  it('resolves an rsID to the same coordinates, keeping the rsID as provenance', async () => {
    const [byRsid] = (await resolver.resolve('rs4149056', 'GRCh38')).targets;
    const [byGene] = (await resolver.resolve('SLCO1B1', 'GRCh38')).targets;

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

    const [target] = (await resolver.resolve('rs4149056', 'GRCh38')).targets;

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

  it('resolves a multi-variant gene to every declared target, featured markers first', async () => {
    const { targets } = await resolver.resolve('APOE', 'GRCh38');

    // *Every* APOE coordinate the machine-selected table carries, not only the two featured
    // markers. Pinning the exact list here would make this test a second copy of the ClinVar
    // release; pinning the ordering and the two markers the demo advertises is what the resolver
    // actually promises.
    assert.ok(targets.length >= 2, `APOE resolved to ${targets.length} coordinate(s)`);
    for (const target of targets) assert.equal(target.chrom, '19');

    const rsids = targets.map((target) => target.rsid);
    assert.ok(rsids.includes('rs429358'), 'the APOE ε4 marker must resolve');
    assert.ok(rsids.includes('rs7412'), 'the APOE ε2 marker must resolve');
    // Both featured markers ahead of every coordinate nobody curated, whatever the positions are:
    // rs405509 sits 3 kb *earlier* on chr19 than either of them, and under the position ordering
    // this replaces it was the coordinate an APOE answer led with.
    const featured = new Set(FEATURED_TARGETS.map((target) => target.rsid));
    const firstUnfeatured = targets.findIndex((target) => !featured.has(target.rsid ?? ''));
    assert.equal(firstUnfeatured, 2, 'both APOE markers are featured, so they take the first two slots');
    assert.ok(
      targets.slice(firstUnfeatured).every((target) => !featured.has(target.rsid ?? '')),
      'no featured marker may sit behind an uncurated coordinate',
    );

    // Within one tier, ascending position — the tie-break that makes a run reproducible.
    const unfeaturedPositions = targets.slice(firstUnfeatured).map((target) => target.pos);
    assert.deepEqual(
      [...unfeaturedPositions].sort((left, right) => left - right),
      unfeaturedPositions,
    );
  });

  it('matches gene symbols and rsIDs case-insensitively', async () => {
    const upper = await resolver.resolve('RS762551', 'GRCh38');
    const lower = await resolver.resolve('cyp1a2', 'GRCh38');

    assert.equal(upper.targets[0]?.rsid, 'rs762551');
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
    assert.ok(resolved.targets.length > 0);

    await assert.rejects(() => resolver.resolve('NOT_A_GENE', 'GRCh38'), TargetNotResolvableError);
  });

  it('rejects an empty target id without querying the snapshot', async () => {
    await assert.rejects(() => resolver.resolve('   ', 'GRCh38'), TargetNotResolvableError);
  });

  it('ranks a featured coordinate ahead of a pathogenic one, and answers a capped gene', async () => {
    // TP53 is the case the two ranking tiers disagree on, in real ClinVar data. rs1042522 is the
    // featured marker and is classified *Benign*; the other 71 TP53 rows are Pathogenic or Likely
    // pathogenic, so on significance alone the featured marker ranks 72nd — outside the cap
    // entirely, which would make "my TP53 status" unanswerable about the one TP53 variant this
    // system carries lay terms and a literature corpus for.
    const { targets, coordinatesListed } = await resolver.resolve('TP53', 'GRCh38');

    assert.equal(coordinatesListed, 72, 'the count is the snapshot\'s, not the capped list\'s');
    assert.equal(targets.length, MAX_TARGETS_PER_QUERY, 'a gene over the cap answers from the cap');
    assert.equal(targets[0]?.rsid, 'rs1042522');
    assert.equal(targets[0]?.clinicalSignificance, 'Benign');
    assert.equal(targets[1]?.clinicalSignificance, 'Likely pathogenic');
    // …and the rest of the ranking is significance before position: every pathogenic row precedes
    // every row that is neither pathogenic nor a drug response.
    const tiers = targets
      .slice(1)
      .map((target) => (/^(Likely p|P)athogenic/.test(target.clinicalSignificance) ? 0 : 1));
    assert.deepEqual([...tiers].sort(), tiers, `TP53 tiers out of order: ${tiers.join('')}`);
  });

  it('puts the variant a question is actually about at the head of its gene', async () => {
    // The two questions the position ordering got wrong, both confirmed against a live answer:
    // VKORC1 led with rs2359612 and buried rs9923231 — the warfarin dosing variant — fourth, and
    // CYP2C19 led with rs12777823, whose ClinVar condition is *warfarin* dosage, ahead of
    // CYP2C19*2 rs4244285, which is what a clopidogrel question means.
    const warfarin = await resolver.resolve('VKORC1', 'GRCh38');
    assert.equal(warfarin.targets[0]?.rsid, 'rs9923231');
    assert.equal(warfarin.coordinatesListed, warfarin.targets.length, 'VKORC1 is under the cap');

    const clopidogrel = await resolver.resolve('CYP2C19', 'GRCh38');
    assert.equal(clopidogrel.targets[0]?.rsid, 'rs4244285');
  });

  it('is deterministic: the same target yields the same order every time', async () => {
    // Determinism is what makes an answer reproducible across runs and machines, and it is not
    // free: one position can carry two rows (rs4244285 is listed G>A and G>T at 94,781,859), so
    // an `ORDER BY` stopping at `pos` leaves ties for the engine to break as it pleases.
    for (const targetId of ['CYP2C19', 'TP53', 'VKORC1', 'BRCA1']) {
      const first = await resolver.resolve(targetId, 'GRCh38');
      const second = await resolver.resolve(targetId, 'GRCh38');
      assert.deepEqual(second, first, `${targetId} resolved differently on a second call`);
    }

    const cyp2c19 = (await resolver.resolve('CYP2C19', 'GRCh38')).targets;
    const rs4244285 = cyp2c19.filter((target) => target.rsid === 'rs4244285');
    assert.equal(rs4244285.length, 2, 'the two-row position this test exists for must be present');
    assert.deepEqual(
      rs4244285.map((target) => `${target.ref}>${target.alt}`),
      ['G>T', 'G>A'],
      'both are featured and share a position, so the significance tier orders them: ' +
        'drug response before "Likely benign / other"',
    );
  });
});

describe('the coordinate cap is a bound, not a refusal', () => {
  let workDir: string;
  let resolver: ClinVarCoordinateResolver;

  before(async () => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'clinvar-cap-'));

    // Two synthetic genes: one over the cap, one exactly at it. Synthetic rather than borrowed
    // from the demo snapshot so the boundary stays pinned whatever the next ClinVar release does
    // to BRCA2's row count.
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
          'Synthetic row for the coordinate-cap test.',
        ].join('\t'),
      );
    const rows = [
      ...geneRows('MANYVAR', MAX_TARGETS_PER_QUERY + 1, 'chr1'),
      ...geneRows('EXACTLIMIT', MAX_TARGETS_PER_QUERY, 'chr2'),
    ];
    const tsvPath = path.join(workDir, 'capped_coordinates.tsv');
    fs.writeFileSync(tsvPath, [header, ...rows].join('\n') + '\n');

    const snapshot = await buildReferenceDatabase({
      tsvPath,
      databasePath: path.join(workDir, 'capped.duckdb'),
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

  it('answers a gene over the cap from the ranked cap, reporting how many exist', async () => {
    const { targets, coordinatesListed } = await resolver.resolve('MANYVAR', 'GRCh38');

    // The regression this replaces: this call used to throw `TargetResolutionLimitExceeded`, which
    // the HTTP layer mapped to a 422 — so "What about the BRCA1 gene?" was an error rather than an
    // answer. The subset is still bounded; what changed is that it is reported instead of refused.
    assert.equal(targets.length, MAX_TARGETS_PER_QUERY);
    assert.equal(coordinatesListed, MAX_TARGETS_PER_QUERY + 1);
    // Ranked, so which coordinates were dropped is a decision and not an accident: these rows all
    // share a tier, so position orders them and the last one is the one left out.
    assert.equal(targets[0]?.pos, 1_000_000);
    assert.equal(targets.at(-1)?.pos, 1_000_000 + MAX_TARGETS_PER_QUERY - 1);
  });

  it('reports a gene under the cap as fully read, so no answer claims a cap it did not hit', async () => {
    const { targets, coordinatesListed } = await resolver.resolve('EXACTLIMIT', 'GRCh38');

    assert.equal(targets.length, MAX_TARGETS_PER_QUERY);
    assert.equal(
      coordinatesListed,
      MAX_TARGETS_PER_QUERY,
      'a gene at exactly the cap has nothing left over, and must not be described as truncated',
    );
  });
});

describe('an unplaceable row is dropped, not guessed at', () => {
  let workDir: string;
  let resolver: ClinVarCoordinateResolver;

  before(async () => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'clinvar-unplaceable-'));

    // One raw row more than the cap, one of which carries an unplaceable chromosome ('ZZ'
    // normalizes to nothing) and ranks first, so it lands inside the `LIMIT` window and is dropped
    // afterwards. What must hold is that the count the answer speaks is the surviving one: a
    // dropped row may cost a slot, but it may never be reported as a coordinate that was read.
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
        'Synthetic row for the unplaceable-row test.',
      ].join('\t');
    const rows = [
      // Lowest position, so the ranking's third tier puts the unplaceable row first and it is
      // certain to be inside the fetched window.
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

  it('never reports a dropped row as a coordinate that was read', async () => {
    const { targets, coordinatesListed } = await resolver.resolve('ONEUNPLACEABLE', 'GRCh38');

    assert.equal(
      targets.length,
      MAX_TARGETS_PER_QUERY - 1,
      'the unplaceable row occupied a slot in the ranked window and was then dropped',
    );
    assert.equal(coordinatesListed, MAX_TARGETS_PER_QUERY + 1, 'the snapshot still lists them all');
    assert.ok(
      targets.every((target) => target.chrom === '3'),
      'nothing unplaceable may survive into a target',
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
