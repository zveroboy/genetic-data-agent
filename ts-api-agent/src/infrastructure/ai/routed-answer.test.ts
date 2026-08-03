/**
 * Pins what a multi-target answer says and what it claims to have read.
 *
 * The query function is injected, so nothing here touches DuckDB, S3 or the resolver: what is under
 * test is composition — which targets are read, in what order, and whether the provenance that
 * comes back describes exactly the reads that happened. `agent.test.ts` covers the same behaviour
 * end to end through the real router and the real tool.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { GenotypeProvenance } from '../database/duckdb.ts';
import {
  MAX_QUERIED_TARGETS,
  type TargetOutcome,
  composeRoutedAnswer,
} from './routed-answer.ts';

const PROVENANCE: GenotypeProvenance = {
  datasetId: 'ds-serving-001',
  datasetChecksumSha256: 'a'.repeat(64),
  referenceBuild: 'GRCh38',
  referenceVersion: 'demo-clinvar-grch38-v3',
  filesScanned: [],
  targetsResolved: 0,
};

/** A target that was found, read out of one named file. */
function found(targetId: string, genotype: string, file: string): TargetOutcome {
  return {
    evidence: [
      {
        rsid: targetId,
        gene: 'APOE',
        userGenotype: genotype,
        phenotype: 'Alzheimer disease',
        clinicalSignificance: 'risk factor',
        evidenceNote: 'n/a',
      },
    ],
    provenance: { ...PROVENANCE, filesScanned: [file], targetsResolved: 1 },
  };
}

/** A target with nothing to report and its own reason, the way `queryGenotype` returns one. */
function absent(note: string): TargetOutcome {
  return { evidence: [], note };
}

/** Records the order targets were queried in, answering each from a table. */
function fromTable(outcomes: Readonly<Record<string, TargetOutcome>>): {
  readonly queried: string[];
  readonly query: (targetId: string) => Promise<TargetOutcome>;
} {
  const queried: string[] = [];
  return {
    queried,
    async query(targetId: string) {
      queried.push(targetId);
      return outcomes[targetId] ?? { evidence: [] };
    },
  };
}

describe('composeRoutedAnswer — every routed target is reported', () => {
  it('reports both markers of one gene, in the order they were routed', async () => {
    const { queried, query } = fromTable({
      rs429358: found('rs429358', 'T/C', 's3://bucket/chrom=19/part-000.parquet'),
      rs7412: found('rs7412', 'C/C', 's3://bucket/chrom=19/part-000.parquet'),
    });

    const result = await composeRoutedAnswer(['rs429358', 'rs7412'], query);

    assert.deepEqual(queried, ['rs429358', 'rs7412']);
    assert.equal(result.evidence.length, 2, 'both genotypes reach the evidence payload');
    // Both genotypes are in the prose, and rs429358's comes first — a reader must be able to line
    // the answer up against the question they wrote.
    assert.match(result.answer, /T\/C for rsID rs429358/);
    assert.match(result.answer, /C\/C for rsID rs7412/);
    assert.ok(
      result.answer.indexOf('rs429358') < result.answer.indexOf('rs7412'),
      'the answer keeps the routed order',
    );
  });

  it("carries each target's own absence reason next to the genotype that was found", async () => {
    const { query } = fromTable({
      CYP2C19: found('rs4244285', '*1/*2', 's3://bucket/chrom=10/part-000.parquet'),
      VKORC1: absent("'VKORC1' is not present in reference snapshot 'demo-clinvar-grch38-v3'."),
    });

    const result = await composeRoutedAnswer(['CYP2C19', 'VKORC1'], query);

    // The half that failed must not vanish behind the half that worked: an answer describing one
    // of two named genes, with no mention of the other, reads as a complete answer.
    assert.match(result.answer, /\*1\/\*2 for rsID rs4244285/);
    assert.match(result.answer, /'VKORC1' is not present in reference snapshot/);
    assert.equal(result.evidence.length, 1);
  });

  it('falls back to the uncalled-position wording when a target has no note of its own', async () => {
    const result = await composeRoutedAnswer(['MTHFR'], async () => ({ evidence: [] }));

    assert.match(result.answer, /No genotype for 'MTHFR' in this dataset/);
    assert.match(result.answer, /not a statement that you carry the reference allele/);
    assert.equal('provenance' in result, false, 'nothing was read, so nothing is claimed as read');
  });
});

describe('composeRoutedAnswer — the cap on one question', () => {
  const SIX = ['BRCA1', 'BRCA2', 'CYP1A2', 'CYP2C19', 'LCT', 'MTHFR'];

  it('reads only the first five targets, and says the list was cut', async () => {
    const { queried, query } = fromTable(
      Object.fromEntries(
        SIX.map((gene, index) => [gene, found(gene, `${index}/${index}`, `s3://bucket/${gene}.parquet`)]),
      ),
    );

    const result = await composeRoutedAnswer(SIX, query);

    assert.equal(MAX_QUERIED_TARGETS, 5);
    assert.deepEqual(queried, SIX.slice(0, 5), 'the sixth target is never read');
    assert.equal(result.evidence.length, 5);
    // Stated, not silent: a list quietly truncated to five reads as a complete list, and the
    // genotype the user asked about last would appear to have been checked and found absent.
    assert.match(result.answer, /named 6 targets; this answer covers the first 5/);
    assert.match(result.answer, /Nothing was read for MTHFR — ask about those separately/);
  });

  it('says nothing about a cap when the question stayed under it', async () => {
    const { query } = fromTable({ LCT: found('rs4988235', 'A/A', 's3://bucket/LCT.parquet') });

    const result = await composeRoutedAnswer(['LCT'], query);

    assert.doesNotMatch(result.answer, /targets; this answer covers/);
    // One target composes to exactly the sentence the single-target path has always produced —
    // nothing bulleted, prefixed or otherwise reshaped around it.
    assert.match(
      result.answer,
      /^Based on your genotype \(A\/A for rsID rs4988235 in gene APOE\), clinical significance is risk factor \(Alzheimer disease\)\. Note: n\/a$/,
    );
  });
});

describe('composeRoutedAnswer — the cap on one gene is spoken, not silent', () => {
  /** One target read from a subset of a gene, the way the repository reports one. */
  function capped(targetId: string, listed: number, read: number): TargetOutcome {
    return {
      ...found(targetId, 'T/C', 's3://bucket/chrom=17/part-000.parquet'),
      coordinateCoverage: { listed, read },
    };
  }

  it('says how many coordinates ClinVar lists, how many were read, and how to reach one', async () => {
    const { query } = fromTable({ BRCA1: capped('rs80357906', 2271, 64) });

    const result = await composeRoutedAnswer(['BRCA1'], query);

    // The three facts a reader needs to know the answer is about part of the gene: the size of the
    // thing, the size of what was read, and what to type to get a specific coordinate. Without
    // them the paragraph names one rsID and reads as a verdict on BRCA1.
    assert.match(result.answer, /ClinVar lists 2,271 coordinates for 'BRCA1'/);
    assert.match(result.answer, /64 of them were read/);
    assert.match(result.answer, /Naming an rsID reads that one coordinate instead\./);
    // Beside the genotype, not instead of it.
    assert.match(result.answer, /T\/C for rsID rs80357906/);
  });

  it('stays silent for a gene under the cap, so the notice means something when it fires', async () => {
    const { query } = fromTable({ VKORC1: capped('rs9923231', 9, 9) });

    const result = await composeRoutedAnswer(['VKORC1'], query);

    assert.doesNotMatch(result.answer, /ClinVar lists/);
    assert.doesNotMatch(result.answer, /were read/);
  });

  it('speaks the cap for the target it applies to, and only that one', async () => {
    const { query } = fromTable({
      BRCA1: capped('rs80357906', 2271, 64),
      MTHFR: capped('rs1801133', 1, 1),
    });

    const result = await composeRoutedAnswer(['BRCA1', 'MTHFR'], query);

    const paragraphs = result.answer.split('\n\n');
    assert.equal(paragraphs.length, 2);
    assert.match(paragraphs[0]!, /ClinVar lists 2,271 coordinates for 'BRCA1'/);
    assert.doesNotMatch(paragraphs[1]!, /ClinVar lists/);
  });

  it('still reports the cap when the dataset had no call at any coordinate read', async () => {
    // The BRCA1 case in the live dataset: 64 coordinates read, no matching call at any of them.
    // The absence sentence alone would suggest the whole gene was checked.
    const { query } = fromTable({
      BRCA1: { evidence: [], coordinateCoverage: { listed: 2271, read: 64 } },
    });

    const result = await composeRoutedAnswer(['BRCA1'], query);

    assert.match(result.answer, /No genotype for 'BRCA1' in this dataset/);
    assert.match(result.answer, /ClinVar lists 2,271 coordinates for 'BRCA1'/);
  });
});

describe('composeRoutedAnswer — provenance describes the whole read, and only it', () => {
  it('unions the files scanned and de-duplicates the shared ones', async () => {
    const chrom19 = 's3://bucket/chrom=19/part-000.parquet';
    const chrom10 = 's3://bucket/chrom=10/part-000.parquet';
    const { query } = fromTable({
      rs429358: found('rs429358', 'T/C', chrom19),
      rs7412: found('rs7412', 'C/C', chrom19),
      CYP2C19: found('rs4244285', '*1/*2', chrom10),
    });

    const result = await composeRoutedAnswer(['rs429358', 'rs7412', 'CYP2C19'], query);

    // Two of the three targets sit on chr19 and were answered out of the same object. Listing it
    // twice would report two scans where there was one; dropping it for the second target would
    // report a genotype whose source file is not in the list.
    assert.deepEqual(result.provenance?.filesScanned, [chrom19, chrom10]);
    assert.equal(result.provenance?.targetsResolved, 3, 'the total is the real total');
    assert.equal(result.provenance?.datasetChecksumSha256, PROVENANCE.datasetChecksumSha256);
  });

  it('claims no read for the targets that were absent', async () => {
    const file = 's3://bucket/chrom=19/part-000.parquet';
    const { query } = fromTable({
      rs429358: found('rs429358', 'T/C', file),
      rs7412: absent("'rs7412' is not present in reference snapshot 'demo-clinvar-grch38-v3'."),
    });

    const result = await composeRoutedAnswer(['rs429358', 'rs7412'], query);

    assert.deepEqual(result.provenance?.filesScanned, [file]);
    assert.equal(result.provenance?.targetsResolved, 1, 'an unplaceable target resolved nothing');
  });

  it('refuses to merge reads of two different datasets into one record', async () => {
    // Impossible today — one request holds one dataset-scoped repository — but the failure it
    // guards is silent and unfixable after the fact: one dataset's checksum stamped on rows the
    // other produced, in the field a reader would use to reproduce the answer.
    const query = async (targetId: string): Promise<TargetOutcome> => ({
      evidence: [],
      provenance: {
        ...PROVENANCE,
        datasetId: targetId === 'LCT' ? 'ds-serving-001' : 'ds-serving-002',
        filesScanned: [`s3://bucket/${targetId}.parquet`],
        targetsResolved: 1,
      },
    });

    await assert.rejects(
      () => composeRoutedAnswer(['LCT', 'MTHFR'], query),
      /read two different datasets or snapshots/,
    );
  });
});
