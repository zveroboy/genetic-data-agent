/**
 * Guards the claim `index.test.ts`'s "reports a dataset whose targets are absent as an answer"
 * case makes in its comment: that the *shipped* `askBioinformaticsAgent` absorbs
 * `TargetNotResolvableError`/`TargetNotPresentError` into a note-bearing answer rather than
 * letting them propagate as a crash. That index.test.ts harness fakes the agent entirely, so it
 * cannot prove anything about `agent.ts`'s own behaviour — this file runs the *real*
 * `askBioinformaticsAgent` and the *real* `createQueryGenotypeTool` (`agent.ts:34-60`) around a
 * fake `GenotypeRepository`, so the only thing under test is the agent's own error handling.
 *
 * `dryRunLocal: true` is used throughout to reach this code deterministically, without a
 * Cerebras or Anthropic API key.
 */
import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';

import {
  TargetNotResolvableError,
  type ReferenceVocabularyEntry,
} from '../database/clinvar-coordinate-resolver.ts';
import type { GenotypeQueryResult, GenotypeRepository } from '../database/duckdb.ts';
import { TargetNotPresentError } from '../database/parquet-dataset-resolver.ts';
import { askBioinformaticsAgent } from './agent.ts';

/**
 * A stand-in for the snapshot's askable surface: three genes, with the text the real table
 * carries for them. Small on purpose — the routing rules themselves are pinned against the
 * shipped table in `question-routing.test.ts`; what this file tests is what the agent does with
 * a routing decision once it has one.
 */
const VOCABULARY: readonly ReferenceVocabularyEntry[] = [
  {
    gene: 'BRCA1',
    rsid: 'rs80357906',
    phenotype: 'Inherited ovarian cancer (without breast cancer)',
    clinicalSignificance: 'Pathogenic',
  },
  {
    gene: 'CYP1A2',
    rsid: 'rs762551',
    phenotype: 'No condition named in ClinVar for this variant',
    clinicalSignificance: 'Likely benign',
  },
  {
    gene: 'LCT',
    rsid: 'rs4988235',
    phenotype: 'LACTASE PERSISTENCE',
    clinicalSignificance: 'association',
  },
];

function repositoryThatThrows(error: Error): GenotypeRepository {
  return {
    datasetId: 'ds-serving-001',
    async synthesizeVariant(): Promise<GenotypeQueryResult> {
      throw error;
    },
  };
}

function repositoryThatAnswers(result: GenotypeQueryResult): GenotypeRepository {
  return {
    datasetId: result.provenance.datasetId,
    async synthesizeVariant(targetId: string): Promise<GenotypeQueryResult> {
      return { ...result, targetId };
    },
  };
}

const SUCCESSFUL_RESULT: GenotypeQueryResult = {
  targetId: 'CYP1A2',
  variants: [
    {
      rsid: 'rs762551',
      gene: 'CYP1A2',
      userGenotype: 'A/A',
      phenotype: 'Fast caffeine metabolizer',
      clinicalSignificance: 'Benign',
      evidenceNote: 'n/a',
    },
  ],
  provenance: {
    datasetId: 'ds-serving-001',
    datasetChecksumSha256: 'a'.repeat(64),
    referenceBuild: 'GRCh38',
    referenceVersion: 'demo-clinvar-grch38-v2',
    filesScanned: ['s3://genomic-artifacts/datasets/ds-serving-001/…/chrom=15/part-000.parquet'],
    targetsResolved: 1,
  },
};

describe('askBioinformaticsAgent — absorbing "nothing to read" outcomes', () => {
  it('turns TargetNotPresentError into an empty-evidence note, not a thrown error', async () => {
    const repository = repositoryThatThrows(
      new TargetNotPresentError('ds-serving-001', 'the requested coordinates'),
    );

    const response = await askBioinformaticsAgent('What is my BRCA1 status?', {
      genotypeRepository: repository,
      referenceVocabulary: VOCABULARY,
      dryRunLocal: true,
    });

    assert.deepEqual(response.evidence, []);
    assert.equal('provenance' in response, false, 'nothing was read, so nothing is claimed as read');
    assert.match(response.answer, /contains no variant/);
  });

  it('turns TargetNotResolvableError into an empty-evidence note, not a thrown error', async () => {
    // A gene the vocabulary advertises can still fail to resolve: the resolver drops snapshot
    // rows whose contig or alleles it cannot normalize, so "listed" and "placeable" are not the
    // same claim. That outcome has to reach the user as an answer, not a crash.
    const repository = repositoryThatThrows(
      new TargetNotResolvableError('BRCA1', 'demo-clinvar-grch38-v2'),
    );

    const response = await askBioinformaticsAgent('Tell me about BRCA1', {
      genotypeRepository: repository,
      referenceVocabulary: VOCABULARY,
      dryRunLocal: true,
    });

    assert.deepEqual(response.evidence, []);
    assert.match(response.answer, /not present in reference snapshot/);
  });

  it('does not absorb an unrelated failure — it propagates', async () => {
    const repository = repositoryThatThrows(new Error('S3 outage: connection reset'));

    await assert.rejects(
      () =>
        askBioinformaticsAgent('What is my BRCA1 status?', {
          genotypeRepository: repository,
          referenceVocabulary: VOCABULARY,
          dryRunLocal: true,
        }),
      /S3 outage/,
    );
  });
});

describe('askBioinformaticsAgent — a question it cannot place', () => {
  /** Fails the test if the genome is read at all: an unrouted question must read nothing. */
  const forbiddenRepository: GenotypeRepository = {
    datasetId: 'ds-serving-001',
    async synthesizeVariant(targetId: string): Promise<GenotypeQueryResult> {
      throw new Error(`the dataset must not be read for an unrouted question, but '${targetId}' was`);
    },
  };

  it('says so and names what it can answer, instead of defaulting to a gene', async () => {
    const response = await askBioinformaticsAgent('What should I have for lunch?', {
      genotypeRepository: forbiddenRepository,
      referenceVocabulary: VOCABULARY,
      dryRunLocal: true,
    });

    assert.match(response.answer, /could not tell which gene/i);
    assert.match(response.answer, /BRCA1, CYP1A2, LCT/);
    assert.deepEqual(response.evidence, []);
    assert.equal('provenance' in response, false);
    assert.equal(
      response.toolsUsed?.includes('query_genotype'),
      false,
      'no genotype was queried, so the tool must not be claimed as used',
    );
  });

  it('refuses to guess between two equally good candidates', async () => {
    const response = await askBioinformaticsAgent('Can I drink milk with my coffee?', {
      genotypeRepository: forbiddenRepository,
      referenceVocabulary: VOCABULARY,
      dryRunLocal: true,
    });

    assert.match(response.answer, /CYP1A2 or LCT/);
    assert.deepEqual(response.evidence, []);
  });

  it('the old default is gone: an off-topic question no longer answers about caffeine', async () => {
    // Before this change the branch chain fell through to rs762551/CYP1A2, so this exact
    // question came back as a confident statement about caffeine metabolism.
    const response = await askBioinformaticsAgent('What is the capital of France?', {
      genotypeRepository: repositoryThatAnswers(SUCCESSFUL_RESULT),
      referenceVocabulary: VOCABULARY,
      dryRunLocal: true,
    });

    assert.doesNotMatch(response.answer, /Based on your genotype/);
    assert.deepEqual(response.evidence, []);
  });
});

describe('askBioinformaticsAgent — provider env gating', () => {
  const savedCerebrasKey = process.env.CEREBRAS_API_KEY;
  const savedAnthropicKey = process.env.ANTHROPIC_API_KEY;

  afterEach(() => {
    if (savedCerebrasKey === undefined) delete process.env.CEREBRAS_API_KEY;
    else process.env.CEREBRAS_API_KEY = savedCerebrasKey;
    if (savedAnthropicKey === undefined) delete process.env.ANTHROPIC_API_KEY;
    else process.env.ANTHROPIC_API_KEY = savedAnthropicKey;
  });

  it('does not fall through to "No AI provider configured" when only ANTHROPIC_API_KEY is set', async () => {
    // There is no Anthropic branch in agent.ts. Before the fix, `ANTHROPIC_API_KEY` alone
    // (with no `CEREBRAS_API_KEY` and no `dryRunLocal`) fell through every branch to a bare
    // 'No AI provider configured.' 200 response — no evidence, no provenance, dataset never
    // read. This is exactly the ambient-env-var trap the fix closes: an unimplemented provider
    // key must degrade to the deterministic local answer path, never to a silent non-answer.
    delete process.env.CEREBRAS_API_KEY;
    process.env.ANTHROPIC_API_KEY = 'test-anthropic-key-should-not-be-a-provider-signal';

    const response = await askBioinformaticsAgent('Tell me about caffeine metabolism', {
      genotypeRepository: repositoryThatAnswers(SUCCESSFUL_RESULT),
      referenceVocabulary: VOCABULARY,
    });

    assert.notEqual(response.answer, 'No AI provider configured.');
    assert.ok(response.evidence && response.evidence.length > 0, 'evidence must be present');
    assert.ok(response.provenance, 'a real answer must carry provenance of what was read');
  });
});
