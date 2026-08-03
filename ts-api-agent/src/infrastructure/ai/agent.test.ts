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
import {
  DEFAULT_CEREBRAS_MODEL,
  askBioinformaticsAgent,
} from './agent.ts';
import { createCallBudget } from './call-budget.ts';
import { MAX_QUERIED_TARGETS } from './routed-answer.ts';

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

/** Fails the test if the genome is read at all: an unrouted question must read nothing. */
const forbiddenRepository: GenotypeRepository = {
  datasetId: 'ds-serving-001',
  async synthesizeVariant(targetId: string): Promise<GenotypeQueryResult> {
    throw new Error(`the dataset must not be read for an unrouted question, but '${targetId}' was`);
  },
};

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
    referenceVersion: 'demo-clinvar-grch38-v3',
    filesScanned: ['s3://genomic-artifacts/datasets/ds-serving-001/…/chrom=15/part-000.parquet'],
    targetsResolved: 1,
  },
  coordinateCoverage: { listed: 1, read: 1 },
};

describe('askBioinformaticsAgent — absorbing "nothing to read" outcomes', () => {
  it('turns TargetNotPresentError into an empty-evidence note, not a thrown error', async () => {
    const repository = repositoryThatThrows(
      new TargetNotPresentError(
        'ds-serving-001',
        'chromosome X, which this dataset does not cover at all (it covers 1–22)',
      ),
    );

    const response = await askBioinformaticsAgent('What is my BRCA1 status?', {
      genotypeRepository: repository,
      referenceVocabulary: VOCABULARY,
      dryRunLocal: true,
    });

    assert.deepEqual(response.evidence, []);
    assert.equal('provenance' in response, false, 'nothing was read, so nothing is claimed as read');
    assert.match(response.answer, /No genotype for 'BRCA1'/);
    // The reason travels with the answer: "there is no object that could hold it" is a statement
    // about this dataset's coverage, and saying so is what keeps it from reading as a negative
    // result about the sample.
    assert.match(response.answer, /does not cover at all/);
    assert.match(response.answer, /not a statement that you carry the reference allele/);
  });

  it('turns TargetNotResolvableError into an empty-evidence note, not a thrown error', async () => {
    // A gene the vocabulary advertises can still fail to resolve: the resolver drops snapshot
    // rows whose contig or alleles it cannot normalize, so "listed" and "placeable" are not the
    // same claim. That outcome has to reach the user as an answer, not a crash.
    const repository = repositoryThatThrows(
      new TargetNotResolvableError('BRCA1', 'demo-clinvar-grch38-v3'),
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

/**
 * A vocabulary for the questions that name more than one target: a gene with two markers, and two
 * genes a single drug question names together. Separate from `VOCABULARY` above so the single-target
 * cases keep routing exactly as they did.
 */
const MULTI_TARGET_VOCABULARY: readonly ReferenceVocabularyEntry[] = [
  {
    gene: 'APOE',
    rsid: 'rs429358',
    phenotype: 'Alzheimer disease, susceptibility to',
    clinicalSignificance: 'risk factor',
  },
  {
    gene: 'APOE',
    rsid: 'rs7412',
    phenotype: 'Hyperlipoproteinemia, type III',
    clinicalSignificance: 'Pathogenic',
  },
  {
    gene: 'CYP2C19',
    rsid: 'rs4244285',
    phenotype: 'Clopidogrel response',
    clinicalSignificance: 'drug response',
  },
  {
    gene: 'VKORC1',
    rsid: 'rs9923231',
    phenotype: 'Warfarin response',
    clinicalSignificance: 'drug response',
  },
];

/**
 * Answers each target from its own row — its own genotype and its own scanned file — and records
 * the order the dataset was actually asked in.
 *
 * A target with no row is unplaceable, which is how a partial read gets tested: one target found,
 * one refused, one answer that has to report both.
 */
function repositoryPerTarget(
  rows: Readonly<Record<string, { gene: string; genotype: string; file: string }>>,
): { readonly queried: string[]; readonly repository: GenotypeRepository } {
  const queried: string[] = [];
  return {
    queried,
    repository: {
      datasetId: 'ds-serving-001',
      async synthesizeVariant(targetId: string): Promise<GenotypeQueryResult> {
        queried.push(targetId);
        const row = rows[targetId];
        if (row === undefined) {
          throw new TargetNotResolvableError(targetId, 'demo-clinvar-grch38-v3');
        }
        return {
          targetId,
          variants: [
            {
              rsid: targetId,
              gene: row.gene,
              userGenotype: row.genotype,
              phenotype: 'n/a',
              clinicalSignificance: 'n/a',
              evidenceNote: 'n/a',
            },
          ],
          provenance: {
            ...SUCCESSFUL_RESULT.provenance,
            filesScanned: [row.file],
            targetsResolved: 1,
          },
          coordinateCoverage: { listed: 1, read: 1 },
        };
      },
    },
  };
}

const CHROM_19 = 's3://genomic-artifacts/datasets/ds-serving-001/…/chrom=19/part-000.parquet';
const CHROM_10 = 's3://genomic-artifacts/datasets/ds-serving-001/…/chrom=10/part-000.parquet';

describe('askBioinformaticsAgent — a question that names several targets', () => {
  it('answers both markers of a gene when the question names both', async () => {
    // The reported defect: this exact question came back as "could be about rs429358 or rs7412 …
    // so I did not guess", with the genome unread. Both rsIDs are real, both are APOE, and the
    // APOE genotype is the pair — naming two markers is a request for two, not a doubt about one.
    const { queried, repository } = repositoryPerTarget({
      rs429358: { gene: 'APOE', genotype: 'T/C', file: CHROM_19 },
      rs7412: { gene: 'APOE', genotype: 'C/C', file: CHROM_19 },
    });

    const response = await askBioinformaticsAgent(
      'What is my APOE genotype (rs429358 and rs7412)?',
      {
        genotypeRepository: repository,
        referenceVocabulary: MULTI_TARGET_VOCABULARY,
        searchLiterature: async () => [],
        dryRunLocal: true,
      },
    );

    assert.deepEqual(queried, ['rs429358', 'rs7412'], 'both markers were read, in question order');
    assert.equal(response.evidence?.length, 2);
    assert.match(response.answer, /T\/C for rsID rs429358/);
    assert.match(response.answer, /C\/C for rsID rs7412/);
    assert.doesNotMatch(response.answer, /did not guess/);
    assert.doesNotMatch(response.answer, /Nothing was read from your genome/);
  });

  it('answers two named genes in one answer', async () => {
    const { queried, repository } = repositoryPerTarget({
      CYP2C19: { gene: 'CYP2C19', genotype: '*1/*2', file: CHROM_10 },
      VKORC1: { gene: 'VKORC1', genotype: 'A/G', file: CHROM_19 },
    });

    const response = await askBioinformaticsAgent('Does my CYP2C19 affect warfarin (VKORC1)?', {
      genotypeRepository: repository,
      referenceVocabulary: MULTI_TARGET_VOCABULARY,
      searchLiterature: async () => [],
      dryRunLocal: true,
    });

    assert.deepEqual(queried, ['CYP2C19', 'VKORC1']);
    assert.match(response.answer, /\*1\/\*2 for rsID CYP2C19/);
    assert.match(response.answer, /A\/G for rsID VKORC1/);
    assert.equal(response.evidence?.length, 2);
  });

  it('reports the target it could not place beside the one it found', async () => {
    // Half an answer presented as a whole one is the failure here: a reader who asked about two
    // markers and is shown one has no way to notice the other was dropped.
    const { repository } = repositoryPerTarget({
      rs429358: { gene: 'APOE', genotype: 'T/C', file: CHROM_19 },
    });

    const response = await askBioinformaticsAgent('Compare rs429358 and rs7412', {
      genotypeRepository: repository,
      referenceVocabulary: MULTI_TARGET_VOCABULARY,
      searchLiterature: async () => [],
      dryRunLocal: true,
    });

    assert.match(response.answer, /T\/C for rsID rs429358/);
    assert.match(response.answer, /'rs7412' is not present in reference snapshot/);
    assert.equal(response.evidence?.length, 1);
    // Only the read that happened is claimed: the unplaceable target resolved no coordinate.
    assert.deepEqual(response.provenance?.filesScanned, [CHROM_19]);
    assert.equal(response.provenance?.targetsResolved, 1);
  });

  it('merges provenance into the union of the files actually read, with no duplicates', async () => {
    const { repository } = repositoryPerTarget({
      rs429358: { gene: 'APOE', genotype: 'T/C', file: CHROM_19 },
      rs7412: { gene: 'APOE', genotype: 'C/C', file: CHROM_19 },
      rs4244285: { gene: 'CYP2C19', genotype: '*1/*2', file: CHROM_10 },
    });

    const response = await askBioinformaticsAgent('Compare rs429358, rs7412 and rs4244285', {
      genotypeRepository: repository,
      referenceVocabulary: MULTI_TARGET_VOCABULARY,
      searchLiterature: async () => [],
      dryRunLocal: true,
    });

    // Two of the three markers are on chr19 and came out of the same object. Listing it twice would
    // claim two scans where there was one; omitting it for the second marker would report a
    // genotype whose source file is not in the list.
    assert.deepEqual(response.provenance?.filesScanned, [CHROM_19, CHROM_10]);
    assert.equal(response.provenance?.targetsResolved, 3, 'the count is the real total');
    assert.equal(response.evidence?.length, 3);
  });

  it('caps one question at five targets and says the list was cut', async () => {
    // A question naming a dozen genes is a scan with a question mark on the end. Cutting it is
    // fine; cutting it silently is not — the targets that were dropped would otherwise look like
    // targets that were checked.
    const genes = ['BRCA1', 'BRCA2', 'CYP1A2', 'CYP2C19', 'LCT', 'MTHFR'];
    const vocabulary: readonly ReferenceVocabularyEntry[] = genes.map((gene, index) => ({
      gene,
      rsid: `rs${index + 1}`,
      phenotype: `${gene} associated condition`,
      clinicalSignificance: 'Pathogenic',
    }));
    const { queried, repository } = repositoryPerTarget(
      Object.fromEntries(
        genes.map((gene) => [gene, { gene, genotype: 'A/A', file: `s3://bucket/${gene}.parquet` }]),
      ),
    );

    const response = await askBioinformaticsAgent(
      'Compare my BRCA1, BRCA2, CYP1A2, CYP2C19, LCT and MTHFR results',
      {
        genotypeRepository: repository,
        referenceVocabulary: vocabulary,
        searchLiterature: async () => [],
        dryRunLocal: true,
      },
    );

    assert.deepEqual(queried, genes.slice(0, MAX_QUERIED_TARGETS), 'the sixth is never read');
    assert.equal(response.evidence?.length, MAX_QUERIED_TARGETS);
    assert.match(response.answer, /named 6 targets; this answer covers the first 5/);
    assert.match(response.answer, /Nothing was read for MTHFR/);
    assert.equal(response.provenance?.filesScanned.length, MAX_QUERIED_TARGETS);
  });

  it('carries the coordinate cap from the repository into the answer it composes', async () => {
    // End to end through the deterministic path: the repository says it read 64 of BRCA1's 2,271
    // coordinates, and the answer has to say so too. This is the wiring the old behaviour did not
    // need — a gene over the cap was an HTTP 422 and never reached an answer at all.
    const response = await askBioinformaticsAgent('What about the BRCA1 gene?', {
      genotypeRepository: repositoryThatAnswers({
        ...SUCCESSFUL_RESULT,
        coordinateCoverage: { listed: 2271, read: 64 },
      }),
      referenceVocabulary: VOCABULARY,
      searchLiterature: async () => [],
      dryRunLocal: true,
    });

    assert.match(response.answer, /ClinVar lists 2,271 coordinates for 'BRCA1'/);
    assert.match(response.answer, /64 of them were read/);
    assert.match(response.answer, /Naming an rsID reads that one coordinate instead\./);
    assert.deepEqual(response.toolsUsed, ['query_genotype']);
  });

  it('still refuses to guess when the target had to be inferred and two genes fit', async () => {
    // The counterpart of the cases above, and the line between them: "milk" and "coffee" name no
    // gene at all, so reading both CYP1A2 and LCT would be this router's inference presented as
    // the user's request. Explicit naming widened; inference did not.
    const response = await askBioinformaticsAgent('Can I drink milk with my coffee?', {
      genotypeRepository: forbiddenRepository,
      referenceVocabulary: VOCABULARY,
      searchLiterature: async () => [],
      dryRunLocal: true,
    });

    assert.match(response.answer, /CYP1A2 or LCT/);
    assert.match(response.answer, /did not guess/);
    assert.deepEqual(response.evidence, []);
    assert.equal('provenance' in response, false);
  });
});

describe('askBioinformaticsAgent — a found paper reaches the answer on every outcome', () => {
  /**
   * One hit, injected. The real tool reaches Qdrant and an embedding model, so under test it
   * always fails into its `{ error }` sentinel — which is precisely why the dropped citation
   * survived: with no hit to carry, a branch that carries it and a branch that discards it
   * produce byte-identical answers.
   */
  const oneHit = async () => [
    { score: 0.6377, title: 'Warfarin Therapy and VKORC1 and CYP Genotype.', pmid: '29334292' },
  ];

  const cases: readonly [string, string, GenotypeRepository][] = [
    ['a genotype was found', 'Can I drink coffee?', repositoryThatAnswers(SUCCESSFUL_RESULT)],
    [
      'the dataset cannot contain the target',
      'What is my BRCA1 status?',
      repositoryThatThrows(
        new TargetNotPresentError('ds-serving-001', 'chromosome X, which this dataset does not cover'),
      ),
    ],
    [
      'the reference cannot place the target',
      'Tell me about BRCA1',
      repositoryThatThrows(new TargetNotResolvableError('BRCA1', 'demo-clinvar-grch38-v3')),
    ],
    ['no gene could be routed', 'What should I have for lunch?', forbiddenRepository],
  ];

  for (const [outcome, question, repository] of cases) {
    it(`cites it when ${outcome}`, async () => {
      const response = await askBioinformaticsAgent(question, {
        genotypeRepository: repository,
        referenceVocabulary: VOCABULARY,
        searchLiterature: oneHit,
        dryRunLocal: true,
      });

      assert.match(response.answer, /Warfarin Therapy and VKORC1/);
      // The similarity travels with the citation. A nearest neighbour in an eight-paper corpus
      // is related reading, not evidence for the sentence above it, and the number is what lets
      // a reader tell the two apart.
      assert.match(response.answer, /similarity 0\.64/);
      assert.equal(response.literatureHits?.length, 1);
      assert.equal(response.toolsUsed?.includes('search_medical_literature'), true);
    });
  }

  it('says nothing about literature when the search returns nothing', async () => {
    const response = await askBioinformaticsAgent('Can I drink coffee?', {
      genotypeRepository: repositoryThatAnswers(SUCCESSFUL_RESULT),
      referenceVocabulary: VOCABULARY,
      searchLiterature: async () => [],
      dryRunLocal: true,
    });

    assert.doesNotMatch(response.answer, /Related reading/);
    assert.equal(
      response.toolsUsed?.includes('search_medical_literature'),
      false,
      'an empty search is not a tool that contributed to the answer',
    );
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

/**
 * The model-driven path, with the chat completions endpoint scripted.
 *
 * Every case here reproduces something a live model actually did against the single-shot exchange
 * this loop replaced — stopping after one tool, answering without opening the dataset, reading a
 * bare `[]` as "you have no such variant". A scripted model is the only way to pin those: the real
 * one is free to behave differently on the next call, which is precisely why the guarantees have
 * to live in this code rather than in the prompt.
 */
describe('askBioinformaticsAgent — the Cerebras tool loop', () => {
  const savedKey = process.env.CEREBRAS_API_KEY;
  const savedModel = process.env.CEREBRAS_MODEL;
  const savedFetch = globalThis.fetch;

  afterEach(() => {
    if (savedKey === undefined) delete process.env.CEREBRAS_API_KEY;
    else process.env.CEREBRAS_API_KEY = savedKey;
    if (savedModel === undefined) delete process.env.CEREBRAS_MODEL;
    else process.env.CEREBRAS_MODEL = savedModel;
    globalThis.fetch = savedFetch;
  });

  /** Assistant turn asking for one `query_genotype` call. */
  function callsGenotype(id: string, targetId: string) {
    return {
      choices: [
        {
          message: {
            role: 'assistant',
            content: null,
            tool_calls: [
              {
                id,
                type: 'function',
                function: { name: 'query_genotype', arguments: JSON.stringify({ targetId }) },
              },
            ],
          },
        },
      ],
    };
  }

  /** Assistant turn with a final answer and no tool calls. */
  function answers(content: string) {
    return { choices: [{ message: { role: 'assistant', content } }] };
  }

  /**
   * Replaces the chat endpoint with a fixed script and records every request body.
   *
   * Only `api.cerebras.ai` is scripted; anything else throws rather than silently reaching the
   * network, so a test that accidentally routes through the literature tool fails loudly instead
   * of depending on a running Qdrant.
   */
  function scriptModel(turns: readonly unknown[]): { requests: any[] } {
    const requests: any[] = [];
    let turn = 0;
    globalThis.fetch = (async (url: any, init: any) => {
      const target = String(url);
      if (!target.includes('api.cerebras.ai')) {
        throw new Error(`unexpected network call in test: ${target}`);
      }
      requests.push(JSON.parse(init.body));
      const body = turns[Math.min(turn++, turns.length - 1)];
      return { ok: true, status: 200, json: async () => body, text: async () => '' };
    }) as unknown as typeof fetch;
    return { requests };
  }

  it('runs every tool call the model makes, across turns, instead of only the first', async () => {
    // A live model opened "am I lactose intolerant?" with a literature search. Taking
    // `tool_calls[0]` and stopping meant the genome was never read and the answer carried no
    // genotype at all.
    process.env.CEREBRAS_API_KEY = 'test-key';
    const { requests } = scriptModel([
      callsGenotype('call-1', 'BRCA1'),
      callsGenotype('call-2', 'CYP1A2'),
      answers('Your CYP1A2 genotype is A/A.'),
    ]);

    const response = await askBioinformaticsAgent('Can I drink coffee?', {
      genotypeRepository: repositoryThatAnswers(SUCCESSFUL_RESULT),
      referenceVocabulary: VOCABULARY,
    });

    assert.equal(requests.length, 3, 'the loop continues while the model keeps calling tools');
    assert.equal(response.toolResults?.length, 2);
    assert.ok(response.evidence && response.evidence.length > 0);
    assert.ok(response.provenance, 'a grounded answer carries the provenance of what was read');
    assert.doesNotMatch(response.answer, /Nothing was read from your genome/);
  });

  it('opens by requiring a tool, and defaults to the configured model', async () => {
    // Asked how to adjust a warfarin dose, a live model called nothing and wrote a general essay
    // about INR monitoring — an answer about this person with their genome unopened.
    process.env.CEREBRAS_API_KEY = 'test-key';
    delete process.env.CEREBRAS_MODEL;
    const { requests } = scriptModel([callsGenotype('call-1', 'CYP1A2'), answers('A/A.')]);

    await askBioinformaticsAgent('Can I drink coffee?', {
      genotypeRepository: repositoryThatAnswers(SUCCESSFUL_RESULT),
      referenceVocabulary: VOCABULARY,
    });

    assert.equal(requests[0].tool_choice, 'required');
    assert.equal(requests[0].model, DEFAULT_CEREBRAS_MODEL);
    // Not on later turns: the model has tool output in hand and must be free to stop.
    assert.equal(requests[1].tool_choice, 'auto');
  });

  it('sends the absence note to the model, not a bare empty array', async () => {
    // `[]` reads as "this person has no such variant". The note is what distinguishes an uncalled
    // position from a chromosome the dataset never contained.
    process.env.CEREBRAS_API_KEY = 'test-key';
    const { requests } = scriptModel([callsGenotype('call-1', 'G6PD'), answers('Not covered.')]);

    await askBioinformaticsAgent('Do I have any G6PD variants?', {
      genotypeRepository: repositoryThatThrows(
        new TargetNotPresentError(
          'ds-serving-001',
          'chromosome X, which this dataset does not cover at all (it covers 1–22)',
        ),
      ),
      referenceVocabulary: VOCABULARY,
    });

    const toolMessage = requests[1].messages.find((m: any) => m.role === 'tool');
    assert.ok(toolMessage, 'the tool result is fed back to the model');
    assert.match(toolMessage.content, /does not cover at all/);
    assert.match(toolMessage.content, /"variants":\[\]/);
  });

  it('labels an answer produced without reading the dataset', async () => {
    // `tool_choice: 'required'` is a request, not a guarantee — a model that ignores it must not
    // have its prose presented as a statement about this person's genome.
    process.env.CEREBRAS_API_KEY = 'test-key';
    scriptModel([answers('Warfarin dosing depends on your INR and clinical profile.')]);

    const response = await askBioinformaticsAgent('How should my warfarin dose be adjusted?', {
      genotypeRepository: forbiddenRepository,
      referenceVocabulary: VOCABULARY,
    });

    assert.match(response.answer, /Nothing was read from your genome/);
    assert.deepEqual(response.toolsUsed, []);
  });

  it('refunds the turns a short question did not use', async () => {
    // A question that answers in two turns must not be billed for four, or a budget of 120 calls
    // silently becomes a budget of 30 questions regardless of how cheap they were.
    process.env.CEREBRAS_API_KEY = 'test-key';
    scriptModel([callsGenotype('call-1', 'CYP1A2'), answers('A/A.')]);
    const budget = createCallBudget({ limit: 100, windowMs: 60_000 });

    await askBioinformaticsAgent('Can I drink coffee?', {
      genotypeRepository: repositoryThatAnswers(SUCCESSFUL_RESULT),
      referenceVocabulary: VOCABULARY,
      callBudget: budget,
    });

    assert.equal(budget.remaining(), 98, 'two calls made, two refunded');
  });

  it('answers from the free path instead of failing when the budget is spent', async () => {
    // The whole reason exhaustion is not an error: the deterministic path reads the same dataset
    // through the same tool and carries the same provenance. Refusing to answer while that is
    // sitting there would be a worse product for no saving.
    process.env.CEREBRAS_API_KEY = 'test-key';
    scriptModel([answers('this must never be reached')]);
    const budget = createCallBudget({ limit: 0, windowMs: 60_000 });

    const response = await askBioinformaticsAgent('Can I drink coffee?', {
      genotypeRepository: repositoryThatAnswers(SUCCESSFUL_RESULT),
      referenceVocabulary: VOCABULARY,
      callBudget: budget,
      searchLiterature: async () => [],
    });

    assert.match(response.answer, /Based on your genotype \(A\/A for rsID rs762551/);
    assert.ok(response.provenance, 'the free answer is still fully evidenced');
    assert.deepEqual(response.toolsUsed, ['query_genotype']);
  });

  it('bills a failed call, rather than making a broken key free to retry', async () => {
    process.env.CEREBRAS_API_KEY = 'test-key';
    globalThis.fetch = (async () => ({
      ok: false,
      status: 404,
      json: async () => ({}),
      text: async () => '{"code":"model_not_found"}',
    })) as unknown as typeof fetch;
    const budget = createCallBudget({ limit: 100, windowMs: 60_000 });

    await assert.rejects(
      () =>
        askBioinformaticsAgent('Can I drink coffee?', {
          genotypeRepository: repositoryThatAnswers(SUCCESSFUL_RESULT),
          referenceVocabulary: VOCABULARY,
          callBudget: budget,
        }),
      /model_not_found/,
    );

    assert.equal(budget.remaining(), 99, 'the call happened, so it counts');
  });

  it('stops at the turn budget and reports what was read, inventing no prose', async () => {
    process.env.CEREBRAS_API_KEY = 'test-key';
    const { requests } = scriptModel([callsGenotype('call-n', 'CYP1A2')]);

    const response = await askBioinformaticsAgent('Can I drink coffee?', {
      genotypeRepository: repositoryThatAnswers(SUCCESSFUL_RESULT),
      referenceVocabulary: VOCABULARY,
    });

    assert.equal(requests.length, 4, 'the turn budget bounds what one question can cost');
    assert.match(response.answer, /stopped after 4 model turns/);
    assert.match(response.answer, /A\/A for rs762551 in CYP1A2/);
  });
});
