/**
 * Every fabrication pinned here was produced by a live model against this system, not invented for
 * a test. The headline case is the one that motivated the module: asked about statin-induced muscle
 * pain, the model called `query_genotype`, received the correct SLCO1B1 row, and then wrote its
 * answer about `CYP3A5 rs776746 C/C`.
 *
 * The second half of the file drives the real `askBioinformaticsAgent` with a scripted chat
 * endpoint. That is deliberately here rather than in `agent.test.ts`: what those cases pin is this
 * module's integration — the accumulated tool facts it is given, and the warning it appends — and
 * `agent.test.ts` owns the tool-loop guarantees instead.
 */
import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';

import type { SynthesizedVariant } from '../../domain/types.ts';
import type { ReferenceVocabularyEntry } from '../database/clinvar-coordinate-resolver.ts';
import type { GenotypeProvenance, GenotypeQueryResult, GenotypeRepository } from '../database/duckdb.ts';
import { askBioinformaticsAgent } from './agent.ts';
import {
  GROUNDING_WARNING_HEADER,
  appendGroundingWarning,
  checkAnswerGrounding,
} from './answer-grounding.ts';
import { createCallBudget } from './call-budget.ts';

/** Four genes the snapshot can place. CYP3A5 is deliberately absent: the model invented it. */
const VOCABULARY: readonly ReferenceVocabularyEntry[] = [
  {
    gene: 'SLCO1B1',
    rsid: 'rs4149056',
    phenotype: 'Statin-induced myopathy',
    clinicalSignificance: 'drug response',
  },
  {
    gene: 'VKORC1',
    rsid: 'rs9923231',
    phenotype: 'Warfarin response',
    clinicalSignificance: 'drug response',
  },
  {
    gene: 'CYP1A2',
    rsid: 'rs762551',
    phenotype: 'No condition named in ClinVar for this variant',
    clinicalSignificance: 'Likely benign',
  },
  {
    gene: 'MTHFR',
    rsid: 'rs1801133',
    phenotype: 'Neural tube defects',
    clinicalSignificance: 'risk factor',
  },
];

const SLCO1B1: SynthesizedVariant = {
  rsid: 'rs4149056',
  gene: 'SLCO1B1',
  userGenotype: 'T/C',
  phenotype: 'Statin-induced myopathy',
  clinicalSignificance: 'drug response',
  evidenceNote: 'ClinVar submission, reviewed by expert panel',
};

const CYP1A2: SynthesizedVariant = {
  rsid: 'rs762551',
  gene: 'CYP1A2',
  userGenotype: 'A/A',
  phenotype: 'Fast caffeine metabolizer',
  clinicalSignificance: 'Benign',
  evidenceNote: 'n/a',
};

/** The `query_genotype` output as the model saw it, which is what the check may trust. */
function toolText(...variants: readonly SynthesizedVariant[]): readonly string[] {
  return [JSON.stringify({ variants })];
}

describe('checkAnswerGrounding — the fabrications a live model actually produced', () => {
  it('flags the observed CYP3A5 rs776746 C/C answer written over a returned SLCO1B1 row', () => {
    const answer =
      'Based on your genetic data you carry the CYP3A5 rs776746 C/C genotype, which is ' +
      'associated with a higher risk of statin-induced myopathy.';

    const findings = checkAnswerGrounding(answer, {
      variants: [SLCO1B1],
      referenceVocabulary: VOCABULARY,
      toolResultText: toolText(SLCO1B1),
    });

    assert.deepEqual(
      findings.map((finding) => [finding.kind, finding.mentioned]),
      [
        ['unsupported-rsid', 'rs776746'],
        ['unknown-gene', 'CYP3A5'],
      ],
    );
    assert.match(findings[0]!.detail, /never returned by any tool/);
    assert.match(findings[1]!.detail, /not a gene in this reference snapshot/);
  });

  it('flags a genotype that disagrees with the userGenotype the tool returned', () => {
    // The subtlest of the three: the gene and the rsID are the ones that were read, so every
    // identifier in the sentence checks out and only the alleles are wrong.
    const findings = checkAnswerGrounding(
      'Your SLCO1B1 rs4149056 genotype is C/C, so your transporter function is strongly reduced.',
      {
        variants: [SLCO1B1],
        referenceVocabulary: VOCABULARY,
        toolResultText: toolText(SLCO1B1),
      },
    );

    assert.equal(findings.length, 1);
    assert.equal(findings[0]!.kind, 'contradicted-genotype');
    assert.match(
      findings[0]!.detail,
      /states C\/C for rs4149056 \(SLCO1B1\), but the dataset returned T\/C/,
    );
  });

  it('finds nothing in a correct answer that quotes the genotype the tool returned', () => {
    const findings = checkAnswerGrounding(
      'Based on your genotype (T/C for rsID rs4149056 in gene SLCO1B1), clinical significance is ' +
        'drug response (Statin-induced myopathy).',
      {
        variants: [SLCO1B1],
        referenceVocabulary: VOCABULARY,
        toolResultText: toolText(SLCO1B1),
      },
    );

    assert.deepEqual(findings, []);
  });

  it('reads the alleles as a set, so C/T and T/C are the same call', () => {
    const findings = checkAnswerGrounding('At rs4149056 you are C/T.', {
      variants: [SLCO1B1],
      referenceVocabulary: VOCABULARY,
      toolResultText: toolText(SLCO1B1),
    });

    assert.deepEqual(findings, []);
  });

  it('does not attribute a genotype across a sentence boundary to the next gene named', () => {
    // Found by the multi-gene case below, before the sentence boundary was part of attribution:
    // `CYP1A2` sits seven characters after `T/C` and `rs4149056` thirteen before it, so
    // nearest-mention alone reported a correct two-gene answer as two contradictions.
    const findings = checkAnswerGrounding(
      'Your SLCO1B1 rs4149056 genotype is T/C. Your CYP1A2 rs762551 genotype is A/A.',
      {
        variants: [SLCO1B1, CYP1A2],
        referenceVocabulary: VOCABULARY,
        toolResultText: toolText(SLCO1B1, CYP1A2),
      },
    );

    assert.deepEqual(findings, []);
  });

  it('attributes each genotype to the nearest variant, not to every variant in the answer', () => {
    // Two genes discussed in adjacent sentences, each quoted correctly. Comparing every genotype
    // against every returned variant would report both of them as contradictions of the other.
    const findings = checkAnswerGrounding(
      'Your SLCO1B1 rs4149056 genotype is T/C, which affects statin transport. Separately, your ' +
        'CYP1A2 rs762551 genotype is A/A, a fast caffeine metabolizer.',
      {
        variants: [SLCO1B1, CYP1A2],
        referenceVocabulary: VOCABULARY,
        toolResultText: toolText(SLCO1B1, CYP1A2),
      },
    );

    assert.deepEqual(findings, []);
  });
});

describe('checkAnswerGrounding — an unknown symbol is only a finding when it carries a claim', () => {
  /** The mechanism sentence a live model wrote, verbatim, under a correct SLCO1B1 answer. */
  const OATP1B1_SENTENCE =
    'The `SLCO1B1` gene encodes a transport protein (OATP1B1) that helps move statins from the ' +
    'blood into the liver, where they perform their cholesterol-lowering effect.';

  it('does not flag OATP1B1, the protein SLCO1B1 encodes, named in explanatory prose', () => {
    // The false positive this rule was rewritten for: a warning appeared under a fully correct
    // answer because the standard protein name for the queried transporter is not a gene symbol in
    // the reference table and happens to contain digits. This repo's own fixtures carry the string
    // ("Intermediate OATP1B1 function."), which is how ordinary the vocabulary is.
    const findings = checkAnswerGrounding(OATP1B1_SENTENCE, {
      variants: [SLCO1B1],
      referenceVocabulary: VOCABULARY,
      toolResultText: toolText(SLCO1B1),
    });

    assert.deepEqual(findings, []);
  });

  it('finds nothing in the whole live answer that produced the false positive', () => {
    const findings = checkAnswerGrounding(
      'Your genotype is T/C at rs4149056 in SLCO1B1, an intermediate-function result. ' +
        OATP1B1_SENTENCE,
      {
        variants: [SLCO1B1],
        referenceVocabulary: VOCABULARY,
        toolResultText: toolText(SLCO1B1),
      },
    );

    assert.deepEqual(findings, []);
  });

  it('does not flag an unknown symbol sharing its sentence with a correctly quoted genotype', () => {
    // The phrasing that would defeat a distance-only fix: the genotype is closer to OATP1B1 than a
    // sentence break away. It is accounted for — it belongs to the variant that was read — so
    // nothing in the sentence implicates the symbol.
    const findings = checkAnswerGrounding(
      'Your rs4149056 genotype is T/C, which reduces OATP1B1 transport capacity.',
      {
        variants: [SLCO1B1],
        referenceVocabulary: VOCABULARY,
        toolResultText: toolText(SLCO1B1),
      },
    );

    assert.deepEqual(findings, []);
  });

  it('still flags an unknown symbol carrying a genotype no read variant accounts for', () => {
    // No invented rsID here, so rule 1 has nothing to say: the orphan genotype is the whole signal.
    const findings = checkAnswerGrounding('Your CYP3A5 status is C/C, so clearance is reduced.', {
      variants: [SLCO1B1],
      referenceVocabulary: VOCABULARY,
      toolResultText: toolText(SLCO1B1),
    });

    assert.deepEqual(
      findings.map((finding) => [finding.kind, finding.mentioned]),
      [['unknown-gene', 'CYP3A5']],
    );
    assert.match(findings[0]!.detail, /attaches C\/C to it/);
  });
});

describe('checkAnswerGrounding — what must never be read as a gene symbol', () => {
  it('does not flag DNA, INR, PK, ALT/AST, B12 or GRCh38 as invented genes', () => {
    // Every one of these appears in real answers this system produces or in the prose a model
    // writes around them — the warfarin essay a live model wrote was largely about INR. A warning
    // on any of them would discredit an otherwise correct answer, which is the failure mode this
    // module can least afford.
    const answer =
      'Your DNA does not set your INR on its own: PK and PD both matter, ALT and AST monitoring ' +
      'is clinical rather than genetic, vitamin B12 status is dietary, and every coordinate here ' +
      'is GRCh38. An AI cannot replace your physician, and no SNP in this VCF changes that.';

    const findings = checkAnswerGrounding(answer, {
      variants: [SLCO1B1],
      referenceVocabulary: VOCABULARY,
      toolResultText: toolText(SLCO1B1),
    });

    assert.deepEqual(findings, []);
  });

  it('does not flag a gene the reference lists but the tools never queried', () => {
    // Decided, not incidental: the reference table is the universe of what this system can know, so
    // a symbol it lists is a real gene and the sentence naming it is at worst off-topic. Flagging
    // it would put a fabrication warning on an answer that fabricated nothing — and a warning that
    // fires on correct answers is one readers learn to skip past. The genotype rule still covers
    // the dangerous half: alleles claimed for a variant that *was* read.
    const findings = checkAnswerGrounding(
      'Statin risk aside, your VKORC1 result is what matters for warfarin dosing.',
      {
        variants: [SLCO1B1],
        referenceVocabulary: VOCABULARY,
        toolResultText: toolText(SLCO1B1),
      },
    );

    assert.deepEqual(findings, []);
  });

  it('does not name INR as an invented gene even in a sentence that does carry a fabrication', () => {
    // This is what the digit requirement still buys once claim adjacency is doing the real work:
    // the invented rsID makes the sentence hold an unaccounted claim, so adjacency alone would
    // report `INR` — the clinical measure — as a fabricated gene standing beside it. The
    // fabrication is still reported; only the misnaming is suppressed.
    const findings = checkAnswerGrounding('You carry rs776746 C/C, which reduces INR stability.', {
      variants: [SLCO1B1],
      referenceVocabulary: VOCABULARY,
      toolResultText: toolText(SLCO1B1),
    });

    assert.deepEqual(
      findings.map((finding) => finding.kind),
      ['unsupported-rsid'],
    );
  });

  it('knowingly misses an invented digit-less symbol carrying a bare genotype, such as TPMT', () => {
    // The accepted cost of keeping the digit requirement, asserted so it stays a decision rather
    // than a surprise: TPMT is shaped exactly like DNA, INR and PK, and nothing here can tell them
    // apart without a real HGNC vocabulary. Bought in exchange for the case above, which is the
    // more likely of the two — models reach for clinical acronyms constantly and for invented
    // digit-less symbols rarely, and one of the two failures puts a wrong word in a warning readers
    // are meant to trust. An invented rsID or a contradicted genotype alongside it is still caught.
    const findings = checkAnswerGrounding('Your TPMT genotype is A/A.', {
      variants: [SLCO1B1],
      referenceVocabulary: VOCABULARY,
      toolResultText: toolText(SLCO1B1),
    });

    assert.deepEqual(findings, []);
  });

  it('accepts a symbol or rsID the tools themselves put in front of the model', () => {
    // The model repeating an absence note or a retrieved paper title back at the user is grounded
    // in a tool result, however unfamiliar the symbol is to the reference table.
    const findings = checkAnswerGrounding(
      "'CYP2C19' is not present in reference snapshot demo-clinvar-grch38-v3, and rs4244285 was " +
        'therefore never read.',
      {
        variants: [],
        referenceVocabulary: VOCABULARY,
        toolResultText: [
          JSON.stringify({
            variants: [],
            note: "'CYP2C19' is not present in reference snapshot 'demo-clinvar-grch38-v3'.",
          }),
          JSON.stringify([{ title: 'CYP2C19 rs4244285 and clopidogrel response', pmid: '1' }]),
        ],
      },
    );

    assert.deepEqual(findings, []);
  });
});

describe('appendGroundingWarning', () => {
  it('leaves the answer text intact and adds a separated warning naming each finding', () => {
    // Not a rewrite: deleting the fabricated sentence would leave a shorter answer that reads as
    // trustworthy, and hide that the model is unreliable. The reader has to be able to see both.
    const answer = 'You carry the CYP3A5 rs776746 C/C genotype.';
    const findings = checkAnswerGrounding(answer, {
      variants: [SLCO1B1],
      referenceVocabulary: VOCABULARY,
      toolResultText: toolText(SLCO1B1),
    });

    const labelled = appendGroundingWarning(answer, findings);

    assert.ok(labelled.startsWith(answer), 'the original prose is preserved verbatim');
    assert.ok(labelled.includes(GROUNDING_WARNING_HEADER));
    assert.match(labelled, /rs776746 was never returned/);
    assert.match(labelled, /CYP3A5 is not a gene in this reference snapshot/);
    assert.match(labelled, /`variants` and `provenance`/);
  });

  it('adds nothing when there is nothing to report', () => {
    assert.equal(appendGroundingWarning('A grounded answer.', []), 'A grounded answer.');
  });
});

/**
 * The scripted-model cases. `askThroughCerebras` is not exported, so these reach it the way a
 * request does — through `askBioinformaticsAgent` with `CEREBRAS_API_KEY` set and the chat
 * endpoint replaced.
 */
describe('askBioinformaticsAgent — grounding the model path', () => {
  const savedKey = process.env.CEREBRAS_API_KEY;
  const savedFetch = globalThis.fetch;

  afterEach(() => {
    if (savedKey === undefined) delete process.env.CEREBRAS_API_KEY;
    else process.env.CEREBRAS_API_KEY = savedKey;
    globalThis.fetch = savedFetch;
  });

  const PROVENANCE: GenotypeProvenance = {
    datasetId: 'ds-serving-001',
    datasetChecksumSha256: 'a'.repeat(64),
    referenceBuild: 'GRCh38',
    referenceVersion: 'demo-clinvar-grch38-v3',
    filesScanned: ['s3://genomic-artifacts/datasets/ds-serving-001/chrom=12/part-000.parquet'],
    targetsResolved: 1,
  };

  /** One row per target, so a two-gene question exercises two separate tool calls. */
  function repositoryOf(rows: Readonly<Record<string, SynthesizedVariant>>): GenotypeRepository {
    return {
      datasetId: PROVENANCE.datasetId,
      async synthesizeVariant(targetId: string): Promise<GenotypeQueryResult> {
        const variant = rows[targetId];
        return {
          targetId,
          variants: variant === undefined ? [] : [variant],
          provenance: PROVENANCE,
          coordinateCoverage: { listed: 1, read: 1 },
        };
      },
    };
  }

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

  function answers(content: string) {
    return { choices: [{ message: { role: 'assistant', content } }] };
  }

  function scriptModel(turns: readonly unknown[]): void {
    let turn = 0;
    globalThis.fetch = (async (url: any) => {
      const target = String(url);
      if (!target.includes('api.cerebras.ai')) {
        throw new Error(`unexpected network call in test: ${target}`);
      }
      const body = turns[Math.min(turn++, turns.length - 1)];
      return { ok: true, status: 200, json: async () => body, text: async () => '' };
    }) as unknown as typeof fetch;
  }

  function ask(question: string, repository: GenotypeRepository) {
    return askBioinformaticsAgent(question, {
      genotypeRepository: repository,
      referenceVocabulary: VOCABULARY,
      callBudget: createCallBudget({ limit: 100, windowMs: 60_000 }),
      searchLiterature: async () => [],
    });
  }

  it('appends the warning and returns the findings for the observed statin fabrication', async () => {
    process.env.CEREBRAS_API_KEY = 'test-key';
    scriptModel([
      callsGenotype('call-1', 'SLCO1B1'),
      answers('You carry CYP3A5 rs776746 C/C, which raises your statin myopathy risk.'),
    ]);

    const response = await ask(
      'Am I at risk of statin-induced muscle pain?',
      repositoryOf({ SLCO1B1 }),
    );

    assert.equal(response.groundingFindings?.length, 2);
    assert.match(response.answer, /rs776746 was never returned by any tool/);
    // The correct row is still returned, unmodified, as the thing the reader should believe.
    assert.deepEqual(response.evidence, [SLCO1B1]);
    assert.ok(response.provenance, 'the fabrication does not cost the answer its provenance');
  });

  it('keeps every variant from every query_genotype call, so a multi-gene answer is grounded', async () => {
    // The bug this closes: only the last non-empty result was kept, so a model that queried two
    // genes had one of them dropped from `variants` while its prose discussed both — and the
    // grounding check would then have called the dropped gene's genotype a contradiction.
    process.env.CEREBRAS_API_KEY = 'test-key';
    scriptModel([
      callsGenotype('call-1', 'SLCO1B1'),
      callsGenotype('call-2', 'CYP1A2'),
      answers(
        'Your SLCO1B1 rs4149056 genotype is T/C. Your CYP1A2 rs762551 genotype is A/A.',
      ),
    ]);

    const response = await ask('Statins and coffee?', repositoryOf({ SLCO1B1, CYP1A2 }));

    assert.deepEqual(response.evidence, [SLCO1B1, CYP1A2]);
    assert.equal(response.groundingFindings, undefined, 'both genes were read, so nothing is unsupported');
    assert.ok(!response.answer.includes(GROUNDING_WARNING_HEADER));
  });

  it('de-duplicates by rsID when the model asks for the same variant twice', async () => {
    process.env.CEREBRAS_API_KEY = 'test-key';
    scriptModel([
      callsGenotype('call-1', 'SLCO1B1'),
      callsGenotype('call-2', 'rs4149056'),
      answers('Your rs4149056 genotype is T/C.'),
    ]);

    const response = await ask(
      'Statin risk?',
      repositoryOf({ SLCO1B1, rs4149056: SLCO1B1 }),
    );

    assert.deepEqual(response.evidence, [SLCO1B1]);
  });

  it('lets a real dataset failure propagate instead of reporting it as an unsupported claim', async () => {
    // The grounding check must never become a place where an S3 or Qdrant outage turns into prose.
    // A failed read is a failed request, exactly as before.
    process.env.CEREBRAS_API_KEY = 'test-key';
    scriptModel([callsGenotype('call-1', 'SLCO1B1'), answers('unreachable')]);

    await assert.rejects(
      () =>
        ask('Am I at risk of statin-induced muscle pain?', {
          datasetId: 'ds-serving-001',
          async synthesizeVariant(): Promise<GenotypeQueryResult> {
            throw new Error('S3 outage: connection reset');
          },
        }),
      /S3 outage/,
    );
  });
});
