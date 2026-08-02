/**
 * Pins question routing against the **shipped** reference table.
 *
 * The vocabulary here is not a fixture: it is read out of
 * `tests/fixtures/clinvar_coordinates_grch38.tsv`, the same committed file
 * `buildReferenceDatabase` loads into the snapshot the serving path resolves against. That is
 * deliberate — the defect this module exists to prevent was a router that disagreed with the
 * table, and a test that routes against its own invented table could not have caught it.
 *
 * No DuckDB, no network, no model: `routeQuestion` is a pure function, which is the property
 * that makes the deterministic local path worth having.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, it } from 'node:test';

import type { ReferenceVocabularyEntry } from '../database/clinvar-coordinate-resolver.ts';
import { REFERENCE_TARGETS, layTermsByGene } from '../database/clinvar-source-records.ts';
import { answerableGenes, conditionTermIndex, routeQuestion } from './question-routing.ts';

const REPO_ROOT = fileURLToPath(new URL('../../../../', import.meta.url));

/** The committed coordinate table, in the shape `ClinVarCoordinateResolver.vocabulary` returns. */
function shippedVocabulary(): readonly ReferenceVocabularyEntry[] {
  const tsv = fs.readFileSync(
    path.join(REPO_ROOT, 'tests/fixtures/clinvar_coordinates_grch38.tsv'),
    'utf8',
  );
  const [header, ...lines] = tsv.trim().split('\n');
  const columns = header!.split('\t');
  return lines.map((line) => {
    const cells = line.split('\t');
    const row = Object.fromEntries(columns.map((column, index) => [column, cells[index]!]));
    return {
      gene: row.gene!,
      rsid: row.rsid!,
      phenotype: row.phenotype!,
      clinicalSignificance: row.clinical_significance!,
    };
  });
}

const VOCABULARY = shippedVocabulary();

/** The target a question resolves to, or a readable description of why it did not. */
function routed(question: string): string {
  const routing = routeQuestion(question, VOCABULARY);
  if (routing.kind === 'resolved') return routing.targetId;
  if (routing.kind === 'ambiguous') return `ambiguous:${[...routing.candidates].sort().join('|')}`;
  return 'unresolved';
}

describe('routing is derived from the table, not from a keyword list', () => {
  it('reaches every gene the table carries, without a branch per gene', () => {
    // The failure this replaces: the table gained CYP2C19, MTHFR and TP53 rows and the router
    // did not, so a genome with real calls at those positions had no way to be asked about them.
    // Each question below is answered by the table's own text or by a declared lay term.
    const byGene: Record<string, string> = {
      APOE: 'Does my Alzheimer dementia risk show up here?',
      BRCA1: 'What is my BRCA1 status?',
      BRCA2: 'Am I predisposed to pancreatic or glioma tumours?',
      CYP1A2: 'Can I drink coffee?',
      CYP2C19: 'Does clopidogrel work for me?',
      CYP2D6: 'Is tamoxifen a good choice for me?',
      F5: 'Am I prone to a blood clot?',
      G6PD: 'Could I get hemolytic anemia from fava beans?',
      LCT: 'Am I lactose intolerant?',
      MTHFR: 'What does my MTHFR variant say?',
      SLCO1B1: 'Will statins give me muscle pain?',
      TP53: 'What about colorectal carcinoma risk?',
      VKORC1: 'Do I need a lower warfarin dose?',
    };

    for (const [gene, question] of Object.entries(byGene)) {
      assert.equal(routed(question), gene, `'${question}' should route to ${gene}`);
    }

    assert.deepEqual(
      answerableGenes(VOCABULARY).filter((gene) => !(gene in byGene)),
      [],
      'every gene in the shipped table must be reachable by some question',
    );
  });

  it('adding a row to the table makes it askable with no code change', () => {
    // The property, stated directly: a gene this file has never heard of, whose only description
    // is the phenotype text a new ClinVar row would carry, routes on that text alone.
    const extended: readonly ReferenceVocabularyEntry[] = [
      ...VOCABULARY,
      {
        gene: 'NAT2',
        rsid: 'rs1801280',
        phenotype: 'Isoniazid response; Sulfamethazine response',
        clinicalSignificance: 'drug response',
      },
    ];

    const routing = routeQuestion('How do I handle isoniazid?', extended);
    assert.equal(routing.kind, 'resolved');
    assert.equal(routing.kind === 'resolved' && routing.targetId, 'NAT2');
    assert.equal(routing.kind === 'resolved' && routing.basis, 'condition');

    // …and the same question against the table without that row is refused, not defaulted.
    assert.equal(routed('How do I handle isoniazid?'), 'unresolved');
  });
});

describe('the explicit handles win outright', () => {
  it('routes an rsID to that rsID, not to its gene', () => {
    // APOE carries two rsIDs; naming one means one of them, and the target has to stay precise.
    const routing = routeQuestion('Tell me about rs7412', VOCABULARY);
    assert.equal(routing.kind === 'resolved' && routing.targetId, 'rs7412');
    assert.equal(routing.kind === 'resolved' && routing.basis, 'rsid');
  });

  it('routes a gene symbol even when lay terms in the same question point elsewhere', () => {
    assert.equal(routed('Does my TP53 result affect my coffee habit?'), 'TP53');
  });

  it('refuses two different rsIDs rather than picking the first', () => {
    assert.equal(routed('Compare rs7412 and rs429358 for me'), 'ambiguous:rs429358|rs7412');
  });
});

describe('the lay-term layer covers only what ClinVar cannot say', () => {
  it('routes the words a patient uses, which appear nowhere in the table', () => {
    for (const [question, gene] of [
      ['Can I drink coffee?', 'CYP1A2'],
      ['Should I avoid dairy?', 'LCT'],
      ['I take a blood thinner — is that a problem?', 'VKORC1'],
      ['Should I be on a statin?', 'SLCO1B1'],
    ] as const) {
      assert.equal(routed(question), gene);
      // The point of the layer: the word is not in the table's own text anywhere.
      const word = question.toLowerCase();
      assert.ok(
        !VOCABULARY.some((entry) =>
          word.split(/[^a-z]+/).some((token) => token.length > 4 && entry.phenotype.toLowerCase().includes(token)),
        ) || gene === 'SLCO1B1',
        `'${question}' should need the lay layer`,
      );
    }
  });

  it('sends the SSRI question to CYP2C19, the gene the guidelines name', () => {
    // This is the routing bug that started this change: the branch chain sent SSRI questions to
    // CYP2D6, which has no call in NA12878 at all, while CYP2C19 — the CPIC gene for
    // citalopram, escitalopram and sertraline — sat in the table with a real het call and no
    // question that could reach it.
    assert.equal(routed('Should I take an SSRI like sertraline?'), 'CYP2C19');
    assert.equal(routed('Which antidepressant suits my genetics?'), 'CYP2C19');
  });

  it('every lay term names a gene the shipped table can actually place', () => {
    const placeable = new Set(answerableGenes(VOCABULARY));
    for (const gene of layTermsByGene().keys()) {
      assert.ok(placeable.has(gene), `lay terms declared for '${gene}', which the table cannot place`);
    }
  });

  it('lay terms live with the targets, so a target can declare its own', () => {
    const cyp2c19 = REFERENCE_TARGETS.find((target) => target.gene === 'CYP2C19');
    assert.ok(cyp2c19?.layTerms?.includes('ssri'));
  });
});

describe('the table’s condition vocabulary is filtered before it is trusted', () => {
  const index = conditionTermIndex(VOCABULARY);

  it('drops terms that more than one gene carries', () => {
    // "Warfarin response" is ClinVar's text for VKORC1 *and* for both APOE rows; "drug response"
    // and "cancer" are shared even more widely. None of them can decide a question, and none of
    // them is in the index — no stopword list was needed to work that out.
    for (const shared of ['warfarin', 'response', 'cancer', 'breast', 'thrombophilia']) {
      assert.equal(index.has(shared), false, `'${shared}' identifies more than one gene`);
    }
  });

  it('keeps the terms that identify exactly one gene', () => {
    assert.equal(index.get('clopidogrel'), 'CYP2C19');
    assert.equal(index.get('tamoxifen'), 'CYP2D6');
    assert.equal(index.get('lactase'), 'LCT');
    assert.equal(index.get('simvastatin'), 'SLCO1B1');
  });

  it('never matches on the placeholder ClinVar uses for "no condition"', () => {
    // CYP1A2's phenotype cell is literally "No condition named in ClinVar for this variant".
    // Its words must not become vocabulary, or every vague question lands on caffeine again —
    // the exact behaviour this change removes.
    for (const word of ['condition', 'named', 'variant', 'clinvar']) {
      assert.equal(index.has(word), false, `'${word}' comes from the no-condition placeholder`);
    }
    assert.equal(routed('What does this variant mean?'), 'unresolved');
  });
});

describe('an unroutable question is refused, never defaulted', () => {
  it('returns unresolved for questions with no genomic content', () => {
    for (const question of [
      'What should I have for lunch?',
      'What is the weather today?',
      'Am I at risk?',
      'What does my genetic data say?',
      'How much protein should I eat?',
      'Do I have acid reflux?',
      '',
      '   ',
    ]) {
      assert.equal(routed(question), 'unresolved', `'${question}' must not resolve`);
    }
  });

  it('reports the candidates when a question fits two genes equally', () => {
    const routing = routeQuestion('Can I drink milk with my statin?', VOCABULARY);
    assert.equal(routing.kind, 'ambiguous');
    assert.deepEqual(routing.kind === 'ambiguous' && [...routing.candidates].sort(), [
      'LCT',
      'SLCO1B1',
    ]);
  });

  it('resolves nothing against an empty vocabulary', () => {
    assert.deepEqual(routeQuestion('Can I drink coffee?', []), { kind: 'unresolved' });
  });
});

describe('matching is symmetric and case-insensitive', () => {
  it('treats singular and plural as the same word on both sides', () => {
    assert.equal(routed('Will a statin hurt?'), 'SLCO1B1');
    assert.equal(routed('Will statins hurt?'), 'SLCO1B1');
    assert.equal(routed('Am I at risk of neural tube defects?'), 'MTHFR');
    assert.equal(routed('Is a neural tube defect likely?'), 'MTHFR');
  });

  it('ignores case and punctuation in gene symbols and rsIDs', () => {
    assert.equal(routed('what about mthfr?'), 'MTHFR');
    assert.equal(routed('RS1042522, please'), 'rs1042522');
  });
});
