/**
 * Pins question routing against the **shipped** reference table.
 *
 * The vocabulary here is not a fixture: it is read out of
 * `tests/fixtures/clinvar_coordinates_grch38.tsv`, the same committed file
 * `buildReferenceDatabase` loads into the snapshot the serving path resolves against. That is
 * deliberate — the defect this module exists to prevent was a router that disagreed with the
 * table, and a test that routes against its own invented table could not have caught it. Now that
 * the table is ~14,000 rows, it is also the only way to catch a term that stops discriminating at
 * scale: every question below is routed over the whole shipped vocabulary, not a sample of it.
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
import { FEATURED_TARGETS, layTermsByGene } from '../database/clinvar-source-records.ts';
import {
  answerableGenes,
  answerableSurface,
  conditionTermIndex,
  featuredVocabulary,
  routeQuestion,
} from './question-routing.ts';

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

/**
 * The targets a question resolves to, or a readable description of why it did not.
 *
 * Several targets join with `+` in routing order, so a case that expects one target still reads as
 * one bare symbol — and a case that silently gained a second target fails instead of passing on the
 * first one.
 */
function routed(question: string): string {
  const routing = routeQuestion(question, VOCABULARY);
  if (routing.kind === 'resolved') return routing.targetIds.join('+');
  if (routing.kind === 'ambiguous') return `ambiguous:${[...routing.candidates].sort().join('|')}`;
  return 'unresolved';
}

describe('routing is derived from the table, not from a keyword list', () => {
  it('reaches every featured gene, without a branch per gene', () => {
    // The failure this replaces: the table gained CYP2C19, MTHFR and TP53 rows and the router
    // did not, so a genome with real calls at those positions had no way to be asked about them.
    // Each question below is answered by the table's own text or by a declared lay term.
    //
    // Scoped to the featured genes on purpose. The table carries ~240 symbols now, and a
    // plain-language question is not expected to reach FBN1 or RPGR — those are reached by name.
    // What must hold is that every gene somebody wrote vocabulary for is still reachable.
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

    const featured = new Set(FEATURED_TARGETS.map((target) => target.gene));
    assert.deepEqual(
      answerableGenes(VOCABULARY).filter((gene) => featured.has(gene) && !(gene in byGene)),
      [],
      'every featured gene must be reachable by some plain-language question',
    );
  });

  it('makes every row in the table askable by name with no code change', () => {
    // The property tiers 1 and 2 carry: a coordinate is askable the moment it is in the table, by
    // its rsID and by its gene symbol, with no synonym and no branch. Read off the shipped table
    // rather than invented, and off a row no part of this file mentions.
    const anonymous = VOCABULARY.find(
      (entry) => !FEATURED_TARGETS.some((target) => target.rsid === entry.rsid),
    )!;
    assert.ok(anonymous, 'the shipped table must carry rows beyond the featured ones');

    const byRsid = routeQuestion(`What about ${anonymous.rsid}?`, VOCABULARY);
    assert.deepEqual(byRsid.kind === 'resolved' && [...byRsid.targetIds], [anonymous.rsid]);
    assert.equal(byRsid.kind === 'resolved' && byRsid.basis, 'rsid');

    const byGene = routeQuestion(`What does my ${anonymous.gene} say?`, VOCABULARY);
    assert.deepEqual(byGene.kind === 'resolved' && [...byGene.targetIds], [anonymous.gene]);
    assert.equal(byGene.kind === 'resolved' && byGene.basis, 'gene-symbol');
  });

  it('makes a featured target askable from its own condition text, with no code change', () => {
    // Tier 4's property, and the boundary that moved. A *featured* target routes on the phenotype
    // text ClinVar carries for it — no synonym, no branch. A row that is merely in the table does
    // not: inferring a gene from a symptom over 14,000 rows of ClinVar prose routed "what does
    // this variant mean?" to GP1BB, so the inferring tiers see the featured rows only.
    const target = { rsid: 'rs1801280', gene: 'NAT2' };
    const extended: readonly ReferenceVocabularyEntry[] = [
      ...VOCABULARY,
      {
        gene: target.gene,
        rsid: target.rsid,
        phenotype: 'Isoniazid response; Sulfamethazine response',
        clinicalSignificance: 'drug response',
      },
    ];
    const featuredTargets = [...FEATURED_TARGETS, target];

    const routing = routeQuestion('How do I handle isoniazid?', extended, { featuredTargets });
    assert.equal(routing.kind, 'resolved');
    assert.deepEqual(routing.kind === 'resolved' && [...routing.targetIds], ['NAT2']);
    assert.equal(routing.kind === 'resolved' && routing.basis, 'condition');

    // In the table but not featured: still askable by name, not from the symptom.
    assert.equal(routeQuestion('How do I handle isoniazid?', extended).kind, 'unresolved');

    // …and the same question against a table without that row at all is refused, not defaulted.
    assert.equal(routed('How do I handle isoniazid?'), 'unresolved');
  });
});

describe('the explicit handles win outright', () => {
  it('routes an rsID to that rsID, not to its gene', () => {
    // APOE carries two rsIDs; naming one means that one, and the target has to stay precise.
    const routing = routeQuestion('Tell me about rs7412', VOCABULARY);
    assert.deepEqual(routing.kind === 'resolved' && [...routing.targetIds], ['rs7412']);
    assert.equal(routing.kind === 'resolved' && routing.basis, 'rsid');
  });

  it('routes a gene symbol even when lay terms in the same question point elsewhere', () => {
    assert.equal(routed('Does my TP53 result affect my coffee habit?'), 'TP53');
  });

  it('answers both rsIDs when the question names both, instead of calling that a tie', () => {
    // The defect this replaces: "What is my APOE genotype (rs429358 and rs7412)?" came back as
    // "could be about rs429358 or rs7412 … so I did not guess". Both rsIDs are real, both are
    // APOE, and the APOE genotype *is* the pair — so the one question this gene needs was the one
    // question the router refused. Ambiguity is "I cannot tell which you meant"; a question that
    // writes out two markers has said which.
    const routing = routeQuestion('What is my APOE genotype (rs429358 and rs7412)?', VOCABULARY);
    assert.equal(routing.kind, 'resolved');
    assert.deepEqual(routing.kind === 'resolved' && [...routing.targetIds], [
      'rs429358',
      'rs7412',
    ]);
    assert.equal(routing.kind === 'resolved' && routing.basis, 'rsid');
  });

  it('answers two named genes, without inferring a third', () => {
    const routing = routeQuestion('Does my CYP2C19 affect warfarin (VKORC1)?', VOCABULARY);
    assert.equal(routing.kind, 'resolved');
    // Two symbols in, two targets out. "warfarin" is ClinVar text under VKORC1 *and* both APOE
    // rows, and it must not add APOE to a list the question wrote out itself: tier 2 matching at
    // all means the inferring tiers never run.
    assert.deepEqual(routing.kind === 'resolved' && [...routing.targetIds], [
      'CYP2C19',
      'VKORC1',
    ]);
    assert.equal(routing.kind === 'resolved' && routing.basis, 'gene-symbol');
  });

  it('reports named targets in the order the question named them', () => {
    // The answer is read next to the question, so the order has to be the user's, not the table's
    // or the alphabet's — the same two markers reversed in the question come back reversed.
    assert.equal(routed('Compare rs7412 and rs429358 for me'), 'rs7412+rs429358');
    assert.equal(routed('Compare rs429358 and rs7412 for me'), 'rs429358+rs7412');
  });

  it('counts a target named twice once', () => {
    // A repeated mention is emphasis, not a second read of the same coordinate.
    assert.equal(routed('rs7412 — and what about rs7412 exactly?'), 'rs7412');
    assert.equal(routed('MTHFR, and again MTHFR'), 'MTHFR');
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
      // The point of the layer: the word is nowhere in the text the condition tier can see, so
      // without the lay layer the question has nothing to match. Checked against the inferring
      // scope (`featuredVocabulary`) rather than all ~14,000 rows: somewhere in ClinVar's whole
      // condition vocabulary almost any English word occurs, and that says nothing about whether
      // *this* question could have been routed without the curated phrase.
      const words = question.toLowerCase().split(/[^a-z]+/);
      assert.ok(
        !featuredVocabulary(VOCABULARY).some((entry) =>
          words.some(
            (token) => token.length > 4 && entry.phenotype.toLowerCase().includes(token),
          ),
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
    const cyp2c19 = FEATURED_TARGETS.find((target) => target.gene === 'CYP2C19');
    assert.ok(cyp2c19?.layTerms?.includes('ssri'));
  });
});

describe('the condition vocabulary is scoped and filtered before it is trusted', () => {
  const index = conditionTermIndex(featuredVocabulary(VOCABULARY));

  it('drops terms that more than one featured gene carries', () => {
    // "Warfarin response" is ClinVar's text for VKORC1 *and* for both APOE rows; "drug response"
    // and "cancer" are shared even more widely. None of them can decide a question, and none of
    // them is in the index — no stopword list was needed to work that out.
    for (const shared of ['warfarin', 'response', 'cancer', 'breast', 'thrombophilia']) {
      assert.equal(index.has(shared), false, `'${shared}' identifies more than one gene`);
    }
  });

  it('keeps the terms that identify exactly one featured gene', () => {
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

  it('is scoped to the featured rows, because the whole table breaks both directions', () => {
    // The measurement that decided the scope, kept as a test so the decision cannot be silently
    // undone. Over the whole ~14,000-row table the same discrimination rule:
    //
    //   - loses `clopidogrel` (CYP2C19, ABCB1, P2RY12 … all carry it), so the pharmacogenomic
    //     question this demo exists to answer stops resolving; and
    //   - gains `mean`, from the one gene whose ClinVar prose says "Increased mean platelet
    //     volume", so "what does this variant mean?" resolves — confidently, to GP1BB.
    //
    // Both are the rule working correctly on text that was never vocabulary. Restricting the scope
    // is the fix; shrinking the table is not, because the table is what makes coordinates
    // placeable.
    const wholeTable = conditionTermIndex(VOCABULARY);
    assert.equal(wholeTable.has('clopidogrel'), false);
    assert.equal(wholeTable.get('mean'), 'GP1BB');

    assert.equal(index.get('clopidogrel'), 'CYP2C19');
    assert.equal(index.has('mean'), false);

    assert.equal(routed('Am I a poor clopidogrel metabolizer?'), 'CYP2C19');
    assert.equal(routed('Am I at risk of neural tube defects?'), 'MTHFR');
    assert.equal(routed('What does this variant mean?'), 'unresolved');
  });

  it('scopes by rsID, not by gene, so a featured gene’s other rows stay out', () => {
    // Filtering by gene would pull in all 2,714 BRCA2 rows and all 2,271 BRCA1 rows, and
    // "colorectal carcinoma" — which several of them name — would identify no single gene again.
    const scoped = featuredVocabulary(VOCABULARY);
    assert.ok(
      scoped.length <= FEATURED_TARGETS.length + 2,
      `the inferring scope grew to ${scoped.length} rows; it must stay the featured rows`,
    );
    assert.equal(routed('What about colorectal carcinoma risk?'), 'TP53');
    assert.equal(routed('Am I predisposed to pancreatic or glioma tumours?'), 'BRCA2');
  });
});

describe('what a refusal may promise', () => {
  it('names the featured genes and counts the rest, instead of listing 238 symbols', () => {
    const surface = answerableSurface(VOCABULARY);

    assert.deepEqual(
      [...surface.featured],
      [...new Set(FEATURED_TARGETS.map((target) => target.gene))].sort((left, right) =>
        left.localeCompare(right, 'en'),
      ),
    );
    // The rest is a count, not a list: `couldNotRouteAnswer` used to paste `answerableGenes` into
    // a sentence, and at 238 symbols that sentence is unreadable.
    assert.ok(surface.featured.length < 20, 'the featured list is what a sentence can carry');
    assert.ok(
      surface.otherVariantCount > 1000,
      'the count must describe the machine-selected table, not the featured rows',
    );
    assert.equal(
      surface.featured.length + answerableGenes(VOCABULARY).length > 20,
      true,
      'the table itself is much wider than the featured list',
    );
  });

  it('offers no featured gene the opened snapshot cannot place', () => {
    // A snapshot is opened at run time and may be older than this code. Reading the featured list
    // straight out would advertise a gene that resolves to nothing.
    const onlyOne = VOCABULARY.filter((entry) => entry.gene === 'MTHFR');
    assert.deepEqual([...answerableSurface(onlyOne).featured], ['MTHFR']);
    assert.equal(answerableSurface(onlyOne).otherVariantCount, 0);
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

  it('does not widen an inferred tie into a multi-target answer', () => {
    // The explicit tiers now return every target a question names; this tier must not copy that.
    // Nobody wrote CYP1A2 or LCT here — "milk" and "statin" are a food and a drug, and the genes
    // are this module's inference. Reading both would answer a question the user did not ask while
    // presenting it as the one they did.
    const routing = routeQuestion('Can I drink milk with my statin?', VOCABULARY);
    assert.equal(routing.kind, 'ambiguous');
    assert.equal('targetIds' in routing, false, 'an inferred tie selects nothing to read');
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
