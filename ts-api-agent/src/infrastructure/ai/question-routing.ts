/**
 * Decides which reference target a natural-language question is about, from the reference table
 * itself.
 *
 * This replaces a chain of five hardcoded `else if` branches in `agent.ts` that mapped keywords
 * to gene symbols and defaulted to CYP1A2 for everything else. That chain was a second,
 * hand-maintained copy of knowledge the coordinate snapshot already holds, and the two drifted
 * exactly the way hand-kept copies do: the table gained CYP2C19, MTHFR and TP53 rows, the
 * branches did not, so three genes this genome has real calls for became unreachable — while an
 * off-topic question silently received a caffeine answer.
 *
 * So nothing here knows any gene. Everything it matches on is either read out of the snapshot at
 * runtime (`ReferenceVocabularyEntry`) or declared as data next to the table
 * (`ReferenceTarget.layTerms` in `clinvar-source-records.ts`). Adding a row to the table makes
 * that row askable with no change to this file.
 *
 * Two properties this module must not break:
 *
 * - **It picks a target, it never widens a scan.** The output is one `targetId` — a gene symbol
 *   or rsID that the caller passes to the ordinary resolve path, which still enforces the
 *   `(referenceBuild, chrom, pos, ref, alt)` join. "Probably this gene" is not a thing this
 *   module can return; it either names one target or says it could not tell.
 * - **It is deterministic and pure.** No model, no network, no clock. Same question and same
 *   vocabulary in, same routing out — which is the whole point of the local path it serves.
 */
import type { ReferenceVocabularyEntry } from '../database/clinvar-coordinate-resolver.ts';
import {
  NO_CONDITION_PHENOTYPE,
  layTermsByGene,
} from '../database/clinvar-source-records.ts';

/** How a target was reached, so an answer can explain itself and a test can pin the tier. */
export type RoutingBasis = 'rsid' | 'gene-symbol' | 'lay-term' | 'condition';

export type QuestionRouting =
  | {
      readonly kind: 'resolved';
      /** Passed verbatim to the coordinate resolver: a gene symbol or an rsID. */
      readonly targetId: string;
      readonly basis: RoutingBasis;
      /** The words that decided it, lower-cased, in match order. */
      readonly matchedTerms: readonly string[];
    }
  | {
      readonly kind: 'ambiguous';
      /** Two or more equally good targets; the question has to choose. */
      readonly candidates: readonly string[];
      readonly basis: RoutingBasis;
      readonly matchedTerms: readonly string[];
    }
  | { readonly kind: 'unresolved' };

/** ClinVar phenotype cells end with a count of the terms that did not fit; not vocabulary. */
const TRUNCATION_SUFFIX = /\s*\(\+\d+\s+more\s+in\s+clinvar\)\s*$/i;

/** Below this, a token is a connective or an ordinal, never a condition name. */
const MIN_TERM_LENGTH = 4;

/**
 * Words that describe the *shape* of a ClinVar condition name rather than name a condition.
 *
 * ClinVar's `CLNDN` prose is built from a small structural vocabulary — "susceptibility to",
 * "familial", "-related disorder", "response", "risk factor" — plus ordinary English that
 * happens to appear inside a disease label ("cell", "type", "loss", "protein", "acid"). Left in,
 * those words route "am I at risk?" or "how much protein should I eat?" to whichever gene
 * happens to carry them, with total confidence.
 *
 * This is a precision filter over ClinVar's own text, not a second copy of the table: no entry
 * here names a gene, a drug, a variant or a disease, and getting it wrong costs a mis-route that
 * the answer still attributes to the gene it queried — never a wrong genotype.
 *
 * Most generic words never reach this list, because a term shared by two or more genes is
 * dropped automatically as non-discriminating (see `conditionTermIndex`). What remains here is
 * the generic words that happen to occur under exactly one gene in a small table.
 */
const STRUCTURAL_TERMS: ReadonlySet<string> = new Set([
  'acid',
  'activated',
  'bone',
  'cancer',
  'cell',
  'clinvar',
  'condition',
  'deficiency',
  'disease',
  'diseases',
  'disorder',
  'disorders',
  'efficacy',
  'familial',
  'genetic',
  'hereditary',
  'impaired',
  'inborn',
  'inherited',
  'loss',
  'metabolism',
  'more',
  'named',
  'onset',
  'other',
  'poor',
  'predisposition',
  'presenile',
  'primary',
  'process',
  'protein',
  'recurrent',
  'related',
  'resistance',
  'response',
  'risk',
  'sensitive',
  'severe',
  'susceptibility',
  'syndrome',
  'toxicity',
  'tumor',
  'tumour',
  'type',
  'variant',
  'without',
]);

/** Splits any text into lower-case alphanumeric words. */
function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((token) => token.length > 0);
}

/**
 * Crude, symmetric singularisation, applied to both sides of every comparison.
 *
 * "statins" and "statin", "defects" and "defect" are the same word to a reader and have to be
 * the same word here. Applying it to the table's terms as well as the question's is what keeps
 * it from mattering that it is crude: it only has to be consistent, not linguistically right.
 */
function stem(token: string): string {
  return token.length > 4 && token.endsWith('s') && !token.endsWith('ss')
    ? token.slice(0, -1)
    : token;
}

function stemAll(tokens: readonly string[]): Set<string> {
  return new Set(tokens.map(stem));
}

/** Every gene the snapshot can place, de-duplicated, in a stable order. */
export function answerableGenes(
  vocabulary: readonly ReferenceVocabularyEntry[],
): readonly string[] {
  return [...new Set(vocabulary.map((entry) => entry.gene))].sort((a, b) => a.localeCompare(b, 'en'));
}

/**
 * `term -> gene`, keeping only the terms that identify exactly one gene.
 *
 * The discrimination rule is what removes the bulk of ClinVar's generic prose without anybody
 * listing it: "response" occurs under five genes, "cancer" under three, "warfarin" under two, so
 * none of them can decide a question and all three drop out on their own. A term survives only
 * when the table itself makes it unambiguous.
 */
export function conditionTermIndex(
  vocabulary: readonly ReferenceVocabularyEntry[],
): ReadonlyMap<string, string> {
  const genesByTerm = new Map<string, Set<string>>();
  for (const entry of vocabulary) {
    // The placeholder is a statement that ClinVar names no condition — matching on its words
    // would route "what does this variant mean?" to the one row with nothing to say.
    if (entry.phenotype === NO_CONDITION_PHENOTYPE) continue;
    const phenotype = entry.phenotype.replace(TRUNCATION_SUFFIX, '');
    for (const token of tokenize(phenotype)) {
      if (token.length < MIN_TERM_LENGTH) continue;
      if (/^\d+$/.test(token)) continue;
      const term = stem(token);
      if (STRUCTURAL_TERMS.has(term) || STRUCTURAL_TERMS.has(token)) continue;
      const genes = genesByTerm.get(term);
      if (genes === undefined) genesByTerm.set(term, new Set([entry.gene]));
      else genes.add(entry.gene);
    }
  }

  const index = new Map<string, string>();
  for (const [term, genes] of genesByTerm) {
    if (genes.size === 1) index.set(term, [...genes][0]!);
  }
  return index;
}

interface Scored {
  readonly target: string;
  readonly score: number;
  readonly terms: readonly string[];
}

/** Best-scoring targets: one entry when there is a clear winner, several on a tie. */
function best(scores: ReadonlyMap<string, string[]>): readonly Scored[] {
  const ranked: Scored[] = [...scores]
    .map(([target, terms]) => ({ target, score: terms.length, terms }))
    .sort((left, right) => right.score - left.score || left.target.localeCompare(right.target, 'en'));
  if (ranked.length === 0) return [];
  const top = ranked[0]!.score;
  return ranked.filter((candidate) => candidate.score === top);
}

function decide(winners: readonly Scored[], basis: RoutingBasis): QuestionRouting | null {
  if (winners.length === 0) return null;
  if (winners.length === 1) {
    return {
      kind: 'resolved',
      targetId: winners[0]!.target,
      basis,
      matchedTerms: winners[0]!.terms,
    };
  }
  return {
    kind: 'ambiguous',
    candidates: winners.map((winner) => winner.target),
    basis,
    matchedTerms: [...new Set(winners.flatMap((winner) => winner.terms))],
  };
}

export interface RouteQuestionOptions {
  /** Overridable so a test can pin routing behaviour without the shipped lay-term data. */
  readonly layTerms?: ReadonlyMap<string, readonly string[]>;
}

/**
 * Picks the target a question is about, or says it cannot tell.
 *
 * The tiers are ordered by how explicit the evidence is, and the first tier that matches wins:
 *
 * 1. **An rsID in the question.** The most specific handle there is; APOE has two, and a question
 *    naming one means that one.
 * 2. **A gene symbol in the question.** "What does my MTHFR variant say" needs no interpretation.
 * 3. **A curated lay term.** The words patients use that ClinVar's prose cannot contain. This tier
 *    sits *above* the condition tier on purpose: ClinVar names "Warfarin response" under both
 *    VKORC1 and APOE, so the plain drug name cannot decide — but the question is about warfarin
 *    *dosing*, which is VKORC1's variant, and that judgement belongs in declared data.
 * 4. **The table's own condition vocabulary.** Everything ClinVar already says a variant is about:
 *    "Clopidogrel response", "Neural tube defects", "Tamoxifen response". This tier is why adding
 *    a row makes it askable — no code, no synonym, no branch.
 *
 * Anything else is `unresolved`. There is deliberately no default target: answering an off-topic
 * question with whichever gene happens to be first is misleading even when the answer names it.
 */
export function routeQuestion(
  question: string,
  vocabulary: readonly ReferenceVocabularyEntry[],
  options: RouteQuestionOptions = {},
): QuestionRouting {
  const rawTokens = tokenize(question);
  if (rawTokens.length === 0 || vocabulary.length === 0) return { kind: 'unresolved' };
  const stemmed = stemAll(rawTokens);
  const rawSet = new Set(rawTokens);

  // 1. rsIDs.
  const rsids = new Map<string, string>();
  for (const entry of vocabulary) {
    if (entry.rsid !== null) rsids.set(entry.rsid.toLowerCase(), entry.rsid);
  }
  const rsidHits = new Map<string, string[]>();
  for (const token of rawTokens) {
    const canonical = rsids.get(token);
    if (canonical !== undefined) rsidHits.set(canonical, [token]);
  }
  const rsidDecision = decide(best(rsidHits), 'rsid');
  if (rsidDecision !== null) return rsidDecision;

  // 2. Gene symbols. Matched on the raw token: a symbol is an identifier, not a word, and
  //    stemming "APOE"-shaped strings would only invent collisions.
  const geneHits = new Map<string, string[]>();
  for (const gene of answerableGenes(vocabulary)) {
    if (rawSet.has(gene.toLowerCase())) geneHits.set(gene, [gene.toLowerCase()]);
  }
  const geneDecision = decide(best(geneHits), 'gene-symbol');
  if (geneDecision !== null) return geneDecision;

  // 3. Curated lay terms, restricted to genes this snapshot can actually place — a lay term for
  //    a gene the table does not carry must not resolve to something unanswerable.
  const placeable = new Set(vocabulary.map((entry) => entry.gene));
  const layTerms = options.layTerms ?? layTermsByGene();
  const layHits = new Map<string, string[]>();
  for (const [gene, phrases] of layTerms) {
    if (!placeable.has(gene)) continue;
    const matched: string[] = [];
    for (const phrase of phrases) {
      const words = tokenize(phrase).map(stem);
      if (words.length > 0 && words.every((word) => stemmed.has(word))) matched.push(phrase);
    }
    if (matched.length > 0) layHits.set(gene, matched);
  }
  const layDecision = decide(best(layHits), 'lay-term');
  if (layDecision !== null) return layDecision;

  // 4. The table's condition vocabulary.
  const conditionHits = new Map<string, string[]>();
  for (const [term, gene] of conditionTermIndex(vocabulary)) {
    if (!stemmed.has(term)) continue;
    const bucket = conditionHits.get(gene);
    if (bucket === undefined) conditionHits.set(gene, [term]);
    else bucket.push(term);
  }
  const conditionDecision = decide(best(conditionHits), 'condition');
  if (conditionDecision !== null) return conditionDecision;

  return { kind: 'unresolved' };
}
