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
 * that row askable *by name* — its rsID and its gene symbol — with no change to this file.
 *
 * ## What the two scopes are, now that the table is ~14,000 rows
 *
 * The explicit tiers (rsID, gene symbol) run against the whole table: naming a target is a
 * request, and every coordinate the snapshot holds can be named. The inferring tiers (lay terms,
 * condition text) run against the **featured** targets only, because inference over 14,000 rows
 * of ClinVar prose is measurably worse than inference over the featured ones:
 *
 * - "Am I a poor clopidogrel metabolizer?" stops resolving. `clopidogrel` appears under CYP2C19,
 *   ABCB1, P2RY12 and others, so the discrimination rule drops it as non-discriminating and the
 *   question this demo is *about* becomes unanswerable.
 * - "What does this variant mean?" starts resolving — to GP1BB, confidently. That is precisely the
 *   silent-default failure this module was written to delete, re-created by scale.
 *
 * Restricting the inferring tiers is what keeps them honest: a symptom is only routed to a gene
 * somebody deliberately connected to that symptom. See the tier list on `routeQuestion`.
 *
 * ## Two properties this module must not break
 *
 * - **It selects targets, it never widens a scan.** The output is `targetIds` — gene symbols or
 *   rsIDs the caller passes one at a time to the ordinary resolve path, which still enforces the
 *   `(referenceBuild, chrom, pos, ref, alt)` join. More than one target only ever comes from the
 *   question spelling out more than one: "probably this gene" is not a thing this module can
 *   return, and a target it *inferred* is one target or an admission that it could not tell.
 * - **It is deterministic and pure.** No model, no network, no clock. Same question and same
 *   vocabulary in, same routing out — which is the whole point of the local path it serves.
 */
import type { ReferenceVocabularyEntry } from '../database/clinvar-coordinate-resolver.ts';
import {
  FEATURED_TARGETS,
  NO_CONDITION_PHENOTYPE,
  type ReferenceTarget,
  layTermsByGene,
} from '../database/clinvar-source-records.ts';

/** How a target was reached, so an answer can explain itself and a test can pin the tier. */
export type RoutingBasis = 'rsid' | 'gene-symbol' | 'lay-term' | 'condition';

export type QuestionRouting =
  | {
      readonly kind: 'resolved';
      /**
       * Passed verbatim to the coordinate resolver, one target at a time: gene symbols or rsIDs.
       *
       * Never empty, and in the order the question named them — an answer that reports two markers
       * in the order the user wrote them is one they can read back against their own question.
       * Several entries only ever come from the explicit tiers; see `routeQuestion`.
       */
      readonly targetIds: readonly string[];
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

/**
 * Every gene the snapshot can place, de-duplicated, in a stable order.
 *
 * Around 240 symbols now, so this is a *matching* surface (tier 2) and not something to read out
 * to a user; `answerableSurface` is what a message is written from.
 */
export function answerableGenes(
  vocabulary: readonly ReferenceVocabularyEntry[],
): readonly string[] {
  return [...new Set(vocabulary.map((entry) => entry.gene))].sort((a, b) => a.localeCompare(b, 'en'));
}

/**
 * What a message may honestly promise: the genes a symptom reaches, and how much else is placeable.
 *
 * Two numbers, because the snapshot answers two different kinds of question and a sentence that
 * merges them misleads either way. `couldNotRouteAnswer` in `agent.ts` used to list
 * `answerableGenes` verbatim; at 238 genes that sentence is a wall of symbols nobody reads, and at
 * 14,000 rows "this snapshot can answer about …" followed by 13 genes would be an undercount of
 * everything else it can place.
 */
export interface AnswerableSurface {
  /** Reachable from a plain-language question, because they carry curated vocabulary. */
  readonly featured: readonly string[];
  /** Placeable coordinates outside the featured genes; reachable by gene symbol or rsID. */
  readonly otherVariantCount: number;
}

export function answerableSurface(
  vocabulary: readonly ReferenceVocabularyEntry[],
  targets: readonly ReferenceTarget[] = FEATURED_TARGETS,
): AnswerableSurface {
  // Intersected with the snapshot, not read straight off the target list: a featured gene the
  // opened snapshot cannot place must not be offered as an answer.
  const placeable = new Set(vocabulary.map((entry) => entry.gene));
  const featured = new Set(
    targets.map((target) => target.gene).filter((gene) => placeable.has(gene)),
  );
  return {
    featured: [...featured].sort((left, right) => left.localeCompare(right, 'en')),
    otherVariantCount: vocabulary.filter((entry) => !featured.has(entry.gene)).length,
  };
}

/**
 * The vocabulary rows the featured targets contribute — the scope the inferring tiers infer over.
 *
 * Matched on the rsID, which is the only handle a `ReferenceVocabularyEntry` shares with a
 * `ReferenceTarget` (the entry is deliberately coordinate-free). Filtering by *gene* instead would
 * pull in every other row of BRCA1, BRCA2, TP53 and CYP2D6 — 5,300 rows rather than 15 — and
 * "colorectal carcinoma" would go back to naming three genes at once and therefore none.
 */
export function featuredVocabulary(
  vocabulary: readonly ReferenceVocabularyEntry[],
  targets: readonly ReferenceTarget[] = FEATURED_TARGETS,
): readonly ReferenceVocabularyEntry[] {
  const featuredRsids = new Set(targets.map((target) => target.rsid.toLowerCase()));
  return vocabulary.filter(
    (entry) => entry.rsid !== null && featuredRsids.has(entry.rsid.toLowerCase()),
  );
}

/**
 * `term -> gene`, keeping only the terms that identify exactly one gene.
 *
 * The discrimination rule is what removes the bulk of ClinVar's generic prose without anybody
 * listing it: "response" occurs under five genes, "cancer" under three, "warfarin" under two, so
 * none of them can decide a question and all three drop out on their own. A term survives only
 * when the table itself makes it unambiguous.
 *
 * Fed the *featured* rows (`featuredVocabulary`), not the whole table. Over the whole table the
 * same rule keeps 629 terms and loses the ones that matter: `clopidogrel` occurs under several
 * genes and drops out, so "am I a poor clopidogrel metabolizer?" resolves to nothing — while
 * "what does this variant mean?" resolves to GP1BB, because exactly one gene's ClinVar prose says
 * "Increased **mean** platelet volume". Both failures are the discrimination rule working as
 * designed on text that was never vocabulary. Precision over a curated set beats recall over an
 * uncurated one when the output is a claim about a person's genome.
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

/**
 * The explicit tiers' decision: every target the question spelled out, in the order it spelled it.
 *
 * Never `ambiguous`. Ambiguity means "I cannot tell which of these you meant", and a question that
 * writes out `rs429358 and rs7412` has said which: both. Scoring that as a tie made the one
 * question APOE actually needs — it has two markers and neither alone is the genotype — the one
 * question the deterministic path refused to answer, while insisting both rsIDs were real.
 *
 * Widening here cannot widen a scan either: each target still goes through the resolver's exact
 * coordinate join on its own, so this returns the user's own list, not a guess at a bigger one.
 */
function namedTargets(
  hits: ReadonlyMap<string, string>,
  basis: RoutingBasis,
): QuestionRouting | null {
  if (hits.size === 0) return null;
  return {
    kind: 'resolved',
    targetIds: [...hits.keys()],
    basis,
    matchedTerms: [...hits.values()],
  };
}

/**
 * The inferring tiers' decision: one winner, or an admission that the words fit several genes.
 *
 * Deliberately *not* the rule above. These tiers derive a target from a symptom or a drug the user
 * never connected to a gene, so widening a tie to every candidate would answer "can I drink milk
 * with my coffee?" by reading CYP1A2 and LCT and presenting the pair as though it had been asked
 * for — an inference wearing the clothes of a request.
 */
function inferredTarget(winners: readonly Scored[], basis: RoutingBasis): QuestionRouting | null {
  if (winners.length === 0) return null;
  if (winners.length === 1) {
    return {
      kind: 'resolved',
      targetIds: [winners[0]!.target],
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
  /**
   * The targets the inferring tiers may infer *to*. Defaults to the shipped featured list.
   *
   * Overridable for the same reason `layTerms` is: a test needs to state the property ("a featured
   * target's own condition text routes it") without depending on which variants happen to be
   * featured today.
   */
  readonly featuredTargets?: readonly ReferenceTarget[];
}

/**
 * Picks the target a question is about, or says it cannot tell.
 *
 * The tiers are ordered by how explicit the evidence is, and the first tier that matches wins:
 *
 * 1. **rsIDs in the question.** The most specific handle there is; APOE has two, and a question
 *    naming both means both.
 * 2. **Gene symbols in the question.** "What does my MTHFR variant say" needs no interpretation,
 *    and neither does "does my CYP2C19 affect warfarin (VKORC1)?" — that is two questions written
 *    as one, not a doubt about which gene was meant.
 * 3. **A curated lay term.** The words patients use that ClinVar's prose cannot contain. This tier
 *    sits *above* the condition tier on purpose: ClinVar names "Warfarin response" under both
 *    VKORC1 and APOE, so the plain drug name cannot decide — but the question is about warfarin
 *    *dosing*, which is VKORC1's variant, and that judgement belongs in declared data.
 * 4. **The featured rows' own condition vocabulary.** Everything ClinVar already says those
 *    variants are about: "Clopidogrel response", "Neural tube defects", "Tamoxifen response".
 *    Featuring a variant is what makes its condition text askable — no code, no synonym, no branch.
 *
 * Tiers 1 and 2 see the whole table; tiers 3 and 4 see the featured rows only (see the module
 * comment for the measurements). So a coordinate in the machine-selected table is reachable the
 * moment it is in the table — by its rsID or its gene symbol — while reaching it from a *symptom*
 * takes an editorial decision, which is exactly the difference between the two lists.
 *
 * The two explicit tiers return *every* target they find; the two inferring tiers return one or
 * confess a tie. That asymmetry is the whole rule: naming a target is a request, and honouring two
 * requests is not guessing — while inferring two targets from one symptom would turn a question
 * into a scan the user never asked for.
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

  // 1. rsIDs. Iterated over the question's tokens rather than the vocabulary, so a question that
  //    names two markers is answered in the order it named them; a repeated mention keeps its
  //    first position instead of becoming a second target.
  const rsids = new Map<string, string>();
  for (const entry of vocabulary) {
    if (entry.rsid !== null) rsids.set(entry.rsid.toLowerCase(), entry.rsid);
  }
  const rsidHits = new Map<string, string>();
  for (const token of rawTokens) {
    const canonical = rsids.get(token);
    if (canonical !== undefined && !rsidHits.has(canonical)) rsidHits.set(canonical, token);
  }
  const rsidDecision = namedTargets(rsidHits, 'rsid');
  if (rsidDecision !== null) return rsidDecision;

  // 2. Gene symbols, in the question's order for the same reason. Matched on the raw token: a
  //    symbol is an identifier, not a word, and stemming "APOE"-shaped strings would only invent
  //    collisions.
  const genesByToken = new Map<string, string>();
  for (const gene of answerableGenes(vocabulary)) genesByToken.set(gene.toLowerCase(), gene);
  const geneHits = new Map<string, string>();
  for (const token of rawTokens) {
    const gene = genesByToken.get(token);
    if (gene !== undefined && !geneHits.has(gene)) geneHits.set(gene, token);
  }
  const geneDecision = namedTargets(geneHits, 'gene-symbol');
  if (geneDecision !== null) return geneDecision;

  // 3. Curated lay terms, restricted to genes this snapshot can actually place — a lay term for
  //    a gene the table does not carry must not resolve to something unanswerable.
  const featured = options.featuredTargets ?? FEATURED_TARGETS;
  const placeable = new Set(vocabulary.map((entry) => entry.gene));
  const layTerms = options.layTerms ?? layTermsByGene(featured);
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
  const layDecision = inferredTarget(best(layHits), 'lay-term');
  if (layDecision !== null) return layDecision;

  // 4. The featured rows' condition vocabulary. Scoped, not whole-table: over 14,000 rows this
  //    tier loses `clopidogrel` to the discrimination rule and gains `mean` (from "Increased mean
  //    platelet volume"), which answers "what does this variant mean?" with a gene.
  const conditionHits = new Map<string, string[]>();
  for (const [term, gene] of conditionTermIndex(featuredVocabulary(vocabulary, featured))) {
    if (!stemmed.has(term)) continue;
    const bucket = conditionHits.get(gene);
    if (bucket === undefined) conditionHits.set(gene, [term]);
    else bucket.push(term);
  }
  const conditionDecision = inferredTarget(best(conditionHits), 'condition');
  if (conditionDecision !== null) return conditionDecision;

  return { kind: 'unresolved' };
}
