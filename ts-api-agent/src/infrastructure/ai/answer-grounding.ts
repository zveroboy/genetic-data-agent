/**
 * Checks a model's prose against what the tools actually returned, and says where they disagree.
 *
 * This exists because of one observed exchange, not a hypothetical. Asked "am I at risk of
 * statin-induced muscle pain?", the model called `query_genotype`, got the correct SLCO1B1 row
 * back, and then wrote an answer about `CYP3A5 rs776746 C/C` — a gene absent from the reference
 * snapshot, an rsID absent from the dataset, a genotype invented whole. The right variant was
 * sitting in the same HTTP response, in `variants`, while the prose contradicted it. The system
 * prompt's first directive is "You must NOT invent or hallucinate genetic variants"; a prompt is
 * not an enforcement mechanism, and nothing downstream of it was checking.
 *
 * Two boundaries this module keeps:
 *
 * - **Pure.** No network, no I/O, no clock, and no import of `agent.ts` — the answer text and the
 *   tool output are the whole input. That is what makes the observed fabrication reproducible in a
 *   unit test instead of only in a paid live call.
 * - **It reports, it does not rewrite.** See `appendGroundingWarning`.
 */
import type { SynthesizedVariant } from '../../domain/types.ts';
import type { ReferenceVocabularyEntry } from '../database/clinvar-coordinate-resolver.ts';

/** Ordered by how badly the claim misleads a reader, worst first. */
export type GroundingFindingKind =
  /** An rsID in the prose that no tool result mentioned. */
  | 'unsupported-rsid'
  /**
   * A symbol the reference snapshot cannot place and no tool returned, carrying a variant-level
   * claim — an rsID nobody read, or a genotype belonging to no read variant. A bare unknown symbol
   * in explanatory prose is domain vocabulary, not a finding.
   */
  | 'unknown-gene'
  /** A genotype attached to a variant the tools did return, with alleles that disagree. */
  | 'contradicted-genotype';

export interface GroundingFinding {
  readonly kind: GroundingFindingKind;
  /** The token as the answer spelled it, so a reader can find the sentence it came from. */
  readonly mentioned: string;
  /** One sentence naming what is unsupported. This is what the appended warning prints. */
  readonly detail: string;
}

export interface GroundingFacts {
  /**
   * Every variant `query_genotype` returned this request, accumulated across all its calls.
   * These are the only genotypes anything in the answer may claim.
   */
  readonly variants: readonly SynthesizedVariant[];
  /**
   * The askable surface of the opened reference snapshot — the full universe of genes and rsIDs
   * this system can know anything about. A symbol outside it cannot be a near miss.
   */
  readonly referenceVocabulary: readonly ReferenceVocabularyEntry[];
  /**
   * What the tools said, verbatim and serialized — absence notes, paper titles, abstracts.
   *
   * Included so a symbol or rsID the model is *quoting back* from a tool result is never reported
   * as invented: the model asking about G6PD and being told "'G6PD' is not present in reference
   * snapshot …" must be free to repeat that sentence, and a paper that names CYP2D6 in its title
   * makes CYP2D6 something the tools put in front of the model. The cost is that a fabrication
   * which happens to collide with a retrieved abstract goes unflagged, which is the right way for
   * this trade to fail — see the false-positive note on `looksLikeGeneSymbol`.
   */
  readonly toolResultText?: readonly string[];
}

/** rsIDs are unambiguous tokens; case varies in prose, the canonical spelling does not. */
const RSID_PATTERN = /\brs\d+\b/gi;

/** Anything that could be a symbol before the shape test below decides whether it is. */
const WORD_PATTERN = /\b[A-Za-z][A-Za-z0-9]{1,9}\b/g;

/**
 * Upper-case, at least two leading letters, at least one digit, 3–10 characters.
 *
 * The second of the two filters on an unknown symbol, and the weaker one — claim adjacency (below)
 * is what actually decides. It is kept anyway, and the reason is the sentence that survives
 * adjacency: "You carry rs776746 C/C, which reduces INR stability." carries a genuinely invented
 * rsID, so the sentence *does* hold an unaccounted claim, and without a shape test `INR` would be
 * named as an invented gene next to it. The warning would be right about the fabrication and wrong
 * about what to call it. The digit is what separates the two: `DNA`, `RNA`, `INR`, `PK`, `AI`,
 * `SNP`, `HDL`, `VCF` and every other all-letter acronym fail it, as do ordinary capitalised prose
 * (`Based`, `Your`) and `B12` (one leading letter — vitamin B12 is named in MTHFR answers). Mixed
 * case fails too, which keeps `GRCh38` and `HbA1c` out.
 *
 * Reconsidered once adjacency was in place, and deliberately kept. Dropping it would buy one case —
 * an invented digit-less symbol carrying a bare genotype, `Your TPMT genotype is A/A.` — and would
 * sell every clinical acronym that happens to share a sentence with a real fabrication. This module
 * exists to be believed when it fires; a warning that misnames `INR` as a gene is not believed.
 */
const GENE_SYMBOL_SHAPE = /^[A-Z]{2,}[A-Z0-9]*[0-9][A-Z0-9]*$/;

function looksLikeGeneSymbol(token: string): boolean {
  if (token.length < 3 || token.length > 10) return false;
  // `RS776746` has the shape but is an rsID; reporting it twice reads as two fabrications.
  if (/^RS\d+$/.test(token)) return false;
  return GENE_SYMBOL_SHAPE.test(token);
}

/**
 * A diploid call written with a separator: `C/T`, `A/A`.
 *
 * Restricted to ACGT alleles on purpose. `N/A` in prose is not a genotype, and the raw-GT
 * fallback spellings (`0/1`) carry no allele letters to compare against.
 */
const GENOTYPE_PATTERN = /\b([ACGT]{1,8})\s*\/\s*([ACGT]{1,8})\b/g;

/**
 * How far a variant-level claim may sit from a symbol and still be a claim about that symbol.
 *
 * Used for both attributions: a genotype to the variant it describes, and an unaccounted claim to
 * the unknown symbol it implicates.
 */
const CLAIM_ATTRIBUTION_WINDOW = 60;

/**
 * Sentence ends, used to keep a genotype from being attributed across one.
 *
 * Distance alone is not enough, and the case that proves it turned up while writing these tests:
 * "Your SLCO1B1 rs4149056 genotype is T/C. Your CYP1A2 rs762551 genotype is A/A." puts `CYP1A2`
 * seven characters after `T/C` and `rs4149056` thirteen before it, so nearest-mention alone reads a
 * correct two-gene answer as a contradiction. A claim and the variant it is about are written in one
 * sentence; that boundary is the cheapest signal that says so.
 *
 * The lookahead is what keeps `similarity 0.64` and `GRCh38.p14` from being read as two sentences.
 */
const SENTENCE_END = /[.!?](?=\s|$)|\n+/g;

/** Sentence ordinal of each offset, so two spans can be asked whether they share a sentence. */
function sentenceIndexer(text: string): (offset: number) => number {
  const ends = matchAll(text, SENTENCE_END).map((match) => match.end);
  return (offset: number) => ends.filter((end) => end <= offset).length;
}

/** Alleles are a set, not a sequence: `T/C` and `C/T` are the same call. */
function normalizeGenotype(genotype: string): string | null {
  const match = /^\s*([ACGT]{1,8})\s*\/\s*([ACGT]{1,8})\s*$/i.exec(genotype);
  if (match === null) return null;
  return [match[1]!.toUpperCase(), match[2]!.toUpperCase()].sort().join('/');
}

function matchAll(text: string, pattern: RegExp): { value: string; start: number; end: number }[] {
  const found: { value: string; start: number; end: number }[] = [];
  for (const match of text.matchAll(new RegExp(pattern.source, pattern.flags))) {
    const start = match.index ?? 0;
    found.push({ value: match[0], start, end: start + match[0].length });
  }
  return found;
}

/** Every case-insensitive occurrence of `needle` as a whole word. */
function mentionsOf(text: string, needle: string): { start: number; end: number }[] {
  const escaped = needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return matchAll(text, new RegExp(`\\b${escaped}\\b`, 'gi')).map(({ start, end }) => ({
    start,
    end,
  }));
}

/** Gap between two spans, zero when they touch or overlap. */
function gap(a: { start: number; end: number }, b: { start: number; end: number }): number {
  if (a.end <= b.start) return b.start - a.end;
  if (b.end <= a.start) return a.start - b.end;
  return 0;
}

/**
 * What the answer claims that the tool results do not support.
 *
 * Empty means "nothing in the prose contradicts the payload" — not "the answer is correct". This
 * compares strings against retrieved rows; it has no opinion on clinical reasoning.
 */
export function checkAnswerGrounding(
  answer: string,
  facts: GroundingFacts,
): readonly GroundingFinding[] {
  if (answer.length === 0) return [];

  const toolText = (facts.toolResultText ?? []).join('\n');
  const toolRsids = new Set(
    matchAll(toolText, RSID_PATTERN).map((match) => match.value.toLowerCase()),
  );
  for (const variant of facts.variants) {
    if (variant.rsid.length > 0) toolRsids.add(variant.rsid.toLowerCase());
  }

  // The candidate universe for symbols: what the reference can place, plus every word the tools
  // actually put in front of the model. A symbol in here is known and is never reported, however
  // the answer used it.
  const knownSymbols = new Set<string>();
  for (const entry of facts.referenceVocabulary) knownSymbols.add(entry.gene.toUpperCase());
  for (const variant of facts.variants) knownSymbols.add(variant.gene.toUpperCase());
  for (const word of matchAll(toolText, WORD_PATTERN)) knownSymbols.add(word.value.toUpperCase());

  const findings: GroundingFinding[] = [];
  const reported = new Set<string>();
  const report = (finding: GroundingFinding): void => {
    const key = `${finding.kind}:${finding.mentioned.toUpperCase()}:${finding.detail}`;
    if (reported.has(key)) return;
    reported.add(key);
    findings.push(finding);
  };

  /**
   * Variant-level claims the returned rows do not account for: an rsID nobody read, or a genotype
   * with no read variant to belong to. These are what make a symbol beside them suspicious.
   */
  const unaccountedClaims: { value: string; start: number; end: number }[] = [];

  // 1. rsIDs no tool result contained. The most specific handle in the domain, so an invented one
  //    is the most specific possible lie: it reads as a position that was looked up.
  for (const mention of matchAll(answer, RSID_PATTERN)) {
    if (toolRsids.has(mention.value.toLowerCase())) continue;
    unaccountedClaims.push(mention);
    report({
      kind: 'unsupported-rsid',
      mentioned: mention.value,
      detail:
        `${mention.value} was never returned by any tool for this question — no variant with ` +
        'that rsID was read from your dataset.',
    });
  }

  // 2. Genotypes attached to a variant the tools *did* return, with alleles that disagree. Each
  //    genotype in the prose is attributed to the nearest variant named in the same sentence,
  //    rather than to every variant in the answer, so an answer that correctly quotes two genes
  //    one after the other does not report each one's call as a contradiction of the other's.
  //
  //    Computed before the symbol rule below, which consumes what this one leaves unaccounted.
  const sentenceOf = sentenceIndexer(answer);
  const named: { variant: SynthesizedVariant; span: { start: number; end: number } }[] = [];
  for (const variant of facts.variants) {
    for (const handle of [variant.rsid, variant.gene]) {
      if (handle.length === 0) continue;
      for (const span of mentionsOf(answer, handle)) named.push({ variant, span });
    }
  }
  for (const claim of matchAll(answer, GENOTYPE_PATTERN)) {
    const claimed = normalizeGenotype(claim.value);
    if (claimed === null) continue;
    let nearest: { variant: SynthesizedVariant; distance: number } | null = null;
    for (const candidate of named) {
      if (sentenceOf(candidate.span.start) !== sentenceOf(claim.start)) continue;
      const distance = gap(claim, candidate.span);
      if (distance > CLAIM_ATTRIBUTION_WINDOW) continue;
      if (nearest === null || distance < nearest.distance) {
        nearest = { variant: candidate.variant, distance };
      }
    }
    // An unattributable genotype is not reported as a contradiction — there is nothing for it to
    // contradict, and guessing which real variant it was "meant" to be is how a check starts
    // inventing claims of its own. It is recorded as unaccounted instead, because a genotype
    // belonging to no variant anybody read is exactly the claim a fabricated symbol carries.
    if (nearest === null) {
      unaccountedClaims.push(claim);
      continue;
    }
    const actual = normalizeGenotype(nearest.variant.userGenotype);
    // An uncomparable `userGenotype` — the raw `0/1` fallback — is not evidence of disagreement.
    if (actual === null || actual === claimed) continue;
    report({
      kind: 'contradicted-genotype',
      mentioned: claim.value,
      detail:
        `the answer states ${claim.value} for ${nearest.variant.rsid} ` +
        `(${nearest.variant.gene}), but the dataset returned ${nearest.variant.userGenotype}.`,
    });
  }

  // 3. Symbols the reference does not even list — but only where the prose attaches a variant-level
  //    claim to them.
  //
  //    The bare-mention version of this rule fired on the first real question it saw. The model
  //    answered the statin question correctly, quoting `T/C at rs4149056 in SLCO1B1`, and then
  //    explained the mechanism: "The `SLCO1B1` gene encodes a transport protein (OATP1B1) that
  //    helps move statins from the blood into the liver …". OATP1B1 is the standard protein name
  //    for what SLCO1B1 encodes — correct domain language, and a string this repo's own fixtures
  //    carry ("Intermediate OATP1B1 function.") — yet it is not a gene symbol in the reference
  //    table and it contains digits, so it was reported as invented and a warning went under a
  //    fully correct answer. That is the worst outcome this module has: it teaches the reader to
  //    skip the mechanism, which is the part of the answer they most need.
  //
  //    What the check is actually for is invented claims about *this person's variants*, not the
  //    model's vocabulary. So an unknown symbol is reported only when an unaccounted claim — an
  //    rsID nobody read, or a genotype belonging to no read variant — sits in its sentence and
  //    within the attribution window. `CYP3A5 rs776746 C/C` carries both and still trips it; a
  //    transporter named in a sentence of pharmacology carries neither.
  for (const word of matchAll(answer, WORD_PATTERN)) {
    if (knownSymbols.has(word.value.toUpperCase())) continue;
    if (!looksLikeGeneSymbol(word.value)) continue;
    const claim = unaccountedClaims.find(
      (candidate) =>
        sentenceOf(candidate.start) === sentenceOf(word.start) &&
        gap(candidate, word) <= CLAIM_ATTRIBUTION_WINDOW,
    );
    if (claim === undefined) continue;
    report({
      kind: 'unknown-gene',
      mentioned: word.value,
      detail:
        `${word.value} is not a gene in this reference snapshot and no tool returned it, yet the ` +
        `answer attaches ${claim.value} to it — nothing about it was read from your dataset.`,
    });
  }

  const severity: Record<GroundingFindingKind, number> = {
    'unsupported-rsid': 0,
    'unknown-gene': 1,
    'contradicted-genotype': 2,
  };
  return findings.sort((left, right) => severity[left.kind] - severity[right.kind]);
}

export const GROUNDING_WARNING_HEADER =
  '\n\n⚠️ Claims above that nothing read from your dataset supports:';

export const GROUNDING_WARNING_FOOTER =
  "\nThe `variants` and `provenance` fields of this response are what was actually read. " +
  'Where the text disagrees with them, the text is wrong.';

/**
 * Appends a labelled warning naming exactly what was unsupported, and changes nothing else.
 *
 * Deliberately not a rewrite, a redaction or a regeneration. Deleting the offending sentence would
 * hide that the model is unreliable — the reader would see a shorter answer and trust it more —
 * and regenerating asks the same model to mark its own work. A contradiction that is *labelled* as
 * one lets a reader do the only correct thing: believe the payload over the prose. The tool
 * results stay the source of truth and are already returned as `variants`/`provenance`.
 */
export function appendGroundingWarning(
  answer: string,
  findings: readonly GroundingFinding[],
): string {
  if (findings.length === 0) return answer;
  const lines = findings.map((finding) => `\n- ${finding.detail}`).join('');
  return answer + GROUNDING_WARNING_HEADER + lines + GROUNDING_WARNING_FOOTER;
}
