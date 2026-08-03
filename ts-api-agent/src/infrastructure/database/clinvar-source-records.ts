/**
 * Derives the versioned coordinate snapshot from the authoritative ClinVar VCF.
 *
 * `tests/fixtures/clinvar_coordinates_grch38.tsv` used to be typed out by hand, and it was
 * wrong: alleles were inverted, two positions were GRCh37 coordinates wearing a GRCh38 label,
 * and one row named a variant ClinVar does not carry at all. Because the serving join is strict
 * on `(referenceBuild, chrom, pos, ref, alt)` — deliberately, so a coordinate mistake can never
 * produce a *wrong* answer — every one of those rows degraded silently into "no clinical
 * variant data found".
 *
 * Nothing in this module asserts where a variant is. It reads what ClinVar says, and the only
 * hand-authored input is the list of rsIDs the demo *features* plus the gene symbol the question
 * router uses to reach each one. Coordinates, alleles, disease names and clinical significance
 * are all copied from the source record.
 *
 * ## Two concepts, deliberately not one list
 *
 * The table used to be exactly the featured list, and so one name covered both. It no longer
 * does, and conflating them again is how either half breaks:
 *
 * - **The coordinate universe** (`isPathogenicExpertReviewed` / `isDrugResponse`, collected by
 *   `collectCoordinateUniverse`) is what this system can *place*: every coordinate a question may
 *   name by gene symbol or rsID and get a real genotype for. It is machine-selected from ClinVar
 *   by classification and review status, so it is tens of thousands of rows and nobody curates it.
 * - **The featured targets** (`FEATURED_TARGETS`) are what this system can answer *from a
 *   symptom*: the handful of variants that carry hand-written lay vocabulary ("coffee", "milk",
 *   "blood thinner") and drive the PubMed corpus. That layer is per-variant editorial work, it
 *   cannot be derived from ClinVar's prose, and it does not scale — which is exactly why it is a
 *   short list and not the table.
 *
 * The featured rows are also in the universe unconditionally, whatever ClinVar classifies them
 * as: CYP1A2 rs762551 is `Likely_benign`, LCT rs4988235 is `association`, SLCO1B1 rs4149056 and
 * CYP2C19 rs4244285 are `drug_response`, so any pathogenicity-based rule alone would delete most
 * of what the demo can answer today.
 *
 * The module is deliberately dependency-free (node builtins only) so the same code can be used
 * by the generator script, by the unit test that re-derives the committed table from a committed
 * extract of the source records, and by the integration test that re-derives that extract from
 * the full 193 MB ClinVar download.
 */
import fs from 'node:fs';
import readline from 'node:readline';
import zlib from 'node:zlib';

import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../domain/datasets.ts';

/** Column order of the coordinate TSV; also the DuckDB snapshot's column order. */
export const COORDINATE_TSV_COLUMNS = [
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
] as const;

/** One variant the demo features: curated lay vocabulary, and a literature corpus of its own. */
export interface ReferenceTarget {
  /** dbSNP identifier, `rs` + digits. ClinVar records the numeric part in INFO `RS=`. */
  readonly rsid: string;
  /**
   * Gene symbol the question router and the UI use as the lookup handle.
   *
   * Normally identical to ClinVar's `GENEINFO` symbol, and `deriveCoordinateRow` records a
   * mismatch in the evidence note rather than silently preferring one over the other.
   */
  readonly gene: string;
  /**
   * Why `gene` differs from ClinVar's `GENEINFO`, when it does.
   *
   * Only one target needs this: rs4988235 physically sits in an intron of *MCM6*, which is what
   * ClinVar names, but it is universally the *LCT* lactase-persistence variant and "LCT" is what
   * the lactose question resolves. Recording the divergence keeps the row honest.
   */
  readonly geneNote?: string;
  /**
   * Everyday words a patient uses for this gene that ClinVar's own prose will never contain.
   *
   * ClinVar names conditions and drugs — "LACTASE PERSISTENCE", "Warfarin response",
   * "simvastatin acid response". Nobody asks "am I lactase persistent?"; they ask about milk,
   * about blood thinners, about coffee. Those words exist nowhere in the source, so the only
   * honest place for them is here, declared per target, next to the rsID they belong to — data,
   * so that adding a target and its vocabulary is one edit in one file rather than a code change
   * in the agent.
   *
   * Deliberately *small*. Everything ClinVar already says about a variant is matched from the
   * derived table itself (`infrastructure/ai/question-routing.ts`); this layer only covers the
   * lay-term gap, and each entry below is a term of art with a documented pharmacogenomic
   * association, not a guess. A phrase matches when every one of its words appears in the
   * question.
   */
  readonly layTerms?: readonly string[];
}

/**
 * The variants the demo *features*, as rsIDs — not as coordinates.
 *
 * These are not the variants the system can place; the coordinate table is (see the module
 * comment). They are the variants a plain-language question can reach: each one carries lay
 * vocabulary ClinVar's own prose cannot contain, and each one gets its own PubMed queries. Both
 * of those are hand-written per variant, so this list grows one editorial decision at a time
 * while the coordinate table grows with every ClinVar release.
 *
 * Every rsID here is also in the coordinate table unconditionally, whatever ClinVar classifies it
 * as. Without that rule the machine selection would drop CYP1A2, LCT, SLCO1B1 and CYP2C19 — the
 * four the demo is most often asked about — and the product would silently stop answering.
 *
 * rs2187668 (HLA-DQA1, celiac susceptibility) was in the hand-written table and is **not** in
 * ClinVar under any record, so it is not listed here: an rsID the source does not carry cannot
 * be derived from the source, and inventing a row is the exact failure this module exists to
 * prevent.
 */
export const FEATURED_TARGETS: readonly ReferenceTarget[] = Object.freeze([
  {
    rsid: 'rs762551',
    gene: 'CYP1A2',
    // CYP1A2 is the principal caffeine 1-demethylase; rs762551 is the *1F allele used to
    // classify fast vs. slow caffeine metabolism. ClinVar names no condition for it at all.
    layTerms: ['coffee', 'caffeine', 'espresso', 'energy drink'],
  },
  {
    rsid: 'rs4149056',
    gene: 'SLCO1B1',
    // ClinVar spells the drugs ("simvastatin acid response", "rosuvastatin response"); it never
    // spells the class, and "statin myopathy" is how the question is actually asked.
    layTerms: ['statin', 'cholesterol medication', 'muscle pain', 'myopathy'],
  },
  {
    rsid: 'rs9923231',
    gene: 'VKORC1',
    // ClinVar does say "Warfarin response" here — but it says it for APOE too, so the plain
    // term is not discriminating. VKORC1 -1639G>A is *the* warfarin dosing variant (CPIC), so
    // the lay layer pins the ambiguity to the gene the question means.
    layTerms: ['warfarin', 'blood thinner', 'anticoagulant', 'coumadin'],
  },
  { rsid: 'rs3892097', gene: 'CYP2D6' },
  {
    rsid: 'rs4244285',
    gene: 'CYP2C19',
    // CYP2C19*2, the loss-of-function allele. CPIC gives CYP2C19 for citalopram, escitalopram
    // and sertraline — the SSRIs — and ClinVar's CLNDN for this record lists only clopidogrel,
    // mephenytoin and proguanil, so no amount of table matching would reach it from "SSRI".
    layTerms: ['ssri', 'antidepressant', 'citalopram', 'escitalopram', 'sertraline'],
  },
  { rsid: 'rs1050828', gene: 'G6PD', layTerms: ['fava bean', 'primaquine'] },
  {
    rsid: 'rs4988235',
    gene: 'LCT',
    geneNote:
      'queried as LCT; ClinVar places it in the MCM6 intron that regulates LCT expression',
    // ClinVar says "LACTASE PERSISTENCE" — the enzyme, not the sugar, and not the food.
    layTerms: ['lactose', 'milk', 'dairy', 'lactase'],
  },
  { rsid: 'rs1801133', gene: 'MTHFR', layTerms: ['folate', 'folic acid', 'homocysteine'] },
  { rsid: 'rs429358', gene: 'APOE' },
  { rsid: 'rs7412', gene: 'APOE' },
  { rsid: 'rs6025', gene: 'F5', layTerms: ['factor v leiden', 'blood clot'] },
  { rsid: 'rs80357906', gene: 'BRCA1' },
  { rsid: 'rs80359550', gene: 'BRCA2' },
  { rsid: 'rs1042522', gene: 'TP53' },
]);

/**
 * The lay-term layer, collapsed to `gene → phrases`.
 *
 * Two targets can share a gene (APOE has two rsIDs), so the phrases are unioned per symbol: the
 * lay layer answers "which gene is this question about", and the reference table then decides
 * which coordinates that gene has.
 */
export function layTermsByGene(
  targets: readonly ReferenceTarget[] = FEATURED_TARGETS,
): ReadonlyMap<string, readonly string[]> {
  const byGene = new Map<string, string[]>();
  for (const target of targets) {
    if (target.layTerms === undefined || target.layTerms.length === 0) continue;
    const bucket = byGene.get(target.gene);
    if (bucket === undefined) byGene.set(target.gene, [...target.layTerms]);
    else for (const term of target.layTerms) if (!bucket.includes(term)) bucket.push(term);
  }
  return byGene;
}

/**
 * The genes a plain-language question can reach, de-duplicated, in a stable order.
 *
 * Not "the genes this system knows": the coordinate table carries hundreds more, and every one of
 * them answers when a question names it. These are the ones a *symptom* reaches, because these are
 * the ones somebody wrote lay vocabulary and literature queries for.
 */
export function featuredGenes(
  targets: readonly ReferenceTarget[] = FEATURED_TARGETS,
): readonly string[] {
  return [...new Set(targets.map((target) => target.gene))].sort((left, right) =>
    left.localeCompare(right, 'en'),
  );
}

/** One parsed ClinVar VCF record, with the verbatim line kept for the committed extract. */
export interface ClinVarRecord {
  /** Contig exactly as ClinVar spells it: plain numerics, `X`, `Y`, `MT`. */
  readonly chrom: string;
  readonly pos: number;
  /** ClinVar Variation ID — the VCF `ID` column. */
  readonly variationId: number;
  readonly ref: string;
  readonly alt: string;
  readonly info: Readonly<Record<string, string>>;
  readonly line: string;
}

/** A row of the coordinate TSV, every field either copied from ClinVar or a declared label. */
export interface CoordinateRow {
  readonly reference_version: string;
  readonly reference_build: string;
  readonly chrom: string;
  readonly pos: number;
  readonly rsid: string;
  readonly ref: string;
  readonly alt: string;
  readonly gene: string;
  readonly phenotype: string;
  readonly clinical_significance: string;
  readonly evidence_note: string;
}

/** VCF 4.x percent-escapes used inside INFO values. */
function unescapeInfoValue(raw: string): string {
  return raw.replace(/%([0-9A-Fa-f]{2})/g, (_match, hex: string) =>
    String.fromCharCode(Number.parseInt(hex, 16)),
  );
}

/** Splits an INFO column into a plain object; flag keys map to the empty string. */
export function parseInfo(info: string): Record<string, string> {
  const parsed: Record<string, string> = {};
  if (info === '.') return parsed;
  for (const field of info.split(';')) {
    if (field.length === 0) continue;
    const separator = field.indexOf('=');
    if (separator === -1) {
      parsed[field] = '';
    } else {
      parsed[field.slice(0, separator)] = unescapeInfoValue(field.slice(separator + 1));
    }
  }
  return parsed;
}

/** Parses one data line, or returns `null` for headers and anything malformed. */
export function parseClinVarLine(line: string): ClinVarRecord | null {
  if (line.length === 0 || line.startsWith('#')) return null;
  const columns = line.split('\t');
  if (columns.length < 8) return null;
  const pos = Number(columns[1]);
  const variationId = Number(columns[2]);
  if (!Number.isInteger(pos) || !Number.isInteger(variationId)) return null;
  return {
    chrom: columns[0]!,
    pos,
    variationId,
    ref: columns[3]!,
    alt: columns[4]!,
    info: parseInfo(columns[7]!),
    line,
  };
}

/** The `rs`-prefixed identifier a record claims, or `null` when it has no `RS=`. */
export function rsidOf(record: ClinVarRecord): string | null {
  const rs = record.info.RS;
  return rs === undefined || rs.length === 0 ? null : `rs${rs}`;
}

/** Streams a VCF's lines, transparently gunzipping `.gz` (including BGZF's gzip members). */
export async function* readVcfLines(vcfPath: string): AsyncGenerator<string> {
  const file = fs.createReadStream(vcfPath);
  const stream = vcfPath.endsWith('.gz') ? file.pipe(zlib.createGunzip()) : file;
  const lines = readline.createInterface({ input: stream, crlfDelay: Infinity });
  try {
    for await (const line of lines) yield line;
  } finally {
    lines.close();
    file.destroy();
  }
}

/**
 * Every record carrying one of the wanted rsIDs, grouped by rsID.
 *
 * Records are kept in source order and *not* filtered here: `selectCanonicalRecord` is what
 * decides, and the committed extract keeps the rejected candidates so that decision stays
 * testable.
 */
export async function collectSourceRecords(
  lines: AsyncIterable<string>,
  targets: readonly ReferenceTarget[] = FEATURED_TARGETS,
): Promise<Map<string, ClinVarRecord[]>> {
  const wanted = new Set(targets.map((target) => target.rsid));
  const found = new Map<string, ClinVarRecord[]>();
  for await (const line of lines) {
    if (line.length === 0 || line.charCodeAt(0) === 35 /* '#' */) continue;
    // Cheap pre-filter: the overwhelming majority of ClinVar's ~3M lines cannot match.
    if (!line.includes('RS=')) continue;
    const record = parseClinVarLine(line);
    if (record === null) continue;
    const rsid = rsidOf(record);
    if (rsid === null || !wanted.has(rsid)) continue;
    const bucket = found.get(rsid);
    if (bucket === undefined) found.set(rsid, [record]);
    else bucket.push(record);
  }
  return found;
}

/**
 * Picks the one record a target resolves to, deterministically.
 *
 * Two rules, in order:
 *
 * 1. **A record must describe an alteration.** ClinVar publishes "identity" records whose ALT is
 *    `.` (`NC_000016.10:g.31096368=`) to carry an assertion about the *reference* allele. They
 *    are real ClinVar content but they are not variants, and the serving join compares an ALT
 *    against user Parquet, so they can never match anything.
 * 2. **Lowest Variation ID wins.** IDs are assigned in accession order, so the lowest is the
 *    earliest-accessioned — in practice the canonical, most-submitted record. That is what makes
 *    rs4244285 resolve to the CYP2C19*2 `G>A` (ID 16897) rather than the later, single-submitter
 *    `G>T` (ID 1703261), and rs1042522 to the Arg72Pro `G>C` (ID 12351) rather than the later
 *    `G>A`/`G>T` records at the same position.
 *
 * Returns `null` when nothing survives rule 1 — never a guess.
 */
export function selectCanonicalRecord(records: readonly ClinVarRecord[]): ClinVarRecord | null {
  const alterations = records.filter((record) => record.alt !== '.' && record.alt.length > 0);
  if (alterations.length === 0) return null;
  return alterations.reduce((best, candidate) =>
    candidate.variationId < best.variationId ? candidate : best,
  );
}

/**
 * The contigs a coordinate may sit on, in ClinVar's own unprefixed spelling.
 *
 * Declared here rather than imported from `parquet-dataset-resolver.ts` on purpose: that module
 * reaches the object store and the manifest contracts, and this one is node-builtins-only so the
 * generator script and the offline tests can use it. The two lists describe the same domain, and
 * a divergence cannot produce a wrong answer — the serving path drops any snapshot row whose
 * contig it cannot normalize (`normalizeChromosome`), so at worst a row is unreachable.
 *
 * ClinVar also publishes records on unplaced scaffolds (`NW_009646201.1`). A scaffold coordinate
 * has no partition to prune to, so a row for one would be dead weight the resolver silently drops.
 */
const CANONICAL_CONTIGS: ReadonlySet<string> = new Set([
  ...Array.from({ length: 22 }, (_unused, index) => String(index + 1)),
  'X',
  'Y',
  'MT',
]);

/** Whether a record sits on a chromosome this system partitions by. */
export function isCanonicalContig(chrom: string): boolean {
  return CANONICAL_CONTIGS.has(chrom);
}

/**
 * Whether a record describes an alteration, as opposed to an assertion about the reference.
 *
 * `.` is ClinVar's ALT for an identity record (`NC_000016.10:g.31096368=`), and an empty REF or
 * ALT is a malformed line. Neither can ever match user Parquet on
 * `(build, chrom, pos, ref, alt)`, so a row for one would resolve and then always answer "not
 * present" — a target that looks answerable and is not.
 */
export function describesAlteration(record: ClinVarRecord): boolean {
  return (
    record.ref.length > 0 && record.alt.length > 0 && record.ref !== '.' && record.alt !== '.'
  );
}

/** `CLNREVSTAT` values that mean a panel or a guideline reviewed the assertion, not one submitter. */
const EXPERT_REVIEW_STATUSES: ReadonlySet<string> = new Set([
  'reviewed_by_expert_panel',
  'practice_guideline',
]);

/**
 * Set 1 of the coordinate universe: pathogenic, and reviewed by somebody with a name.
 *
 * `Pathogenic` is matched **case-sensitively**, and that is not a style choice:
 * `Conflicting_classifications_of_pathogenicity` contains a lower-case `pathogenicity`, and a
 * case-insensitive test would pull every conflicting record into a table this system presents as
 * pathogenic findings. `Likely_pathogenic` is listed separately for the same reason — its `p` is
 * lower-case, so `includes('Pathogenic')` alone does not see it.
 *
 * The review-status gate is what keeps the table at ~12k rows rather than ~3M: a single-submitter
 * assertion with no criteria is still ClinVar content, but it is not something this demo should
 * place on a person's genome.
 */
export function isPathogenicExpertReviewed(record: ClinVarRecord): boolean {
  const significance = record.info.CLNSIG ?? '';
  const pathogenic =
    significance.includes('Pathogenic') || significance.includes('Likely_pathogenic');
  if (!pathogenic) return false;
  return EXPERT_REVIEW_STATUSES.has(record.info.CLNREVSTAT ?? '');
}

/**
 * Set 2 of the coordinate universe: every pharmacogenomic record, at any review level.
 *
 * This set is the demo's actual subject. `drug_response` records are mostly *not* pathogenic and
 * mostly *not* expert-reviewed — SLCO1B1 rs4149056 and CYP2C19 rs4244285 are exactly this shape —
 * so set 1 excludes almost all of them, and a table built from set 1 alone would answer clinical
 * risk questions while having nothing to say about any drug.
 *
 * Matched per value, not as a substring: `CLNSIG` is `|`-separated, and a record classified
 * `Likely_benign|drug_response|other` (CYP2D6 rs3892097) is a drug-response record.
 */
export function isDrugResponse(record: ClinVarRecord): boolean {
  const significance = record.info.CLNSIG ?? '';
  return significance.split('|').includes('drug_response');
}

/** What a full pass over ClinVar selected, and what it deliberately passed over. */
export interface CoordinateUniverseStats {
  readonly dataLines: number;
  readonly withoutRsid: number;
  readonly pathogenicExpertReviewed: number;
  readonly drugResponse: number;
  readonly inBothSets: number;
  readonly skippedNonCanonicalContig: number;
  readonly skippedEmptyAllele: number;
  readonly skippedNoGeneSymbol: number;
  readonly duplicateCoordinates: number;
}

/** The two halves of the table, plus the accounting that explains the row count. */
export interface CoordinateUniverse {
  /**
   * Every record carrying a featured rsID, grouped and unfiltered — the committed extract's
   * content, and the input `deriveTable` chooses the featured rows from.
   */
  readonly featuredRecords: ReadonlyMap<string, ClinVarRecord[]>;
  /** One record per machine-selected coordinate, the lowest VariationID at each. */
  readonly selectedRecords: readonly ClinVarRecord[];
  readonly stats: CoordinateUniverseStats;
}

/** `(chrom, pos, ref, alt)` — the identity a coordinate row has, before it is labelled. */
function coordinateKey(chrom: string, pos: number, ref: string, alt: string): string {
  return `${chrom}\t${pos}\t${ref}\t${alt}`;
}

/**
 * One streaming pass over ClinVar that collects both halves of the table.
 *
 * One pass, not two, because the source is a 193 MB gzip stream and reading it twice doubles a
 * minute of I/O for nothing. The featured records are collected unfiltered (the extract has to
 * keep the rejected candidates, or `selectCanonicalRecord` stops being testable offline) while the
 * machine selection is reduced to one record per coordinate as it goes, so peak memory is the size
 * of the table and not the size of ClinVar.
 *
 * Duplicate coordinates are resolved by lowest VariationID, the same rule
 * `selectCanonicalRecord` applies to a contested rsID, so the two halves cannot disagree about
 * which record describes a position.
 */
export async function collectCoordinateUniverse(
  lines: AsyncIterable<string>,
  targets: readonly ReferenceTarget[] = FEATURED_TARGETS,
): Promise<CoordinateUniverse> {
  const wanted = new Set(targets.map((target) => target.rsid));
  const featuredRecords = new Map<string, ClinVarRecord[]>();
  const selected = new Map<string, ClinVarRecord>();

  let dataLines = 0;
  let withoutRsid = 0;
  let pathogenicExpertReviewed = 0;
  let drugResponse = 0;
  let inBothSets = 0;
  let skippedNonCanonicalContig = 0;
  let skippedEmptyAllele = 0;
  let skippedNoGeneSymbol = 0;
  let duplicateCoordinates = 0;

  for await (const line of lines) {
    if (line.length === 0 || line.charCodeAt(0) === 35 /* '#' */) continue;
    dataLines += 1;
    // Cheap pre-filter, and a real selection rule: all three sets require an `RS=`, because an
    // rsID is the handle a question names and the provenance a row is read back with.
    if (!line.includes('RS=')) {
      withoutRsid += 1;
      continue;
    }
    const record = parseClinVarLine(line);
    if (record === null) continue;
    const rsid = rsidOf(record);
    if (rsid === null) {
      withoutRsid += 1;
      continue;
    }

    if (wanted.has(rsid)) {
      const bucket = featuredRecords.get(rsid);
      if (bucket === undefined) featuredRecords.set(rsid, [record]);
      else bucket.push(record);
    }

    const pathogenic = isPathogenicExpertReviewed(record);
    const drug = isDrugResponse(record);
    if (!pathogenic && !drug) continue;

    if (!isCanonicalContig(record.chrom)) {
      skippedNonCanonicalContig += 1;
      continue;
    }
    if (!describesAlteration(record)) {
      skippedEmptyAllele += 1;
      continue;
    }
    // A row with no `GENEINFO` has no gene cell that is not invented, and the vocabulary would
    // advertise the empty symbol as answerable. Dropped rather than labelled with a guess.
    if (geneInfoSymbol(record) === null) {
      skippedNoGeneSymbol += 1;
      continue;
    }

    if (pathogenic) pathogenicExpertReviewed += 1;
    if (drug) drugResponse += 1;
    if (pathogenic && drug) inBothSets += 1;

    const key = coordinateKey(record.chrom, record.pos, record.ref, record.alt);
    const existing = selected.get(key);
    if (existing === undefined) {
      selected.set(key, record);
    } else {
      duplicateCoordinates += 1;
      if (record.variationId < existing.variationId) selected.set(key, record);
    }
  }

  return {
    featuredRecords,
    selectedRecords: [...selected.values()],
    stats: {
      dataLines,
      withoutRsid,
      pathogenicExpertReviewed,
      drugResponse,
      inBothSets,
      skippedNonCanonicalContig,
      skippedEmptyAllele,
      skippedNoGeneSymbol,
      duplicateCoordinates,
    },
  };
}

/** ClinVar's placeholders for "a submitter gave no disease name". */
const UNINFORMATIVE_DISEASE_NAMES = new Set(['not_specified', 'not_provided', '.', '']);

/** How many `CLNDN` terms a phenotype cell carries; BRCA1/BRCA2 list well over twenty. */
const MAX_PHENOTYPE_TERMS = 4;

/**
 * What a phenotype cell says when ClinVar names no condition at all.
 *
 * Exported because it is a *label*, not vocabulary: anything matching a question against the
 * table's condition text has to skip it, or "what does this variant mean?" matches the row that
 * has nothing to say.
 */
export const NO_CONDITION_PHENOTYPE = 'No condition named in ClinVar for this variant';

/** ClinVar spells spaces as `_` inside INFO values. */
function humanize(value: string): string {
  return value.replaceAll('_', ' ').trim();
}

/**
 * The disease names ClinVar records for a variant, or an explicit statement that it records
 * none.
 *
 * Never a curated clinical claim: if `CLNDN` holds only `not_provided`/`not_specified`, the cell
 * says exactly that. The old table's "Slow caffeine metabolizer" for rs762551 was precisely such
 * a fabrication — ClinVar classifies that variant `Likely_benign` and names no condition, and
 * the allele it describes is the *fast*-metabolizer one.
 */
export function derivePhenotype(record: ClinVarRecord): string {
  const raw = record.info.CLNDN ?? '';
  const terms: string[] = [];
  for (const term of raw.split('|')) {
    if (UNINFORMATIVE_DISEASE_NAMES.has(term)) continue;
    const readable = humanize(term);
    if (readable.length === 0 || terms.includes(readable)) continue;
    terms.push(readable);
  }
  if (terms.length === 0) {
    return NO_CONDITION_PHENOTYPE;
  }
  const shown = terms.slice(0, MAX_PHENOTYPE_TERMS);
  const remaining = terms.length - shown.length;
  return remaining > 0 ? `${shown.join('; ')} (+${remaining} more in ClinVar)` : shown.join('; ');
}

/** ClinVar's own `CLNSIG`, made readable and nothing else. */
export function deriveClinicalSignificance(record: ClinVarRecord): string {
  const raw = record.info.CLNSIG ?? '';
  const values = raw
    .split('|')
    .map(humanize)
    .filter((value) => value.length > 0);
  return values.length === 0 ? 'No classification in ClinVar' : values.join(' / ');
}

/** First gene symbol of a `GENEINFO=SYMBOL:id|SYMBOL:id` value, or `null`. */
export function geneInfoSymbol(record: ClinVarRecord): string | null {
  const raw = record.info.GENEINFO ?? '';
  const first = raw.split('|')[0] ?? '';
  const symbol = first.split(':')[0] ?? '';
  return symbol.length === 0 ? null : symbol;
}

/**
 * Provenance, not prose.
 *
 * Everything here is a value the source record carries: which ClinVar record this row is, how
 * strongly ClinVar reviewed it, which gene ClinVar assigns it to, and the population frequency
 * ClinVar publishes. A reader can check every clause against `clinvar.vcf.gz` without trusting
 * anyone's summary.
 */
export function deriveEvidenceNote(target: ReferenceTarget, record: ClinVarRecord): string {
  const parts = [`ClinVar VariationID ${record.variationId}`];
  const review = record.info.CLNREVSTAT;
  if (review !== undefined && review.length > 0) {
    parts.push(`review status: ${humanize(review)}`);
  }
  const symbol = geneInfoSymbol(record);
  if (symbol !== null) {
    parts.push(symbol === target.gene ? `gene ${symbol}` : `ClinVar gene ${symbol}`);
  }
  const frequency = record.info.AF_TGP;
  if (frequency !== undefined && frequency.length > 0) {
    parts.push(`1000 Genomes allele frequency ${frequency}`);
  }
  if (target.geneNote !== undefined) {
    parts.push(target.geneNote);
  }
  return `${parts.join('; ')}.`;
}

/** Raised when a declared target cannot be derived, rather than being quietly dropped. */
export class UnderivableTargetError extends Error {
  readonly rsid: string;

  constructor(rsid: string, detail: string) {
    super(`'${rsid}' cannot be derived from the ClinVar source: ${detail}`);
    this.name = 'UnderivableTarget';
    this.rsid = rsid;
  }
}

/** Contigs are written `chr`-prefixed, the spelling the snapshot has always used. */
function chrPrefixed(chrom: string): string {
  return chrom.startsWith('chr') ? chrom : `chr${chrom}`;
}

/** Builds one TSV row from a target and the ClinVar record it resolved to. */
export function deriveCoordinateRow(
  target: ReferenceTarget,
  record: ClinVarRecord,
  referenceVersion: string = REFERENCE_VERSION,
  referenceBuild: string = REFERENCE_BUILD,
): CoordinateRow {
  return {
    reference_version: referenceVersion,
    reference_build: referenceBuild,
    chrom: chrPrefixed(record.chrom),
    pos: record.pos,
    rsid: target.rsid,
    ref: record.ref,
    alt: record.alt,
    gene: target.gene,
    phenotype: derivePhenotype(record),
    clinical_significance: deriveClinicalSignificance(record),
    evidence_note: deriveEvidenceNote(target, record),
  };
}

export interface DerivedTable {
  readonly rows: readonly CoordinateRow[];
  /** Targets that resolved to no usable ClinVar record, with the reason. */
  readonly dropped: readonly { readonly rsid: string; readonly reason: string }[];
}

/**
 * Canonical row order: the same `(chrom, pos, ref, alt)` byte ordering the manifest inventory
 * and the resolver's `ORDER BY` use, so the file's order does not depend on the order targets
 * happen to be declared in.
 */
function compareRows(left: CoordinateRow, right: CoordinateRow): number {
  return (
    left.chrom.localeCompare(right.chrom, 'en') ||
    left.pos - right.pos ||
    left.ref.localeCompare(right.ref, 'en') ||
    left.alt.localeCompare(right.alt, 'en')
  );
}

/** Turns collected source records into the featured rows, dropping nothing silently. */
export function deriveTable(
  sourceRecords: ReadonlyMap<string, ClinVarRecord[]>,
  targets: readonly ReferenceTarget[] = FEATURED_TARGETS,
  referenceVersion: string = REFERENCE_VERSION,
  referenceBuild: string = REFERENCE_BUILD,
): DerivedTable {
  const rows: CoordinateRow[] = [];
  const dropped: { rsid: string; reason: string }[] = [];
  for (const target of targets) {
    const candidates = sourceRecords.get(target.rsid) ?? [];
    if (candidates.length === 0) {
      dropped.push({ rsid: target.rsid, reason: 'no ClinVar record carries this rsID' });
      continue;
    }
    const record = selectCanonicalRecord(candidates);
    if (record === null) {
      dropped.push({
        rsid: target.rsid,
        reason: `all ${candidates.length} ClinVar record(s) are reference-identity records (ALT '.')`,
      });
      continue;
    }
    rows.push(deriveCoordinateRow(target, record, referenceVersion, referenceBuild));
  }
  rows.sort(compareRows);
  return { rows, dropped };
}

/** What a union derivation produced, on top of the featured rows it started from. */
export interface DerivedCoordinateTable extends DerivedTable {
  /** Rows contributed by the featured list. */
  readonly featuredRowCount: number;
  /** Rows contributed by the machine selection, after the featured rows won their coordinates. */
  readonly selectedRowCount: number;
  /** Machine-selected coordinates a featured row already described. */
  readonly supersededByFeatured: number;
}

/**
 * The whole coordinate table: the machine selection, plus the featured rows, deduplicated by
 * `(build, chrom, pos, ref, alt)` with the featured row winning.
 *
 * The featured row has to win rather than merely be present. The two derivations disagree about
 * two cells on purpose: rs4988235's gene is `LCT` here and `MCM6` in ClinVar's `GENEINFO` (the
 * variant sits in an MCM6 intron and regulates LCT), and its evidence note says so. A machine row
 * at the same coordinate would spell that gene `MCM6`, and the lactose question — which routes to
 * `LCT` — would stop resolving. Same rule, same reason, for anything else a target declares about
 * itself: `geneNote` and the queried symbol are editorial claims, and the featured list is where
 * they are made.
 *
 * `dropped` still carries any featured target the source cannot place. A featured target that
 * silently vanished into a 14,000-row table is the one failure this whole file exists to prevent.
 */
export function deriveCoordinateTable(
  universe: CoordinateUniverse,
  targets: readonly ReferenceTarget[] = FEATURED_TARGETS,
  referenceVersion: string = REFERENCE_VERSION,
  referenceBuild: string = REFERENCE_BUILD,
): DerivedCoordinateTable {
  const featured = deriveTable(universe.featuredRecords, targets, referenceVersion, referenceBuild);
  const claimed = new Set(
    featured.rows.map((row) => coordinateKey(row.chrom, row.pos, row.ref, row.alt)),
  );

  const rows: CoordinateRow[] = [...featured.rows];
  let supersededByFeatured = 0;
  for (const record of universe.selectedRecords) {
    const gene = geneInfoSymbol(record);
    const rsid = rsidOf(record);
    // Both were required for selection; re-checked because this function is also called with
    // hand-built records by the test that pins the union rule.
    if (gene === null || rsid === null) continue;
    const row = deriveCoordinateRow({ rsid, gene }, record, referenceVersion, referenceBuild);
    if (claimed.has(coordinateKey(row.chrom, row.pos, row.ref, row.alt))) {
      supersededByFeatured += 1;
      continue;
    }
    claimed.add(coordinateKey(row.chrom, row.pos, row.ref, row.alt));
    rows.push(row);
  }

  rows.sort(compareRows);
  return {
    rows,
    dropped: featured.dropped,
    featuredRowCount: featured.rows.length,
    selectedRowCount: rows.length - featured.rows.length,
    supersededByFeatured,
  };
}

/** A TSV cell may not contain the delimiters that would silently re-shape the table. */
function assertCellIsFlat(value: string, column: string, rsid: string): void {
  if (/[\t\r\n]/.test(value)) {
    throw new UnderivableTargetError(rsid, `derived '${column}' contains a tab or newline`);
  }
}

/** Renders the coordinate TSV. Deterministic: same records in, same bytes out. */
export function renderCoordinateTsv(rows: readonly CoordinateRow[]): string {
  const lines = [COORDINATE_TSV_COLUMNS.join('\t')];
  for (const row of rows) {
    const cells = COORDINATE_TSV_COLUMNS.map((column) => String(row[column]));
    cells.forEach((cell, index) =>
      assertCellIsFlat(cell, COORDINATE_TSV_COLUMNS[index]!, row.rsid),
    );
    lines.push(cells.join('\t'));
  }
  return `${lines.join('\n')}\n`;
}

/** Header of the committed source extract; explains what the file is to whoever opens it. */
export const SOURCE_EXTRACT_HEADER: readonly string[] = Object.freeze([
  '##fileformat=VCFv4.1',
  '##source=ClinVar',
  '##reference=GRCh38',
  '##extract=every ClinVar record carrying one of the rsIDs in FEATURED_TARGETS,',
  '##extract=copied verbatim from data/clinvar.vcf.gz by',
  '##extract=scripts/generate_clinvar_reference_tsv.ts --extract.',
  '##extract=Rejected candidates (reference-identity records, later duplicate accessions)',
  '##extract=are kept on purpose so the selection rule stays testable offline.',
  '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO',
]);

/**
 * Renders the committed extract: the source lines, verbatim, in a stable order.
 *
 * Verbatim matters. The extract is only trustworthy as a stand-in for the 193 MB download if
 * re-deriving the table from it and from the full file give the same answer, and that is exactly
 * what `tests/integration/clinvar_reference_source.test.ts` checks.
 *
 * Scope: the *featured* half only. Committing the machine-selected half verbatim would mean
 * committing several MB of ClinVar a second time, so the offline tests check that half by its
 * rule (`clinvar-source-records.test.ts` pins the union against hand-built records) and by the
 * shape of the committed table, rather than against a copy of the source.
 */
export function renderSourceExtract(sourceRecords: ReadonlyMap<string, ClinVarRecord[]>): string {
  const all: ClinVarRecord[] = [];
  for (const records of sourceRecords.values()) all.push(...records);
  all.sort(
    (left, right) =>
      left.chrom.localeCompare(right.chrom, 'en') ||
      left.pos - right.pos ||
      left.variationId - right.variationId,
  );
  return `${[...SOURCE_EXTRACT_HEADER, ...all.map((record) => record.line)].join('\n')}\n`;
}

/** Parses a rendered extract back into the grouping `deriveTable` consumes. */
export function parseSourceExtract(contents: string): Map<string, ClinVarRecord[]> {
  const found = new Map<string, ClinVarRecord[]>();
  for (const line of contents.split('\n')) {
    const record = parseClinVarLine(line);
    if (record === null) continue;
    const rsid = rsidOf(record);
    if (rsid === null) continue;
    const bucket = found.get(rsid);
    if (bucket === undefined) found.set(rsid, [record]);
    else bucket.push(record);
  }
  return found;
}
