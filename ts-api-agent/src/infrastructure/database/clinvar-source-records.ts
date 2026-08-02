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
 * hand-authored input is the list of rsIDs the demo is interested in plus the gene symbol the
 * question router uses to reach each one. Coordinates, alleles, disease names and clinical
 * significance are all copied from the source record.
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

/** One variant the demo wants to be able to answer about. */
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
 * The variants the demo advertises, as rsIDs — not as coordinates.
 *
 * rs2187668 (HLA-DQA1, celiac susceptibility) was in the hand-written table and is **not** in
 * ClinVar under any record, so it is not listed here: an rsID the source does not carry cannot
 * be derived from the source, and inventing a row is the exact failure this module exists to
 * prevent.
 */
export const REFERENCE_TARGETS: readonly ReferenceTarget[] = Object.freeze([
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
  targets: readonly ReferenceTarget[] = REFERENCE_TARGETS,
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
  targets: readonly ReferenceTarget[] = REFERENCE_TARGETS,
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

/** Turns collected source records into the table, dropping nothing silently. */
export function deriveTable(
  sourceRecords: ReadonlyMap<string, ClinVarRecord[]>,
  targets: readonly ReferenceTarget[] = REFERENCE_TARGETS,
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
  '##extract=every ClinVar record carrying one of the rsIDs in REFERENCE_TARGETS,',
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
