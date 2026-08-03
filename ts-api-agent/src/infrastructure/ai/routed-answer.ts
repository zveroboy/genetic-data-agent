/**
 * Turns the targets a question routed to into one answer, and one honest record of what was read.
 *
 * Its own module rather than a branch inside `answerLocally`: reporting N outcomes means a sentence
 * per target, a merge of N provenance records and a truncation notice, which is more shape than a
 * function that also owns routing, the literature suffix and the unrouted case should hold.
 *
 * The three properties it exists to keep:
 *
 * - **Every routed target is reported.** A found genotype, or that target's own reason for having
 *   none. A question naming two markers must not come back describing one of them with the other
 *   quietly dropped — the reader has no way to notice the omission.
 * - **`provenance` describes the whole read and nothing more.** The union of the files the scans
 *   actually touched, de-duplicated, and the real total of coordinates resolved. Returning the
 *   first target's record would understate what was read; concatenating file lists without
 *   de-duplicating would claim reads that never happened.
 * - **It chooses nothing.** It queries the targets it is handed, in the order it is handed them,
 *   through the caller's own query function — so the resolver's exact `(build, chrom, pos, ref,
 *   alt)` join stays the only thing that ever turns a target into rows. The headline of each
 *   paragraph is `evidence[0]`, which is the first row of the ranking the resolver produced and
 *   the repository preserved; this module does not re-sort it and must not, or the coordinate a
 *   gene's answer leads with becomes a property of two orderings that can disagree.
 */
import type { CoordinateCoverage, GenotypeProvenance } from '../database/duckdb.ts';

/**
 * The most targets one question may read.
 *
 * A question naming twelve genes is a scan with a question mark on the end: it costs twelve bounded
 * scans of the dataset, produces a page nobody reads, and is the shape a caller would use to
 * enumerate somebody's genome one "question" at a time. Five covers the real multi-target
 * questions — a gene's two markers, a drug and the two enzymes that clear it — and the rest is
 * refused *out loud*, in the answer, because a list silently cut to five reads as a complete list.
 */
export const MAX_QUERIED_TARGETS = 5;

/** One target's query result: rows, what was read to get them, or why there are none. */
export interface TargetOutcome {
  readonly evidence: readonly any[];
  readonly provenance?: GenotypeProvenance;
  /** The target's own absence note, when the reference or the dataset could not offer it. */
  readonly note?: string;
  /**
   * How many coordinates the reference listed for this target and how many were read.
   *
   * Optional because the two absence outcomes have no coverage to report: a target the snapshot
   * cannot place resolved nothing, and one the dataset cannot hold was never scanned.
   */
  readonly coordinateCoverage?: CoordinateCoverage;
}

export interface RoutedAnswer {
  readonly answer: string;
  readonly evidence: any[];
  /** Absent when nothing was read at all, so an answer never claims a read it did not make. */
  readonly provenance?: GenotypeProvenance;
}

/**
 * One target's outcome, in the wording a single-target answer has always used.
 *
 * The absence sentence carries the caveat because "no rows came back" is not "you carry the
 * reference allele": NA12878 has no call at CYP2D6 rs3892097 because that region sits outside
 * GIAB's high-confidence set — the position was never assessed. Telling the two apart properly is
 * an open problem; saying which one this is *not* costs a clause and keeps the answer honest.
 */
function describeOutcome(targetId: string, outcome: TargetOutcome): string {
  const found = outcome.evidence[0];
  const sentence =
    found !== undefined
      ? `Based on your genotype (${found.userGenotype} for rsID ${found.rsid} in gene ${found.gene}), clinical significance is ${found.clinicalSignificance} (${found.phenotype}). Note: ${found.evidenceNote}`
      : (outcome.note ??
        `No genotype for '${targetId}' in this dataset: the reference places it, but the ` +
          'dataset reports no matching call at those coordinates. That is not a statement that ' +
          'you carry the reference allele — a position can be missing because it was never assessed.');
  return sentence + describeCoordinateCoverage(targetId, outcome.coordinateCoverage);
}

/**
 * Says out loud that the gene was read from a subset, when it was.
 *
 * A gene symbol is not one variant: `demo-clinvar-grch38-v3` lists 2,714 coordinates under BRCA2
 * and 215 under CYP2D6, and one question reads at most `MAX_TARGETS_PER_QUERY` of them. Answering
 * from the top of a ranking is fine; answering from it *silently* is not, because the sentence
 * above names one rsID and reads as a statement about the gene. So the shortfall is spoken, the
 * same way the five-target cap below says a list was cut and `describeAbsence` says why nothing
 * was read — and the way out is named too, since an rsID resolves to exactly one coordinate and
 * is never capped.
 *
 * Silent when nothing was left out: a gene with one coordinate does not need a paragraph about
 * ranking, and a note that fires on every answer is a note nobody reads on the one that matters.
 */
function describeCoordinateCoverage(
  targetId: string,
  coverage: CoordinateCoverage | undefined,
): string {
  if (coverage === undefined || coverage.listed <= coverage.read) return '';
  return (
    ` ClinVar lists ${coverage.listed.toLocaleString('en-US')} coordinates for '${targetId}' in ` +
    `this reference snapshot; ${coverage.read} of them were read — the highest-ranked, featured ` +
    'markers first, then pathogenic and drug-response classifications, then position. Naming an ' +
    'rsID reads that one coordinate instead.'
  );
}

/**
 * One provenance record for several reads, inventing no field.
 *
 * `filesScanned` is the union in first-read order: two targets on the same chromosome are answered
 * out of the same Parquet object, and listing it twice would report two scans where there was one.
 * `targetsResolved` is the sum, because each record counts the coordinates *its* target resolved
 * to and the honest total for the answer is all of them.
 *
 * The identity fields — dataset, checksum, build, reference version — are copied from the first
 * record because every read in one question goes through one dataset-scoped repository, so they
 * describe the manifest and the open snapshot rather than the target. Copying them would be a lie
 * if that ever stopped holding, which is why the disagreement throws instead of picking a winner:
 * a checksum reported for rows it did not produce is exactly the kind of provenance that is worse
 * than none.
 */
function mergeProvenance(reads: readonly GenotypeProvenance[]): GenotypeProvenance | undefined {
  const first = reads[0];
  if (first === undefined) return undefined;

  const filesScanned: string[] = [];
  let targetsResolved = 0;
  for (const read of reads) {
    if (
      read.datasetId !== first.datasetId ||
      read.datasetChecksumSha256 !== first.datasetChecksumSha256 ||
      read.referenceBuild !== first.referenceBuild ||
      read.referenceVersion !== first.referenceVersion
    ) {
      throw new Error(
        `one question read two different datasets or snapshots ('${first.datasetId}' @ ` +
          `'${first.referenceVersion}' and '${read.datasetId}' @ '${read.referenceVersion}'); ` +
          'a single provenance record cannot describe both',
      );
    }
    for (const file of read.filesScanned) {
      if (!filesScanned.includes(file)) filesScanned.push(file);
    }
    targetsResolved += read.targetsResolved;
  }

  return { ...first, filesScanned, targetsResolved };
}

/**
 * Queries every routed target and composes the answer, up to `MAX_QUERIED_TARGETS`.
 *
 * Sequential, in the order the router produced. Each target is its own bounded scan holding its own
 * DuckDB session, so firing five at once would multiply one request's live sessions and
 * object-store connections by five for no gain a reader can see — and the answer has to come out in
 * the question's order regardless.
 */
export async function composeRoutedAnswer(
  targetIds: readonly string[],
  queryTarget: (targetId: string) => Promise<TargetOutcome>,
): Promise<RoutedAnswer> {
  const queried = targetIds.slice(0, MAX_QUERIED_TARGETS);
  const paragraphs: string[] = [];
  const evidence: any[] = [];
  const reads: GenotypeProvenance[] = [];

  for (const targetId of queried) {
    const outcome = await queryTarget(targetId);
    evidence.push(...outcome.evidence);
    if (outcome.provenance !== undefined) reads.push(outcome.provenance);
    paragraphs.push(describeOutcome(targetId, outcome));
  }

  const dropped = targetIds.slice(queried.length);
  if (dropped.length > 0) {
    paragraphs.push(
      `Your question named ${targetIds.length} targets; this answer covers the first ` +
        `${MAX_QUERIED_TARGETS} (${queried.join(', ')}). Nothing was read for ` +
        `${dropped.join(', ')} — ask about those separately.`,
    );
  }

  const provenance = mergeProvenance(reads);
  return {
    answer: paragraphs.join('\n\n'),
    evidence,
    ...(provenance === undefined ? {} : { provenance }),
  };
}
