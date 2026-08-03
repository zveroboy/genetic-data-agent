/**
 * Dataset-scoped genotype repository: the only path from a question to somebody's genome.
 *
 * A repository is opened for **one published dataset** and answers only from that dataset's
 * manifest-declared Parquet objects, read remotely. There is no process-wide instance, no
 * local `.duckdb` copy of user data, no fixture fallback and no synthesis from a VCF on disk:
 * if the manifest is missing, the target cannot be placed, or the coordinates fall outside
 * every declared position range, the request fails rather than degrading into something that
 * looks like an answer.
 *
 * The query shape is what keeps the read small, and each part of it is load bearing:
 *
 * - `read_parquet([...])` takes an **explicit list of immutable URIs** built from validated
 *   manifest descriptors. Never a glob, never a prefix, never a caller-supplied path.
 * - `hive_partitioning = true, hive_types_autocast = 0` — both, always. Bare
 *   `hive_partitioning` lets DuckDB infer `chrom`'s type from the partitions a scan happens to
 *   touch, so the identical dataset yields `VARCHAR` for a whole-dataset scan and `BIGINT` for
 *   the pruned single-partition scan this module always produces. See
 *   `contracts/ingestion-v1.md`, "Reading the dataset" — that section, not this comment, is
 *   normative.
 * - The literal partition value and the parameterized position predicates sit **directly above
 *   the scan**, so Parquet statistics prune row groups before any join runs. Dynamic filtering
 *   from the candidate join alone would still be correct, but it is an optimizer courtesy, not
 *   a guarantee, and getting it wrong costs S3 bandwidth rather than correctness — which is
 *   exactly the kind of regression nobody notices.
 */
import type { SynthesizedVariant } from '../../domain/types.ts';
import type { ClinVarCoordinateResolver, VariantTarget } from './clinvar-coordinate-resolver.ts';
import type {
  DuckDbParam,
  DuckDbSessionFactory,
  DuckDbSessionTraffic,
} from './duckdb-session-factory.ts';
import {
  type ParquetDatasetResolver,
  type ResolvedParquetDataset,
  parquetObjectUri,
  selectCandidateObjects,
} from './parquet-dataset-resolver.ts';
import type { ParquetObject } from '../../application/ingestion-contracts.ts';

/** Hard ceiling on rows returned to the agent for one target. */
export const MAX_VARIANT_ROWS = 256;

/**
 * The option set every read of a published dataset must use, exported so it cannot drift
 * between this module and the tests that pin its behaviour against the real engine.
 *
 * `hive_types_autocast = 0` is not optional. It is frozen normatively in
 * `contracts/ingestion-v1.md` ("Reading the dataset"), because DuckDB otherwise infers the
 * Hive column's type from the partition values a scan happens to touch: the same dataset then
 * yields `chrom` as `VARCHAR` for a whole-dataset scan and `BIGINT` for the pruned,
 * single-partition scan this module always produces.
 */
export const HIVE_PARTITION_READ_OPTIONS = 'hive_partitioning = true, hive_types_autocast = 0';

/** What was read, and against what, for every answer the agent is handed. */
export interface GenotypeProvenance {
  readonly datasetId: string;
  readonly datasetChecksumSha256: string;
  readonly referenceBuild: string;
  readonly referenceVersion: string;
  /** The exact immutable object URIs passed to `read_parquet`, in canonical order. */
  readonly filesScanned: readonly string[];
  readonly targetsResolved: number;
}

/**
 * How much of what the reference lists for the target this read covered.
 *
 * Required, not optional, and carried on every result rather than only on the truncated ones: the
 * caller that words the answer has to be able to say "64 of 2,714" without knowing anything about
 * `MAX_TARGETS_PER_QUERY`, and a field a repository may omit is a field the one gene that needed
 * it will be missing.
 */
export interface CoordinateCoverage {
  /** What the reference snapshot lists for the target. */
  readonly listed: number;
  /** How many of them the scan was bounded to — the highest-ranked ones. */
  readonly read: number;
}

export interface GenotypeQueryResult {
  readonly targetId: string;
  readonly variants: readonly SynthesizedVariant[];
  readonly provenance: GenotypeProvenance;
  readonly coordinateCoverage: CoordinateCoverage;
}

export interface GenotypeRepository {
  readonly datasetId: string;
  synthesizeVariant(targetId: string): Promise<GenotypeQueryResult>;
}

export interface GenotypeRepositoryFactory {
  open(datasetId: string): Promise<GenotypeRepository>;
}

export interface GenotypeRepositoryDependencies {
  readonly datasetResolver: ParquetDatasetResolver;
  readonly coordinateResolver: ClinVarCoordinateResolver;
  readonly sessionFactory: DuckDbSessionFactory;
}

/**
 * Raised when the opened reference snapshot does not describe the same genome the dataset was
 * ingested against. Pruning coordinates from one build against Parquet written for another
 * silently returns the wrong person's answer for the right question.
 */
export class ReferenceSnapshotMismatchError extends Error {
  readonly datasetId: string;

  constructor(datasetId: string, detail: string) {
    super(`dataset '${datasetId}' cannot be served: ${detail}`);
    this.name = 'ReferenceSnapshotMismatch';
    this.datasetId = datasetId;
  }
}

/**
 * Raised when the remote object store fails mid-scan — a transport/IO fault, not a SQL fault.
 * Named to agree with Task 8's API contract, which names this failure `RemoteDatasetUnavailable`.
 *
 * Without this, an S3 outage during `read_parquet` propagates from `session.query` as a raw
 * DuckDB IO error, indistinguishable from a SQL fault (a malformed query, a schema mismatch). A
 * caller that wants to retry an outage but not a bug needs the two told apart.
 */
export class RemoteDatasetUnavailableError extends Error {
  readonly datasetId: string;

  constructor(datasetId: string, detail: string, options?: { cause?: unknown }) {
    super(`dataset '${datasetId}' could not be read from the object store: ${detail}`, options);
    this.name = 'RemoteDatasetUnavailable';
    this.datasetId = datasetId;
  }
}

/**
 * DuckDB's own error messages are prefixed with the exception category (`IO Error: …`,
 * `HTTP Error: …`, `Binder Error: …`, …). The transport/IO categories are the ones a genuine
 * object-store outage produces; everything else — a binder, parser, catalog or constraint fault
 * — is a real SQL fault and must stay distinct, not get folded into "the store is unavailable".
 */
const OBJECT_STORE_IO_ERROR_PREFIXES = [
  'IO Error:',
  'HTTP Error:',
  'Connection Error:',
  'Network Error:',
  'TLS Error:',
];

function isObjectStoreIoError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return OBJECT_STORE_IO_ERROR_PREFIXES.some((prefix) => message.startsWith(prefix));
}

/**
 * One coordinate and its place in the resolver's ranking.
 *
 * The rank travels into SQL and back out on every row because the scan is split per chromosome:
 * within a group `ORDER BY` restores the ranking, but the groups themselves are concatenated in
 * partition order, and re-deriving the ranking here from `(pos, ref, alt)` would reinstate exactly
 * the position ordering the ranking exists to override.
 */
interface RankedTarget {
  /** 0 is the coordinate the answer leads with. */
  readonly rank: number;
  readonly target: VariantTarget;
}

interface ChromosomeGroup {
  readonly chrom: string;
  readonly objects: readonly ParquetObject[];
  readonly targets: readonly RankedTarget[];
}

/**
 * Splits the work per partition value, because the partition value reaches SQL as a literal
 * and one literal can only describe one partition.
 */
function groupByChromosome(
  candidates: readonly ParquetObject[],
  targets: readonly VariantTarget[],
): readonly ChromosomeGroup[] {
  const chromosomes: string[] = [];
  for (const object of candidates) {
    if (!chromosomes.includes(object.chrom)) chromosomes.push(object.chrom);
  }
  // Ranked by arrival order: the resolver returns its ranking as the array order, so the index
  // *is* the rank. Nothing here re-decides it.
  const ranked = targets.map((target, rank) => ({ rank, target }));
  return chromosomes.map((chrom) => ({
    chrom,
    objects: candidates.filter((object) => object.chrom === chrom),
    targets: ranked.filter((entry) => entry.target.chrom === chrom),
  }));
}

/**
 * Builds one chromosome group's query.
 *
 * The only interpolated values are the partition literal — which
 * `ParquetDatasetResolver.resolve` has already constrained to the frozen contig allowlist —
 * and the object URIs, which come from validated manifest descriptors whose keys the wire
 * schema restricts to a safe character set. Everything derived from the reference lookup
 * (positions, alleles, clinical text and the coordinate's rank) is bound.
 *
 * The result is ordered by that rank and not by position: the first row is what the answer leads
 * with, so the ordering has to be the resolver's ranking rather than a second opinion formed here.
 */
function buildGroupQuery(group: ChromosomeGroup): { sql: string; values: DuckDbParam[] } {
  const values: DuckDbParam[] = [];

  // Spelled out as aliased `UNION ALL` selects rather than `SELECT * FROM (VALUES …)` so that
  // no generated query ever contains a `*`; that keeps "this SQL has no wildcard in it" a
  // property a reader — and a test — can check by looking.
  const candidateRows = group.targets
    .map(({ rank, target }) => {
      values.push(
        target.pos,
        target.ref,
        target.alt,
        target.rsid,
        target.gene,
        target.phenotype,
        target.clinicalSignificance,
        target.evidenceNote,
        rank,
      );
      return (
        'SELECT CAST(? AS UINTEGER) AS pos, CAST(? AS VARCHAR) AS ref, ' +
        'CAST(? AS VARCHAR) AS alt, CAST(? AS VARCHAR) AS rsid, CAST(? AS VARCHAR) AS gene, ' +
        'CAST(? AS VARCHAR) AS phenotype, CAST(? AS VARCHAR) AS clinical_significance, ' +
        'CAST(? AS VARCHAR) AS evidence_note, CAST(? AS UINTEGER) AS target_rank'
      );
    })
    .join('\n      UNION ALL ');

  const positions = group.targets.map(({ target }) => target.pos);
  if (positions.length === 0) {
    // Every group is derived from `selectCandidateObjects`, which only ever includes an object
    // because *some* target's chrom matched it — so a group can never be built for a chrom with
    // no targets. Made explicit rather than left implicit: `Math.min(...[])`/`Math.max(...[])`
    // would silently produce `Infinity`/`-Infinity` bounds instead of failing loudly.
    throw new Error(
      `internal invariant violated: chromosome group '${group.chrom}' has candidate objects ` +
        'but no targets to bound a scan with',
    );
  }
  const fileList = group.objects
    .map((object) => `'${parquetObjectUri(object)}'`)
    .join(',\n        ');

  // Bounds first, then the exact set: the range lets Parquet row-group statistics eliminate
  // whole row groups, the IN list eliminates the remaining rows.
  const lowerBound = Math.min(...positions);
  const upperBound = Math.max(...positions);
  values.push(lowerBound, upperBound, ...positions);
  const positionList = positions.map(() => 'CAST(? AS UINTEGER)').join(', ');

  const sql = `
    WITH candidate AS (
      ${candidateRows}
    ),
    scanned AS (
      SELECT pos, ref, alt, gt_raw
      FROM read_parquet(
        [
        ${fileList}
        ],
        ${HIVE_PARTITION_READ_OPTIONS}
      )
      WHERE chrom = '${group.chrom}'
        AND pos BETWEEN CAST(? AS UINTEGER) AND CAST(? AS UINTEGER)
        AND pos IN (${positionList})
    )
    SELECT
      c.rsid AS rsid,
      c.gene AS gene,
      CASE
        WHEN s.gt_raw LIKE '%0/0%' OR s.gt_raw LIKE '%0|0%' THEN c.ref || '/' || c.ref
        WHEN s.gt_raw LIKE '%0/1%' OR s.gt_raw LIKE '%1/0%'
          OR s.gt_raw LIKE '%0|1%' OR s.gt_raw LIKE '%1|0%' THEN c.ref || '/' || c.alt
        WHEN s.gt_raw LIKE '%1/1%' OR s.gt_raw LIKE '%1|1%' THEN c.alt || '/' || c.alt
        ELSE s.gt_raw
      END AS user_genotype,
      c.phenotype AS phenotype,
      c.clinical_significance AS clinical_significance,
      c.evidence_note AS evidence_note,
      c.target_rank AS target_rank
    FROM scanned s
    JOIN candidate c ON s.pos = c.pos AND s.ref = c.ref AND s.alt = c.alt
    ORDER BY c.target_rank
    LIMIT ${MAX_VARIANT_ROWS};
  `;

  return { sql, values };
}

/**
 * Narrows one SQL row to the domain shape, and is the only place the two naming conventions
 * meet. The column names above are physical — they are what the Parquet files and the reference
 * snapshot actually store — while `SynthesizedVariant` is a wire payload and therefore camelCase.
 * Renaming the SQL aliases instead would push snake_case out to the browser or push camelCase
 * into the Parquet schema; neither is free.
 */
function toSynthesizedVariant(row: Record<string, unknown>): SynthesizedVariant {
  return {
    rsid: String(row.rsid ?? ''),
    gene: String(row.gene ?? ''),
    userGenotype: String(row.user_genotype ?? ''),
    phenotype: String(row.phenotype ?? ''),
    clinicalSignificance: String(row.clinical_significance ?? ''),
    evidenceNote: String(row.evidence_note ?? ''),
  };
}

/**
 * Says *why* no declared object can hold the target, from the manifest alone.
 *
 * The two cases are not the same claim and must not be reported as one. A chromosome absent
 * from the inventory means the dataset never covered that part of the genome — NA12878's
 * published Parquet has chr1–chr22 and no chrX, so every X-linked target is unanswerable here
 * for a reason that has nothing to do with the sample. A chromosome that *is* present but whose
 * declared position range stops short of the coordinate is a narrower statement about this
 * callset's extent. Neither is "we looked and you carry the reference allele".
 */
export function describeAbsence(
  dataset: Pick<ResolvedParquetDataset, 'parquetObjects'>,
  targets: readonly VariantTarget[],
): string {
  const declaredChroms = new Set(dataset.parquetObjects.map((object) => object.chrom));
  const wanted = [...new Set(targets.map((target) => target.chrom))];
  const absent = wanted.filter((chrom) => !declaredChroms.has(chrom));
  if (absent.length > 0) {
    return (
      `chromosome ${formatChromosomes(absent)}, which this dataset does not cover at all ` +
      `(it covers ${formatChromosomes([...declaredChroms])})`
    );
  }
  return (
    `${wanted.join(', ')}:${targets.map((target) => target.pos).join(', ')}, where the ` +
    'chromosome is present but its declared position range does not reach that coordinate'
  );
}

/**
 * Chromosome names in genomic order, with runs collapsed.
 *
 * A partition value sorts as a string, so the raw inventory reads "1, 10, 11, … 2, 20" — which
 * in an answer looks like a defect rather than a complete autosomal set. "1–22" is the same
 * fact, legible.
 */
function formatChromosomes(chroms: readonly string[]): string {
  const numbered = chroms
    .filter((chrom) => /^\d+$/.test(chrom))
    .map(Number)
    .sort((left, right) => left - right);
  const named = chroms.filter((chrom) => !/^\d+$/.test(chrom)).sort();

  const runs: string[] = [];
  for (let start = 0; start < numbered.length; ) {
    let end = start;
    while (end + 1 < numbered.length && numbered[end + 1] === numbered[end]! + 1) end++;
    // A run of two is written out: "1, 2" is no longer than "1–2" and does not invite the
    // reader to wonder what was elided.
    runs.push(
      end > start + 1
        ? `${numbered[start]}–${numbered[end]}`
        : numbered.slice(start, end + 1).join(', '),
    );
    start = end + 1;
  }
  return [...runs, ...named].join(', ');
}

export function createGenotypeRepositoryFactory(
  dependencies: GenotypeRepositoryDependencies,
): GenotypeRepositoryFactory {
  const { datasetResolver, coordinateResolver, sessionFactory } = dependencies;

  return {
    async open(datasetId: string): Promise<GenotypeRepository> {
      const dataset: ResolvedParquetDataset = await datasetResolver.resolve(datasetId);

      // Both halves of the reference identity are checked. The build alone is not enough: two
      // snapshots of the same build can disagree about a variant's coordinates, and the
      // manifest records which one the dataset was ingested against.
      if (coordinateResolver.referenceBuild !== dataset.referenceBuild) {
        throw new ReferenceSnapshotMismatchError(
          datasetId,
          `it was ingested against build '${dataset.referenceBuild}' but the reference snapshot ` +
            `describes '${coordinateResolver.referenceBuild}'`,
        );
      }
      if (coordinateResolver.referenceVersion !== dataset.referenceVersion) {
        throw new ReferenceSnapshotMismatchError(
          datasetId,
          `it declares reference version '${dataset.referenceVersion}' but the open snapshot is ` +
            `'${coordinateResolver.referenceVersion}'`,
        );
      }

      return {
        datasetId,

        async synthesizeVariant(targetId: string): Promise<GenotypeQueryResult> {
          // Coordinates first. An unplaceable target throws here, before anything has been
          // headed, opened or read — it can never widen into a scan.
          //
          // `targets` is the resolver's ranking, in order and at most `MAX_TARGETS_PER_QUERY`
          // long; `coordinatesListed` is how many the snapshot holds for the target. Both are
          // reported, because "we read 64 of BRCA2's 2,714" is a fact about the answer and not
          // about the reference.
          const { targets, coordinatesListed } = await coordinateResolver.resolve(
            targetId,
            dataset.referenceBuild,
          );

          const candidates = selectCandidateObjects(dataset, targets);
          // Empty candidates are `TargetNotPresent`, and every remaining candidate is
          // re-verified against the manifest with bounded HEADs before it is queried. The
          // reason for the emptiness is computed here because this is the last frame that still
          // holds the coordinates; the resolver only ever sees an empty array.
          const filesScanned = await datasetResolver.verifyCandidates(
            dataset,
            candidates,
            candidates.length === 0 ? describeAbsence(dataset, targets) : undefined,
          );

          const rows: Record<string, unknown>[] = [];
          const startedAt = performance.now();
          let traffic: DuckDbSessionTraffic = { s3Requests: 0, bytesRead: 0 };
          const session = await sessionFactory.open();
          try {
            for (const group of groupByChromosome(candidates, targets)) {
              const { sql, values } = buildGroupQuery(group);
              try {
                rows.push(...(await session.query(sql, values)));
              } catch (error) {
                // A transport/IO fault reading the object store mid-scan is a distinct outcome
                // from a SQL fault: the query was fine, the store was not. Everything that is
                // not recognizably an IO fault propagates unchanged, so a real bug in the
                // generated SQL is never misreported as "the dataset is unavailable".
                if (isObjectStoreIoError(error)) {
                  throw new RemoteDatasetUnavailableError(
                    dataset.datasetId,
                    (error as Error).message,
                    { cause: error },
                  );
                }
                throw error;
              }
            }
            // Read before `close()`: the traffic counters live in the session's own in-memory
            // log and go away with it. A failure to read them must not mask a query failure, so
            // this sits on the success path only — and must not *become* one either, hence the
            // catch: an answer the caller can have is not withheld because a counter would not
            // read. The warning is what keeps that from being silent.
            try {
              traffic = await session.readTraffic();
            } catch (error) {
              console.warn(
                `[serving-metrics] traffic counters unavailable for '${dataset.datasetId}': ` +
                  `${(error as Error).message}`,
              );
            }
          } finally {
            await session.close();
          }
          const queryLatencyMs = Math.round(performance.now() - startedAt);

          // The serving path's one metrics record. Every field is measured, not estimated:
          // `s3Requests`/`bytesRead` come from the engine's own log, the latency is wall clock
          // around the scan, and the dataset checksum and reference version identify exactly
          // what was read. No gene, rsID, position or genotype is recorded — a metrics stream is
          // not a place to accumulate somebody's clinical profile.
          console.log(
            `[serving-metrics] ${JSON.stringify({
              datasetId: dataset.datasetId,
              datasetChecksumSha256: dataset.datasetChecksumSha256,
              referenceVersion: dataset.referenceVersion,
              selectedFileCount: filesScanned.length,
              inventorySize: dataset.parquetObjects.length,
              s3RequestCount: traffic.s3Requests,
              bytesRead: traffic.bytesRead,
              queryLatencyMs,
            })}`,
          );

          // Merged back into one ranked list. Each group's rows arrive ranked, but a target whose
          // coordinates span two partitions is scanned as two groups appended in partition order,
          // which would put a chr2 coordinate ranked second behind every chr1 coordinate. The
          // slice below then keeps the highest-ranked rows rather than the ones from whichever
          // chromosome was read first.
          rows.sort((left, right) => Number(left.target_rank) - Number(right.target_rank));

          return {
            targetId,
            variants: rows.slice(0, MAX_VARIANT_ROWS).map(toSynthesizedVariant),
            coordinateCoverage: { listed: coordinatesListed, read: targets.length },
            provenance: {
              datasetId: dataset.datasetId,
              datasetChecksumSha256: dataset.datasetChecksumSha256,
              referenceBuild: dataset.referenceBuild,
              referenceVersion: dataset.referenceVersion,
              filesScanned,
              targetsResolved: targets.length,
            },
          };
        },
      };
    },
  };
}
