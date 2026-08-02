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
import type { DuckDbParam, DuckDbSessionFactory } from './duckdb-session-factory.ts';
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

export interface GenotypeQueryResult {
  readonly targetId: string;
  readonly variants: readonly SynthesizedVariant[];
  readonly provenance: GenotypeProvenance;
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

interface ChromosomeGroup {
  readonly chrom: string;
  readonly objects: readonly ParquetObject[];
  readonly targets: readonly VariantTarget[];
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
  return chromosomes.map((chrom) => ({
    chrom,
    objects: candidates.filter((object) => object.chrom === chrom),
    targets: targets.filter((target) => target.chrom === chrom),
  }));
}

/**
 * Builds one chromosome group's query.
 *
 * The only interpolated values are the partition literal — which
 * `ParquetDatasetResolver.resolve` has already constrained to the frozen contig allowlist —
 * and the object URIs, which come from validated manifest descriptors whose keys the wire
 * schema restricts to a safe character set. Everything derived from the reference lookup
 * (positions and alleles) is bound.
 */
function buildGroupQuery(group: ChromosomeGroup): { sql: string; values: DuckDbParam[] } {
  const values: DuckDbParam[] = [];

  // Spelled out as aliased `UNION ALL` selects rather than `SELECT * FROM (VALUES …)` so that
  // no generated query ever contains a `*`; that keeps "this SQL has no wildcard in it" a
  // property a reader — and a test — can check by looking.
  const candidateRows = group.targets
    .map((target) => {
      values.push(
        target.pos,
        target.ref,
        target.alt,
        target.rsid,
        target.gene,
        target.phenotype,
        target.clinicalSignificance,
        target.evidenceNote,
      );
      return (
        'SELECT CAST(? AS UINTEGER) AS pos, CAST(? AS VARCHAR) AS ref, ' +
        'CAST(? AS VARCHAR) AS alt, CAST(? AS VARCHAR) AS rsid, CAST(? AS VARCHAR) AS gene, ' +
        'CAST(? AS VARCHAR) AS phenotype, CAST(? AS VARCHAR) AS clinical_significance, ' +
        'CAST(? AS VARCHAR) AS evidence_note'
      );
    })
    .join('\n      UNION ALL ');

  const positions = group.targets.map((target) => target.pos);
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
      c.evidence_note AS evidence_note
    FROM scanned s
    JOIN candidate c ON s.pos = c.pos AND s.ref = c.ref AND s.alt = c.alt
    ORDER BY c.pos, c.ref, c.alt
    LIMIT ${MAX_VARIANT_ROWS};
  `;

  return { sql, values };
}

/** Rows come back as plain JSON primitives; this only narrows them to the domain shape. */
function toSynthesizedVariant(row: Record<string, unknown>): SynthesizedVariant {
  return {
    rsid: String(row.rsid ?? ''),
    gene: String(row.gene ?? ''),
    user_genotype: String(row.user_genotype ?? ''),
    phenotype: String(row.phenotype ?? ''),
    clinical_significance: String(row.clinical_significance ?? ''),
    evidence_note: String(row.evidence_note ?? ''),
  };
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
          const targets = await coordinateResolver.resolve(targetId, dataset.referenceBuild);

          const candidates = selectCandidateObjects(dataset, targets);
          // Empty candidates are `TargetNotPresent`, and every remaining candidate is
          // re-verified against the manifest with bounded HEADs before it is queried.
          const filesScanned = await datasetResolver.verifyCandidates(dataset, candidates);

          const rows: Record<string, unknown>[] = [];
          const session = await sessionFactory.open();
          try {
            for (const group of groupByChromosome(candidates, targets)) {
              const { sql, values } = buildGroupQuery(group);
              rows.push(...(await session.query(sql, values)));
            }
          } finally {
            await session.close();
          }

          return {
            targetId,
            variants: rows.slice(0, MAX_VARIANT_ROWS).map(toSynthesizedVariant),
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
