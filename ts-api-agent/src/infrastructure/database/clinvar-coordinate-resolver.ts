/**
 * Resolves a gene symbol or rsID to exact genomic coordinates against a versioned ClinVar
 * snapshot, before any user Parquet is read.
 *
 * This step is what makes targeted serving possible at all. "CYP1A2" is not a location; only
 * `(referenceBuild, chrom, pos, ref, alt)` is, and only a location can prune partitions and
 * Parquet row groups. Consequently the resolver has exactly two outcomes — at least one
 * coordinate, or a throw. It never returns an empty list, because a caller could read that as
 * "no constraint" and scan the whole genome to answer a question the reference could not even
 * place.
 *
 * The canonical match key is `(referenceBuild, normalizedChrom, pos, normalizedRef,
 * normalizedAlt)`. The rsID is carried as provenance, not used as the only join key: rsIDs are
 * merged, retired and re-pointed between dbSNP releases, so joining user data on rsID alone
 * would silently mix builds.
 *
 * Out of scope, deliberately: liftover between builds and full indel left-normalization. A
 * build the snapshot does not describe is a `ReferenceBuildMismatch`, never a best-effort
 * translation.
 */
import { DuckDBInstance } from '@duckdb/node-api';
import type { DuckDBConnection } from '@duckdb/node-api';

import {
  REFERENCE_COORDINATES_TABLE,
  type ReferenceSnapshot,
  ReferenceSnapshotError,
  openReferenceSnapshot,
} from './reference-bootstrap.ts';
import { CANONICAL_CHROMOSOMES } from './parquet-dataset-resolver.ts';

/** Upper bound on the coordinates one target may expand to; a gene, not a chromosome. */
export const MAX_TARGETS_PER_QUERY = 64;

/** One resolved variant coordinate plus the clinical context that goes with it. */
export interface VariantTarget {
  readonly referenceBuild: string;
  readonly referenceVersion: string;
  /** Canonical partition value: `1`..`22`, `X`, `Y` or `MT`. */
  readonly chrom: string;
  readonly pos: number;
  readonly ref: string;
  readonly alt: string;
  /** Provenance only. Never the sole join key against user data. */
  readonly rsid: string | null;
  readonly gene: string;
  readonly phenotype: string;
  readonly clinicalSignificance: string;
  readonly evidenceNote: string;
}

/** Raised when a caller asks for a build the opened snapshot does not describe. */
export class ReferenceBuildMismatchError extends Error {
  readonly requestedBuild: string;
  readonly snapshotBuild: string;
  readonly referenceVersion: string;

  constructor(requestedBuild: string, snapshot: ReferenceSnapshot) {
    super(
      `reference build '${requestedBuild}' was requested but snapshot ` +
        `'${snapshot.referenceVersion}' describes '${snapshot.referenceBuild}'; liftover is out of scope`,
    );
    this.name = 'ReferenceBuildMismatch';
    this.requestedBuild = requestedBuild;
    this.snapshotBuild = snapshot.referenceBuild;
    this.referenceVersion = snapshot.referenceVersion;
  }
}

/** Raised when the snapshot places no coordinate for the requested gene or rsID. */
export class TargetNotResolvableError extends Error {
  readonly targetId: string;
  readonly referenceVersion: string;

  constructor(targetId: string, referenceVersion: string) {
    super(
      `'${targetId}' resolves to no coordinate in reference snapshot '${referenceVersion}'; ` +
        'an unplaceable target is never answered by scanning the dataset',
    );
    this.name = 'TargetNotResolvable';
    this.targetId = targetId;
    this.referenceVersion = referenceVersion;
  }
}

export interface ClinVarCoordinateResolver {
  readonly referenceVersion: string;
  readonly referenceBuild: string;
  /** At least one coordinate, or a throw. Never an empty list. */
  resolve(targetId: string, referenceBuild: string): Promise<readonly VariantTarget[]>;
  close(): Promise<void>;
}

/**
 * Maps a contig spelling onto the frozen partition-value domain, or `null` when it is not a
 * chromosome this system knows.
 *
 * `chr12` and `12` are the same chromosome; `chrM` and `MT` are the same mitochondrion. The
 * partition directories use the unprefixed, upper-case form, so everything that will be
 * compared against them has to arrive in it — including the reference snapshot, which stores
 * the `chr`-prefixed spelling its source VCF used.
 */
export function normalizeChromosome(raw: string): string | null {
  const trimmed = raw.trim().toUpperCase();
  const bare = trimmed.startsWith('CHR') ? trimmed.slice(3) : trimmed;
  const canonical = bare === 'M' ? 'MT' : bare;
  return CANONICAL_CHROMOSOMES.includes(canonical) ? canonical : null;
}

/** Alleles are compared literally against Parquet content, so their spelling is normalized. */
function normalizeAllele(raw: string): string | null {
  const allele = raw.trim().toUpperCase();
  return /^[ACGTN]+$/.test(allele) ? allele : null;
}

export interface OpenClinVarCoordinateResolverOptions {
  readonly databasePath: string;
}

/**
 * Opens a built snapshot read-only for serving.
 *
 * Read-only is not decoration: it is the guarantee that answering a question cannot mutate the
 * reference every other question is answered against.
 */
export async function openClinVarCoordinateResolver(
  options: OpenClinVarCoordinateResolverOptions,
): Promise<ClinVarCoordinateResolver> {
  const snapshot = await openReferenceSnapshot(options.databasePath);

  const instance = await DuckDBInstance.create(options.databasePath, {
    access_mode: 'READ_ONLY',
  });
  let connection: DuckDBConnection;
  try {
    connection = await instance.connect();
  } catch (error) {
    instance.closeSync();
    throw new ReferenceSnapshotError(
      options.databasePath,
      `could not be connected to: ${(error as Error).message}`,
    );
  }

  let closed = false;

  return {
    referenceVersion: snapshot.referenceVersion,
    referenceBuild: snapshot.referenceBuild,

    async resolve(targetId: string, referenceBuild: string): Promise<readonly VariantTarget[]> {
      if (closed) {
        throw new ReferenceSnapshotError(options.databasePath, 'has already been closed');
      }
      if (referenceBuild !== snapshot.referenceBuild) {
        throw new ReferenceBuildMismatchError(referenceBuild, snapshot);
      }

      const trimmed = targetId.trim();
      if (trimmed.length === 0) {
        throw new TargetNotResolvableError(targetId, snapshot.referenceVersion);
      }

      // Gene symbol *or* rsID, both case-insensitively, both as bound parameters — the target
      // id is the one value on this path that came from outside.
      const rows = (
        await connection.runAndReadAll(
          `
            SELECT chrom, pos, rsid, ref, alt, gene, phenotype, clinical_significance, evidence_note
            FROM ${REFERENCE_COORDINATES_TABLE}
            WHERE reference_version = $1
              AND reference_build = $2
              AND (upper(gene) = upper($3) OR lower(rsid) = lower($3))
            ORDER BY chrom, pos, ref, alt
            LIMIT ${MAX_TARGETS_PER_QUERY};
          `,
          [snapshot.referenceVersion, snapshot.referenceBuild, trimmed],
        )
      ).getRowObjects();

      const targets: VariantTarget[] = [];
      const seen = new Set<string>();
      for (const row of rows) {
        const chrom = normalizeChromosome(String(row.chrom));
        const ref = normalizeAllele(String(row.ref));
        const alt = normalizeAllele(String(row.alt));
        // A snapshot row this system cannot place is dropped rather than guessed at; if that
        // leaves nothing, the target is simply not resolvable.
        if (chrom === null || ref === null || alt === null) continue;

        const pos = Number(row.pos);
        const key = `${snapshot.referenceBuild}\t${chrom}\t${pos}\t${ref}\t${alt}`;
        if (seen.has(key)) continue;
        seen.add(key);

        targets.push({
          referenceBuild: snapshot.referenceBuild,
          referenceVersion: snapshot.referenceVersion,
          chrom,
          pos,
          ref,
          alt,
          rsid: row.rsid === null ? null : String(row.rsid),
          gene: String(row.gene),
          phenotype: String(row.phenotype),
          clinicalSignificance: String(row.clinical_significance),
          evidenceNote: String(row.evidence_note),
        });
      }

      if (targets.length === 0) {
        throw new TargetNotResolvableError(targetId, snapshot.referenceVersion);
      }

      // Coordinate order, not the snapshot's storage order: '1' < '12' < '2' byte-wise, the
      // same ordering the manifest inventory uses.
      return targets.sort(
        (left, right) =>
          left.chrom.localeCompare(right.chrom, 'en') ||
          left.pos - right.pos ||
          left.ref.localeCompare(right.ref, 'en') ||
          left.alt.localeCompare(right.alt, 'en'),
      );
    },

    async close(): Promise<void> {
      if (closed) return;
      closed = true;
      connection.disconnectSync();
      instance.closeSync();
    },
  };
}
