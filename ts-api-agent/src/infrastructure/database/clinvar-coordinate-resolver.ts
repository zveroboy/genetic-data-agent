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
 * A gene symbol is not one coordinate either. `demo-clinvar-grch38-v3` places 2,714 coordinates
 * under BRCA2 and 72 under TP53, so "which coordinates" is a ranking question, and the ranking
 * — not the storage order, and not position order — is what decides both which ones fit under
 * `MAX_TARGETS_PER_QUERY` and which one an answer leads with. See `TARGET_RANKING_SQL`.
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
import { FEATURED_TARGETS } from './clinvar-source-records.ts';

/**
 * Upper bound on the coordinates one target may expand to; a gene, not a chromosome.
 *
 * A bound, not a refusal. It used to be both: a gene resolving past it threw
 * `TargetResolutionLimitExceeded` rather than answer from a subset, which was right while the
 * table held 14 hand-picked rows and no gene could reach it. Against the 13,853-row machine
 * selection it refused 35 of 238 genes outright — "What about the BRCA1 gene?" became an HTTP 422
 * where it had returned an honest "the reference places it, the dataset reports no matching call".
 * A question that used to work and now errors is a regression, so the bound stayed and the
 * refusal went: the highest-ranked `MAX_TARGETS_PER_QUERY` are read and the answer says how many
 * were left, which is the same thing this codebase already does with the five-target routing cap.
 */
export const MAX_TARGETS_PER_QUERY = 64;

/**
 * The rsIDs the demo features, lower-cased, as the ranking's first tier.
 *
 * Derived from `FEATURED_TARGETS` rather than from a column, because being featured is an
 * editorial property of this product and not a property of any ClinVar release — and because
 * adding a column would rename the table's content, which forces a reference-version bump and a
 * re-ingestion of every published dataset to answer a question about ordering.
 */
const FEATURED_RSIDS: readonly string[] = Object.freeze([
  ...new Set(FEATURED_TARGETS.map((target) => target.rsid.toLowerCase())),
]);

/**
 * How the coordinates of one target are ranked, as a SQL fragment.
 *
 * In SQL and not in TypeScript because the cap is applied by `LIMIT`: ranking after the fetch
 * would rank the 64 rows with the lowest positions against each other, and BRCA1's featured
 * marker sits at 43,057,062 — 11 kb past the 64th-lowest row. The tier that decides which
 * coordinates are even readable has to be visible to the same `ORDER BY` the `LIMIT` applies to.
 *
 * The three tiers, in order, each answering a different question:
 *
 * 1. **Featured first** (`FEATURED_RSIDS`). This is what the featured concept is *for*: the
 *    variant the product carries lay terms and a literature corpus for is the variant the
 *    question was about. It is what puts VKORC1 rs9923231 — the warfarin dosing variant — at the
 *    head of a warfarin answer instead of rs2359612, which merely sits 4 kb earlier on chr16, and
 *    CYP2C19*2 rs4244285 at the head of a clopidogrel answer instead of rs12777823, whose ClinVar
 *    condition is *warfarin* dosage. It also survives the cap: TP53's rs1042522 is classified
 *    Benign and would rank behind all 71 pathogenic TP53 rows on significance alone.
 * 2. **Then clinical significance**: pathogenic and likely pathogenic, then drug response, then
 *    everything else. For a gene nobody curated — 225 of the 238 — this is the only signal the
 *    table carries about which coordinate a reader would want first.
 * 3. **Then position ascending**, so the answer is the same on every run and every machine.
 *    `chrom`, `ref` and `alt` follow it to make the order total: one position carries two rows
 *    for rs4244285 (G>A and G>T), and an `ORDER BY` with ties in it is not deterministic.
 *
 * The significance tiers match whole `/`-separated components, never a substring: ClinVar spells
 * "Conflicting classifications of pathogenicity", which contains "pathogenic" and is emphatically
 * not a pathogenic classification, and a `LIKE '%pathogenic%'` test would rank 5 such rows ahead
 * of every real drug-response variant in the table.
 */
const TARGET_RANKING_SQL = (featuredPlaceholders: string): string => `
  CASE WHEN lower(rsid) IN (${featuredPlaceholders}) THEN 0 ELSE 1 END,
  CASE
    WHEN list_has_any(
           list_transform(str_split(lower(clinical_significance), '/'), part -> trim(part)),
           ['pathogenic', 'likely pathogenic']
         ) THEN 0
    WHEN list_contains(
           list_transform(str_split(lower(clinical_significance), '/'), part -> trim(part)),
           'drug response'
         ) THEN 1
    ELSE 2
  END,
  pos, chrom, ref, alt`;

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

/**
 * What one target resolved to: the coordinates that will be read, and how many exist.
 *
 * Both halves are returned together, from the one query that knows both, because the pair is what
 * makes the cap speakable. `targets` alone cannot distinguish a gene with exactly
 * `MAX_TARGETS_PER_QUERY` coordinates from BRCA2's 2,714 truncated to the same 64, and a caller
 * that cannot tell them apart is the silent truncation this design set out to remove.
 */
export interface ResolvedTargets {
  /** Ranked best first, at most `MAX_TARGETS_PER_QUERY` long, never empty. */
  readonly targets: readonly VariantTarget[];
  /** How many coordinates the snapshot lists for the target, cap or no cap. */
  readonly coordinatesListed: number;
}

/**
 * One answerable thing the snapshot describes, without its coordinates.
 *
 * This is the *askable surface*: which handles resolve (`gene`, `rsid`) and what the reference
 * itself says a variant is about (`phenotype`, `clinicalSignificance`). It exists so that
 * deciding which target a natural-language question means can be derived from the table instead
 * of from a hand-kept list that the table's growth silently invalidates.
 *
 * Deliberately coordinate-free: nothing that consumes it is allowed to turn a question into a
 * scan. Picking a target here is always followed by a normal `resolve`, which is what enforces
 * the `(referenceBuild, chrom, pos, ref, alt)` join.
 */
export interface ReferenceVocabularyEntry {
  readonly gene: string;
  readonly rsid: string | null;
  readonly phenotype: string;
  readonly clinicalSignificance: string;
}

export interface ClinVarCoordinateResolver {
  readonly referenceVersion: string;
  readonly referenceBuild: string;
  /** At least one coordinate, or a throw. Never an empty list. */
  resolve(targetId: string, referenceBuild: string): Promise<ResolvedTargets>;
  /** Every gene/rsID the snapshot can place, with the text the snapshot carries for it. */
  vocabulary(): Promise<readonly ReferenceVocabularyEntry[]>;
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
  // Read once and kept: the snapshot is opened READ_ONLY and its identity is fixed for the life
  // of this resolver, so the askable surface cannot change under us. Caching it keeps question
  // routing off the per-request DuckDB path entirely.
  let cachedVocabulary: readonly ReferenceVocabularyEntry[] | null = null;

  return {
    referenceVersion: snapshot.referenceVersion,
    referenceBuild: snapshot.referenceBuild,

    async resolve(targetId: string, referenceBuild: string): Promise<ResolvedTargets> {
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
      // id is the one value on this path that came from outside. The featured rsIDs are bound
      // too, from `$4` on: they are module constants and could be interpolated safely, but a
      // query with one interpolation habit is a query somebody extends with a second one.
      //
      // `COUNT(*) OVER ()` is what makes "the 64 read" distinguishable from "the 64 there are":
      // the window is computed over every matching row, before `LIMIT` discards any, so it costs
      // one pass over a local, read-only reference table and no second round trip that a
      // concurrent writer could make disagree with the first.
      const featuredPlaceholders = FEATURED_RSIDS.map((_unused, index) => `$${index + 4}`).join(
        ', ',
      );
      const rows = (
        await connection.runAndReadAll(
          `
            SELECT chrom, pos, rsid, ref, alt, gene, phenotype, clinical_significance,
                   evidence_note, COUNT(*) OVER () AS coordinates_listed
            FROM ${REFERENCE_COORDINATES_TABLE}
            WHERE reference_version = $1
              AND reference_build = $2
              AND (upper(gene) = upper($3) OR lower(rsid) = lower($3))
            ORDER BY ${TARGET_RANKING_SQL(featuredPlaceholders)}
            LIMIT ${MAX_TARGETS_PER_QUERY};
          `,
          [snapshot.referenceVersion, snapshot.referenceBuild, trimmed, ...FEATURED_RSIDS],
        )
      ).getRowObjects();

      // Read off the first row and not off `rows.length`, which the `LIMIT` caps, and before the
      // filtering below, which can drop the row it was read from.
      const coordinatesListed = rows.length === 0 ? 0 : Number(rows[0]!.coordinates_listed);

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

      // Returned in the order the ranking produced, never re-sorted here. Re-sorting by
      // coordinate is precisely the defect this replaces: it made the answer's headline whichever
      // coordinate happened to sit lowest on the chromosome, so a warfarin question led with
      // rs2359612 and buried rs9923231 fourth.
      //
      // `coordinatesListed` counts the snapshot's rows; `targets.length` counts what survived
      // normalisation, so a fetched row this system cannot place spends its slot rather than
      // being backfilled by a second query. The two numbers are reported side by side in the
      // answer, so the gap is stated rather than hidden — which is the only property that matters
      // here, and one an extra round trip per question would not improve.
      return { targets, coordinatesListed };
    },

    async vocabulary(): Promise<readonly ReferenceVocabularyEntry[]> {
      if (closed) {
        throw new ReferenceSnapshotError(options.databasePath, 'has already been closed');
      }
      if (cachedVocabulary !== null) return cachedVocabulary;

      // Scoped to this snapshot's own version/build, like `resolve` — a row belonging to some
      // other labelled snapshot must not be advertised as answerable by this one. Ordered so the
      // list a user is shown is stable across restarts.
      const rows = (
        await connection.runAndReadAll(
          `
            SELECT gene, rsid, phenotype, clinical_significance
            FROM ${REFERENCE_COORDINATES_TABLE}
            WHERE reference_version = $1
              AND reference_build = $2
            ORDER BY gene, rsid;
          `,
          [snapshot.referenceVersion, snapshot.referenceBuild],
        )
      ).getRowObjects();

      cachedVocabulary = Object.freeze(
        rows.map((row) => ({
          gene: String(row.gene),
          rsid: row.rsid === null ? null : String(row.rsid),
          phenotype: String(row.phenotype),
          clinicalSignificance: String(row.clinical_significance),
        })),
      );
      return cachedVocabulary;
    },

    async close(): Promise<void> {
      if (closed) return;
      closed = true;
      connection.disconnectSync();
      instance.closeSync();
    },
  };
}
