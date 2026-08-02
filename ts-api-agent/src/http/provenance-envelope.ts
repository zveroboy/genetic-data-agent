import type { GenotypeProvenance } from '../infrastructure/database/duckdb.ts';
import type { ResolvedParquetDataset } from '../infrastructure/database/parquet-dataset-resolver.ts';

/**
 * What was read, and against what.
 *
 * The manifest half — checksum, layout/schema version, fingerprint, artifact version, reference
 * identity — describes the dataset the request was authorised against, and is present on every
 * answer. The read half — the exact object URIs and how many coordinates were resolved — comes
 * from the genotype tool and is empty when the agent never queried genotypes, because claiming
 * a scan that did not happen is the same lie as claiming a variant that was not found.
 */
export function provenanceEnvelope(
  dataset: ResolvedParquetDataset,
  read: GenotypeProvenance | undefined,
) {
  if (read !== undefined && read.datasetId !== dataset.datasetId) {
    throw new Error(
      `internal invariant violated: provenance for '${read.datasetId}' was produced while ` +
        `serving '${dataset.datasetId}'`,
    );
  }
  return {
    datasetId: dataset.datasetId,
    datasetChecksumSha256: dataset.datasetChecksumSha256,
    artifactFormat: dataset.manifest.artifactFormat,
    artifactVersion: dataset.manifest.artifactVersion,
    layoutVersion: dataset.manifest.layoutVersion,
    schemaVersion: dataset.manifest.schemaVersion,
    schemaFingerprint: dataset.manifest.schemaFingerprint,
    referenceBuild: dataset.referenceBuild,
    referenceVersion: dataset.referenceVersion,
    filesScanned: read?.filesScanned ?? [],
    targetsResolved: read?.targetsResolved ?? 0,
  };
}
