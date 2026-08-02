/**
 * The seeded dataset allowlist.
 *
 * This is the only place an S3 bucket/key for ingestion is decided. API input selects a
 * catalog key and nothing else: arbitrary uploads, URLs, `s3://` URIs and filesystem paths
 * are rejected here rather than being sanitised downstream.
 */
import {
  DATASET_KEYS,
  REFERENCE_BUILD,
  REFERENCE_VERSION,
  isDatasetKey,
} from '../domain/datasets.ts';
import type { DatasetCatalogEntry, DatasetKey } from '../domain/datasets.ts';

export class UnknownDatasetKeyError extends Error {
  readonly requestedKey: string;

  constructor(requestedKey: string) {
    super(
      `unknown dataset key '${requestedKey}'; expected one of: ${DATASET_KEYS.join(', ')}`,
    );
    this.name = 'UnknownDatasetKeyError';
    this.requestedKey = requestedKey;
  }
}

function seed(
  key: DatasetKey,
  displayName: string,
  description: string,
  bucket: string,
  objectKey: string,
): DatasetCatalogEntry {
  return Object.freeze({
    key,
    displayName,
    description,
    source: Object.freeze({ bucket, key: objectKey }),
    expectedReferenceBuild: REFERENCE_BUILD,
    referenceVersion: REFERENCE_VERSION,
  });
}

const ENTRIES: Readonly<Record<DatasetKey, DatasetCatalogEntry>> = Object.freeze({
  'demo-small': seed(
    'demo-small',
    'Demo Small',
    'Small synthetic demo VCF used for fast end-to-end runs.',
    'genomic-data',
    'samples/demo_user.vcf',
  ),
  'na12878-full': seed(
    'na12878-full',
    'NA12878 (GIAB HG001)',
    'Public Genome in a Bottle NA12878/HG001 whole-genome VCF, gzip compressed.',
    'genomic-data',
    'samples/na12878_hg001.vcf.gz',
  ),
});

export const datasetCatalog = Object.freeze({
  /** Resolves a caller-supplied string to a seeded entry, or throws. */
  get(requestedKey: string): DatasetCatalogEntry {
    if (!isDatasetKey(requestedKey)) {
      throw new UnknownDatasetKeyError(requestedKey);
    }
    return ENTRIES[requestedKey];
  },

  /** All seeded entries, in declaration order. */
  list(): readonly DatasetCatalogEntry[] {
    return DATASET_KEYS.map((key) => ENTRIES[key]);
  },
});
