/**
 * End-to-end smoke check of the serving path against a real published dataset.
 *
 * It initializes nothing from a VCF fixture: there is no local user-data database to seed. A
 * genome is only queryable once an ingestion run has published
 * `datasets/{datasetId}/manifest.json`, so this script requires the id of such a dataset and
 * fails loudly without one rather than answering from a stand-in.
 *
 *   DATASET_ID=demo-small-… node ts-api-agent/src/test_e2e.ts
 *
 * This is a developer smoke script, not a test: the committed proof of the same path is
 * `tests/integration/cross_language_ingestion.test.ts`, which drives the whole slice from
 * `POST /api/ingestions`. Use `make demo` against a running stack for the end-to-end version.
 *
 * `dryRunLocal` pins the deterministic answer path so the check is about the *data* — what was
 * read, from which objects, against which reference snapshot — and not about a model's wording.
 */
import { artifactBucketFromEnv } from './application/artifact-bucket.ts';
import { askBioinformaticsAgent } from './infrastructure/ai/agent.ts';
import { openClinVarCoordinateResolver } from './infrastructure/database/clinvar-coordinate-resolver.ts';
import {
  createDuckDbSessionFactory,
  duckDbS3SessionConfigFromEnv,
} from './infrastructure/database/duckdb-session-factory.ts';
import { createGenotypeRepositoryFactory } from './infrastructure/database/duckdb.ts';
import { createParquetDatasetResolver } from './infrastructure/database/parquet-dataset-resolver.ts';
import {
  buildReferenceDatabase,
  defaultReferenceSnapshotOptions,
} from './infrastructure/database/reference-bootstrap.ts';
import { S3ObjectStore } from './infrastructure/object-store/s3-object-store.ts';

// Shared with `index.ts` and `worker.ts` via `artifactBucketFromEnv`, so this script cannot read
// `ARTIFACT_BUCKET` while the rest of the system reads `S3_ARTIFACT_BUCKET` and silently drift
// apart on a rename.
const ARTIFACT_BUCKET = artifactBucketFromEnv();

async function runE2eTest() {
  const datasetId = process.env.DATASET_ID;
  if (datasetId === undefined || datasetId.length === 0) {
    throw new Error(
      'set DATASET_ID to a dataset whose manifest has been published; the serving path has no ' +
        'fixture dataset to fall back to',
    );
  }

  console.log('[E2E Test] Step 1: Opening the versioned ClinVar coordinate snapshot...');
  const snapshot = await buildReferenceDatabase(defaultReferenceSnapshotOptions());
  const coordinateResolver = await openClinVarCoordinateResolver({ databasePath: snapshot.path });
  console.log(`[E2E Test] Reference snapshot ${snapshot.referenceVersion} (${snapshot.rowCount} targets).`);

  const objectStore = S3ObjectStore.fromEnv();
  const repositoryFactory = createGenotypeRepositoryFactory({
    datasetResolver: createParquetDatasetResolver({ objectStore, artifactBucket: ARTIFACT_BUCKET }),
    coordinateResolver,
    sessionFactory: createDuckDbSessionFactory({
      s3: duckDbS3SessionConfigFromEnv(process.env, ARTIFACT_BUCKET),
    }),
  });

  try {
    console.log(`[E2E Test] Step 2: Opening published dataset '${datasetId}'...`);
    const genotypeRepository = await repositoryFactory.open(datasetId);

    const question = 'Can I drink coffee?';
    console.log(`\n[E2E Test] Step 3: Querying AI Agent with question: "${question}"...`);
    const response = await askBioinformaticsAgent(question, {
      genotypeRepository,
      // The same snapshot the repository resolves against: routing the question and reading the
      // genotype have to agree about what is askable.
      referenceVocabulary: await coordinateResolver.vocabulary(),
      dryRunLocal: true,
    });

    console.log('\n--- E2E Test Response ---');
    console.log('Answer:', response.answer);
    console.log('Evidence:', JSON.stringify(response.evidence, null, 2));
    console.log('Provenance:', JSON.stringify(response.provenance, null, 2));
    console.log('-------------------------\n');

    if (!response.evidence || response.evidence.length === 0) {
      throw new Error('E2E Test FAILED: No evidence extracted for CYP1A2 / rs762551');
    }

    const caffeineVariant = response.evidence.find((v: any) => v.rsid === 'rs762551');
    if (!caffeineVariant) {
      throw new Error('E2E Test FAILED: rs762551 not found in query_genotype tool output');
    }

    const provenance = response.provenance;
    if (!provenance || provenance.datasetId !== datasetId) {
      throw new Error(
        `E2E Test FAILED: provenance names dataset '${provenance?.datasetId}', asked for '${datasetId}'`,
      );
    }
    if (provenance.filesScanned.length === 0) {
      throw new Error('E2E Test FAILED: an answer with no scanned object is not evidence');
    }

    console.log(
      `✔ E2E Test PASSED: read ${caffeineVariant.userGenotype} for rs762551 (CYP1A2) from ` +
        `${provenance.filesScanned.length} remote Parquet object(s):\n` +
        provenance.filesScanned.map((uri) => `    ${uri}`).join('\n') +
        `\n  dataset checksum ${provenance.datasetChecksumSha256}` +
        `\n  reference ${provenance.referenceBuild}/${provenance.referenceVersion}`,
    );
  } finally {
    await coordinateResolver.close();
    objectStore.destroy();
  }
}

runE2eTest().catch((err) => {
  console.error('E2E Test FAILED with error:', err);
  process.exit(1);
});
