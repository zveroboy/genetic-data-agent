/**
 * Two genomes, ingested by the same pipeline into the same artifact bucket, must never be able
 * to answer for each other.
 *
 * Both datasets come out of the *real* ingestion path — `POST /api/ingestions`, the real
 * Workflow, the real Rust Activity — from two source VCFs that disagree on every genotype this
 * test asks about. Each is reached through a separate allowlisted catalog entry that exists only
 * inside this test process.
 *
 * Isolation is checked three ways, because "the answers differ" is the weakest of them:
 *
 * 1. **Answer.** Each dataset reports its own genotype for the same question.
 * 2. **URI.** Neither answer's provenance names an object belonging to the other dataset, and
 *    the two manifests declare disjoint key sets.
 * 3. **Traffic.** Every object-store operation performed while serving one dataset addresses a
 *    key under that dataset's own prefix. A leak that produced the right answer by accident
 *    would still fail this.
 *
 * Then the negative case: a declared partition is corrupted, and another removed — both objects
 * this run uploaded itself. The dataset must refuse to answer with a named failure, not return
 * whatever survived.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DeleteObjectCommand, PutObjectCommand, type S3Client } from '@aws-sdk/client-s3';

import { manifestKeyFor } from '../../ts-api-agent/src/application/control-plane-activities.ts';
import { datasetCatalog } from '../../ts-api-agent/src/application/dataset-catalog.ts';
import {
  type DatasetManifest,
  DatasetManifestSchema,
} from '../../ts-api-agent/src/application/ingestion-contracts.ts';
import type { DatasetKey } from '../../ts-api-agent/src/domain/datasets.ts';
import { REFERENCE_BUILD, REFERENCE_VERSION } from '../../ts-api-agent/src/domain/datasets.ts';
import { createApp } from '../../ts-api-agent/src/index.ts';
import { askBioinformaticsAgent } from '../../ts-api-agent/src/infrastructure/ai/agent.ts';
import { openClinVarCoordinateResolver } from '../../ts-api-agent/src/infrastructure/database/clinvar-coordinate-resolver.ts';
import { createDuckDbSessionFactory } from '../../ts-api-agent/src/infrastructure/database/duckdb-session-factory.ts';
import { createParquetDatasetResolver } from '../../ts-api-agent/src/infrastructure/database/parquet-dataset-resolver.ts';
import { buildReferenceDatabase } from '../../ts-api-agent/src/infrastructure/database/reference-bootstrap.ts';
import { createTemporalIngestionClient } from '../../ts-api-agent/src/infrastructure/temporal/temporal-ingestion-client.ts';
import {
  recordPublicationOrder,
  type PublicationRecorder,
} from './support/recording-object-store.ts';
import {
  type ControlPlaneWorker,
  OwnedBuckets,
  REPO_ROOT,
  type RunningApi,
  type RustWorker,
  S3_ACCESS_KEY,
  S3_ENDPOINT,
  S3_REGION,
  S3_SECRET_KEY,
  type TemporalDevServer,
  buildRustWorker,
  clearLlmProviderKeys,
  newObjectStore,
  newRunId,
  newS3Client,
  postJson,
  putSourceObject,
  startApi,
  startControlPlaneWorker,
  startMinio,
  startRustWorker,
  startTemporalDevServer,
  testCatalog,
  testCatalogEntry,
  waitForIngestion,
} from './support/stack.ts';

const RUN_ID = newRunId();
const SOURCE_BUCKET = `isolation-src-${RUN_ID}`;
const ARTIFACT_BUCKET = `isolation-art-${RUN_ID}`;
const INGESTION_TIMEOUT_MS = 5 * 60_000;

/**
 * Both fixtures carry the same three clinical sites at the same coordinates and disagree on
 * every genotype. Opposite homozygotes, so a leak cannot hide behind a shared heterozygote.
 */
interface Fixture {
  readonly label: string;
  readonly datasetKey: DatasetKey;
  readonly sourceKey: string;
  /** rs762551 (CYP1A2, chr15 74749576 C>A). */
  readonly cyp1a2: { gt: string; expected: string };
  /** rs4149056 (SLCO1B1, chr12 21178615 T>C). */
  readonly slco1b1: { gt: string; expected: string };
}

const FIXTURES: readonly Fixture[] = [
  {
    label: 'alpha',
    datasetKey: 'demo-small',
    sourceKey: 'samples/alpha.vcf',
    cyp1a2: { gt: '0/0', expected: 'C/C' },
    slco1b1: { gt: '0/0', expected: 'T/T' },
  },
  {
    label: 'beta',
    datasetKey: 'na12878-full',
    sourceKey: 'samples/beta.vcf',
    cyp1a2: { gt: '1/1', expected: 'A/A' },
    slco1b1: { gt: '1/1', expected: 'C/C' },
  },
];

/** MTHFR rs1801133, chr1 11796321 G>A — a third partition neither question ever reads. */
const MTHFR_LINE = '1\t11796321\trs1801133\tG\tA\t99\tPASS\tGENE=MTHFR\tGT\t0/1\n';

function fixtureVcf(fixture: Fixture): string {
  return (
    '##fileformat=VCFv4.2\n' +
    `##source=DatasetIsolationIntegrationTest:${fixture.label}\n` +
    '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE\n' +
    `15\t74749576\trs762551\tC\tA\t99\tPASS\tGENE=CYP1A2\tGT\t${fixture.cyp1a2.gt}\n` +
    `12\t21178615\trs4149056\tT\tC\t99\tPASS\tGENE=SLCO1B1\tGT\t${fixture.slco1b1.gt}\n` +
    MTHFR_LINE
  );
}

describe('dataset isolation (two real ingestions, one bucket)', () => {
  let s3: S3Client;
  let buckets: OwnedBuckets;
  let temporal: TemporalDevServer;
  let rustWorker: RustWorker;
  let controlPlane: ControlPlaneWorker;
  let api: RunningApi;
  let servingStore: PublicationRecorder;
  let objectStore: ReturnType<typeof newObjectStore>;
  let coordinateResolver: Awaited<ReturnType<typeof openClinVarCoordinateResolver>>;
  let ingestionClient: ReturnType<typeof createTemporalIngestionClient>;
  let workDir = '';
  let stagingRoot = '';

  const datasetIds = new Map<string, string>();
  const manifests = new Map<string, DatasetManifest>();

  before(async () => {
    clearLlmProviderKeys();
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'isolation-e2e-'));
    stagingRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'isolation-staging-'));

    await startMinio();
    s3 = newS3Client();
    buckets = new OwnedBuckets(s3);
    await buckets.create(SOURCE_BUCKET);
    await buckets.create(ARTIFACT_BUCKET);

    for (const fixture of FIXTURES) {
      const local = path.join(workDir, `${fixture.label}.vcf`);
      fs.writeFileSync(local, fixtureVcf(fixture));
      await putSourceObject(s3, SOURCE_BUCKET, fixture.sourceKey, local);
    }

    temporal = await startTemporalDevServer();
    await buildRustWorker();
    rustWorker = await startRustWorker({ address: temporal.address, stagingRoot });

    objectStore = newObjectStore();
    // The same decorator the publication-order test uses, here for its operation log: it records
    // every key the serving path addresses, which is how "no cross-dataset traffic" is checked.
    servingStore = recordPublicationOrder(objectStore, objectStore);

    controlPlane = await startControlPlaneWorker({
      address: temporal.address,
      objectStore,
      artifactBucket: ARTIFACT_BUCKET,
      catalog: testCatalog(
        Object.fromEntries(
          FIXTURES.map((fixture) => [
            fixture.datasetKey,
            testCatalogEntry(fixture.datasetKey, SOURCE_BUCKET, fixture.sourceKey),
          ]),
        ),
      ),
    });

    const snapshot = await buildReferenceDatabase({
      tsvPath: path.join(REPO_ROOT, 'tests/fixtures/clinvar_coordinates_grch38.tsv'),
      databasePath: path.join(workDir, 'reference.duckdb'),
      referenceVersion: REFERENCE_VERSION,
      referenceBuild: REFERENCE_BUILD,
    });
    coordinateResolver = await openClinVarCoordinateResolver({ databasePath: snapshot.path });
    ingestionClient = createTemporalIngestionClient({ address: temporal.address });

    api = await startApi(
      createApp({
        catalog: datasetCatalog,
        ingestionClient,
        datasetResolver: createParquetDatasetResolver({
          objectStore: servingStore,
          artifactBucket: ARTIFACT_BUCKET,
        }),
        coordinateResolver,
        sessionFactory: createDuckDbSessionFactory({
          s3: {
            endpoint: new URL(S3_ENDPOINT).host,
            region: S3_REGION,
            accessKeyId: S3_ACCESS_KEY,
            secretAccessKey: S3_SECRET_KEY,
            useSsl: new URL(S3_ENDPOINT).protocol === 'https:',
            urlStyle: 'path',
            scope: `s3://${ARTIFACT_BUCKET}/`,
          },
        }),
        askAgent: (question, options) => askBioinformaticsAgent(question, options),
      }),
    );

    for (const fixture of FIXTURES) {
      const started = await postJson(`${api.baseUrl}/api/ingestions`, {
        datasetKey: fixture.datasetKey,
      });
      assert.equal(started.status, 202, JSON.stringify(started.body));
      const terminal = await waitForIngestion(
        api.baseUrl,
        started.body.workflowId,
        INGESTION_TIMEOUT_MS,
      );
      assert.equal(
        terminal.state,
        'COMPLETED',
        `${fixture.label} did not complete: ${JSON.stringify(terminal)}`,
      );
      datasetIds.set(fixture.label, started.body.datasetId);
      manifests.set(
        fixture.label,
        DatasetManifestSchema.parse(
          await objectStore.getJson({
            bucket: ARTIFACT_BUCKET,
            key: manifestKeyFor(started.body.datasetId),
          }),
        ),
      );
    }
  });

  after(async () => {
    await api?.stop();
    await ingestionClient?.close();
    await coordinateResolver?.close();
    await controlPlane?.stop();
    await rustWorker?.stop();
    await temporal?.stop();
    objectStore?.destroy();
    await buckets?.removeAll();
    s3?.destroy();
    if (workDir) fs.rmSync(workDir, { recursive: true, force: true });
    if (stagingRoot) fs.rmSync(stagingRoot, { recursive: true, force: true });
  });

  /** Keys the serving store addressed while running `work`, in order. */
  async function keysTouchedDuring(work: () => Promise<void>): Promise<string[]> {
    const before_ = servingStore.operations.length;
    await work();
    return servingStore.operations.slice(before_).map((operation) => operation.key);
  }

  it('gives each dataset its own identity and its own objects', () => {
    const [alpha, beta] = FIXTURES.map((fixture) => manifests.get(fixture.label)!);
    assert.notEqual(alpha!.datasetId, beta!.datasetId);
    assert.notEqual(
      alpha!.datasetChecksumSha256,
      beta!.datasetChecksumSha256,
      'two genomes that disagree on every genotype cannot share a content checksum',
    );

    const alphaKeys = new Set(alpha!.parquetObjects.map((object) => object.key));
    for (const object of beta!.parquetObjects) {
      assert.ok(!alphaKeys.has(object.key), `key '${object.key}' is claimed by both manifests`);
    }
    for (const [label, manifest] of manifests) {
      const datasetId = datasetIds.get(label)!;
      for (const object of manifest.parquetObjects) {
        assert.ok(
          object.key.startsWith(`datasets/${datasetId}/`),
          `${label} declares '${object.key}', outside its own dataset prefix`,
        );
      }
    }
  });

  it('answers the same question differently, from its own objects only', async () => {
    for (const fixture of FIXTURES) {
      const datasetId = datasetIds.get(fixture.label)!;
      const manifest = manifests.get(fixture.label)!;
      const otherLabel = FIXTURES.find((entry) => entry.label !== fixture.label)!.label;
      const otherKeys = manifests
        .get(otherLabel)!
        .parquetObjects.map((object) => object.key);

      let response: any;
      const touched = await keysTouchedDuring(async () => {
        const asked = await postJson(`${api.baseUrl}/ask`, {
          datasetId,
          question: 'Can I drink coffee?',
        });
        assert.equal(asked.status, 200, JSON.stringify(asked.body));
        response = asked.body;
      });

      const variant = response.variants.find((entry: any) => entry.rsid === 'rs762551');
      assert.ok(variant, `${fixture.label}: no rs762551 evidence`);
      assert.equal(
        variant.userGenotype,
        fixture.cyp1a2.expected,
        `${fixture.label} answered with the wrong genotype`,
      );
      assert.equal(response.provenance.datasetId, datasetId);
      assert.equal(
        response.provenance.datasetChecksumSha256,
        manifest.datasetChecksumSha256,
      );

      const chrom15 = manifest.parquetObjects.find((object) => object.chrom === '15')!;
      assert.deepEqual(response.provenance.filesScanned, [
        `s3://${ARTIFACT_BUCKET}/${chrom15.key}`,
      ]);

      // Traffic, not just answers: nothing addressed while serving this dataset may name the
      // other dataset's prefix, and nothing may name the other dataset's objects.
      const foreign = touched.filter((key) => !key.startsWith(`datasets/${datasetId}/`));
      assert.deepEqual(foreign, [], `${fixture.label} touched keys outside its own prefix`);
      for (const key of otherKeys) {
        assert.ok(!touched.includes(key), `${fixture.label} touched '${key}' from ${otherLabel}`);
      }
    }
  });

  it('does not let one dataset id resolve another dataset manifest', async () => {
    const alphaId = datasetIds.get('alpha')!;
    const betaId = datasetIds.get('beta')!;

    // A manifest parked at the wrong key is rejected on its own `datasetId`, not served.
    const stray = `${alphaId}-stray`;
    await s3.send(
      new PutObjectCommand({
        Bucket: ARTIFACT_BUCKET,
        Key: manifestKeyFor(stray),
        Body: JSON.stringify(manifests.get('beta')!),
        ContentType: 'application/json',
      }),
    );

    const asked = await postJson(`${api.baseUrl}/ask`, {
      datasetId: stray,
      question: 'Can I drink coffee?',
    });
    assert.equal(asked.status, 409, JSON.stringify(asked.body));
    assert.equal(asked.body.error, 'DatasetResolutionFailed');
    assert.match(asked.body.message, /MANIFEST_DATASET_ID_MISMATCH/);
    assert.match(asked.body.message, new RegExp(betaId));
    assert.equal(asked.body.variants, undefined, 'a refused dataset returns no evidence at all');

    // Removes only the object this test just created.
    await s3.send(new DeleteObjectCommand({ Bucket: ARTIFACT_BUCKET, Key: manifestKeyFor(stray) }));
  });

  it('refuses to answer from a corrupted partition instead of returning partial evidence', async () => {
    const alphaId = datasetIds.get('alpha')!;
    const manifest = manifests.get('alpha')!;
    const chrom15 = manifest.parquetObjects.find((object) => object.chrom === '15')!;

    // Overwrites an object this run uploaded, in a bucket this run created, with content that
    // no longer matches the identity the manifest declares.
    await s3.send(
      new PutObjectCommand({
        Bucket: ARTIFACT_BUCKET,
        Key: chrom15.key,
        Body: Buffer.from('this is not a parquet file'),
        Metadata: { sha256: chrom15.checksumSha256 },
      }),
    );

    const asked = await postJson(`${api.baseUrl}/ask`, {
      datasetId: alphaId,
      question: 'Can I drink coffee?',
    });
    assert.equal(asked.status, 409, JSON.stringify(asked.body));
    assert.equal(asked.body.error, 'ObjectVerificationFailed');
    assert.match(asked.body.message, /ETAG_MISMATCH|SIZE_MISMATCH|CHECKSUM_METADATA_MISMATCH/);
    assert.match(asked.body.message, new RegExp(chrom15.key.replaceAll('=', '=')));
    assert.equal(asked.body.variants, undefined, 'no evidence may survive a failed verification');
  });

  it('refuses to answer when a declared partition is gone', async () => {
    const alphaId = datasetIds.get('alpha')!;
    const manifest = manifests.get('alpha')!;
    const chrom15 = manifest.parquetObjects.find((object) => object.chrom === '15')!;

    await s3.send(new DeleteObjectCommand({ Bucket: ARTIFACT_BUCKET, Key: chrom15.key }));

    const asked = await postJson(`${api.baseUrl}/ask`, {
      datasetId: alphaId,
      question: 'Can I drink coffee?',
    });
    assert.equal(asked.status, 409, JSON.stringify(asked.body));
    assert.equal(asked.body.error, 'ObjectVerificationFailed');
    assert.match(asked.body.message, /OBJECT_MISSING/);
    assert.equal(asked.body.variants, undefined);
  });

  it('leaves the other dataset answering correctly while the first is broken', async () => {
    const betaId = datasetIds.get('beta')!;
    const beta = FIXTURES.find((fixture) => fixture.label === 'beta')!;

    const asked = await postJson(`${api.baseUrl}/ask`, {
      datasetId: betaId,
      question: 'Can I drink coffee?',
    });
    assert.equal(asked.status, 200, JSON.stringify(asked.body));
    const variant = asked.body.variants.find((entry: any) => entry.rsid === 'rs762551');
    assert.equal(variant.userGenotype, beta.cyp1a2.expected);
    assert.equal(asked.body.provenance.datasetId, betaId);
  });
});
