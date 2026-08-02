/**
 * HTTP contract tests for the dataset lifecycle.
 *
 * These are mostly tests about what the API refuses to do. The endpoint this replaces accepted
 * an arbitrary `fileKey`, derived a user identity from a filename, and — when Temporal was
 * unreachable — ran a timer that reported a dataset as ingested while nothing was ingesting it.
 * Every one of those is a way to tell a caller that somebody's genome is queryable when no
 * manifest exists, so the assertions below pin the opposite:
 *
 * - the catalog is exactly the two seeded keys, and carries no S3 identity at all;
 * - anything that is not a seeded key — an `s3://` URI, an HTTP URL, a filesystem path, a
 *   traversal — is a `400`, and starts nothing;
 * - a request body may not carry a Parquet URI or a manifest override, in either endpoint;
 * - an unreachable orchestrator is a `503` with no state, no ids and no progress;
 * - `/ask` needs an explicit published dataset, and every serving-layer failure surfaces as
 *   itself rather than as a fixture-shaped answer.
 *
 * The serving dependencies are faked at the ports `createApp` takes, but the *real*
 * `createParquetDatasetResolver` and `createGenotypeRepositoryFactory` run inside the app, so
 * manifest validation, candidate selection and object verification are the production ones.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  computeDatasetChecksumSha256,
  sha256Hex,
} from './application/dataset-checksum.ts';
import {
  type DatasetManifest,
  DatasetManifestSchema,
  type ParquetObject,
} from './application/ingestion-contracts.ts';
import {
  type IngestionClient,
  IngestionRunNotFoundError,
  IngestionServiceUnavailableError,
  type StartIngestionRequest,
  ingestionWorkflowIdFor,
} from './application/ingestion-client.ts';
import { datasetCatalog } from './application/dataset-catalog.ts';
import type { IngestionProgress } from './application/workflows.ts';
import { DATASET_KEYS } from './domain/datasets.ts';
import {
  ReferenceBuildMismatchError,
  type ClinVarCoordinateResolver,
  TargetNotResolvableError,
  type VariantTarget,
} from './infrastructure/database/clinvar-coordinate-resolver.ts';
import { QueryBudgetExceededError } from './infrastructure/database/duckdb-session-factory.ts';
import type {
  DuckDbSession,
  DuckDbSessionFactory,
} from './infrastructure/database/duckdb-session-factory.ts';
import { RemoteDatasetUnavailableError } from './infrastructure/database/duckdb.ts';
import type { GenotypeRepository } from './infrastructure/database/duckdb.ts';
import {
  DatasetNotPublishedError,
  TargetNotPresentError,
  createParquetDatasetResolver,
} from './infrastructure/database/parquet-dataset-resolver.ts';
import {
  type ConditionalPutOutcome,
  type ObjectHead,
  type ObjectLocation,
  type ObjectStore,
  headManyBounded,
} from './infrastructure/object-store/object-store.ts';
import type { AgentResponse } from './infrastructure/ai/agent.ts';
import { type AppDependencies, type BioinformaticsAgent, createApp } from './index.ts';

const ARTIFACT_BUCKET = 'genomic-artifacts';
const DATASET_ID = 'demo-small-0a1b2c3d';
const ARTIFACT_VERSION = 'v1';
const ATTEMPT_PREFIX = `datasets/${DATASET_ID}/versions/${ARTIFACT_VERSION}/attempt-1/`;

const goldenManifest = DatasetManifestSchema.parse(
  JSON.parse(
    readFileSync(
      fileURLToPath(new URL('../../contracts/fixtures/dataset-manifest.json', import.meta.url)),
      'utf8',
    ),
  ),
);

function parquetObject(chrom: string, minPos: number, maxPos: number): ParquetObject {
  const relativePath = `chrom=${chrom}/part-000.parquet`;
  const key = `${ATTEMPT_PREFIX}variants/${relativePath}`;
  return {
    bucket: ARTIFACT_BUCKET,
    key,
    etag: `etag-${key}`,
    versionId: null,
    chrom,
    checksumSha256: sha256Hex(relativePath),
    byteSize: 8192,
    rowCount: 1000,
    minPos,
    maxPos,
  };
}

const INVENTORY = [parquetObject('12', 20_000_000, 60_000_000)];
const EXPECTED_FILE_URI = `s3://${ARTIFACT_BUCKET}/${INVENTORY[0]!.key}`;

function manifest(overrides: Partial<DatasetManifest> = {}): DatasetManifest {
  return DatasetManifestSchema.parse({
    ...structuredClone(goldenManifest),
    datasetId: DATASET_ID,
    artifactVersion: ARTIFACT_VERSION,
    attemptPrefix: ATTEMPT_PREFIX,
    parquetObjects: INVENTORY,
    datasetChecksumSha256: computeDatasetChecksumSha256(ATTEMPT_PREFIX, INVENTORY),
    ...overrides,
  });
}

class FakeObjectStore implements ObjectStore {
  readonly objects = new Map<string, { body: string; head: ObjectHead }>();

  seedManifest(value: DatasetManifest): void {
    const key = `datasets/${value.datasetId}/manifest.json`;
    const body = JSON.stringify(value);
    this.objects.set(`${ARTIFACT_BUCKET}/${key}`, {
      body,
      head: {
        bucket: ARTIFACT_BUCKET,
        key,
        etag: 'manifest-etag',
        versionId: null,
        contentLength: Buffer.byteLength(body, 'utf8'),
        checksumSha256: sha256Hex(body),
      },
    });
    for (const object of value.parquetObjects) {
      this.objects.set(`${object.bucket}/${object.key}`, {
        body: '',
        head: {
          bucket: object.bucket,
          key: object.key,
          etag: object.etag,
          versionId: object.versionId,
          contentLength: object.byteSize,
          checksumSha256: object.checksumSha256,
        },
      });
    }
  }

  async head(location: ObjectLocation): Promise<ObjectHead | null> {
    return this.objects.get(`${location.bucket}/${location.key}`)?.head ?? null;
  }

  async headMany(locations: readonly ObjectLocation[]): Promise<readonly (ObjectHead | null)[]> {
    return headManyBounded((location) => this.head(location), locations, 4);
  }

  async getJson(location: ObjectLocation): Promise<unknown> {
    const stored = this.objects.get(`${location.bucket}/${location.key}`);
    return stored === undefined ? null : JSON.parse(stored.body);
  }

  async putJsonConditional(): Promise<ConditionalPutOutcome> {
    throw new Error('the serving path must never write');
  }
}

/** One SQL row as DuckDB hands it back: the physical column names, still snake_case. */
const PARQUET_ROW = {
  rsid: 'rs4149056',
  gene: 'SLCO1B1',
  user_genotype: 'T/C',
  phenotype: 'Statins myopathy risk',
  clinical_significance: 'Risk Factor',
  evidence_note: 'Intermediate OATP1B1 function.',
};

class FakeSessionFactory implements DuckDbSessionFactory {
  openCount = 0;
  rows: Record<string, unknown>[] = [];

  async open(): Promise<DuckDbSession> {
    this.openCount += 1;
    const owner = this;
    return {
      async query() {
        return owner.rows;
      },
      async close() {},
    };
  }
}

function variantTarget(): VariantTarget {
  return {
    referenceBuild: 'GRCh38',
    referenceVersion: 'demo-clinvar-grch38-v1',
    chrom: '12',
    pos: 21_178_615,
    ref: 'T',
    alt: 'C',
    rsid: 'rs4149056',
    gene: 'SLCO1B1',
    phenotype: 'Statins myopathy risk',
    clinicalSignificance: 'Risk Factor',
    evidenceNote: 'Intermediate OATP1B1 function.',
  };
}

class FakeCoordinateResolver implements ClinVarCoordinateResolver {
  readonly referenceVersion = 'demo-clinvar-grch38-v1';
  readonly referenceBuild = 'GRCh38';

  async resolve(): Promise<readonly VariantTarget[]> {
    return [variantTarget()];
  }

  async close(): Promise<void> {}
}

const RUNNING_PROGRESS: IngestionProgress = {
  datasetId: DATASET_ID,
  datasetKey: 'demo-small',
  state: 'VERIFYING_OBJECTS',
  unobservedStates: ['PUBLISHING_MANIFEST'],
  message: 'Verifying the uploaded Parquet inventory and publishing the manifest.',
};

class FakeIngestionClient implements IngestionClient {
  readonly started: StartIngestionRequest[] = [];
  startFailure: Error | null = null;
  progressFailure: Error | null = null;
  progress: IngestionProgress = RUNNING_PROGRESS;

  async start(request: StartIngestionRequest): Promise<void> {
    if (this.startFailure !== null) throw this.startFailure;
    this.started.push(request);
  }

  async getProgress(): Promise<IngestionProgress> {
    if (this.progressFailure !== null) throw this.progressFailure;
    return this.progress;
  }
}

interface Harness {
  readonly app: ReturnType<typeof createApp>;
  readonly ingestion: FakeIngestionClient;
  readonly sessions: FakeSessionFactory;
  readonly store: FakeObjectStore;
  /** Every question the injected agent was asked, with the dataset it was scoped to. */
  readonly asked: { question: string; datasetId: string }[];
}

/**
 * Builds the app over fake ports. `askAgent` defaults to an agent that actually uses the
 * repository it was handed, so a passing provenance assertion means the dataset-scoped
 * repository really reached the agent — not that the route echoed a literal back.
 */
function harness(
  options: {
    published?: boolean;
    rows?: Record<string, unknown>[];
    askAgent?: BioinformaticsAgent;
  } = {},
): Harness {
  const store = new FakeObjectStore();
  if (options.published !== false) store.seedManifest(manifest());

  const sessions = new FakeSessionFactory();
  sessions.rows = options.rows ?? [PARQUET_ROW];

  const ingestion = new FakeIngestionClient();
  const asked: { question: string; datasetId: string }[] = [];

  const askAgent: BioinformaticsAgent =
    options.askAgent ??
    (async (question, { genotypeRepository }): Promise<AgentResponse> => {
      asked.push({ question, datasetId: genotypeRepository.datasetId });
      const result = await genotypeRepository.synthesizeVariant('SLCO1B1');
      return {
        answer: `Answered '${question}' from ${result.variants.length} variant(s).`,
        evidence: [...result.variants],
        provenance: result.provenance,
        toolsUsed: ['query_genotype'],
      };
    });

  const dependencies: AppDependencies = {
    catalog: datasetCatalog,
    ingestionClient: ingestion,
    datasetResolver: createParquetDatasetResolver({
      objectStore: store,
      artifactBucket: ARTIFACT_BUCKET,
    }),
    coordinateResolver: new FakeCoordinateResolver(),
    sessionFactory: sessions,
    askAgent,
  };

  return { app: createApp(dependencies), ingestion, sessions, store, asked };
}

async function postJson(app: Harness['app'], path: string, body: unknown): Promise<Response> {
  return app.request(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

/** An agent that fails the way the serving layer fails, without reaching the serving layer. */
function failingAgent(error: Error): BioinformaticsAgent {
  return async () => {
    throw error;
  };
}

/**
 * Captures the app's own `console.error` diagnostics.
 *
 * The API is supposed to log a 5xx before answering, so a test that provokes one would
 * otherwise spray stack traces over the report. Captured rather than discarded, so a test can
 * also assert that a detail withheld from the caller really did reach the log.
 */
async function capturingServerLog(
  run: () => Response | Promise<Response>,
): Promise<{ response: Response; logged: string }> {
  const original = console.error;
  const lines: string[] = [];
  console.error = (...args: unknown[]) => {
    lines.push(args.map((arg) => (arg instanceof Error ? arg.message : String(arg))).join(' '));
  };
  try {
    return { response: await run(), logged: lines.join('\n') };
  } finally {
    console.error = original;
  }
}

describe('GET /api/datasets/catalog', () => {
  it('returns exactly the two seeded entries', async () => {
    const { app } = harness();

    const response = await app.request('/api/datasets/catalog');
    assert.equal(response.status, 200);

    const body = (await response.json()) as { datasets: { key: string }[] };
    assert.deepEqual(
      body.datasets.map((entry) => entry.key),
      [...DATASET_KEYS],
    );
    assert.equal(body.datasets.length, 2);
  });

  it('carries display metadata but no S3 identity', async () => {
    const { app } = harness();

    const raw = await (await app.request('/api/datasets/catalog')).text();

    assert.doesNotMatch(raw, /s3:\/\//, 'the catalog must not hand the browser an object URI');
    assert.doesNotMatch(raw, /bucket/i, 'the catalog must not hand the browser a bucket name');
    assert.doesNotMatch(raw, /\.vcf/i, 'the catalog must not hand the browser a source object key');
    assert.match(raw, /displayName/);
    assert.match(raw, /referenceVersion/);
  });
});

describe('POST /api/ingestions', () => {
  it('accepts a seeded key and starts exactly one real run', async () => {
    const { app, ingestion } = harness();

    const response = await postJson(app, '/api/ingestions', { datasetKey: 'demo-small' });
    assert.equal(response.status, 202);

    const body = (await response.json()) as {
      datasetId: string;
      datasetKey: string;
      workflowId: string;
    };
    assert.equal(body.datasetKey, 'demo-small');
    assert.match(body.datasetId, /^demo-small-[0-9a-f-]{36}$/);
    assert.equal(body.workflowId, ingestionWorkflowIdFor(body.datasetId));

    assert.equal(ingestion.started.length, 1);
    assert.deepEqual(ingestion.started[0], {
      workflowId: body.workflowId,
      datasetId: body.datasetId,
      datasetKey: 'demo-small',
    });
  });

  it('mints a fresh identity per run', async () => {
    const { app } = harness();

    const first = (await (await postJson(app, '/api/ingestions', { datasetKey: 'demo-small' })).json()) as Record<string, string>;
    const second = (await (await postJson(app, '/api/ingestions', { datasetKey: 'demo-small' })).json()) as Record<string, string>;

    assert.notEqual(first.datasetId, second.datasetId);
    assert.notEqual(first.workflowId, second.workflowId);
  });

  for (const rejected of [
    's3://genomic-data/samples/na12878_hg001.vcf.gz',
    'https://example.com/genome.vcf',
    '/etc/passwd',
    'tests/fixtures/demo_user.vcf',
    '../demo-small',
    'demo-small/../../na12878-full',
    'DEMO-SMALL',
    '',
  ]) {
    it(`rejects the non-catalog key ${JSON.stringify(rejected)} with 400 and starts nothing`, async () => {
      const { app, ingestion } = harness();

      const response = await postJson(app, '/api/ingestions', { datasetKey: rejected });

      assert.equal(response.status, 400);
      const body = (await response.json()) as Record<string, unknown>;
      assert.equal(body.error, 'UnknownDatasetKey');
      assert.deepEqual(body.allowedDatasetKeys, [...DATASET_KEYS]);
      assert.equal(ingestion.started.length, 0);
    });
  }

  it('rejects a missing dataset key', async () => {
    const { app, ingestion } = harness();

    const response = await postJson(app, '/api/ingestions', {});

    assert.equal(response.status, 400);
    assert.equal(((await response.json()) as Record<string, unknown>).error, 'UnknownDatasetKey');
    assert.equal(ingestion.started.length, 0);
  });

  for (const [label, extra] of [
    ['a Parquet URI', { parquetUri: 's3://genomic-artifacts/datasets/x/variants/part-000.parquet' }],
    ['a manifest override', { manifest: { parquetObjects: [] } }],
    ['an artifact bucket', { bucket: 'somebody-elses-bucket' }],
    ['a source path', { fileKey: 'tests/fixtures/demo_user.vcf' }],
  ] as const) {
    it(`rejects a body carrying ${label}`, async () => {
      const { app, ingestion } = harness();

      const response = await postJson(app, '/api/ingestions', {
        datasetKey: 'demo-small',
        ...extra,
      });

      assert.equal(response.status, 400);
      const body = (await response.json()) as Record<string, unknown>;
      assert.equal(body.error, 'UnrecognizedRequestField');
      assert.equal(ingestion.started.length, 0, 'a rejected body must not start a run');
    });
  }

  it('returns 503 with no progress when the orchestrator is unreachable', async () => {
    const { app, ingestion } = harness();
    ingestion.startFailure = new IngestionServiceUnavailableError(
      'localhost:7233',
      'connection refused',
    );

    const { response } = await capturingServerLog(() =>
      postJson(app, '/api/ingestions', { datasetKey: 'demo-small' }),
    );

    assert.equal(response.status, 503);
    const body = (await response.json()) as Record<string, unknown>;
    assert.equal(body.error, 'IngestionServiceUnavailable');
    for (const fabricated of ['state', 'unobservedStates', 'datasetId', 'workflowId', 'percentage']) {
      assert.equal(
        fabricated in body,
        false,
        `a run that never started must not report '${fabricated}'`,
      );
    }
  });
});

describe('GET /api/ingestions/:workflowId', () => {
  it('returns the workflow-reported state and its unobserved states verbatim', async () => {
    const { app } = harness();

    const response = await app.request(`/api/ingestions/${ingestionWorkflowIdFor(DATASET_ID)}`);

    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), RUNNING_PROGRESS);
  });

  it('reports an unknown run as 404', async () => {
    const { app, ingestion } = harness();
    ingestion.progressFailure = new IngestionRunNotFoundError('genomic-ingestion-nope');

    const response = await app.request('/api/ingestions/genomic-ingestion-nope');

    assert.equal(response.status, 404);
    assert.equal(((await response.json()) as Record<string, unknown>).error, 'IngestionRunNotFound');
  });

  it('reports an unreachable orchestrator as 503, never as progress', async () => {
    const { app, ingestion } = harness();
    ingestion.progressFailure = new IngestionServiceUnavailableError('localhost:7233', 'down');

    const { response } = await capturingServerLog(() =>
      app.request(`/api/ingestions/${ingestionWorkflowIdFor(DATASET_ID)}`),
    );

    assert.equal(response.status, 503);
    const body = (await response.json()) as Record<string, unknown>;
    assert.equal(body.error, 'IngestionServiceUnavailable');
    assert.equal('state' in body, false);
  });
});

describe('POST /ask', () => {
  it('requires an explicit dataset id', async () => {
    const { app, asked, sessions } = harness();

    const response = await postJson(app, '/ask', { question: 'What is my CYP1A2 genotype?' });

    assert.equal(response.status, 400);
    assert.equal(((await response.json()) as Record<string, unknown>).error, 'MissingDatasetId');
    assert.equal(asked.length, 0, 'no dataset means no agent call');
    assert.equal(sessions.openCount, 0, 'no dataset means no DuckDB session');
  });

  it('requires a question', async () => {
    const { app, asked } = harness();

    const response = await postJson(app, '/ask', { datasetId: DATASET_ID, question: '   ' });

    assert.equal(response.status, 400);
    assert.equal(((await response.json()) as Record<string, unknown>).error, 'MissingQuestion');
    assert.equal(asked.length, 0);
  });

  for (const [label, extra] of [
    ['a Parquet URI', { parquetUri: `s3://${ARTIFACT_BUCKET}/datasets/other/variants/p.parquet` }],
    ['a manifest override', { manifest: { parquetObjects: [] } }],
    ['a file list', { filesScanned: [`s3://${ARTIFACT_BUCKET}/anything.parquet`] }],
  ] as const) {
    it(`rejects a body carrying ${label}`, async () => {
      const { app, asked, sessions } = harness();

      const response = await postJson(app, '/ask', {
        datasetId: DATASET_ID,
        question: 'What is my SLCO1B1 genotype?',
        ...extra,
      });

      assert.equal(response.status, 400);
      assert.equal(
        ((await response.json()) as Record<string, unknown>).error,
        'UnrecognizedRequestField',
      );
      assert.equal(asked.length, 0);
      assert.equal(sessions.openCount, 0);
    });
  }

  it('returns 409 for a dataset with no published manifest, without touching DuckDB', async () => {
    const { app, sessions, asked } = harness({ published: false });

    const response = await postJson(app, '/ask', {
      datasetId: DATASET_ID,
      question: 'What is my SLCO1B1 genotype?',
    });

    assert.equal(response.status, 409);
    const body = (await response.json()) as Record<string, unknown>;
    assert.equal(body.error, 'DatasetNotPublished');
    assert.equal(body.datasetId, DATASET_ID);
    assert.equal('answer' in body, false, 'an unpublished dataset must not produce an answer');
    assert.equal('variants' in body, false);
    assert.equal(sessions.openCount, 0);
    assert.equal(asked.length, 0);
  });

  it('rejects a dataset id that is not a single safe path segment', async () => {
    const { app } = harness();

    const response = await postJson(app, '/ask', {
      datasetId: '../../etc/passwd',
      question: 'What is my SLCO1B1 genotype?',
    });

    assert.equal(response.status, 400);
    assert.equal(
      ((await response.json()) as Record<string, unknown>).error,
      'DatasetResolutionFailed',
    );
  });

  it('answers from the published dataset with a complete camelCase provenance envelope', async () => {
    const { app, asked } = harness();

    const response = await postJson(app, '/ask', {
      datasetId: DATASET_ID,
      question: 'Am I at risk of statin myopathy?',
    });

    assert.equal(response.status, 200);
    const body = (await response.json()) as Record<string, any>;

    assert.deepEqual(asked, [
      { question: 'Am I at risk of statin myopathy?', datasetId: DATASET_ID },
    ]);

    assert.deepEqual(body.variants, [
      {
        rsid: 'rs4149056',
        gene: 'SLCO1B1',
        userGenotype: 'T/C',
        phenotype: 'Statins myopathy risk',
        clinicalSignificance: 'Risk Factor',
        evidenceNote: 'Intermediate OATP1B1 function.',
      },
    ]);

    const published = manifest();
    assert.deepEqual(body.provenance, {
      datasetId: DATASET_ID,
      datasetChecksumSha256: published.datasetChecksumSha256,
      artifactFormat: published.artifactFormat,
      artifactVersion: ARTIFACT_VERSION,
      layoutVersion: published.layoutVersion,
      schemaVersion: published.schemaVersion,
      schemaFingerprint: published.schemaFingerprint,
      referenceBuild: published.referenceBuild,
      referenceVersion: published.referenceVersion,
      filesScanned: [EXPECTED_FILE_URI],
      targetsResolved: 1,
    });
  });

  it('emits only camelCase keys on the wire', async () => {
    const { app } = harness();

    const raw = await (
      await postJson(app, '/ask', { datasetId: DATASET_ID, question: 'SLCO1B1?' })
    ).text();

    for (const [key] of raw.matchAll(/"([A-Za-z0-9_]+)":/g)) {
      assert.doesNotMatch(key!, /_/, `wire key ${key} is not camelCase`);
    }
  });

  it('reuses the caller-supplied dataset for every message and never starts an ingestion', async () => {
    const { app, asked, ingestion } = harness();

    for (const question of ['First question?', 'Second question?', 'Third question?']) {
      const response = await postJson(app, '/ask', { datasetId: DATASET_ID, question });
      assert.equal(response.status, 200);
    }

    assert.deepEqual(
      asked.map((call) => call.datasetId),
      [DATASET_ID, DATASET_ID, DATASET_ID],
    );
    assert.equal(ingestion.started.length, 0, 'reopening a conversation must not re-ingest');
  });

  it('reports a dataset whose targets are absent as an answer, not as fabricated variants', async () => {
    // `TargetNotPresent` is what the resolver raises when the manifest declares no object that
    // can contain the coordinates. The shipped agent absorbs it into a note, so the honest API
    // outcome is a 200 with an empty variant list — never a fixture-shaped variant.
    const { app } = harness({
      askAgent: async () => ({
        answer: `Dataset '${DATASET_ID}' contains no variant at those coordinates.`,
        evidence: [],
        toolsUsed: ['query_genotype'],
      }),
    });

    const response = await postJson(app, '/ask', { datasetId: DATASET_ID, question: 'BRCA1?' });

    assert.equal(response.status, 200);
    const body = (await response.json()) as Record<string, any>;
    assert.deepEqual(body.variants, []);
    assert.deepEqual(body.provenance.filesScanned, [], 'nothing was scanned, so nothing is claimed');
    assert.equal(body.provenance.targetsResolved, 0);
  });

  for (const [error, status] of [
    [new DatasetNotPublishedError(DATASET_ID), 409],
    [new ReferenceBuildMismatchError('GRCh37', {
      path: '/tmp/snapshot.duckdb',
      referenceVersion: 'demo-clinvar-grch38-v1',
      referenceBuild: 'GRCh38',
      rowCount: 1,
    }), 409],
    [new TargetNotResolvableError('NOT_A_GENE', 'demo-clinvar-grch38-v1'), 422],
    [new TargetNotPresentError(DATASET_ID, 'the requested coordinates'), 404],
    [new RemoteDatasetUnavailableError(DATASET_ID, 'IO Error: connection reset'), 503],
    [new QueryBudgetExceededError(10_000), 504],
  ] as const) {
    it(`maps ${error.name} to ${status} without substituting a fixture result`, async () => {
      const { app } = harness({ askAgent: failingAgent(error) });

      const { response } = await capturingServerLog(() =>
        postJson(app, '/ask', {
          datasetId: DATASET_ID,
          question: 'What is my SLCO1B1 genotype?',
        }),
      );

      assert.equal(response.status, status);
      const body = (await response.json()) as Record<string, unknown>;
      assert.equal(body.error, error.name);
      assert.equal(typeof body.message, 'string');
      for (const fabricated of ['answer', 'variants', 'evidence', 'provenance']) {
        assert.equal(fabricated in body, false, `a failed query must not return '${fabricated}'`);
      }
    });
  }

  it('logs an unmapped internal failure without leaking it to the caller', async () => {
    const { app } = harness({ askAgent: failingAgent(new Error('/srv/secret/path exploded')) });

    const { response, logged } = await capturingServerLog(() =>
      postJson(app, '/ask', { datasetId: DATASET_ID, question: 'SLCO1B1?' }),
    );

    assert.equal(response.status, 500);
    const raw = await response.text();
    assert.doesNotMatch(raw, /secret/, 'an internal message must not reach the caller');
    assert.match(raw, /InternalError/);
    assert.match(logged, /\/srv\/secret\/path exploded/, 'but it must reach the server log');
  });
});

describe('the runtime path', () => {
  const source = readFileSync(fileURLToPath(new URL('./index.ts', import.meta.url)), 'utf8');

  it('initialises no fixtures at module load', () => {
    assert.doesNotMatch(source, /autoInitFixtures/);
    assert.doesNotMatch(source, /fixtures\//, 'no fixture path may appear in the runtime path');
  });

  it('runs no timer, so no progress can be simulated', () => {
    assert.doesNotMatch(source, /setTimeout|setInterval/);
  });
});
