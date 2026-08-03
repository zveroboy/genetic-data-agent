/**
 * HTTP surface of the control plane: the seeded catalog, one real ingestion lifecycle, and
 * dataset-scoped question answering.
 *
 * Two properties hold everywhere below, and everything else here exists to protect them.
 *
 * **Nothing is ever simulated.** A run is reported only from the Workflow's own query, so an
 * unreachable orchestrator is a `503` and not a progress bar; an answer is produced only from a
 * dataset with a published manifest, so an unpublished dataset is a `409` and not a stand-in
 * result. There is no timer, no module-load bootstrap and no process-wide repository in this
 * file — a global one would let any question reach any user's data.
 *
 * **A request selects, it does not describe.** The only dataset input the API accepts is a
 * seeded catalog key (to ingest) or a previously minted dataset id (to query). Request bodies
 * are closed: an unexpected field is a `400`, so no caller can smuggle in a Parquet URI, a
 * bucket or a manifest override and have it sanitised downstream instead of refused.
 *
 * `createApp` takes every dependency explicitly. Tests inject fakes at those ports;
 * `startServer` builds the real adapters and fails loudly if it cannot, which is what keeps
 * "the serving path reads real published Parquet" from quietly degrading into something else.
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { Hono } from 'hono';
import type { Context } from 'hono';

import { artifactBucketFromEnv } from './application/artifact-bucket.ts';
import { newDatasetId } from './application/dataset-catalog.ts';
import {
  type IngestionClient,
  ingestionWorkflowIdFor,
  isIngestionWorkflowId,
} from './application/ingestion-client.ts';
import { DATASET_KEYS, isDatasetKey } from './domain/datasets.ts';
import type { DatasetCatalogEntry } from './domain/datasets.ts';
import { nameOf, statusFor } from './http/error-status.ts';
import { readClosedJsonObject } from './http/closed-json-body.ts';
import { provenanceEnvelope } from './http/provenance-envelope.ts';
import { nodeListener } from './http/node-listener.ts';
import type { AgentResponse } from './infrastructure/ai/agent.ts';
import type {
  ClinVarCoordinateResolver,
  ReferenceVocabularyEntry,
} from './infrastructure/database/clinvar-coordinate-resolver.ts';
import type { DuckDbSessionFactory } from './infrastructure/database/duckdb-session-factory.ts';
import { createGenotypeRepositoryFactory } from './infrastructure/database/duckdb.ts';
import type { GenotypeRepository } from './infrastructure/database/duckdb.ts';
import type { ParquetDatasetResolver } from './infrastructure/database/parquet-dataset-resolver.ts';

/** The read side of the seeded allowlist. `application/dataset-catalog.ts` satisfies it. */
export interface DatasetCatalogPort {
  get(requestedKey: string): DatasetCatalogEntry;
  list(): readonly DatasetCatalogEntry[];
}

/**
 * The agent, as the HTTP layer needs it: a question, the one dataset it may read, and the
 * askable surface of the reference snapshot that dataset was ingested against.
 *
 * The vocabulary is passed in rather than looked up by the agent for the same reason the
 * repository is: the agent has no ambient access to anything. It decides which target a question
 * means from the table it will then query, so the two can never disagree about what is askable.
 *
 * `askBioinformaticsAgent` satisfies this signature.
 */
export type BioinformaticsAgent = (
  question: string,
  options: {
    genotypeRepository: GenotypeRepository;
    referenceVocabulary: readonly ReferenceVocabularyEntry[];
  },
) => Promise<AgentResponse>;

export interface AppDependencies {
  readonly catalog: DatasetCatalogPort;
  readonly ingestionClient: IngestionClient;
  readonly datasetResolver: ParquetDatasetResolver;
  readonly coordinateResolver: ClinVarCoordinateResolver;
  readonly sessionFactory: DuckDbSessionFactory;
  readonly askAgent: BioinformaticsAgent;
  /** Overridable for tests; defaults to the zero-build page in `public/`. */
  readonly uiHtmlPath?: string;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Turns a failure into a response.
 *
 * Mapped failures keep their name and message: they are part of the contract and a caller needs
 * them to tell "retry later" from "this dataset will never answer that". Anything unmapped is a
 * bug, so it is logged in full and reported as an opaque `InternalError` rather than echoing an
 * internal message — which may carry a path or a query — back over the wire.
 */
function errorResponse(c: Context, error: unknown) {
  const status = statusFor(error);
  if (status === undefined) {
    console.error('[api] unhandled failure:', error);
    return c.json(
      { error: 'InternalError', message: 'the request could not be completed' },
      500,
    );
  }
  if (status >= 500) console.error(`[api] ${nameOf(error)}:`, messageOf(error));

  const datasetId = (error as { datasetId?: unknown }).datasetId;
  return c.json(
    {
      error: nameOf(error),
      message: messageOf(error),
      ...(typeof datasetId === 'string' ? { datasetId } : {}),
    },
    status,
  );
}

const DEFAULT_UI_HTML_PATH = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '../public/index.html',
);

export function createApp(dependencies: AppDependencies): Hono {
  const {
    catalog,
    ingestionClient,
    datasetResolver,
    coordinateResolver,
    sessionFactory,
    askAgent,
  } = dependencies;
  const uiHtmlPath = dependencies.uiHtmlPath ?? DEFAULT_UI_HTML_PATH;

  // One factory, no repository. The repository is opened per request, for one dataset, and is
  // unreachable from anywhere but the handler that opened it.
  const repositoryFactory = createGenotypeRepositoryFactory({
    datasetResolver,
    coordinateResolver,
    sessionFactory,
  });

  const app = new Hono();

  // Read on each request rather than cached: the page is deliberately zero-build, and an
  // editor save should show up on reload without restarting the server.
  const ui = () =>
    fs.existsSync(uiHtmlPath)
      ? fs.readFileSync(uiHtmlPath, 'utf8')
      : '<h1>public/index.html not found</h1>';

  app.get('/', (c) => c.html(ui()));
  app.get('/ui', (c) => c.html(ui()));
  app.get('/health', (c) => c.json({ status: 'ok', service: 'ts-api-agent' }));

  /**
   * The datasets a caller may choose between — display metadata only.
   *
   * The seeded S3 identity is deliberately withheld. The browser has no use for it, and a
   * catalog that publishes bucket and object key invites exactly the round trip this API
   * refuses to accept: a client sending back a URI it read here.
   */
  app.get('/api/datasets/catalog', (c) =>
    c.json({
      datasets: catalog.list().map((entry) => ({
        key: entry.key,
        displayName: entry.displayName,
        description: entry.description,
        expectedReferenceBuild: entry.expectedReferenceBuild,
        referenceVersion: entry.referenceVersion,
      })),
    }),
  );

  /**
   * Starts one real ingestion run.
   *
   * Both identities are minted here, before the Workflow starts, so the caller gets the id it
   * will later poll and query even if the response is lost in flight. If the orchestrator
   * cannot be reached the answer is `503` and nothing else: no id, no state, no progress.
   */
  app.post('/api/ingestions', async (c) => {
    const parsed = await readClosedJsonObject(c, ['datasetKey']);
    if (!parsed.ok) return c.json({ error: parsed.error, message: parsed.message }, 400);

    const datasetKey = parsed.body.datasetKey;
    if (!isDatasetKey(datasetKey)) {
      return c.json(
        {
          error: 'UnknownDatasetKey',
          message:
            'ingestion selects a seeded catalog key; object keys, URLs and filesystem paths ' +
            'are not accepted',
          requestedDatasetKey: typeof datasetKey === 'string' ? datasetKey : null,
          allowedDatasetKeys: [...DATASET_KEYS],
        },
        400,
      );
    }

    const datasetId = newDatasetId(datasetKey);
    const workflowId = ingestionWorkflowIdFor(datasetId);
    try {
      await ingestionClient.start({ workflowId, datasetId, datasetKey });
    } catch (error) {
      return errorResponse(c, error);
    }
    return c.json({ datasetId, datasetKey, workflowId }, 202);
  });

  /**
   * The Workflow's own progress, forwarded unchanged — `state` alongside `unobservedStates`.
   *
   * Nothing is added here. `VERIFYING_OBJECTS` and `PUBLISHING_MANIFEST` happen inside one
   * activity, so the Workflow cannot witness the boundary; interpolating the transition on the
   * way out would invent an observation the system never made.
   *
   * The id is checked against the shape `ingestionWorkflowIdFor` produces *before* it reaches
   * the orchestrator. Without this, an id naming some other Temporal workflow — one this
   * process never started — would be forwarded straight into a `getProgress` query; the
   * resulting query error has no mapping in `ERROR_STATUS` and would surface as an opaque
   * `500`. An id that merely *looks* right but names no run this process started still reaches
   * the query below and gets the orchestrator's own real `404`.
   */
  app.get('/api/ingestions/:workflowId', async (c) => {
    const workflowId = c.req.param('workflowId');
    if (!isIngestionWorkflowId(workflowId)) {
      return c.json(
        {
          error: 'IngestionRunNotFound',
          message: `no ingestion run '${workflowId}' exists`,
        },
        404,
      );
    }
    try {
      return c.json(await ingestionClient.getProgress(workflowId));
    } catch (error) {
      return errorResponse(c, error);
    }
  });

  /**
   * Answers one question against one published dataset.
   *
   * Order matters: the manifest is resolved first, so an unpublished or untrustworthy dataset
   * is refused before a DuckDB session exists, before credentials are attached and before the
   * agent is told anything. Only then is the request-scoped repository opened and handed to the
   * agent, which reaches genotypes exclusively through the tool built around it.
   */
  app.post('/ask', async (c) => {
    const parsed = await readClosedJsonObject(c, ['datasetId', 'question']);
    if (!parsed.ok) return c.json({ error: parsed.error, message: parsed.message }, 400);

    const { datasetId, question } = parsed.body;
    if (typeof datasetId !== 'string' || datasetId.length === 0) {
      return c.json(
        {
          error: 'MissingDatasetId',
          message:
            'every question names the published dataset it may read; start an ingestion and ' +
            'use the datasetId it returned',
        },
        400,
      );
    }
    if (typeof question !== 'string' || question.trim().length === 0) {
      return c.json({ error: 'MissingQuestion', message: 'question must be a non-empty string' }, 400);
    }

    try {
      const dataset = await datasetResolver.resolve(datasetId);
      const genotypeRepository = await repositoryFactory.open(datasetId);
      // Read from the same open snapshot the repository resolves coordinates against — and
      // cached there, so this is not a per-request DuckDB round trip. Fetched after the
      // repository is opened, which is what has already proven the dataset and the snapshot
      // describe the same reference version.
      const referenceVocabulary = await coordinateResolver.vocabulary();
      const response = await askAgent(question, { genotypeRepository, referenceVocabulary });

      return c.json({
        datasetId,
        answer: response.answer,
        toolsUsed: response.toolsUsed ?? [],
        variants: response.evidence ?? [],
        // Machine-readable form of the warning already appended to `answer`, so a client can
        // present "the prose contradicts `variants`" as something other than a paragraph.
        groundingFindings: response.groundingFindings ?? [],
        literatureHits: response.literatureHits ?? [],
        provenance: provenanceEnvelope(dataset, response.provenance),
      });
    } catch (error) {
      return errorResponse(c, error);
    }
  });

  return app;
}

/**
 * Builds the production adapters.
 *
 * Every import here is dynamic so that importing this module — which a test does to reach
 * `createApp` — costs no S3 client, no DuckDB native binding and no Temporal SDK. Construction
 * is eager and unguarded on purpose: a missing reference snapshot or an unset `S3_ENDPOINT` is
 * a misconfigured deployment, and starting anyway would leave a server that looks healthy and
 * cannot answer a single question truthfully.
 */
async function buildRuntimeDependencies(): Promise<AppDependencies & { close(): Promise<void> }> {
  const [
    { datasetCatalog },
    { askBioinformaticsAgent },
    { openClinVarCoordinateResolver },
    { defaultReferenceSnapshotOptions },
    { createDuckDbSessionFactory, duckDbS3SessionConfigFromEnv, queryDeadlineFromEnv },
    { createParquetDatasetResolver },
    { S3ObjectStore },
    { createTemporalIngestionClient },
  ] = await Promise.all([
    import('./application/dataset-catalog.ts'),
    import('./infrastructure/ai/agent.ts'),
    import('./infrastructure/database/clinvar-coordinate-resolver.ts'),
    import('./infrastructure/database/reference-bootstrap.ts'),
    import('./infrastructure/database/duckdb-session-factory.ts'),
    import('./infrastructure/database/parquet-dataset-resolver.ts'),
    import('./infrastructure/object-store/s3-object-store.ts'),
    import('./infrastructure/temporal/temporal-ingestion-client.ts'),
  ]);

  const artifactBucket = artifactBucketFromEnv();
  const objectStore = S3ObjectStore.fromEnv();
  const ingestionClient = createTemporalIngestionClient();
  const coordinateResolver = await openClinVarCoordinateResolver({
    databasePath: defaultReferenceSnapshotOptions().databasePath,
  });

  return {
    catalog: datasetCatalog,
    ingestionClient,
    datasetResolver: createParquetDatasetResolver({ objectStore, artifactBucket }),
    coordinateResolver,
    sessionFactory: createDuckDbSessionFactory({
      s3: duckDbS3SessionConfigFromEnv(process.env, artifactBucket),
      queryDeadlineMs: queryDeadlineFromEnv(process.env),
      allowExtensionInstall: process.env.DUCKDB_ALLOW_EXTENSION_INSTALL === 'true',
    }),
    askAgent: (question, options) => askBioinformaticsAgent(question, options),
    async close() {
      await ingestionClient.close();
      await coordinateResolver.close();
      objectStore.destroy();
    },
  };
}

async function startServer(): Promise<void> {
  const port = Number(process.env.PORT) || 3000;
  const dependencies = await buildRuntimeDependencies();
  const server = http.createServer(nodeListener(createApp(dependencies)));

  const shutdown = () => {
    // `close` alone waits for keep-alive sockets, which a polling browser always holds open.
    server.closeAllConnections();
    server.close(() => {
      void dependencies.close().finally(() => process.exit(0));
    });
  };
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);

  server.listen(port, () => {
    console.log(`[ts-api-agent] listening on http://localhost:${port}`);
  });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  startServer().catch((error) => {
    console.error('[ts-api-agent] failed to start:', error);
    process.exit(1);
  });
}
