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
import type { ContentfulStatusCode } from 'hono/utils/http-status';

import { newDatasetId } from './application/dataset-catalog.ts';
import { type IngestionClient, ingestionWorkflowIdFor } from './application/ingestion-client.ts';
import { DATASET_KEYS, isDatasetKey } from './domain/datasets.ts';
import type { DatasetCatalogEntry } from './domain/datasets.ts';
import type { AgentResponse } from './infrastructure/ai/agent.ts';
import type { ClinVarCoordinateResolver } from './infrastructure/database/clinvar-coordinate-resolver.ts';
import type { DuckDbSessionFactory } from './infrastructure/database/duckdb-session-factory.ts';
import { createGenotypeRepositoryFactory } from './infrastructure/database/duckdb.ts';
import type { GenotypeProvenance, GenotypeRepository } from './infrastructure/database/duckdb.ts';
import type {
  ParquetDatasetResolver,
  ResolvedParquetDataset,
} from './infrastructure/database/parquet-dataset-resolver.ts';

/** The read side of the seeded allowlist. `application/dataset-catalog.ts` satisfies it. */
export interface DatasetCatalogPort {
  get(requestedKey: string): DatasetCatalogEntry;
  list(): readonly DatasetCatalogEntry[];
}

/**
 * The agent, as the HTTP layer needs it: a question plus the one dataset it may read.
 * `askBioinformaticsAgent` satisfies this signature.
 */
export type BioinformaticsAgent = (
  question: string,
  options: { genotypeRepository: GenotypeRepository },
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

/**
 * Status for every failure the serving path and the orchestrator can raise, keyed by the
 * error's `name`.
 *
 * Matching on the name rather than on `instanceof` is deliberate. These names are the frozen
 * cross-layer contract (`contracts/ingestion-v1.md` and the serving modules pin each one with
 * an explicit `this.name`), and importing the classes would drag the DuckDB native binding and
 * the reference snapshot module into every consumer of the HTTP layer for nothing.
 *
 * The three families:
 *
 * - `409` — the dataset exists as an id but cannot be served as published: no manifest, a
 *   manifest that no longer matches its objects, or a reference snapshot that describes a
 *   different genome than the one the dataset was ingested against. Answering anyway would
 *   mean returning the wrong person's answer to the right question.
 * - `4xx` on the target — the question named something the reference cannot place, or that the
 *   dataset provably does not contain. Neither is widened into a scan.
 * - `5xx` — the object store or the query budget gave out. These are the ones a caller may
 *   retry; none of them may be answered from anything else.
 */
const ERROR_STATUS: Readonly<Record<string, ContentfulStatusCode>> = Object.freeze({
  IngestionServiceUnavailable: 503,
  IngestionRunNotFound: 404,

  DatasetNotPublished: 409,
  ParquetObjectVerificationFailed: 409,
  ReferenceSnapshotMismatch: 409,
  ReferenceBuildMismatch: 409,
  DatasetPublicationConflict: 409,

  TargetNotResolvable: 422,
  TargetResolutionLimitExceeded: 422,
  TargetNotPresent: 404,

  RemoteDatasetUnavailable: 503,
  ReferenceSnapshotUnavailable: 503,
  HttpfsExtensionUnavailable: 503,
  QueryBudgetExceeded: 504,
  SessionConfigurationTimedOut: 504,
});

function nameOf(error: unknown): string {
  const name = (error as { name?: unknown } | null)?.name;
  return typeof name === 'string' ? name : '';
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function statusFor(error: unknown): ContentfulStatusCode | undefined {
  const name = nameOf(error);
  if (name === 'DatasetResolutionFailed') {
    // One resolution code is the caller's fault — an id that is not a single safe path segment
    // never named a dataset in the first place. Every other code means the *published* artifact
    // is not trustworthy, which is a conflict with the dataset's state, not a bad request.
    return (error as { code?: unknown }).code === 'DATASET_ID_UNSAFE' ? 400 : 409;
  }
  return ERROR_STATUS[name];
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

type ParsedBody =
  | { readonly ok: true; readonly body: Record<string, unknown> }
  | { readonly ok: false; readonly error: string; readonly message: string };

/**
 * Reads a closed JSON object body.
 *
 * "Closed" is the load-bearing part: a field the endpoint does not know about is rejected
 * rather than ignored. Ignoring it would make `{"datasetKey":"demo-small","bucket":"…"}` and
 * `{"datasetKey":"demo-small"}` indistinguishable to the caller, and the day someone starts
 * reading `bucket` the API silently gains an override nobody reviewed.
 */
async function readClosedJsonObject(
  c: Context,
  allowedFields: readonly string[],
): Promise<ParsedBody> {
  let parsed: unknown;
  try {
    parsed = await c.req.json();
  } catch {
    return { ok: false, error: 'MalformedRequestBody', message: 'the body must be a JSON object' };
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { ok: false, error: 'MalformedRequestBody', message: 'the body must be a JSON object' };
  }

  const unexpected = Object.keys(parsed).filter((key) => !allowedFields.includes(key));
  if (unexpected.length > 0) {
    return {
      ok: false,
      error: 'UnrecognizedRequestField',
      message:
        `this endpoint accepts only ${allowedFields.join(', ')}; got ${unexpected.join(', ')}. ` +
        'Object URIs, buckets, source paths and manifest overrides are never taken from a request.',
    };
  }
  return { ok: true, body: parsed as Record<string, unknown> };
}

/**
 * What was read, and against what.
 *
 * The manifest half — checksum, layout/schema version, fingerprint, artifact version, reference
 * identity — describes the dataset the request was authorised against, and is present on every
 * answer. The read half — the exact object URIs and how many coordinates were resolved — comes
 * from the genotype tool and is empty when the agent never queried genotypes, because claiming
 * a scan that did not happen is the same lie as claiming a variant that was not found.
 */
function provenanceEnvelope(
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
   */
  app.get('/api/ingestions/:workflowId', async (c) => {
    try {
      return c.json(await ingestionClient.getProgress(c.req.param('workflowId')));
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
      const response = await askAgent(question, { genotypeRepository });

      return c.json({
        datasetId,
        answer: response.answer,
        toolsUsed: response.toolsUsed ?? [],
        variants: response.evidence ?? [],
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
 * Bucket every published artifact and manifest lives in.
 *
 * Deliberately the same variable and default as `application/worker.ts`'s
 * `artifactBucketFromEnv`, and deliberately not imported from it: that module pulls in
 * `@temporalio/worker` and its native core, which the API process has no other reason to load.
 * The two must agree — the Worker writes manifests where this process looks for them — so if
 * either default changes, both do.
 */
function artifactBucketFromEnv(env: NodeJS.ProcessEnv = process.env): string {
  const configured = env.S3_ARTIFACT_BUCKET ?? '';
  return configured.length > 0 ? configured : 'genomic-artifacts';
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
    { createDuckDbSessionFactory, duckDbS3SessionConfigFromEnv },
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

/** Bridges Node's `http` server onto Hono's fetch handler; no framework adapter needed. */
function nodeListener(app: Hono): http.RequestListener {
  return async (req, res) => {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
    const method = req.method ?? 'GET';

    const headers = new Headers();
    for (const [key, value] of Object.entries(req.headers)) {
      if (value === undefined) continue;
      headers.set(key, Array.isArray(value) ? value.join(', ') : value);
    }

    let body: Buffer | undefined;
    if (method !== 'GET' && method !== 'HEAD') {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      body = Buffer.concat(chunks);
    }

    const response = await app.fetch(new Request(url, { method, headers, body }));

    res.statusCode = response.status;
    response.headers.forEach((value, key) => res.setHeader(key, value));
    res.end(Buffer.from(await response.arrayBuffer()));
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
