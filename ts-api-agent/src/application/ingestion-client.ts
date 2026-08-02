/**
 * The control plane's port onto one ingestion run.
 *
 * Everything the HTTP surface needs from Temporal is these two operations: start the real
 * Workflow for an already-minted identity, and read back the progress the Workflow itself
 * publishes. Naming them here rather than reaching for `@temporalio/client` inside a route
 * handler buys two things:
 *
 * - **A testable seam.** `index.test.ts` injects a fake implementation, so the API's contract —
 *   `202` with a fresh identity, `503` when the orchestrator is unreachable, `404` for an
 *   unknown run — is provable without a Temporal server. No fake ever produces *progress*: the
 *   only thing a test can make the API report is what a real query returned.
 * - **A single spelling of the run identity.** `ingestionWorkflowIdFor` is the one place a
 *   `datasetId` becomes a `workflowId`, shared by the HTTP endpoint and the CLI trigger, so the
 *   two cannot drift into starting runs the other cannot find.
 *
 * This module deliberately imports nothing at runtime. `IngestionProgress` and `DatasetKey`
 * arrive as types only, so importing the port costs no Temporal SDK load.
 */
import type { DatasetKey } from '../domain/datasets.ts';
import type { IngestionProgress } from './workflows.ts';

/**
 * The Workflow id one ingestion run is started under.
 *
 * Derived from the dataset id, which is minted fresh per run, so the id is unique without a
 * clock or a counter and a caller holding a `datasetId` can always find its run again.
 */
export function ingestionWorkflowIdFor(datasetId: string): string {
  return `genomic-ingestion-${datasetId}`;
}

/**
 * One run's identity, resolved by the caller *before* the Workflow starts.
 *
 * There is no path, URL or bucket in here, and there is no field a request body could add:
 * the S3 identity comes from the seeded allowlist inside `inspectDatasetSource`.
 */
export interface StartIngestionRequest {
  readonly workflowId: string;
  readonly datasetId: string;
  readonly datasetKey: DatasetKey;
}

export interface IngestionClient {
  /** Starts the real Workflow, or throws. Never resolves without a started run. */
  start(request: StartIngestionRequest): Promise<void>;

  /**
   * The progress the Workflow itself reports, verbatim — including the
   * `state`/`unobservedStates` split. Nothing here infers, interpolates or times a transition.
   */
  getProgress(workflowId: string): Promise<IngestionProgress>;
}

/**
 * Raised when the orchestrator cannot be reached.
 *
 * This is the failure that must surface as `503` rather than as fabricated progress: an
 * ingestion that was never started has no state to report, and reporting one anyway would
 * describe a dataset as arriving when nothing is producing it.
 */
export class IngestionServiceUnavailableError extends Error {
  readonly address: string;

  constructor(address: string, detail: string, options?: { cause?: unknown }) {
    super(`the ingestion orchestrator at '${address}' is unavailable: ${detail}`, options);
    this.name = 'IngestionServiceUnavailable';
    this.address = address;
  }
}

/** Raised when the requested run id names no execution the orchestrator knows about. */
export class IngestionRunNotFoundError extends Error {
  readonly workflowId: string;

  constructor(workflowId: string, options?: { cause?: unknown }) {
    super(`no ingestion run '${workflowId}' exists`, options);
    this.name = 'IngestionRunNotFound';
    this.workflowId = workflowId;
  }
}
