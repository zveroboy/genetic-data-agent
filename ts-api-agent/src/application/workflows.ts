/**
 * `GenomicIngestionWorkflow` — the orchestration half of the cross-language ingestion pipeline.
 *
 * Three Activities across two task queues:
 *
 * 1. `inspectDatasetSource` (`genomic-control-plane`, TypeScript) pins the seeded catalog key to
 *    an immutable S3 source identity and derives the one prefix this attempt may write under.
 * 2. `buildDatasetArtifact` (`genomic-ingestion-rust`, Rust) streams that object into DuckDB
 *    staging, exports chromosome-partitioned Zstandard Parquet, and uploads it to an
 *    attempt-unique immutable prefix. It is scheduled *by name*: no TypeScript implementation of
 *    it exists, is registered, or may exist.
 * 3. `publishDataset` (`genomic-control-plane`, TypeScript) HEAD-verifies the complete uploaded
 *    inventory and writes `datasets/{datasetId}/manifest.json` last.
 *
 * The manifest is the only readiness signal. Anything that fails before it — a bad VCF, a
 * changed source object, a failed object verification, a cancellation — leaves at most an orphan
 * attempt prefix, which no query path will ever look at because no manifest names it.
 *
 * This module is Workflow code, so it is deterministic by construction: every identifier it
 * needs arrives in its input, and it performs no filesystem, S3, UUID, clock, DuckDB or network
 * operation. It deliberately imports only `@temporalio/workflow` and the pure domain vocabulary;
 * in particular it never imports `./dataset-checksum.ts`, which needs `node:crypto`. Checksums
 * reach the Workflow only as opaque values inside Activity results.
 */
import {
  ActivityCancellationType,
  type ActivityOptions,
  defineQuery,
  proxyActivities,
  setHandler,
} from '@temporalio/workflow';

import type { ControlPlaneActivities } from './control-plane-activities.ts';
import type {
  BuildDatasetArtifactInput,
  BuildDatasetArtifactResult,
  DatasetManifest,
} from './ingestion-contracts.ts';
import type { DatasetKey, IngestionState } from '../domain/datasets.ts';
import { NON_RETRYABLE_FAILURE_TYPES } from '../domain/datasets.ts';

/** Task queue serving Workflows and the two TypeScript control-plane Activities. */
export const CONTROL_PLANE_TASK_QUEUE = 'genomic-control-plane';

/**
 * Activity-only task queue served exclusively by the Rust Worker. A TypeScript Activity is
 * never registered here, and `buildDatasetArtifact` is never registered in TypeScript.
 */
export const RUST_INGESTION_TASK_QUEUE = 'genomic-ingestion-rust';

/**
 * Control-plane failures a retry cannot fix.
 *
 * Temporal matches `RetryPolicy.nonRetryableErrorTypes` against `ApplicationFailure.type`, which
 * the SDK derives from the thrown error's *constructor* name, so these are class names.
 *
 * Everything absent from this list stays retryable, which is the intended classification for the
 * transient object-store conditions: a socket timeout, a 5xx, and specifically
 * `ConditionalWriteIndeterminateError` — an S3 409 means a concurrent conditional write left the
 * outcome unknown, not that a conflicting manifest exists, so treating it as a conflict would
 * turn a transient race into a permanent failure.
 */
export const NON_RETRYABLE_CONTROL_PLANE_ERROR_TYPES = [
  /** The requested key is not in the seeded allowlist. */
  'UnknownDatasetKeyError',
  /** A structurally valid payload violates an inventory or identity invariant. */
  'ContractValidationError',
  /**
   * A published object does not match the identity the inventory declares. Listed under both
   * spellings: Temporal matches the constructor name, while the error's own `name` — the
   * spelling that reaches an operator and the HTTP layer — is `ObjectVerificationFailed`, so an
   * `ApplicationFailure` raised with that name is classified the same way.
   */
  'ObjectVerificationError',
  'ObjectVerificationFailed',
  /** A different, already published manifest owns this dataset id. */
  'DatasetPublicationConflict',
  /** A payload does not match the frozen wire schema. */
  'ZodError',
] as const;

export const CONTROL_PLANE_ACTIVITY_OPTIONS: ActivityOptions = {
  taskQueue: CONTROL_PLANE_TASK_QUEUE,
  scheduleToCloseTimeout: '15 minutes',
  startToCloseTimeout: '5 minutes',
  retry: {
    initialInterval: '1 second',
    backoffCoefficient: 2,
    maximumInterval: '30 seconds',
    maximumAttempts: 5,
    nonRetryableErrorTypes: [...NON_RETRYABLE_CONTROL_PLANE_ERROR_TYPES],
  },
};

export const RUST_INGESTION_ACTIVITY_OPTIONS: ActivityOptions = {
  taskQueue: RUST_INGESTION_TASK_QUEUE,
  scheduleToCloseTimeout: '45 minutes',
  startToCloseTimeout: '30 minutes',
  heartbeatTimeout: '15 seconds',
  /**
   * The SDK default is `TRY_CANCEL`, which abandons the Activity mid-upload and records no
   * `ActivityTaskCanceled` event. Waiting bounds how long an orphaned attempt prefix keeps
   * growing after a cancellation, and makes the cancellation observable in history.
   */
  cancellationType: ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
  retry: {
    maximumAttempts: 3,
    nonRetryableErrorTypes: [...NON_RETRYABLE_FAILURE_TYPES],
  },
};

/** The Rust half of the pipeline. Declared here, implemented only in `rust-ingestion-worker`. */
export interface RustIngestionActivities {
  buildDatasetArtifact(input: BuildDatasetArtifactInput): Promise<BuildDatasetArtifactResult>;
}

/**
 * Everything the Workflow needs, resolved by the caller before the Workflow starts. The catalog
 * key selects a seeded S3 object; the dataset id names the artifact prefix. Neither a path nor a
 * URL nor a bucket can be injected through here.
 */
export interface GenomicIngestionWorkflowInput {
  readonly datasetId: string;
  readonly datasetKey: DatasetKey;
}

/**
 * What `getProgress` answers.
 *
 * `state` is the latest state the Workflow can *prove* was entered — it changes only when an
 * Activity the Workflow scheduled has completed or been scheduled. `unobservedStates` carries
 * the states a currently running Activity may already have advanced through but which the
 * Workflow has no way to witness, so a reader can tell a proven transition from an inferred one.
 */
export interface IngestionProgress {
  readonly datasetId: string;
  readonly datasetKey: string;
  readonly state: IngestionState;
  readonly unobservedStates: readonly IngestionState[];
  readonly message: string;
}

export const getProgressQuery = defineQuery<IngestionProgress>('getProgress');

const STATE_MESSAGES: Record<IngestionState, string> = {
  RESOLVING: 'Pinning the seeded catalog key to an immutable S3 source object.',
  BUILDING: 'Streaming the source VCF into partitioned Parquet on the Rust ingestion queue.',
  VERIFYING_OBJECTS:
    'Verifying the uploaded Parquet inventory and publishing the manifest. Both happen inside ' +
    'one activity, so the workflow cannot witness the boundary between them.',
  PUBLISHING_MANIFEST: 'Writing the dataset manifest, the only readiness signal.',
  COMPLETED: 'Manifest published; the dataset is queryable.',
  FAILED: 'Ingestion failed; no manifest was published and the dataset is not queryable.',
};

/**
 * `publishDataset` verifies the inventory and then writes the manifest without reporting back in
 * between, so `PUBLISHING_MANIFEST` is real but never separately observable from out here.
 */
const UNOBSERVED_STATES: Partial<Record<IngestionState, readonly IngestionState[]>> = {
  VERIFYING_OBJECTS: ['PUBLISHING_MANIFEST'],
};

function progressFor(
  input: GenomicIngestionWorkflowInput,
  state: IngestionState,
  detail?: string,
): IngestionProgress {
  return {
    datasetId: input.datasetId,
    datasetKey: input.datasetKey,
    state,
    unobservedStates: UNOBSERVED_STATES[state] ?? [],
    message: detail ?? STATE_MESSAGES[state],
  };
}

function failureMessage(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 0 ? message : STATE_MESSAGES.FAILED;
}

const controlPlane = proxyActivities<ControlPlaneActivities>(CONTROL_PLANE_ACTIVITY_OPTIONS);
const rust = proxyActivities<RustIngestionActivities>(RUST_INGESTION_ACTIVITY_OPTIONS);

export async function GenomicIngestionWorkflow(
  input: GenomicIngestionWorkflowInput,
): Promise<DatasetManifest> {
  let progress = progressFor(input, 'RESOLVING');
  setHandler(getProgressQuery, () => progress);

  try {
    const artifactInput = await controlPlane.inspectDatasetSource(
      input.datasetId,
      input.datasetKey,
    );

    progress = progressFor(input, 'BUILDING');
    const artifact = await rust.buildDatasetArtifact(artifactInput);

    // Verification and publication are one activity; see `UNOBSERVED_STATES`.
    progress = progressFor(input, 'VERIFYING_OBJECTS');
    const manifest = await controlPlane.publishDataset(artifactInput, artifact);

    progress = progressFor(input, 'COMPLETED');
    return manifest;
  } catch (error) {
    // Reached by a failed or cancelled activity alike. Either way no manifest exists, so any
    // objects the attempt uploaded stay orphaned and unqueryable.
    progress = progressFor(input, 'FAILED', failureMessage(error));
    throw error;
  }
}
