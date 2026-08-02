/**
 * `GenomicIngestionWorkflow` tests.
 *
 * The Workflow is exercised as a *real* Temporal Workflow function: the Activity proxies it
 * builds with `proxyActivities` are the real ones, and `runWorkflow` (see
 * `./workflow-test-harness.ts`) drives them by installing a minimal Workflow activator on
 * `globalThis.__TEMPORAL_ACTIVATOR__` — the same hook the SDK's own Workflow sandbox installs.
 * Assertions are therefore about the `scheduleActivity` commands the Workflow actually emits
 * (Activity type, task queue, timeouts, retry policy, cancellation type), the order in which it
 * emits them, and the state the `getProgress` query reports at each point. Nothing here asserts
 * "a mock was called".
 *
 * See `./workflow-test-harness.ts` for why `@temporalio/testing`'s time-skipping environment
 * is not used here and what would need to change to adopt it.
 *
 * Payloads are the frozen golden fixtures under `contracts/fixtures/`, so no test here
 * restates the wire schema.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { ActivityFailure, ApplicationFailure, CancelledFailure } from '@temporalio/common';

import { UnknownDatasetKeyError } from './dataset-catalog.ts';
import {
  DatasetPublicationConflict,
  DatasetSourceUnavailableError,
} from './control-plane-activities.ts';
import { ObjectVerificationError } from './object-identity.ts';
import {
  BuildDatasetArtifactInputSchema,
  BuildDatasetArtifactResultSchema,
  ContractValidationError,
  DatasetManifestSchema,
  type BuildDatasetArtifactInput,
  type BuildDatasetArtifactResult,
  type DatasetManifest,
} from './ingestion-contracts.ts';
import { ConditionalWriteIndeterminateError } from '../infrastructure/object-store/object-store.ts';
import { NON_RETRYABLE_FAILURE_TYPES, type IngestionState } from '../domain/datasets.ts';
import { runWorkflow, type WorkflowEnvironment } from './workflow-test-harness.ts';
import {
  CONTROL_PLANE_ACTIVITY_OPTIONS,
  CONTROL_PLANE_TASK_QUEUE,
  GenomicIngestionWorkflow,
  NON_RETRYABLE_CONTROL_PLANE_ERROR_TYPES,
  RUST_INGESTION_ACTIVITY_OPTIONS,
  RUST_INGESTION_TASK_QUEUE,
  type GenomicIngestionWorkflowInput,
} from './workflows.ts';

// ---------------------------------------------------------------------------------------------
// Golden payloads
// ---------------------------------------------------------------------------------------------

const moduleDir = dirname(fileURLToPath(import.meta.url));

function readFixture(name: string): unknown {
  return JSON.parse(readFileSync(join(moduleDir, '../../../contracts/fixtures', name), 'utf8'));
}

const GOLDEN_INPUT: BuildDatasetArtifactInput = BuildDatasetArtifactInputSchema.parse(
  readFixture('build-dataset-artifact.input.json'),
);
const GOLDEN_RESULT: BuildDatasetArtifactResult = BuildDatasetArtifactResultSchema.parse(
  readFixture('build-dataset-artifact.result.json'),
);
const GOLDEN_MANIFEST: DatasetManifest = DatasetManifestSchema.parse(
  readFixture('dataset-manifest.json'),
);

const WORKFLOW_INPUT: GenomicIngestionWorkflowInput = {
  datasetId: GOLDEN_INPUT.datasetId,
  datasetKey: 'demo-small',
};

/** Drives the Workflow through a successful run, recording the state after every step. */
async function runToCompletion(): Promise<{
  env: WorkflowEnvironment<DatasetManifest>;
  states: IngestionState[];
  manifest: DatasetManifest;
}> {
  const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
  const states: IngestionState[] = [env.progress().state];

  await env.complete(1, GOLDEN_INPUT);
  states.push(env.progress().state);

  await env.complete(2, GOLDEN_RESULT);
  states.push(env.progress().state);

  await env.complete(3, GOLDEN_MANIFEST);
  states.push(env.progress().state);

  return { env, states, manifest: await env.result };
}

/** The Activity type Temporal would match against `nonRetryableErrorTypes` for `error`. */
function failureType(error: Error): string | undefined {
  return ApplicationFailure.fromError(error).type ?? undefined;
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

describe('the happy path walks the frozen ingestion state sequence', () => {
  it('reports RESOLVING -> BUILDING -> VERIFYING_OBJECTS -> COMPLETED', async () => {
    const { states } = await runToCompletion();
    assert.deepEqual(states, ['RESOLVING', 'BUILDING', 'VERIFYING_OBJECTS', 'COMPLETED']);
  });

  it('schedules exactly three activities, in order', async () => {
    const { env } = await runToCompletion();
    assert.deepEqual(
      env.scheduled.map((activity) => activity.activityType),
      ['inspectDatasetSource', 'buildDatasetArtifact', 'publishDataset'],
    );
  });

  it('returns the published manifest as the workflow result', async () => {
    const { manifest } = await runToCompletion();
    assert.deepEqual(manifest, GOLDEN_MANIFEST);
  });

  it('passes only the dataset id and catalog key into resolution', async () => {
    const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
    assert.deepEqual(env.scheduled[0]?.args, [WORKFLOW_INPUT.datasetId, WORKFLOW_INPUT.datasetKey]);
    await env.complete(1, GOLDEN_INPUT);
    await env.complete(2, GOLDEN_RESULT);
    await env.complete(3, GOLDEN_MANIFEST);
    await env.result;
  });

  it('feeds the resolved source identity to the builder unchanged', async () => {
    const { env } = await runToCompletion();
    assert.deepEqual(env.scheduled[1]?.args, [GOLDEN_INPUT]);
  });
});

describe('the progress query never fabricates a transition', () => {
  it('marks PUBLISHING_MANIFEST unobserved while publishDataset runs', async () => {
    const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
    await env.complete(1, GOLDEN_INPUT);
    await env.complete(2, GOLDEN_RESULT);

    // Verification and manifest publication happen inside one activity, so the workflow can
    // prove VERIFYING_OBJECTS was entered and can only report PUBLISHING_MANIFEST as a state
    // the running activity may already have reached.
    const progress = env.progress();
    assert.equal(progress.state, 'VERIFYING_OBJECTS');
    assert.deepEqual(progress.unobservedStates, ['PUBLISHING_MANIFEST']);

    await env.complete(3, GOLDEN_MANIFEST);
    assert.deepEqual(env.progress().unobservedStates, []);
    await env.result;
  });

  it('reports no unobserved state in any other phase', async () => {
    const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
    assert.deepEqual(env.progress().unobservedStates, []);
    await env.complete(1, GOLDEN_INPUT);
    assert.deepEqual(env.progress().unobservedStates, []);
    await env.complete(2, GOLDEN_RESULT);
    await env.complete(3, GOLDEN_MANIFEST);
    await env.result;
  });

  it('echoes the dataset identity the workflow was started with', async () => {
    const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
    const progress = env.progress();
    assert.equal(progress.datasetId, WORKFLOW_INPUT.datasetId);
    assert.equal(progress.datasetKey, WORKFLOW_INPUT.datasetKey);
    await env.complete(1, GOLDEN_INPUT);
    await env.complete(2, GOLDEN_RESULT);
    await env.complete(3, GOLDEN_MANIFEST);
    await env.result;
  });
});

describe('activities are dispatched to the task queue that owns them', () => {
  it('sends the processor to the Rust queue and nothing else', async () => {
    const { env } = await runToCompletion();
    const byQueue = new Map(env.scheduled.map((a) => [a.activityType, a.taskQueue]));
    assert.deepEqual(Object.fromEntries(byQueue), {
      inspectDatasetSource: CONTROL_PLANE_TASK_QUEUE,
      buildDatasetArtifact: RUST_INGESTION_TASK_QUEUE,
      publishDataset: CONTROL_PLANE_TASK_QUEUE,
    });
  });

  it('keeps the two queues distinct', () => {
    assert.equal(CONTROL_PLANE_TASK_QUEUE, 'genomic-control-plane');
    assert.equal(RUST_INGESTION_TASK_QUEUE, 'genomic-ingestion-rust');
    assert.notEqual(CONTROL_PLANE_TASK_QUEUE, RUST_INGESTION_TASK_QUEUE);
  });

  it('gives the long processor its own timeouts, heartbeat and retry budget', async () => {
    const { env } = await runToCompletion();
    const build = env.scheduled.find((a) => a.activityType === 'buildDatasetArtifact');
    assert.ok(build);
    assert.equal(build.scheduleToCloseSeconds, 45 * 60);
    assert.equal(build.startToCloseSeconds, 30 * 60);
    assert.equal(build.heartbeatSeconds, 15);
    assert.equal(build.maximumAttempts, 3);
  });

  it('waits for the processor to finish cancelling instead of abandoning it', async () => {
    const { env } = await runToCompletion();
    const build = env.scheduled.find((a) => a.activityType === 'buildDatasetArtifact');
    // The SDK default is TRY_CANCEL, which abandons the activity mid-upload and writes no
    // ActivityTaskCanceled event. Waiting bounds how long an orphan attempt prefix keeps
    // growing after cancellation.
    assert.equal(build?.cancellationType, 'WAIT_CANCELLATION_COMPLETED');
  });

  it('does not put a heartbeat timeout on the short control-plane activities', async () => {
    const { env } = await runToCompletion();
    for (const activity of env.scheduled.filter((a) => a.taskQueue === CONTROL_PLANE_TASK_QUEUE)) {
      assert.equal(activity.heartbeatSeconds, null, `${activity.activityType} heartbeats`);
    }
  });
});

describe('retry classification', () => {
  it('marks the three deterministic processor failures non-retryable', async () => {
    const { env } = await runToCompletion();
    const build = env.scheduled.find((a) => a.activityType === 'buildDatasetArtifact');
    assert.deepEqual(build?.nonRetryableErrorTypes, [...NON_RETRYABLE_FAILURE_TYPES]);
    assert.deepEqual(
      [...NON_RETRYABLE_FAILURE_TYPES],
      ['InvalidVcfFormat', 'SourceObjectChanged', 'ArtifactValidationFailed'],
    );
  });

  it('marks every deterministic control-plane failure non-retryable', async () => {
    const { env } = await runToCompletion();
    const deterministic: Error[] = [
      new UnknownDatasetKeyError('s3://attacker/file.vcf'),
      new ContractValidationError('BUCKET_MISMATCH', 'wrong bucket'),
      new ObjectVerificationError('ETAG_MISMATCH', 'a/b.parquet', 'differs'),
      new DatasetPublicationConflict('datasets/ds/manifest.json', 'already published'),
      BuildDatasetArtifactInputSchema.safeParse({}).error!,
    ];

    for (const activity of env.scheduled.filter((a) => a.taskQueue === CONTROL_PLANE_TASK_QUEUE)) {
      for (const error of deterministic) {
        assert.ok(
          activity.nonRetryableErrorTypes.includes(failureType(error)!),
          `${activity.activityType} would retry ${failureType(error)}, which cannot succeed`,
        );
      }
      // The verification error's constructor name is what Temporal matches, but the frozen wire
      // name differs; both spellings must be classified the same way.
      assert.ok(activity.nonRetryableErrorTypes.includes('ObjectVerificationFailed'));
    }
  });

  it('leaves an indeterminate conditional write retryable', async () => {
    const indeterminate = new ConditionalWriteIndeterminateError({
      bucket: 'genomic-artifacts',
      key: 'datasets/ds-test-001/manifest.json',
    });
    const { env } = await runToCompletion();

    for (const activity of env.scheduled.filter((a) => a.taskQueue === CONTROL_PLANE_TASK_QUEUE)) {
      assert.ok(
        !activity.nonRetryableErrorTypes.includes(failureType(indeterminate)!),
        'a raced conditional write is transient and must be retried, not treated as a conflict',
      );
    }
  });

  it('leaves transient object-store failures retryable', async () => {
    const transient: Error[] = [
      Object.assign(new Error('socket hang up'), { name: 'TimeoutError' }),
      Object.assign(new Error('503 Service Unavailable'), { name: 'ObjectStoreUnavailable' }),
      new DatasetSourceUnavailableError(
        { bucket: 'genomic-data', key: 'samples/demo_user.vcf' },
        'does not exist',
      ),
    ];
    const { env } = await runToCompletion();

    for (const activity of env.scheduled.filter((a) => a.taskQueue === CONTROL_PLANE_TASK_QUEUE)) {
      for (const error of transient) {
        assert.ok(
          !activity.nonRetryableErrorTypes.includes(failureType(error)!),
          `${failureType(error)} is transient and must stay retryable`,
        );
      }
    }
  });

  it('leaves the processor free to retry a failed upload or a flaky object store', async () => {
    const { env } = await runToCompletion();
    const build = env.scheduled.find((a) => a.activityType === 'buildDatasetArtifact');
    // Both are declared failure types of the Rust activity; neither is deterministic.
    assert.ok(!build?.nonRetryableErrorTypes.includes('ObjectStoreUnavailable'));
    assert.ok(!build?.nonRetryableErrorTypes.includes('ArtifactWriteFailed'));
  });

  it('bounds every activity so a permanently failing run terminates', async () => {
    const { env } = await runToCompletion();
    for (const activity of env.scheduled) {
      assert.ok(
        typeof activity.maximumAttempts === 'number' && activity.maximumAttempts > 0,
        `${activity.activityType} has an unbounded retry budget`,
      );
    }
  });
});

describe('a failed build never reaches publication', () => {
  it('surfaces an invalid VCF and stops', async () => {
    const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
    await env.complete(1, GOLDEN_INPUT);

    const invalid = ApplicationFailure.create({
      message: "line 42: malformed VCF record",
      type: 'InvalidVcfFormat',
      nonRetryable: true,
    });
    await env.fail(2, invalid);

    await assert.rejects(env.result, ActivityFailure);
    assert.deepEqual(
      env.scheduled.map((a) => a.activityType),
      ['inspectDatasetSource', 'buildDatasetArtifact'],
      'publishDataset must not be scheduled after a build failure',
    );
    const progress = env.progress();
    assert.equal(progress.state, 'FAILED');
    assert.match(progress.message, /malformed VCF record/);
  });

  it('surfaces a transient object-store failure without publishing', async () => {
    // This harness has no retry loop and never simulates server-side retry, so it cannot show
    // an exhausted retry budget — only a single delivered failure. The property this test
    // actually proves (a build failure of this shape does not lead to publication) is the same
    // one 'surfaces an invalid VCF and stops' proves for a different failure type; retrying is
    // the server's job, and the workflow only ever observes whichever outcome is delivered.
    const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
    await env.complete(1, GOLDEN_INPUT);

    await env.fail(
      2,
      ApplicationFailure.create({
        message: 'S3 returned 503 while uploading chrom=12/part-000.parquet',
        type: 'ObjectStoreUnavailable',
        nonRetryable: false,
      }),
    );

    await assert.rejects(env.result, ActivityFailure);
    assert.equal(env.scheduled.filter((a) => a.activityType === 'publishDataset').length, 0);
    assert.equal(env.progress().state, 'FAILED');
    assert.match(env.progress().message, /503/);
  });

  it('stops when the source object cannot be resolved', async () => {
    const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
    await env.fail(
      1,
      new UnknownDatasetKeyError('s3://attacker/file.vcf'),
    );

    await assert.rejects(env.result, ActivityFailure);
    assert.deepEqual(env.scheduled.map((a) => a.activityType), ['inspectDatasetSource']);
    assert.equal(env.progress().state, 'FAILED');
  });
});

describe('object verification failure leaves the dataset unpublished', () => {
  it('does not reschedule publication after a verification failure', async () => {
    const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
    await env.complete(1, GOLDEN_INPUT);
    await env.complete(2, GOLDEN_RESULT);
    await env.fail(
      3,
      new ObjectVerificationError(
        'CHECKSUM_METADATA_MISMATCH',
        GOLDEN_RESULT.parquetObjects[1]!.key,
        'content changed after upload',
      ),
    );

    await assert.rejects(env.result, ActivityFailure);
    assert.equal(
      env.scheduled.filter((a) => a.activityType === 'publishDataset').length,
      1,
      'the workflow must not retry publication itself',
    );
    assert.equal(env.progress().state, 'FAILED');
  });

  it('hands the complete inventory to publication, in the builder order', async () => {
    const { env, manifest } = await runToCompletion();
    const publish = env.scheduled.find((a) => a.activityType === 'publishDataset');

    // The workflow neither filters, reorders, deduplicates nor summarises the inventory: the
    // activity verifies every object the builder declared, in the builder's canonical order.
    assert.deepEqual(publish?.args, [GOLDEN_INPUT, GOLDEN_RESULT]);
    const passed = (publish!.args[1] as BuildDatasetArtifactResult).parquetObjects;
    assert.deepEqual(
      passed.map((object) => object.key),
      GOLDEN_RESULT.parquetObjects.map((object) => object.key),
    );
    assert.deepEqual(
      manifest.parquetObjects.map((object) => object.key),
      GOLDEN_RESULT.parquetObjects.map((object) => object.key),
    );
    assert.equal(manifest.parquetObjects.length, GOLDEN_RESULT.parquetObjects.length);
  });
});

describe('cancellation', () => {
  it('cancels the in-flight processor and publishes nothing', async () => {
    const env = runWorkflow(GenomicIngestionWorkflow, WORKFLOW_INPUT);
    await env.complete(1, GOLDEN_INPUT);
    assert.deepEqual(env.scheduled.map((a) => a.activityType), [
      'inspectDatasetSource',
      'buildDatasetArtifact',
    ]);

    await env.cancel();
    assert.deepEqual(env.cancelRequests, [2], 'the build activity must be asked to cancel');

    await env.fail(2, new CancelledFailure('activity cancelled'));

    await assert.rejects(env.result, ActivityFailure);
    assert.equal(
      env.scheduled.filter((a) => a.activityType === 'publishDataset').length,
      0,
      'a cancelled attempt prefix stays unqueryable because no manifest is ever written',
    );
    assert.equal(env.progress().state, 'FAILED');
  });
});

describe('the workflow is deterministic', () => {
  const source = readFileSync(join(moduleDir, 'workflows.ts'), 'utf8');

  it('imports nothing that would drag Node built-ins into the sandbox', () => {
    const valueImports = [...source.matchAll(/^import (?!type )[^;]*?from '([^']+)';$/gms)].map(
      (match) => match[1]!,
    );
    assert.deepEqual([...valueImports].sort(), [
      '../domain/datasets.ts',
      '@temporalio/workflow',
    ]);
  });

  it('never imports the checksum helpers, which need node:crypto', () => {
    assert.doesNotMatch(source, /from '\.\/dataset-checksum\.ts'/);
  });

  // Prose in the module header may name a forbidden API; code may not. These patterns match
  // call and import syntax only, the same distinction `ingestion-contracts.test.ts` draws.
  it('calls no nondeterministic API', () => {
    assert.doesNotMatch(source, /\b(Date\.now|Math\.random|uuid4|fetch|setTimeout)\s*\(/);
    assert.doesNotMatch(source, /\bnew Date\s*\(/);
    assert.doesNotMatch(source, /\bprocess\.env\b/);
  });

  it('launches no process and reads no module outside its two imports', () => {
    assert.doesNotMatch(source, /\brequire\s*\(|\bimport\s*\(/);
    assert.doesNotMatch(source, /\b(exec|execAsync|execSync|spawn|spawnSync)\s*\(/);
  });
});

describe('activity options are valid Temporal options', () => {
  it('pins both proxies to their queue', () => {
    // `proxyActivities` validates its options eagerly, so importing this module at all proves
    // both option sets are well formed; these assertions pin the values that matter.
    assert.equal(CONTROL_PLANE_ACTIVITY_OPTIONS.taskQueue, CONTROL_PLANE_TASK_QUEUE);
    assert.equal(RUST_INGESTION_ACTIVITY_OPTIONS.taskQueue, RUST_INGESTION_TASK_QUEUE);
  });

  it('exposes the control-plane non-retryable list it actually uses', () => {
    assert.deepEqual(
      RUST_INGESTION_ACTIVITY_OPTIONS.retry?.nonRetryableErrorTypes,
      [...NON_RETRYABLE_FAILURE_TYPES],
    );
    assert.deepEqual(
      CONTROL_PLANE_ACTIVITY_OPTIONS.retry?.nonRetryableErrorTypes,
      [...NON_RETRYABLE_CONTROL_PLANE_ERROR_TYPES],
    );
  });
});
