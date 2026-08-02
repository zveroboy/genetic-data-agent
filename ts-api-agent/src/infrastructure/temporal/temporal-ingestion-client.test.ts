/**
 * Unit tests over the Temporal adapter itself, not over the HTTP layer's mapping of an
 * already-classified error.
 *
 * `index.test.ts` injects a fake `IngestionClient` that throws `IngestionServiceUnavailableError`
 * or `IngestionRunNotFoundError` directly — it proves the route maps those names to `503`/`404`,
 * not that a refused connection or a real Temporal query failure actually becomes one. This file
 * drives the two places a real Temporal SDK call can fail — `Connection.connect` and a workflow
 * handle's `query` — through the classification logic in `createTemporalIngestionClient` itself:
 *
 * - a stubbed `Connection.connect` that rejects becomes `IngestionServiceUnavailableError`;
 * - a handle whose `query` throws `ServiceError` (a gRPC transport fault) becomes the same;
 * - a handle whose `query` throws `WorkflowNotFoundError` becomes `IngestionRunNotFoundError`;
 * - anything else is not silently swallowed into either — it propagates with its own identity,
 *   which is what keeps an application-level rejection (e.g. "workflow already started") from
 *   being misreported as an outage.
 *
 * No test here ever lets an assertion pass because a fabricated `state` was returned instead of
 * a thrown classification error — every assertion is on the *rejection*, and the one success
 * path exercised (retrying after a failed, uncached connect) asserts the resulting progress came
 * from a stubbed `query`, never from a timer or a default.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Connection, ServiceError, WorkflowClient, WorkflowNotFoundError } from '@temporalio/client';

import {
  IngestionRunNotFoundError,
  IngestionServiceUnavailableError,
} from '../../application/ingestion-client.ts';
import { createTemporalIngestionClient } from './temporal-ingestion-client.ts';

const WORKFLOW_ID = 'genomic-ingestion-demo-small-abc123';
const START_REQUEST = {
  workflowId: WORKFLOW_ID,
  datasetId: 'demo-small-abc123',
  datasetKey: 'demo-small' as const,
};

describe('createTemporalIngestionClient — a refused connection', () => {
  it('surfaces as IngestionServiceUnavailable from getProgress, not a raw connect error', async (t) => {
    t.mock.method(Connection, 'connect', async () => {
      throw new Error('connect ECONNREFUSED 127.0.0.1:7233');
    });

    const client = createTemporalIngestionClient({ address: 'localhost:7233' });
    await assert.rejects(() => client.getProgress(WORKFLOW_ID), IngestionServiceUnavailableError);
  });

  it('surfaces as IngestionServiceUnavailable from start, not a raw connect error', async (t) => {
    t.mock.method(Connection, 'connect', async () => {
      throw new Error('connect ECONNREFUSED 127.0.0.1:7233');
    });

    const client = createTemporalIngestionClient({ address: 'localhost:7233' });
    await assert.rejects(() => client.start(START_REQUEST), IngestionServiceUnavailableError);
  });

  it('does not cache the failure: the next request retries and can succeed', async (t) => {
    let attempts = 0;
    t.mock.method(Connection, 'connect', async () => {
      attempts += 1;
      if (attempts === 1) throw new Error('connect ECONNREFUSED 127.0.0.1:7233');
      return {} as Connection;
    });
    t.mock.method(WorkflowClient.prototype, 'getHandle', () => ({
      query: async () => ({
        datasetId: 'demo-small-abc123',
        datasetKey: 'demo-small',
        state: 'RESOLVING',
        unobservedStates: [],
        message: 'stubbed progress from the second, successful connect',
      }),
    }));

    const client = createTemporalIngestionClient({ address: 'localhost:7233' });

    await assert.rejects(() => client.getProgress(WORKFLOW_ID), IngestionServiceUnavailableError);

    const progress = await client.getProgress(WORKFLOW_ID);
    assert.equal(attempts, 2, 'a failed connect must not be cached for the next request');
    assert.equal(progress.state, 'RESOLVING');
    assert.equal(
      progress.message,
      'stubbed progress from the second, successful connect',
      'the reported progress must come from the stubbed query, not be invented locally',
    );
  });
});

describe('createTemporalIngestionClient — a query against a connected client', () => {
  it('classifies a gRPC ServiceError as IngestionServiceUnavailable', async (t) => {
    t.mock.method(Connection, 'connect', async () => ({}) as Connection);
    t.mock.method(WorkflowClient.prototype, 'getHandle', () => ({
      query: async () => {
        throw new ServiceError('14 UNAVAILABLE: connection reset before headers');
      },
    }));

    const client = createTemporalIngestionClient({ address: 'localhost:7233' });
    await assert.rejects(() => client.getProgress(WORKFLOW_ID), IngestionServiceUnavailableError);
  });

  it('classifies a WorkflowNotFoundError as IngestionRunNotFound, carrying the workflowId', async (t) => {
    t.mock.method(Connection, 'connect', async () => ({}) as Connection);
    t.mock.method(WorkflowClient.prototype, 'getHandle', () => ({
      query: async () => {
        throw new WorkflowNotFoundError('workflow execution not found', WORKFLOW_ID, undefined);
      },
    }));

    const client = createTemporalIngestionClient({ address: 'localhost:7233' });
    await assert.rejects(
      () => client.getProgress(WORKFLOW_ID),
      (error: unknown) => {
        assert.ok(error instanceof IngestionRunNotFoundError);
        assert.equal((error as IngestionRunNotFoundError).workflowId, WORKFLOW_ID);
        return true;
      },
    );
  });

  it('does not swallow an unrecognised query failure into either mapped error', async (t) => {
    t.mock.method(Connection, 'connect', async () => ({}) as Connection);
    t.mock.method(WorkflowClient.prototype, 'getHandle', () => ({
      query: async () => {
        throw new Error('an application-level failure, not a transport fault');
      },
    }));

    const client = createTemporalIngestionClient({ address: 'localhost:7233' });
    await assert.rejects(
      () => client.getProgress(WORKFLOW_ID),
      (error: unknown) => {
        assert.ok(!(error instanceof IngestionServiceUnavailableError));
        assert.ok(!(error instanceof IngestionRunNotFoundError));
        assert.match((error as Error).message, /an application-level failure/);
        return true;
      },
    );
  });

  it('does not swallow an unrecognised start failure (e.g. workflow-already-started) into IngestionServiceUnavailable', async (t) => {
    t.mock.method(Connection, 'connect', async () => ({}) as Connection);
    t.mock.method(WorkflowClient.prototype, 'start', async () => {
      throw new Error('Workflow execution already started');
    });

    const client = createTemporalIngestionClient({ address: 'localhost:7233' });
    await assert.rejects(
      () => client.start(START_REQUEST),
      (error: unknown) => {
        assert.ok(!(error instanceof IngestionServiceUnavailableError));
        assert.match((error as Error).message, /already started/);
        return true;
      },
    );
  });
});
