/**
 * Temporal adapter for the `IngestionClient` port.
 *
 * It does two things and refuses to do a third. It starts `GenomicIngestionWorkflow` under the
 * identity the caller minted, and it forwards the Workflow's own `getProgress` query. It has no
 * fallback: when the orchestrator cannot be reached, an `IngestionServiceUnavailableError`
 * leaves the API with nothing to report but `503`, which is the point — a timer that walked a
 * caller through `RESOLVING → BUILDING → COMPLETED` with no Worker running would describe a
 * published dataset that does not exist.
 *
 * The connection is established lazily and cached, because `Connection.connect` performs a
 * round trip and the previous per-request connect leaked one TCP connection per poll. A
 * *failed* connect is deliberately not cached: a Temporal server that comes up a second later
 * must be picked up by the next request rather than being remembered as down forever.
 */
import { Client, Connection, ServiceError, WorkflowNotFoundError } from '@temporalio/client';

import {
  type IngestionClient,
  IngestionRunNotFoundError,
  IngestionServiceUnavailableError,
  type StartIngestionRequest,
} from '../../application/ingestion-client.ts';
import {
  CONTROL_PLANE_TASK_QUEUE,
  GenomicIngestionWorkflow,
  type IngestionProgress,
  getProgressQuery,
} from '../../application/workflows.ts';

export const DEFAULT_TEMPORAL_ADDRESS = 'localhost:7233';

export function temporalAddressFromEnv(env: NodeJS.ProcessEnv = process.env): string {
  const configured = env.TEMPORAL_HOST ?? '';
  return configured.length > 0 ? configured : DEFAULT_TEMPORAL_ADDRESS;
}

export interface TemporalIngestionClientOptions {
  readonly address?: string;
}

export interface TemporalIngestionClient extends IngestionClient {
  /** Releases the cached connection. Safe to call when none was ever established. */
  close(): Promise<void>;
}

function detailOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * A gRPC transport fault — the server is unreachable, overloaded or timing out. Distinct from
 * an application-level rejection such as "this workflow id already exists", which must keep its
 * own meaning rather than being reported as an outage.
 */
function isTransportFault(error: unknown): boolean {
  return error instanceof ServiceError;
}

export function createTemporalIngestionClient(
  options: TemporalIngestionClientOptions = {},
): TemporalIngestionClient {
  const address = options.address ?? temporalAddressFromEnv();
  let pending: Promise<{ connection: Connection; client: Client }> | null = null;

  async function connected(): Promise<Client> {
    if (pending === null) {
      const attempt = Connection.connect({ address }).then((connection) => ({
        connection,
        client: new Client({ connection }),
      }));
      // Forget a failed attempt so the next request retries, and attach a handler here so the
      // rejection is never unhandled while nothing is awaiting the cached promise.
      attempt.catch(() => {
        if (pending === attempt) pending = null;
      });
      pending = attempt;
    }

    try {
      return (await pending).client;
    } catch (error) {
      throw new IngestionServiceUnavailableError(address, detailOf(error), { cause: error });
    }
  }

  return {
    async start(request: StartIngestionRequest): Promise<void> {
      const client = await connected();
      try {
        await client.workflow.start(GenomicIngestionWorkflow, {
          taskQueue: CONTROL_PLANE_TASK_QUEUE,
          workflowId: request.workflowId,
          args: [{ datasetId: request.datasetId, datasetKey: request.datasetKey }],
        });
      } catch (error) {
        if (isTransportFault(error)) {
          throw new IngestionServiceUnavailableError(address, detailOf(error), { cause: error });
        }
        throw error;
      }
    },

    async getProgress(workflowId: string): Promise<IngestionProgress> {
      const client = await connected();
      try {
        return await client.workflow.getHandle(workflowId).query(getProgressQuery);
      } catch (error) {
        if (error instanceof WorkflowNotFoundError) {
          throw new IngestionRunNotFoundError(workflowId, { cause: error });
        }
        if (isTransportFault(error)) {
          throw new IngestionServiceUnavailableError(address, detailOf(error), { cause: error });
        }
        throw error;
      }
    },

    async close(): Promise<void> {
      const outstanding = pending;
      pending = null;
      if (outstanding === null) return;
      // A connection that never came up has nothing to close.
      await outstanding.then(({ connection }) => connection.close()).catch(() => undefined);
    },
  };
}
