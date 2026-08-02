import { NativeConnection, Worker } from '@temporalio/worker';

/** Task queue polled by the TypeScript Workflow Worker for the feasibility probe. */
export const PROBE_WORKFLOW_TASK_QUEUE = 'genomic-ingestion-probe';

/**
 * Creates a Workflow-only TypeScript Worker. It registers no activities on purpose:
 * `rustActivityProbe` must be served by the Rust Worker on `genomic-ingestion-rust`.
 */
export async function createProbeWorker(address: string): Promise<Worker> {
  const connection = await NativeConnection.connect({ address });
  return Worker.create({
    connection,
    namespace: 'default',
    taskQueue: PROBE_WORKFLOW_TASK_QUEUE,
    workflowsPath: new URL('./temporal_probe_workflow.ts', import.meta.url).pathname,
  });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const worker = await createProbeWorker(process.env.TEMPORAL_ADDRESS ?? 'localhost:7233');
  console.log(`[probe-workflow-worker] ready task_queue=${PROBE_WORKFLOW_TASK_QUEUE}`);
  await worker.run();
}
