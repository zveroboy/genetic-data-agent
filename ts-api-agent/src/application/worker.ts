import { Worker } from '@temporalio/worker';
import * as activities from './activities.ts';

async function run() {
  const worker = await Worker.create({
    workflowsPath: new URL('./workflows.ts', import.meta.url).pathname,
    activities,
    taskQueue: 'genomic-ingestion',
  });

  console.log('[Temporal Worker] Started listening on taskQueue: "genomic-ingestion"');
  await worker.run();
}

if (import.meta.url === `file://${process.argv[1]}`) {
  run().catch((err) => {
    console.error('[Temporal Worker] Error:', err);
    process.exit(1);
  });
}
