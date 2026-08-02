/**
 * Manual trigger for one ingestion run.
 *
 * The only thing a caller chooses is a seeded catalog key. There is no path, no URL and no
 * bucket to pass: `inspectDatasetSource` resolves the key to its allowlisted S3 object, and the
 * dataset id minted here names the immutable artifact prefix that run may write under.
 *
 *   node ts-api-agent/src/application/trigger_workflow.ts [demo-small|na12878-full]
 */
import { Client, Connection } from '@temporalio/client';

import { datasetCatalog, newDatasetId } from './dataset-catalog.ts';
import { CONTROL_PLANE_TASK_QUEUE, GenomicIngestionWorkflow } from './workflows.ts';

async function run(): Promise<void> {
  const entry = datasetCatalog.get(process.argv[2] ?? 'demo-small');
  const datasetId = newDatasetId(entry.key);
  const address = process.env.TEMPORAL_HOST ?? 'localhost:7233';

  console.log(`[Temporal Client] Connecting to ${address}...`);
  const connection = await Connection.connect({ address });

  try {
    const client = new Client({ connection });
    const handle = await client.workflow.start(GenomicIngestionWorkflow, {
      taskQueue: CONTROL_PLANE_TASK_QUEUE,
      workflowId: `genomic-ingestion-${datasetId}`,
      args: [{ datasetId, datasetKey: entry.key }],
    });

    console.log(`✔ Started ${handle.workflowId}`);
    console.log(`  Dataset:  ${entry.key} (${entry.displayName})`);
    console.log(`  Source:   s3://${entry.source.bucket}/${entry.source.key}`);
    console.log(
      `  Temporal: http://localhost:8233/namespaces/default/workflows/${handle.workflowId}`,
    );
  } finally {
    await connection.close();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  run().catch((err) => {
    console.error('[Temporal Client] Failed to start workflow:', err);
    process.exit(1);
  });
}
