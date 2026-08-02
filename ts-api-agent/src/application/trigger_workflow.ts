/**
 * Manual trigger for one ingestion run.
 *
 * The only thing a caller chooses is a seeded catalog key. There is no path, no URL and no
 * bucket to pass: `inspectDatasetSource` resolves the key to its allowlisted S3 object, and the
 * dataset id minted here names the immutable artifact prefix that run may write under.
 *
 *   node ts-api-agent/src/application/trigger_workflow.ts [demo-small|na12878-full]
 *
 * It goes through the same `IngestionClient` adapter `POST /api/ingestions` uses, so a run
 * started from the shell is indistinguishable from one started by the API — same Workflow, same
 * task queue, and above all the same `workflowId` spelling, so `GET /api/ingestions/:workflowId`
 * can poll a run this script started.
 */
import { datasetCatalog, newDatasetId } from './dataset-catalog.ts';
import { ingestionWorkflowIdFor } from './ingestion-client.ts';
import { createTemporalIngestionClient } from '../infrastructure/temporal/temporal-ingestion-client.ts';

async function run(): Promise<void> {
  const entry = datasetCatalog.get(process.argv[2] ?? 'demo-small');
  const datasetId = newDatasetId(entry.key);
  const workflowId = ingestionWorkflowIdFor(datasetId);

  const client = createTemporalIngestionClient();
  try {
    await client.start({ workflowId, datasetId, datasetKey: entry.key });

    console.log(`✔ Started ${workflowId}`);
    console.log(`  Dataset:  ${entry.key} (${entry.displayName})`);
    console.log(`  Source:   s3://${entry.source.bucket}/${entry.source.key}`);
    console.log(`  Progress: GET /api/ingestions/${workflowId}`);
    console.log(`  Temporal: http://localhost:8233/namespaces/default/workflows/${workflowId}`);
  } finally {
    await client.close();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  run().catch((err) => {
    console.error('[Temporal Client] Failed to start workflow:', err);
    process.exit(1);
  });
}
