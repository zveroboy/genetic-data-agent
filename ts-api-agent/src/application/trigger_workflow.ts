import { Connection, Client } from '@temporalio/client';
import { GenomicIngestionWorkflow } from './workflows.ts';
import path from 'path';
import fs from 'fs';

async function run() {
  console.log('[Temporal Client] Connecting to localhost:7233...');
  const connection = await Connection.connect({ address: 'localhost:7233' });
  const client = new Client({ connection });

  const workflowId = `genomic-ingestion-${Date.now()}`;
  const vcfArg = process.argv[2] || 'tests/fixtures/demo_user.vcf';
  const vcfPath = path.resolve(process.cwd(), vcfArg);

  if (!fs.existsSync(vcfPath)) {
    throw new Error(`VCF file not found at path: ${vcfPath}`);
  }

  const userId = vcfArg.includes('na12878') ? 'user-na12878' : 'user-demo-01';

  console.log(`[Temporal Client] Starting GenomicIngestionWorkflow (ID: ${workflowId})...`);
  console.log(`  User ID:  ${userId}`);
  console.log(`  VCF File: ${vcfPath}`);

  const handle = await client.workflow.start(GenomicIngestionWorkflow, {
    taskQueue: 'genomic-ingestion',
    workflowId,
    args: [userId, vcfPath],
  });

  console.log(`\n✔ Workflow started successfully!`);
  console.log(`  WorkflowID: ${handle.workflowId}`);
  console.log(`  👉 View live in Temporal Web UI: http://localhost:8233/namespaces/default/workflows/${handle.workflowId}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  run().catch((err) => {
    console.error('[Temporal Client] Failed to start workflow:', err);
    process.exit(1);
  });
}
