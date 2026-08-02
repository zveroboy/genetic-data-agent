/**
 * The TypeScript Temporal Worker: control plane only.
 *
 * It serves `genomic-control-plane` — the Workflow plus the two short S3 Activities
 * (`inspectDatasetSource`, `publishDataset`). It deliberately does **not** register
 * `buildDatasetArtifact`: that Activity exists only in `rust-ingestion-worker`, is polled only
 * from `genomic-ingestion-rust`, and is scheduled from here by name. Nothing in this process
 * launches a Rust binary, shells out, or touches a local VCF; the only thing that crosses the
 * language boundary is an S3 object reference.
 */
import { Worker } from '@temporalio/worker';

import { createControlPlaneActivities } from './control-plane-activities.ts';
import { CONTROL_PLANE_TASK_QUEUE } from './workflows.ts';
import { S3ObjectStore } from '../infrastructure/object-store/s3-object-store.ts';

/** Matches the bucket `scripts/seed_demo_s3.sh` creates. */
export const DEFAULT_ARTIFACT_BUCKET = 'genomic-artifacts';

/**
 * Bucket every published artifact and manifest is written to. It is Worker configuration, never
 * wire input, which is what lets `publishDataset` reject an otherwise well-formed request that
 * targets a different bucket.
 */
export function artifactBucketFromEnv(env: NodeJS.ProcessEnv = process.env): string {
  const configured = env.S3_ARTIFACT_BUCKET ?? '';
  return configured.length > 0 ? configured : DEFAULT_ARTIFACT_BUCKET;
}

async function run(): Promise<void> {
  const objectStore = S3ObjectStore.fromEnv();
  const artifactBucket = artifactBucketFromEnv();

  const worker = await Worker.create({
    workflowsPath: new URL('./workflows.ts', import.meta.url).pathname,
    activities: createControlPlaneActivities({ objectStore, artifactBucket }),
    taskQueue: CONTROL_PLANE_TASK_QUEUE,
  });

  console.log(
    `[Temporal Worker] control plane listening on '${CONTROL_PLANE_TASK_QUEUE}', ` +
      `publishing artifacts to bucket '${artifactBucket}'`,
  );

  try {
    await worker.run();
  } finally {
    objectStore.destroy();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  run().catch((err) => {
    console.error('[Temporal Worker] Error:', err);
    process.exit(1);
  });
}
