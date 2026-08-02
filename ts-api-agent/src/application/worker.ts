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
import { NativeConnection, Worker } from '@temporalio/worker';

import { artifactBucketFromEnv } from './artifact-bucket.ts';
import { createControlPlaneActivities } from './control-plane-activities.ts';
import { CONTROL_PLANE_TASK_QUEUE } from './workflows.ts';
import { S3ObjectStore } from '../infrastructure/object-store/s3-object-store.ts';
import { temporalAddressFromEnv } from '../infrastructure/temporal/temporal-ingestion-client.ts';

/** The namespace both Workers poll. One namespace, two task queues. */
export function temporalNamespaceFromEnv(env: NodeJS.ProcessEnv = process.env): string {
  const configured = env.TEMPORAL_NAMESPACE ?? '';
  return configured.length > 0 ? configured : 'default';
}

async function run(): Promise<void> {
  const objectStore = S3ObjectStore.fromEnv();
  const artifactBucket = artifactBucketFromEnv();
  const address = temporalAddressFromEnv();
  const namespace = temporalNamespaceFromEnv();

  // Explicit connection and namespace. Without them the SDK silently defaults to
  // `localhost:7233`, which is right on a developer's machine and wrong in every container: the
  // Worker starts, fails to connect, exits, and restarts forever while the API happily reports
  // `503` — the same symptom as an orchestrator outage, from a misconfiguration.
  const connection = await NativeConnection.connect({ address });

  const worker = await Worker.create({
    connection,
    namespace,
    workflowsPath: new URL('./workflows.ts', import.meta.url).pathname,
    activities: createControlPlaneActivities({ objectStore, artifactBucket }),
    taskQueue: CONTROL_PLANE_TASK_QUEUE,
  });

  console.log(
    `[Temporal Worker] control plane listening on '${CONTROL_PLANE_TASK_QUEUE}' at ` +
      `'${address}' (namespace '${namespace}'), publishing artifacts to bucket ` +
      `'${artifactBucket}'`,
  );

  try {
    await worker.run();
  } finally {
    await connection.close().catch(() => undefined);
    objectStore.destroy();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  run().catch((err) => {
    console.error('[Temporal Worker] Error:', err);
    process.exit(1);
  });
}
