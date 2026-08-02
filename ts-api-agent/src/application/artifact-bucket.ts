/**
 * The one bucket every published artifact and manifest lives in.
 *
 * `application/worker.ts` (the Rust/TS control-plane Worker) and `index.ts` (the API's
 * `datasetResolver`) must agree on this value: the Worker writes manifests where this function
 * says to, and the API looks for them at the same place. They used to each define their own
 * copy of this function with a comment asking whoever edited one to also edit the other — a
 * promise nothing enforced. Divergence is silent and confusing: every `/ask` would fail with a
 * false `409 DatasetNotPublished` because the API is checking a bucket the Worker never wrote
 * to. This module is the single definition both import, so there is no second copy left to
 * drift.
 *
 * Deliberately has zero runtime imports beyond `node:process` types, so importing it never
 * drags in `@temporalio/worker`'s native core (the reason the two copies existed in the first
 * place) or any S3 client.
 */

/** Matches the bucket `scripts/seed_demo_s3.sh` creates. */
export const DEFAULT_ARTIFACT_BUCKET = 'genomic-artifacts';

export function artifactBucketFromEnv(env: NodeJS.ProcessEnv = process.env): string {
  const configured = env.S3_ARTIFACT_BUCKET ?? '';
  return configured.length > 0 ? configured : DEFAULT_ARTIFACT_BUCKET;
}
