import type { ContentfulStatusCode } from 'hono/utils/http-status';

/**
 * Status for every failure the serving path and the orchestrator can raise, keyed by the
 * error's `name`.
 *
 * Matching on the name rather than on `instanceof` is deliberate. These names are the frozen
 * cross-layer contract (`contracts/ingestion-v1.md` and the serving modules pin each one with
 * an explicit `this.name`), and importing the classes would drag the DuckDB native binding and
 * the reference snapshot module into every consumer of the HTTP layer for nothing.
 *
 * The three families:
 *
 * - `409` — the dataset exists as an id but cannot be served as published: no manifest, a
 *   manifest that no longer matches its objects, or a reference snapshot that describes a
 *   different genome than the one the dataset was ingested against. Answering anyway would
 *   mean returning the wrong person's answer to the right question.
 * - `4xx` on the target — the question named something the reference cannot place, or that the
 *   dataset provably does not contain. Neither is widened into a scan.
 * - `5xx` — the object store or the query budget gave out. These are the ones a caller may
 *   retry; none of them may be answered from anything else.
 */
export const ERROR_STATUS: Readonly<Record<string, ContentfulStatusCode>> = Object.freeze({
  IngestionServiceUnavailable: 503,
  IngestionRunNotFound: 404,

  DatasetNotPublished: 409,
  ObjectVerificationFailed: 409,
  ReferenceSnapshotMismatch: 409,
  ReferenceBuildMismatch: 409,
  DatasetPublicationConflict: 409,

  TargetNotResolvable: 422,
  TargetNotPresent: 404,

  RemoteDatasetUnavailable: 503,
  ReferenceSnapshotUnavailable: 503,
  HttpfsExtensionUnavailable: 503,
  QueryBudgetExceeded: 504,
  SessionConfigurationTimedOut: 504,
});

export function nameOf(error: unknown): string {
  const name = (error as { name?: unknown } | null)?.name;
  return typeof name === 'string' ? name : '';
}

export function statusFor(error: unknown): ContentfulStatusCode | undefined {
  const name = nameOf(error);
  if (name === 'DatasetResolutionFailed') {
    // One resolution code is the caller's fault — an id that is not a single safe path segment
    // never named a dataset in the first place. Every other code means the *published* artifact
    // is not trustworthy, which is a conflict with the dataset's state, not a bad request.
    return (error as { code?: unknown }).code === 'DATASET_ID_UNSAFE' ? 400 : 409;
  }
  return ERROR_STATUS[name];
}
