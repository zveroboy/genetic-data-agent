# Cross-Language Genomic Ingestion Design

**Date:** 2026-08-02

## Purpose

Turn the educational MVP into a truthful, demonstrable vertical slice without implementing arbitrary user uploads. The system will continue to offer a fixed catalog of VCF datasets already present in S3/MinIO, while every operation after dataset selection is real: Temporal orchestration, S3 access, Rust processing, DuckDB artifact creation, validation, publication, and dataset-scoped querying.

The design optimizes for engineering depth visible during an interview, not for complete clinical-production readiness.

## Decisions

1. TypeScript remains the control plane: HTTP API, dataset catalog, Temporal Workflow, lightweight S3 activities, dataset lifecycle, agent tools, and Qdrant integration.
2. Rust becomes a real Temporal Activity Worker on `genomic-ingestion-rust`. It does not host Workflows.
3. Rust owns the data plane: downloading a source VCF, streaming VCF/VCF.GZ parsing, DuckDB construction, structural validation, checksum calculation, and staging artifact upload.
4. S3 object references, never local paths, cross the TypeScript/Rust boundary.
5. DuckDB is an immutable, per-dataset serving artifact. Rust writes a temporary database and never appends to a previously published artifact.
6. A manifest written last is the publication commit marker. An artifact without a published manifest is not queryable.
7. ClinVar-derived annotations and PubMed literature are global reference data. They are bootstrapped independently of user datasets. Qdrant contains literature only.
8. `/ask` requires a `datasetId`; it must not query a global DuckDB singleton or silently substitute fixture data.
9. The Rust SDK is intentionally accepted at Public Preview maturity and pinned to `temporalio-sdk` 0.5.0 for this educational project.

## Data Classification

### Seeded user-like datasets

- `demo-small` maps to `s3://genomic-data/samples/demo_user.vcf`.
- `na12878-full` maps to `s3://genomic-data/samples/na12878_hg001.vcf.gz`.

The client sends only the catalog key. Arbitrary S3 URIs, HTTP URLs, filesystem paths, and uploads are out of scope.

### Global reference data

- The small `clinvar_benchmark.tsv` remains a deterministic demo reference fixture.
- Full ClinVar import is a separately versioned bootstrap concern and is not required for the first vertical slice.
- PubMed documents and embeddings are loaded once into Qdrant and are shared by all datasets.

### Test-only data

`na12878_clinical_benchmark.vcf` is a synthetic clinical-patient fixture. It must not be presented as a real NA12878-derived clinical truth dataset.

## Components

### Dataset catalog

`DatasetCatalog` is an allowlist mapping a public `datasetKey` to a bucket, object key, expected file kind, and display metadata. The API rejects unknown keys before starting a Workflow.

### TypeScript Workflow Worker

`GenomicIngestionWorkflow` orchestrates three retry boundaries:

1. `inspectDatasetSource` on `genomic-control-plane` resolves the catalog entry and records immutable source metadata such as ETag and content length.
2. `buildDatasetArtifact` on `genomic-ingestion-rust` creates and uploads a validated staging DuckDB artifact.
3. `publishDataset` on `genomic-control-plane` copies the staging artifact to its final key and writes the manifest last.

The Workflow exposes an aggregate progress query. Detailed progress within the Rust Activity is exposed through Temporal heartbeat details.

### Rust Activity Worker

The Rust Worker is activity-only and registers the exact Activity Type `buildDatasetArtifact`. It:

1. Downloads the exact S3 object version described by the input contract.
2. Streams plain or gzip-compressed VCF input without loading all lines or variants into memory.
3. Writes to an attempt-scoped temporary DuckDB file using bounded batches.
4. Creates `user_variants` and `dataset_metadata` tables and required indexes.
5. Validates table presence, nonzero variant count, parse rejection counts, and reference build metadata.
6. Calculates SHA-256 for the completed database.
7. Uploads it to an attempt-scoped staging key.
8. Returns artifact metadata to the Workflow.

The Activity sends heartbeats for `DOWNLOADING`, `PARSING`, `WRITING_DUCKDB`, and `UPLOADING`. It responds to cancellation and removes temporary files during cleanup.

### Publication manifest

The manifest is JSON stored at `datasets/{datasetId}/manifest.json` and contains:

- contract version;
- dataset ID and catalog key;
- source bucket, key, and ETag;
- final artifact URI and SHA-256;
- variant and rejected-record counts;
- reference build;
- creation timestamp generated outside Workflow replay-sensitive code;
- processor version.

The final DuckDB key is `datasets/{datasetId}/variants.duckdb`. `publishDataset` is idempotent: the same manifest may be written again only when its checksum and source identity match.

### Dataset-scoped query path

`DatasetArtifactResolver` loads the published manifest, downloads the immutable DuckDB artifact into a checksum-keyed local read cache, verifies its checksum, and returns a local read-only path. `DuckDbGenotypeRepository` is constructed per dataset path. Agent tools receive a request-scoped repository instead of importing a global singleton.

The demo ClinVar reference database remains global and read-only. DuckDB attaches it for the genotype-to-annotation join. Missing datasets, missing manifests, invalid checksums, and unavailable references are explicit failures; fixture fallback is permitted only in tests.

## Cross-Language Contract

The shared payload uses JSON-compatible primitives and camelCase field names. Rust structs use `#[serde(rename_all = "camelCase")]`. Dates, `BigInt`, buffers, class instances, and serialized language-native errors are prohibited.

The primary input contains `datasetId`, `datasetKey`, immutable source identity, and staging target identity. The result contains the staging artifact URI, SHA-256, variant counts, reference build, and processor version.

Stable application failure types are:

- `InvalidVcfFormat` — non-retryable;
- `SourceObjectChanged` — non-retryable for the current Workflow input;
- `ObjectStoreUnavailable` — retryable;
- `ArtifactWriteFailed` — retryable;
- `ArtifactValidationFailed` — non-retryable when caused by deterministic content.

## Idempotency and Failure Semantics

Every API start request creates a new `datasetId`; Temporal retries retain that ID. Each Rust Activity attempt writes to its own temporary local file and staging object. No retry appends to an existing DuckDB database.

The publication manifest is the only readiness signal. Failed or cancelled runs may leave staging objects, but never a published manifest. Staging garbage collection is outside the first vertical slice and may be added later.

Temporal timeouts are configured per Activity rather than shared globally. The Rust Activity has a heartbeat timeout and a sufficiently long start-to-close timeout. Invalid VCF failures do not retry. Network and worker-loss failures do retry.

## Temporal UI Outcome

One Workflow execution shows distinct Workers and queues:

```text
inspectDatasetSource     TS    genomic-control-plane
buildDatasetArtifact     Rust  genomic-ingestion-rust
publishDataset           TS    genomic-control-plane
```

The pending Rust Activity displays its Worker identity, attempt, heartbeat phase, processed bytes, and processed variants. A worker crash is visible as a failed/timed-out attempt followed by a retry on another Rust Worker.

## API Lifecycle

1. `GET /api/datasets/catalog` lists the two seeded choices.
2. `POST /api/ingestions` accepts `{ "datasetKey": "demo-small" }` and returns `202` with `datasetId`, `workflowId`, and status URL.
3. `GET /api/ingestions/{workflowId}` returns Workflow status and aggregate progress.
4. `POST /ask` accepts `{ "datasetId": "...", "question": "..." }` only after the manifest exists.
5. Reopening a conversation reuses the published `datasetId`; it does not rerun ingestion.

## Testing Strategy

- Contract fixtures are deserialized and serialized in both TypeScript and Rust.
- TypeScript unit tests cover allowlisting, Workflow routing/options, publication idempotency, manifest resolution, and API validation.
- Rust unit tests cover plain/gzip streaming parse, malformed records, bounded batch writes, schema validation, and error mapping.
- A Temporal integration test starts a TS Workflow Worker and Rust Activity Worker against a dev/test server and verifies the Activity Type, task queue, result, and heartbeat.
- An object-store integration test uses MinIO and verifies source ETag pinning, staging upload, manifest-last publication, checksum verification, and retry idempotency.
- The final E2E test ingests `demo-small`, waits for completion, asks a known genotype question using the resulting `datasetId`, and verifies that the answer came from that artifact.

## Scope Boundaries

The first implementation does not include arbitrary uploads, authentication, multi-tenant authorization, full ClinVar normalization, clinical-grade allele/haplotype interpretation, Parquet export, automatic staging garbage collection, or production rollout of a Public Preview Rust SDK.

Qdrant remains a global literature repository. Improving PubMed abstract quality is useful but independent and must not block the ingestion vertical slice.

## Delivery Milestones

1. **Core contracts and truthfulness:** catalog, explicit dataset lifecycle, no global/fallback query behavior, and reliable unit tests.
2. **Real cross-language execution:** activity-only Rust Temporal Worker, S3-based contract, heartbeats, cancellation, and immutable DuckDB staging.
3. **Published serving path:** manifest-last publication, dataset-scoped query repository, Docker wiring, and E2E demonstration.

The recommended interview-ready stopping point is completion of milestone 3. Full ClinVar and PubMed improvements are subsequent projects.
