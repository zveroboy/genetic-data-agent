# Cross-Language Genomic Ingestion Design

**Date:** 2026-08-02

## Purpose

Turn the educational MVP into a truthful, demonstrable vertical slice without implementing arbitrary user uploads. The system continues to offer a fixed catalog of VCF datasets already present in S3/MinIO, while every operation after dataset selection is real: Temporal orchestration, S3 access, Rust processing, partitioned Parquet creation, validation, publication, and dataset-scoped querying through DuckDB.

The design optimizes for engineering depth visible during an interview, not for complete clinical-production readiness.

## Decisions

1. TypeScript remains the control plane: HTTP API, dataset catalog, Temporal Workflow, lightweight S3 activities, dataset lifecycle, agent tools, and Qdrant integration.
2. Rust becomes a real Temporal Activity Worker on `genomic-ingestion-rust`. It does not host Workflows.
3. Rust owns the data plane: downloading a source VCF, streaming VCF/VCF.GZ parsing, local DuckDB staging, structural validation, sorted partitioned Parquet export, checksum calculation, and upload to an attempt-unique immutable version prefix.
4. S3 object references, never local paths, cross the TypeScript/Rust boundary.
5. Partitioned Parquet in S3 is the immutable persistent representation of user variants. DuckDB is an embedded processing and query engine, not the persistent user-data format.
6. A manifest written last is the publication commit marker. A Parquet prefix without a published manifest is not queryable.
7. ClinVar-derived annotations and PubMed literature are global reference data. They are bootstrapped independently of user datasets. Qdrant contains literature only.
8. `/ask` requires a `datasetId`; it resolves an immutable manifest and queries only its explicit Parquet objects. It must not query a global user-data DuckDB singleton or silently substitute fixture data.
9. The Rust SDK is intentionally accepted at Public Preview maturity and pinned to `temporalio-sdk` 0.5.0 for this educational project.

## Data Classification

### Seeded user-like datasets

- `demo-small` maps to `s3://genomic-data/samples/demo_user.vcf`.
- `na12878-full` maps to `s3://genomic-data/samples/na12878_hg001.vcf.gz`.

The client sends only the catalog key. Arbitrary S3 URIs, HTTP URLs, filesystem paths, and uploads are out of scope.

### Global reference data

- A generated `clinvar_coordinates_grch38.tsv` containing reference version, assembly, chromosome, position, rsID, ref, alt, gene, and clinical annotation is the deterministic demo coordinate reference. It is bootstrapped once into a small read-only reference DuckDB.
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
2. `buildDatasetArtifact` on `genomic-ingestion-rust` creates and uploads a validated Parquet dataset below an attempt-unique immutable version prefix.
3. `publishDataset` on `genomic-control-plane` verifies every object uploaded by the successful attempt below its unique immutable version prefix and writes the manifest last. It does not copy the Parquet payload again.

The Workflow exposes an aggregate progress query. Detailed progress within the Rust Activity is exposed through Temporal heartbeat details.

### Rust Activity Worker

The Rust Worker is activity-only and registers the exact Activity Type `buildDatasetArtifact`. It:

1. Downloads the exact S3 object version described by the input contract.
2. Streams plain or gzip-compressed VCF input without loading all lines or variants into memory.
3. Writes bounded batches to an attempt-scoped local DuckDB staging database.
4. Creates and validates `user_variants` and `dataset_metadata`, then exports rows ordered by `(chrom, pos, ref, alt)` to Zstandard-compressed Parquet partitioned by `chrom`.
5. Uses row groups of approximately 100,000 rows so remote queries can combine useful compression with row-group pruning.
6. Validates Parquet schemas, row counts, chromosome partitions, min/max positions, parse rejection counts, and reference build metadata.
7. Calculates SHA-256 and size for every Parquet object and a deterministic dataset checksum over the canonical ordered file inventory.
8. Uploads the files to an attempt-scoped prefix below the allowed immutable version prefix.
9. Returns the complete uploaded file inventory to the Workflow.

The Activity sends heartbeats for `DOWNLOADING_SOURCE`, `PARSING`, `WRITING_DUCKDB`, `EXPORTING_PARQUET`, `UPLOADING_PARTITION`, and `FINALIZING`. (This design document originally named five phases, two of them under different spellings; the implemented set is these six, frozen in `contracts/ingestion-v1.md`, which governs.) It responds to cancellation and removes the local staging database and Parquet directory during cleanup.

### Parquet layout

The persistent dataset layout is:

```text
datasets/{datasetId}/
├── manifest.json
└── versions/{artifactVersion}/attempt-{attempt}/variants/
    ├── chrom=1/part-000.parquet
    ├── chrom=2/part-000.parquet
    ├── ...
    └── chrom=X/part-000.parquet
```

Partitioning by chromosome enables file pruning. `chrom` is represented by the Hive path segment and omitted from the physical Parquet columns; `read_parquet(..., hive_partitioning = true, hive_types_autocast = 0)` restores it as a logical column — both options are mandatory, because with autocast left on DuckDB infers `chrom`'s type from the partitions a scan happened to touch. See `contracts/ingestion-v1.md`, "Reading the dataset", which governs. The non-partition attempt segment uses `attempt-{attempt}`, not `attempt={attempt}`, so it cannot become an accidental Hive column. Ingestion normalizes `chr1`/`1` forms to the canonical values `1`–`22`, `X`, `Y`, or `MT`; unexpected contigs are rejected or handled by an explicitly versioned policy rather than becoming arbitrary path components. Sorting within a partition by genomic position produces tight Parquet min/max statistics and enables row-group pruning for coordinate queries. The serving query must resolve gene or rsID targets to chromosome, position, reference allele, and alternate allele before scanning user Parquet.

### Publication manifest

The manifest is JSON stored at `datasets/{datasetId}/manifest.json`.

> **This list was the design's intent and is no longer the contract.** The manifest schema is
> frozen and `.strict()` — an unknown field is a rejection, not a warning — so the authoritative
> field list is `DatasetManifestSchema` in
> `ts-api-agent/src/application/ingestion-contracts.ts`, documented in
> `contracts/ingestion-v1.md`. Three fields sketched here are **not** in it: a contract version
> (that field belongs to the activity *input* envelope, not the manifest), the catalog key and
> the source object identity (a manifest describes the artifact, not how it was requested), and a
> creation timestamp (nothing in the serving path reads one, and a manifest that is otherwise a
> pure function of its content would stop being byte-identical across a re-publish of the same
> dataset). What the frozen manifest actually contains:

- `artifactFormat: "parquet-dataset"`, `layoutVersion`, `schemaVersion`, and `schemaFingerprint`;
- `datasetId` and `artifactVersion`;
- `partitionSpec: ['chrom']` and `sortOrder: ['chrom', 'pos', 'ref', 'alt']`;
- `attemptPrefix` — the immutable prefix the objects live under — and `datasetChecksumSha256`;
- `parquetObjects`: an ordered inventory with bucket, key, ETag, nullable version ID, chromosome, SHA-256, byte size, row count, minimum position, and maximum position;
- `variantCount` and `rejectedRecordCount`;
- `referenceBuild` and `referenceVersion`;
- `processorVersion`.

Rust uploads directly to an attempt-unique immutable prefix. `publishDataset` performs bounded `HEAD` verification of the complete inventory, then writes `manifest.json`; it never copies the potentially large Parquet payload a second time. It is idempotent: the same manifest may be written again only when its dataset checksum, file inventory, and source identity match. Query code uses the manifest inventory rather than an S3 glob so it never reads an unpublished or foreign object.

### Dataset-scoped query path

`ParquetDatasetResolver` loads and validates the published manifest, verifies the selected immutable object identities, and constructs explicit Parquet URIs only from trusted bucket/key descriptors. It does not download the complete dataset. Production S3 policy denies overwrite for published version prefixes; ETag, size, and checksum metadata are verified before query.

`DuckDbGenotypeRepository` opens a request-scoped in-memory DuckDB connection, loads `httpfs`, configures S3/MinIO credentials from trusted server configuration, and queries the selected Parquet objects through `read_parquet`. DuckDB may retain its HTTP metadata cache, but there is no full-dataset local cache.

The demo ClinVar reference database remains global and read-only. DuckDB first resolves a requested gene or rsID to assembly-aware `(chrom, pos, ref, alt)` targets using ClinVar, then restricts the remote Parquet scan to matching chromosome partitions and joins on coordinates and alleles. This layout allows S3 range reads, projection pushdown, partition pruning, and row-group pruning instead of downloading the complete genome.

Missing datasets, missing manifests, unexpected object identities, unavailable `httpfs`, and unavailable references are explicit failures; fixture fallback is permitted only in tests. S3 paths come exclusively from validated manifests and are never interpolated from user input. The resolver enforces a 1 MiB manifest limit, at most 128 Parquet files for this MVP, allowed bucket/prefix checks, four DuckDB threads, a 512 MiB memory limit, and a 10-second query deadline with cancellation.

## Cross-Language Contract

The shared payload uses JSON-compatible primitives and camelCase field names. Rust structs use `#[serde(rename_all = "camelCase")]`. Dates, `BigInt`, buffers, class instances, and serialized language-native errors are prohibited.

The primary input contains `datasetId`, `datasetKey`, immutable source identity, artifact version, allowed version-prefix identity, reference build, and reference version. The result contains the successful attempt prefix, ordered uploaded Parquet object inventory, deterministic dataset content checksum, variant counts, reference build, and processor version.

The pure processor first produces local descriptors containing relative path, chromosome, SHA-256, byte size, row count, min/max positions, and schema fingerprint. Only the Temporal Activity/S3 adapter maps them to wire `ParquetObject` descriptors containing required `bucket`, `key`, and `etag`, nullable `versionId`, plus the local statistics. The dataset content checksum is computed from canonical relative descriptors and is therefore independent of an Activity attempt's S3 prefix.

Stable application failure types are:

- `InvalidVcfFormat` — non-retryable;
- `SourceObjectChanged` — non-retryable for the current Workflow input;
- `ObjectStoreUnavailable` — retryable;
- `ArtifactWriteFailed` — retryable;
- `ArtifactValidationFailed` — non-retryable when caused by deterministic content.

## Idempotency and Failure Semantics

Every API start request creates a new `datasetId`; Temporal retries retain that ID. Each Rust Activity attempt writes to its own temporary local DuckDB database, local Parquet directory, and S3 attempt prefix. No retry appends to an existing local database or published Parquet prefix.

The publication manifest is the only readiness signal. Failed or cancelled runs may leave unpublished version/attempt objects, but never a published manifest. Published prefixes are immutable; a changed genome creates a new dataset version/prefix. Orphan-prefix garbage collection is outside the first vertical slice and may be added later.

Temporal timeouts are configured per Activity rather than shared globally. The Rust Activity has a heartbeat timeout and a sufficiently long start-to-close timeout. Invalid VCF failures do not retry. Network and worker-loss failures do retry.

## Temporal UI Outcome

One Workflow execution shows distinct Workers and queues:

```text
inspectDatasetSource     TS    genomic-control-plane
buildDatasetArtifact     Rust  genomic-ingestion-rust
publishDataset           TS    genomic-control-plane
```

The pending Rust Activity displays its Worker identity, attempt, heartbeat phase, processed bytes, processed variants, exported partitions, and uploaded Parquet objects. A worker crash is visible as a failed/timed-out attempt followed by a retry on another Rust Worker.

## API Lifecycle

1. `GET /api/datasets/catalog` lists the two seeded choices.
2. `POST /api/ingestions` accepts `{ "datasetKey": "demo-small" }` and returns `202` with `datasetId`, `workflowId`, and status URL.
3. `GET /api/ingestions/{workflowId}` returns Workflow status and aggregate progress.
4. `POST /ask` accepts `{ "datasetId": "...", "question": "..." }` only after the manifest exists and queries its Parquet files remotely through DuckDB.
5. Reopening a conversation reuses the published `datasetId`; it does not rerun ingestion.

## Testing Strategy

- Contract fixtures are deserialized and serialized in both TypeScript and Rust.
- TypeScript unit tests cover allowlisting, Workflow routing/options, multi-object publication idempotency, manifest resolution, safe file-list construction, and API validation.
- Rust unit tests cover plain/gzip streaming parse, malformed records, bounded batch writes, deterministic chromosome partitioning, Parquet schema/statistics validation, and error mapping.
- A Temporal integration test starts a TS Workflow Worker and Rust Activity Worker against a dev/test server and verifies the Activity Type, task queue, result, and heartbeat.
- An object-store integration test uses MinIO and verifies source ETag pinning, multi-object version-prefix upload, manifest-last publication, per-file checksums, dataset checksum, and retry idempotency.
- A remote-query integration test proves through DuckDB profiling or request accounting that a chromosome/position query does not download the complete dataset and does not scan unrelated chromosome partitions.
- The final E2E test ingests `demo-small`, waits for completion, asks a known genotype question using the resulting `datasetId`, and verifies that the answer came from that manifest and Parquet dataset.

## Scope Boundaries

The first implementation does not include arbitrary uploads, authentication, multi-tenant authorization, full ClinVar normalization, clinical-grade allele/haplotype interpretation, a general lakehouse table format such as Iceberg/Delta, automatic orphan-prefix garbage collection, or production rollout of a Public Preview Rust SDK.

Qdrant remains a global literature repository. Improving PubMed abstract quality is useful but independent and must not block the ingestion vertical slice.

## Delivery Milestones

1. **Core contracts and truthfulness:** catalog, explicit dataset lifecycle, no global/fallback query behavior, and reliable unit tests.
2. **Real cross-language execution:** activity-only Rust Temporal Worker, S3-based contract, heartbeats, cancellation, local DuckDB processing, and immutable partitioned Parquet output.
3. **Published serving path:** manifest-last publication, remote DuckDB-over-Parquet queries, dataset-scoped repository, Docker wiring, and E2E demonstration.

The recommended interview-ready stopping point is completion of milestone 3. Full ClinVar and PubMed improvements are subsequent projects.
