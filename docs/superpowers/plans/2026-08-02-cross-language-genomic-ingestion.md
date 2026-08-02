# Cross-Language Genomic Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a truthful end-to-end path in which a TypeScript Temporal Workflow ingests an allowlisted S3 VCF through a real Rust Temporal Activity Worker, publishes immutable chromosome-partitioned Parquet in S3, and queries only the required remote partitions and row groups through DuckDB from the agent API.

**Architecture:** TypeScript owns the control plane and schedules a Rust-only Activity queue through Temporal. Rust streams the selected S3 object into an attempt-local DuckDB staging database, validates it, exports rows sorted by genomic coordinates to partitioned Parquet, and uploads directly to an attempt-unique immutable version prefix; TypeScript verifies the complete inventory and writes the manifest last without copying the payload again. Query requests resolve the manifest by `datasetId`, use the global reference database to resolve gene/rsID to coordinates, and run request-scoped in-memory DuckDB queries against the selected Parquet objects through S3 range reads.

**Tech Stack:** Node.js 25, TypeScript 5.7, Hono, Temporal TypeScript SDK 1.11, Rust 2021, Temporal Rust SDK 0.5.0 Public Preview, Tokio, Rayon, DuckDB with Parquet/httpfs, AWS SDK for JavaScript/Rust, MinIO/S3, Qdrant, Node test runner.

## Global Constraints

- Arbitrary uploads and arbitrary URL/path ingestion remain out of scope; only `demo-small` and `na12878-full` are accepted.
- Cross-worker payloads contain JSON-compatible primitives only and use camelCase field names.
- `genomic-control-plane` is the TypeScript Workflow/Activity queue; `genomic-ingestion-rust` is activity-only.
- S3 object references, not local paths or shared volumes, cross the language boundary.
- `temporalio-sdk`, `temporalio-client`, `temporalio-common`, and `temporalio-macros` are pinned to `0.5.0`; the Public Preview status is documented.
- Published Parquet objects are immutable and queryable only when a matching manifest exists.
- Rust retries never append to an existing local DuckDB staging database or published Parquet prefix.
- Parquet is partitioned by chromosome and sorted within each partition by `(pos, ref, alt)` with approximately 100,000 rows per row group.
- `/ask` reads the explicit Parquet inventory from the manifest; it never builds an S3 path from user input and never globs an unpublished prefix.
- The serving path must not download the complete user dataset; DuckDB uses `httpfs`, projection pushdown, partition pruning, and row-group pruning.
- Gene/rsID targets are resolved through a versioned ClinVar reference to `(referenceBuild, chrom, pos, ref, alt)` before remote scan; build mismatch or unresolved targets never trigger a full scan.
- Qdrant stores global literature only; no user genotype is written to Qdrant.
- Fixture fallback is allowed in automated tests only and is never silent in runtime paths.

---

## File Structure

### Shared contract and fixtures

- `contracts/ingestion-v1.md` — canonical wire names, payload examples, failure taxonomy.
- `contracts/fixtures/build-dataset-artifact.input.json` — golden input consumed by TS and Rust tests.
- `contracts/fixtures/build-dataset-artifact.result.json` — golden output consumed by TS and Rust tests.
- `contracts/fixtures/dataset-manifest.json` — golden published manifest with an ordered Parquet object inventory.

### TypeScript control plane

- `ts-api-agent/src/domain/datasets.ts` — domain types and state names.
- `ts-api-agent/src/application/dataset-catalog.ts` — allowlisted seeded datasets.
- `ts-api-agent/src/application/ingestion-contracts.ts` — Zod wire schemas.
- `ts-api-agent/src/application/workflows.ts` — deterministic orchestration and progress query.
- `ts-api-agent/src/application/control-plane-activities.ts` — S3 inspect and manifest-last publish activities.
- `ts-api-agent/src/application/worker.ts` — TS Workflow plus control-plane Activity Worker.
- `ts-api-agent/src/infrastructure/object-store/s3-object-store.ts` — S3 adapter.
- `ts-api-agent/src/infrastructure/database/parquet-dataset-resolver.ts` — manifest validation and safe explicit Parquet URI selection.
- `ts-api-agent/src/infrastructure/database/duckdb.ts` — request-scoped in-memory DuckDB-over-Parquet repository.
- `ts-api-agent/src/infrastructure/ai/tools.ts` — request-scoped genotype tool.
- `ts-api-agent/src/infrastructure/ai/agent.ts` — receives `datasetId`/repository dependencies.
- `ts-api-agent/src/index.ts` — catalog, ingestion/status, and dataset-scoped ask endpoints.

### Rust data plane

- `rust-ingestion-worker/src/contracts.rs` — serde wire structs and stable failure names.
- `rust-ingestion-worker/src/object_store.rs` — S3/MinIO download and upload adapter.
- `rust-ingestion-worker/src/vcf.rs` — streaming plain/gzip VCF reader and record parser.
- `rust-ingestion-worker/src/artifact.rs` — attempt-local DuckDB staging, sorted Parquet export, and validator.
- `rust-ingestion-worker/src/temporal_activities.rs` — thin Activity wrapper, heartbeat, cancellation, failure mapping.
- `rust-ingestion-worker/src/bin/temporal_worker.rs` — activity-only Worker bootstrap.
- `rust-ingestion-worker/src/main.rs` — retain only explicit CLI/debug behavior; do not masquerade as a Temporal worker.

### Integration and operations

- `tests/integration/cross_language_ingestion.test.ts` — TS Workflow to Rust Activity proof.
- `tests/integration/dataset_isolation.test.ts` — two Parquet datasets produce isolated query results.
- `tests/integration/remote_parquet_pruning.test.ts` — unrelated chromosome partitions and complete-dataset downloads are not used by a targeted query.
- `docker-compose.yml` — local Temporal, MinIO, Qdrant, TS Worker, and Rust Worker topology.
- `docker-compose.prod.yml` — same worker separation without runtime Cargo compilation.
- `rust-ingestion-worker/Dockerfile` — multi-stage release image.
- `ts-api-agent/Dockerfile` — TS API/control-plane image.
- `scripts/seed_demo_s3.sh` — idempotent seeded-object bootstrap.

---

### Task 1: Feasibility Gates — Cross-Language Temporal and Remote Parquet

**Files:**
- Modify: `rust-ingestion-worker/Cargo.toml`
- Create: `rust-ingestion-worker/src/bin/temporal_probe_worker.rs`
- Create: `ts-api-agent/src/application/temporal_probe_workflow.ts`
- Create: `ts-api-agent/src/application/temporal_probe_worker.ts`
- Create: `tests/integration/temporal_rust_probe.test.ts`
- Create: `tests/integration/remote_parquet_probe.test.ts`
- Modify: `ts-api-agent/package.json`

**Interfaces:**
- Consumes: Temporal at `TEMPORAL_ADDRESS`, namespace `default`.
- Consumes: authenticated MinIO at `S3_ENDPOINT` through DuckDB `httpfs`.
- Produces: Activity Type `rustActivityProbe` on `genomic-ingestion-rust`; payload `{ message: string, iterations: number }`; result `{ echoed: string, workerLanguage: "rust" }`.
- Proves: an in-memory Node/DuckDB session can query an explicit S3 Parquet URI with `chrom` and `pos` predicates without downloading a complete dataset or reading an unrelated chromosome object.

- [ ] **Step 1: Add the pinned Public Preview Rust SDK dependencies**

Add these dependencies and commit the resulting `Cargo.lock` changes:

```toml
temporalio-sdk = "=0.5.0"
temporalio-client = "=0.5.0"
temporalio-common = "=0.5.0"
temporalio-macros = "=0.5.0"
```

- [ ] **Step 2: Write the Rust contract serialization test first**

In the probe worker test module, deserialize the exact TS payload and assert `message == "hello"` and `iterations == 20`; serialize the result and assert the JSON field is `workerLanguage`, not `worker_language`.

```rust
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProbeInput { message: String, iterations: u32 }

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProbeResult { echoed: String, worker_language: String }
```

- [ ] **Step 3: Run the Rust test and verify the missing implementation fails**

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml temporal_probe_contract`

Expected: FAIL because the probe types/serializer are not implemented.

- [ ] **Step 4: Implement the activity-only Rust probe Worker**

Register `rustActivityProbe` on `genomic-ingestion-rust`. The Activity loops every 100 ms, heartbeats `{ "iteration": n }`, checks cancellation before continuing, and returns the result above. Set the Worker identity to a value beginning with `rust-ingestion-worker@`.

- [ ] **Step 5: Implement the isolated TS probe Workflow**

Schedule the external Activity by name with:

```ts
const rustProbe = proxyActivities<{
  rustActivityProbe(input: { message: string; iterations: number }): Promise<{
    echoed: string;
    workerLanguage: 'rust';
  }>;
}>({
  taskQueue: 'genomic-ingestion-rust',
  startToCloseTimeout: '30 seconds',
  heartbeatTimeout: '1 second',
  retry: { maximumAttempts: 2 },
});
```

- [ ] **Step 6: Write the integration test before wiring production ingestion**

The test starts the TS Workflow Worker and Rust probe Worker, executes `temporalRustProbeWorkflow({ message: "hello", iterations: 20 })`, asserts the returned language, then starts a second execution and cancels it. Query Temporal history and assert Activity Type `rustActivityProbe` used task queue `genomic-ingestion-rust`.

- [ ] **Step 7: Run the Temporal feasibility gate**

Run: `node --test tests/integration/temporal_rust_probe.test.ts`

Expected: PASS; the Temporal UI pending Activity shows a Rust worker identity and heartbeat detail.

- [ ] **Step 8: Write the remote Parquet feasibility test**

Create a chromosome-1 object plus a chromosome-12 Zstandard Parquet object containing at least 300,000 ordered rows in three approximately 100,000-row groups. Put target and non-target chromosome-12 positions in different row groups. Open DuckDB in memory through the current Node binding, load `httpfs`, configure path-style MinIO credentials from test environment, and query only the manifest-selected chromosome-12 URI:

```sql
SELECT rsid, gt_raw
FROM read_parquet(
  ['s3://probe/variants/chrom=12/part-000.parquet'],
  hive_partitioning = true
)
WHERE chrom = '12' AND pos = 21178615;
```

Assert the result, capture `EXPLAIN ANALYZE`, and use MinIO request logs or an instrumented S3 proxy to prove no GET/range request targets the chromosome-1 object after counters are reset. For chromosome 12, distinguish footer reads from data reads and assert bytes read are materially below the full object size and exclude nonmatching row-group data. Repeat with both URIs to document the cost of passing an unpruned file list; application-side manifest pruning remains mandatory.

- [ ] **Step 9: Run the remote-query feasibility gate**

Run: `node --test tests/integration/remote_parquet_probe.test.ts`

Expected: PASS without downloading the complete chromosome-12 object and without any read from the unrelated chromosome partition. Record the DuckDB binding version, loaded `httpfs` extension version, selected files, footer/data byte ranges, total bytes read, object sizes, and query profile in the test report.

- [ ] **Step 10: Apply both gate decisions**

Proceed to Task 2 only when payload compatibility, heartbeat, cancellation, retry, authenticated MinIO Parquet access, explicit-file selection, and targeted remote reads all pass. If Temporal behavior is unsupported by SDK 0.5.0, stop this plan and retain the production fallback `TS Activity -> spawn(shell: false) -> Rust processor`; do not build a partial custom Temporal Core integration. If the current Node DuckDB binding cannot reliably query remote Parquet, stop and select a supported binding/version before freezing contracts.

- [ ] **Step 11: Commit both gates**

```bash
git add rust-ingestion-worker/Cargo.toml Cargo.lock rust-ingestion-worker/src/bin/temporal_probe_worker.rs ts-api-agent/src/application/temporal_probe_workflow.ts ts-api-agent/src/application/temporal_probe_worker.ts ts-api-agent/package.json tests/integration/temporal_rust_probe.test.ts tests/integration/remote_parquet_probe.test.ts
git commit -m "spike: prove Rust activities and remote Parquet queries"
```

### Task 2: Freeze Cross-Language Contracts and Seeded Dataset Catalog

**Files:**
- Create: `contracts/ingestion-v1.md`
- Create: `contracts/fixtures/build-dataset-artifact.input.json`
- Create: `contracts/fixtures/build-dataset-artifact.result.json`
- Create: `contracts/fixtures/dataset-manifest.json`
- Create: `ts-api-agent/src/domain/datasets.ts`
- Create: `ts-api-agent/src/application/ingestion-contracts.ts`
- Create: `ts-api-agent/src/application/dataset-catalog.ts`
- Create: `ts-api-agent/src/application/ingestion-contracts.test.ts`
- Create: `rust-ingestion-worker/src/contracts.rs`
- Modify: `rust-ingestion-worker/src/lib.rs`

**Interfaces:**
- Produces: `DatasetKey`, `S3ObjectRef`, `BuildDatasetArtifactInput`, `BuildDatasetArtifactResult`, `DatasetManifest`, and `IngestionHeartbeat` in both languages.
- Produces: `datasetCatalog.get("demo-small" | "na12878-full")` and `datasetCatalog.list()`.

- [ ] **Step 1: Add golden JSON fixtures**

Use this exact input shape:

```json
{
  "contractVersion": 1,
  "datasetId": "ds-test-001",
  "datasetKey": "demo-small",
  "source": {
    "bucket": "genomic-data",
    "key": "samples/demo_user.vcf",
    "etag": "fixture-etag",
    "versionId": null,
    "contentLength": 1024
  },
  "reference": {
    "build": "GRCh38",
    "version": "demo-clinvar-grch38-v1"
  },
  "target": {
    "bucket": "genomic-artifacts",
    "artifactVersion": "iv-test-001",
    "allowedPrefix": "datasets/ds-test-001/versions/iv-test-001/"
  }
}
```

The result fixture contains `attemptPrefix`, `datasetChecksumSha256`, `variantCount`, `rejectedRecordCount`, `referenceBuild`, `processorVersion`, and an ordered `parquetObjects` array. Each object contains required `bucket`, `key`, and `etag`, nullable `versionId`, `chrom`, `checksumSha256`, `byteSize`, `rowCount`, `minPos`, and `maxPos`.

The manifest fixture additionally fixes `artifactFormat: "parquet-dataset"`, `layoutVersion: 1`, `schemaVersion: 1`, `schemaFingerprint`, `artifactVersion`, `referenceVersion`, `partitionSpec: ["chrom"]`, and `sortOrder: ["chrom", "pos", "ref", "alt"]`. Contract tests reject noncanonical file ordering, duplicate keys, keys outside the allowed dataset/version prefix, mismatched chromosome partition values, and a dataset checksum that does not match the canonical descriptor list.

- [ ] **Step 2: Write failing TypeScript contract and catalog tests**

Cover valid golden files, rejection of extra fields, unknown contract versions, an unknown dataset key, and exact mappings:

```ts
assert.deepEqual(datasetCatalog.get('demo-small').source, {
  bucket: 'genomic-data',
  key: 'samples/demo_user.vcf',
});
assert.throws(() => datasetCatalog.get('s3://attacker/file.vcf'));
```

- [ ] **Step 3: Run TypeScript tests and observe failure**

Run: `node --test ts-api-agent/src/application/ingestion-contracts.test.ts`

Expected: FAIL because the schemas and catalog do not exist.

- [ ] **Step 4: Implement strict Zod schemas and catalog**

Define `DatasetKeySchema = z.enum(['demo-small', 'na12878-full'])`. Use `.strict()` on wire objects. Keep display names separate from S3 identity so API input cannot override bucket or key.
Each catalog entry also fixes `expectedReferenceBuild: 'GRCh38'` and `referenceVersion: 'demo-clinvar-grch38-v1'`; source headers that contradict the expected build fail ingestion.

- [ ] **Step 5: Write failing Rust golden-fixture tests**

Deserialize the same files with `#[serde(rename_all = "camelCase", deny_unknown_fields)]`, round-trip them, and assert both variants of `DatasetKey` serialize to the TS strings.

- [ ] **Step 6: Implement Rust contract structs and stable error enum**

Define stable error names exactly:

```rust
pub enum FailureType {
    InvalidVcfFormat,
    SourceObjectChanged,
    ObjectStoreUnavailable,
    ArtifactWriteFailed,
    ArtifactValidationFailed,
}
```

- [ ] **Step 7: Run both contract suites**

Run: `node --test ts-api-agent/src/application/ingestion-contracts.test.ts`

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml contracts`

Expected: PASS.

- [ ] **Step 8: Commit the frozen contract**

```bash
git add contracts ts-api-agent/src/domain/datasets.ts ts-api-agent/src/application/ingestion-contracts.ts ts-api-agent/src/application/ingestion-contracts.test.ts ts-api-agent/src/application/dataset-catalog.ts rust-ingestion-worker/src/contracts.rs rust-ingestion-worker/src/lib.rs
git commit -m "feat: define versioned ingestion contracts"
```

### Task 3: Build and Test the Pure Rust Parquet Dataset Processor

**Files:**
- Create: `rust-ingestion-worker/src/vcf.rs`
- Create: `rust-ingestion-worker/src/artifact.rs`
- Create: `rust-ingestion-worker/tests/artifact_builder_test.rs`
- Modify: `rust-ingestion-worker/src/lib.rs`
- Modify: `rust-ingestion-worker/src/models.rs`
- Modify: `rust-ingestion-worker/Cargo.toml`
- Delete after migration: `rust-ingestion-worker/src/activities/mod.rs`

**Interfaces:**
- Consumes: `ArtifactBuildRequest { source_path, staging_db_path, parquet_output_dir, dataset_id, source_etag }`.
- Produces: `ArtifactStats { dataset_checksum_sha256, local_parquet_files, variant_count, rejected_record_count, reference_build }`, where each local descriptor has `relative_path`, `chrom`, checksum, size, row count, min/max positions, and schema fingerprint but no S3 key or ETag.
- Produces progress through a `ProgressSink` trait independent of Temporal.

- [ ] **Step 1: Add processor dependencies**

Add `sha2 = "=0.11.0"`, `hex = "=0.4.3"`, and `tempfile = "=3.27.0"`; retain `flate2`, `duckdb`, `rayon`, and `tokio`. Do not add Temporal imports to `vcf.rs` or `artifact.rs`.

- [ ] **Step 2: Write failing parser tests**

Test the existing `demo_user.vcf`, a gzipped copy created in a test temp directory, header-only input, a missing `GT` field, invalid POS, phased `0|1`, and a multiallelic `1/2` record. Assert malformed records increment `rejectedRecordCount` instead of panicking. Test canonical chromosome normalization and reject an unsafe/unexpected contig before it can become a partition path.

- [ ] **Step 3: Write the bounded-memory acceptance test**

Generate 100,000 VCF records through a buffered writer, run the processor with `batch_size = 1_000`, and use an instrumented `ProgressSink` to assert no reported in-memory batch exceeds 1,000 records. Assert the exact Parquet column types/nullability, Zstandard compression, chromosome directories, position order, row-group size near 100,000, record conservation across files, and canonical file-descriptor ordering.

- [ ] **Step 4: Run tests and observe the current all-in-memory design fail**

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml --test artifact_builder_test`

Expected: FAIL because the streaming reader, progress trait, staging tables, sorted Parquet export, and overwrite-safe builder do not exist.

- [ ] **Step 5: Implement plain/gzip streaming input**

Detect gzip using the first two bytes `0x1f, 0x8b`, wrap the reader in `MultiGzDecoder` when needed, and yield parsed records incrementally. Do not accept `Vec<String>` as the processing API.

- [ ] **Step 6: Implement attempt-local DuckDB construction**

Open only a new output path. Fail if the path exists. Create:

```sql
CREATE TABLE user_variants (
  chrom VARCHAR NOT NULL,
  pos UINTEGER NOT NULL,
  rsid VARCHAR,
  ref VARCHAR NOT NULL,
  alt VARCHAR NOT NULL,
  gt_raw VARCHAR NOT NULL
);
CREATE TABLE dataset_metadata (
  dataset_id VARCHAR NOT NULL,
  source_etag VARCHAR NOT NULL,
  reference_build VARCHAR NOT NULL,
  variant_count UBIGINT NOT NULL,
  rejected_record_count UBIGINT NOT NULL,
  processor_version VARCHAR NOT NULL
);
```

Append bounded batches and commit metadata after parsing. Use the local DuckDB only as a staging/processing engine. Export with the equivalent of:

```sql
COPY (
  SELECT chrom, pos, rsid, ref, alt, gt_raw
  FROM user_variants
  ORDER BY chrom, pos, ref, alt
)
TO '<attempt-parquet-directory>' (
  FORMAT PARQUET,
  PARTITION_BY (chrom),
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE 100000
);
```

Close the staging database, enumerate every generated Parquet file, validate its schema and statistics through DuckDB, and calculate per-file SHA-256 plus a deterministic dataset content checksum over local descriptors sorted by `(chrom, relativePath)`. S3 keys, ETags, and version IDs do not exist at this layer and are added only after upload in Task 5.

The physical file schema contains `pos`, `rsid`, `ref`, `alt`, and `gt_raw`; `chrom` is encoded by `chrom=<value>` directories and restored as a logical column only through `read_parquet(..., hive_partitioning = true)`. Attempt/version directory segments must not contain `=`.

- [ ] **Step 7: Replace fake Temporal context with `ProgressSink`**

Provide `NoopProgressSink` for CLI/tests. Delete the custom `ActivityContext` whose heartbeat only logs JSON. Temporal adaptation belongs in Task 5.

- [ ] **Step 8: Run processor tests**

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml artifact_builder`

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml vcf`

Expected: PASS, including gzip, bounded-batch, chromosome partitioning, position ordering, row-group sizing, and deterministic inventory cases.

- [ ] **Step 9: Commit the pure data plane**

```bash
git add rust-ingestion-worker/Cargo.toml Cargo.lock rust-ingestion-worker/src rust-ingestion-worker/tests/artifact_builder_test.rs
git commit -m "feat: stream VCF into partitioned Parquet datasets"
```

### Task 4: Implement S3 Adapters and Manifest-Last Publication

**Files:**
- Modify: `ts-api-agent/package.json`
- Create: `ts-api-agent/src/infrastructure/object-store/object-store.ts`
- Create: `ts-api-agent/src/infrastructure/object-store/s3-object-store.ts`
- Create: `ts-api-agent/src/application/control-plane-activities.ts`
- Create: `ts-api-agent/src/application/control-plane-activities.test.ts`
- Create: `rust-ingestion-worker/src/object_store.rs`
- Create: `rust-ingestion-worker/tests/minio_object_store_test.rs`
- Modify: `rust-ingestion-worker/Cargo.toml`
- Modify: `scripts/seed_demo_s3.sh`

**Interfaces:**
- Produces TS `ObjectStore` methods `head`, `headMany`, `putJsonConditional`, `getJson`, `listPrefix`, and `downloadToFile`.
- Produces `inspectDatasetSource(datasetId, datasetKey)` and `publishDataset(input, result)`.
- Produces Rust `ObjectStore` methods `download_exact` and `upload_file`.

- [ ] **Step 1: Add S3 dependencies**

Add `@aws-sdk/client-s3` to TypeScript. Add `aws-config = "=1.10.0"` and `aws-sdk-s3 = "=1.140.0"` to Rust after confirming `rustc --version` is at least 1.94.1; if it is older, update the project toolchain explicitly before compiling rather than silently selecting a different dependency set.

- [ ] **Step 2: Write fake-object-store tests for source inspection**

Assert that catalog mapping controls bucket/key, the returned input includes ETag, nullable S3 version ID, and content length, missing ETag fails, and the caller cannot inject a URL or alternate bucket.

- [ ] **Step 3: Write publication idempotency tests**

Use an in-memory `ObjectStore` fake and verify call order:

```text
HEAD every declared Parquet object below the successful immutable attempt prefix
VERIFY ETag/version, size, checksum metadata, partition, and canonical order
CONDITIONAL PUT datasets/{datasetId}/manifest.json
```

Assert objects are verified with bounded concurrency in canonical manifest order and no Parquet copy operation occurs. A second identical call succeeds without changing identity; a conflicting existing manifest raises `DatasetPublicationConflict`; no manifest is written when any HEAD, size, checksum metadata, prefix, or inventory verification fails. Queryability begins only after the final conditional manifest write.

- [ ] **Step 4: Implement TypeScript S3 adapter and activities**

Configure endpoint, region, credentials, and path-style mode from explicit environment variables. Never form public HTTP URLs from `s3://` strings. Escape no SQL or shell content in this adapter.

- [ ] **Step 5: Write Rust MinIO tests**

Test exact ETag matching, wrong ETag rejection as `SourceObjectChanged`, streaming download to a temp file, uploading multiple Parquet files with SHA-256 metadata, and retrying an identical attempt prefix.

- [ ] **Step 6: Implement Rust S3 adapter**

Use the AWS SDK client configured for MinIO path-style access. Verify source ETag/version before and after download. Derive an `attempt-{attempt}` prefix below the exact allowed version prefix from Temporal Activity metadata and upload only the locally enumerated Parquet inventory there; never accept an arbitrary destination key from VCF content. Configure production bucket policy to deny overwrite of published version-prefix objects; listing is for cleanup/audit only and never drives query selection.

- [ ] **Step 7: Make demo seeding idempotent and explicit**

Seed exactly:

```text
s3://genomic-data/samples/demo_user.vcf
s3://genomic-data/samples/na12878_hg001.vcf.gz
```

Skip an existing object only when its checksum matches. Keep ClinVar bootstrap separate.

- [ ] **Step 8: Run adapter tests**

Run: `node --test ts-api-agent/src/application/control-plane-activities.test.ts`

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml --test minio_object_store_test -- --ignored`

Expected: unit tests PASS; MinIO integration test PASS when the compose dependency is running.

- [ ] **Step 9: Commit storage boundaries**

```bash
git add ts-api-agent/package.json package-lock.json ts-api-agent/src/infrastructure/object-store ts-api-agent/src/application/control-plane-activities.ts ts-api-agent/src/application/control-plane-activities.test.ts rust-ingestion-worker/Cargo.toml Cargo.lock rust-ingestion-worker/src/object_store.rs rust-ingestion-worker/tests/minio_object_store_test.rs scripts/seed_demo_s3.sh
git commit -m "feat: add versioned S3 artifact publication"
```

### Task 5: Wrap the Processor in the Real Rust Temporal Activity Worker

**Files:**
- Create: `rust-ingestion-worker/src/temporal_activities.rs`
- Create: `rust-ingestion-worker/src/bin/temporal_worker.rs`
- Create: `rust-ingestion-worker/tests/temporal_activity_test.rs`
- Modify: `rust-ingestion-worker/src/lib.rs`
- Remove: `rust-ingestion-worker/src/bin/temporal_probe_worker.rs`
- Remove: `ts-api-agent/src/application/temporal_probe_workflow.ts`
- Remove: `ts-api-agent/src/application/temporal_probe_worker.ts`

**Interfaces:**
- Consumes: `BuildDatasetArtifactInput` from Task 2.
- Produces: Activity Type `buildDatasetArtifact` and `BuildDatasetArtifactResult`.

- [ ] **Step 1: Write Activity failure-mapping tests**

Assert `InvalidVcfFormat`, `SourceObjectChanged`, and deterministic `ArtifactValidationFailed` become non-retryable Temporal application failures with those exact type names. Assert object-store connection failures and temporary disk failures remain retryable.

- [ ] **Step 2: Write heartbeat adapter tests**

Drive a fake processor through all phases and assert heartbeat payloads follow:

```json
{
  "phase": "PARSING",
  "processedBytes": 4096,
  "processedVariants": 2500,
  "currentPartition": "12",
  "completedFiles": 3,
  "uploadedBytes": 1048576
}
```

- [ ] **Step 3: Implement the thin Temporal Activity**

The wrapper creates attempt-scoped local DuckDB/Parquet paths and derives an `attempt-{attempt}` S3 prefix below the contract's allowed immutable version prefix using Workflow ID, Activity ID, and attempt number; constructs the S3 adapter; calls the pure processor; maps local descriptors to object keys, uploads them with bounded concurrency, records returned ETags/version IDs, and returns the complete wire result. It recomputes/validates the dataset content checksum from canonical relative descriptors and never mixes descriptors from different attempts. It never writes the publication manifest.

- [ ] **Step 4: Implement cancellation-aware progress**

At every heartbeat boundary, check Activity cancellation and stop reading/writing before cleanup. Use phases `DOWNLOADING_SOURCE`, `PARSING`, `WRITING_DUCKDB`, `EXPORTING_PARQUET`, `UPLOADING_PARTITION`, and `FINALIZING`. Never delete source objects, published objects, or another Activity attempt's prefix during cancellation.

- [ ] **Step 5: Implement activity-only Worker bootstrap**

Connect using `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE`, register `buildDatasetArtifact`, configure `WorkerTaskTypes::activity_only()`, task queue `genomic-ingestion-rust`, and identity prefix `rust-ingestion-worker@`.

- [ ] **Step 6: Run Rust Activity tests**

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml temporal_activity`

Expected: PASS.

- [ ] **Step 7: Re-run the cross-language probe using the production Activity Type**

Replace the probe integration payload with the golden `buildDatasetArtifact` fixture backed by MinIO and assert the returned dataset checksum, complete multi-file inventory, per-file checksums/statistics, variant count, heartbeat presence, and Rust worker identity.

Run: `node --test tests/integration/temporal_rust_probe.test.ts`

Expected: PASS.

- [ ] **Step 8: Commit the Rust Activity Worker**

```bash
git add rust-ingestion-worker/src rust-ingestion-worker/tests tests/integration/temporal_rust_probe.test.ts ts-api-agent/src/application
git commit -m "feat: run ingestion as a Rust Temporal activity"
```

### Task 6: Replace the Simulated Workflow with Explicit Cross-Queue Orchestration

**Files:**
- Modify: `ts-api-agent/src/application/workflows.ts`
- Modify: `ts-api-agent/src/application/worker.ts`
- Create: `ts-api-agent/src/application/workflows.test.ts`
- Delete: `ts-api-agent/src/application/activities.ts`
- Modify: `ts-api-agent/src/application/trigger_workflow.ts`

**Interfaces:**
- Consumes: `inspectDatasetSource`, external `buildDatasetArtifact`, `publishDataset`.
- Produces: `GenomicIngestionWorkflow(input): Promise<DatasetManifest>` and query `getProgress`.

- [ ] **Step 1: Write Workflow tests with mocked activities**

Cover the exact state sequence `RESOLVING -> BUILDING -> VERIFYING_OBJECTS -> PUBLISHING_MANIFEST -> COMPLETED`, Rust task queue selection, complete ordered file-inventory verification, nonretryable invalid VCF behavior, retryable object-store failure, cancellation, and the invariant that `publishDataset` is never scheduled after build or object verification failure.

- [ ] **Step 2: Run tests and observe failure against the current Workflow**

Run: `node --test ts-api-agent/src/application/workflows.test.ts`

Expected: FAIL because the Workflow still passes `userId` and local `fileKey` to TS activities with shared timeout/retry settings.

- [ ] **Step 3: Define separate Activity proxies**

Use `genomic-control-plane` for short S3 operations and `genomic-ingestion-rust` for the long processor:

```ts
const rust = proxyActivities<RustActivities>({
  taskQueue: 'genomic-ingestion-rust',
  scheduleToCloseTimeout: '45 minutes',
  startToCloseTimeout: '30 minutes',
  heartbeatTimeout: '15 seconds',
  retry: {
    maximumAttempts: 3,
    nonRetryableErrorTypes: [
      'InvalidVcfFormat',
      'SourceObjectChanged',
      'ArtifactValidationFailed',
    ],
  },
});
```

- [ ] **Step 4: Implement deterministic Workflow state**

The Workflow receives a complete serializable input containing IDs and catalog key. It performs no filesystem, S3, UUID, date, DuckDB, or network operations directly. Return the published Parquet dataset manifest as the Workflow result. Cancellation before manifest publication may leave only an orphan version/attempt prefix, which remains unqueryable.

- [ ] **Step 5: Restrict the TS Worker to control-plane code**

Register Workflows plus `inspectDatasetSource` and `publishDataset`. Do not register `buildDatasetArtifact` in TypeScript and do not import Rust process-launching code.

- [ ] **Step 6: Delete shell execution and false validation paths**

Remove `execAsync`, Cargo path guessing, fallback fixture ingestion, and `validateDataset` existence checks by deleting the obsolete activities module after all references are migrated.

- [ ] **Step 7: Run Workflow and build tests**

Run: `node --test ts-api-agent/src/application/workflows.test.ts`

Run: `npm run build --workspace=ts-api-agent`

Expected: PASS.

- [ ] **Step 8: Commit Workflow orchestration**

```bash
git add ts-api-agent/src/application
git commit -m "refactor: orchestrate Rust ingestion from TypeScript"
```

### Task 7: Query Published Parquet Remotely Through Dataset-Scoped DuckDB

**Files:**
- Create: `ts-api-agent/src/infrastructure/database/parquet-dataset-resolver.ts`
- Create: `ts-api-agent/src/infrastructure/database/parquet-dataset-resolver.test.ts`
- Create: `ts-api-agent/src/infrastructure/database/clinvar-coordinate-resolver.ts`
- Create: `ts-api-agent/src/infrastructure/database/clinvar-coordinate-resolver.test.ts`
- Create: `ts-api-agent/src/infrastructure/database/reference-bootstrap.ts`
- Create: `ts-api-agent/src/infrastructure/database/duckdb-session-factory.ts`
- Create: `ts-api-agent/src/infrastructure/database/duckdb-session-factory.test.ts`
- Modify: `ts-api-agent/src/infrastructure/database/duckdb.ts`
- Create: `ts-api-agent/src/infrastructure/database/duckdb.test.ts`
- Modify: `ts-api-agent/src/infrastructure/ai/tools.ts`
- Modify: `ts-api-agent/src/infrastructure/ai/agent.ts`
- Create: `ts-api-agent/src/infrastructure/ai/tools.test.ts`
- Create: `tests/integration/remote_parquet_pruning.test.ts`
- Create: `tests/fixtures/clinvar_coordinates_grch38.tsv`
- Modify: `scripts/generate_clinical_benchmark_vcf.ts`

**Interfaces:**
- Produces: `ParquetDatasetResolver.resolve(datasetId): Promise<ResolvedParquetDataset>`.
- Produces: `ClinVarCoordinateResolver.resolve(targetId, referenceBuild): Promise<VariantTarget[]>` where each target contains `chrom`, `pos`, `ref`, `alt`, `rsid`, and clinical metadata.
- Produces: `DuckDbSessionFactory.open(): Promise<DuckDbSession>` configured for authenticated S3 reads.
- Produces: `GenotypeRepositoryFactory.open(datasetId): Promise<GenotypeRepository>`.
- Produces: `createQueryGenotypeTool(repository)`; no global user-data repository import.

- [ ] **Step 1: Write manifest resolver tests**

Use a fake object store. Assert missing/unpublished manifests fail; more than 128 files or a manifest larger than 1 MiB fails; unknown layout/schema versions fail; unordered or duplicate descriptors fail; bucket/key descriptors outside `genomic-artifacts/datasets/{datasetId}/` fail; partition values must match `chrom=<value>` paths; and the canonical descriptor inventory reproduces `datasetChecksumSha256`. Assert an unknown dataset causes no Parquet S3 request.

- [ ] **Step 2: Write coordinate resolver tests**

Generate a deterministic reference fixture with columns `reference_version`, `reference_build`, `chrom`, `pos`, `rsid`, `ref`, `alt`, `gene`, `phenotype`, `clinical_significance`, and `evidence_note`. Cover gene and rsID resolution to exact GRCh38 `(chrom, pos, ref, alt)` targets, chromosome normalization (`chr12` to `12`), a `ReferenceBuildMismatch`, and an unknown target returning `TargetNotResolvable`. Explicitly assert an unresolved target cannot fall back to scanning all Parquet files. Liftover and complete indel left-normalization remain outside scope.

- [ ] **Step 3: Write remote query and two-dataset isolation tests first**

Create two chromosome-partitioned Parquet datasets in MinIO with opposing `rs762551` genotypes and valid manifests. Query both repositories and assert different results. For a chromosome-12 target, assert only manifest files whose `chrom == "12"` and `minPos <= target.pos <= maxPos` are passed to `read_parquet`; use request accounting to assert no data read from chromosome 1 and no complete-dataset download.

- [ ] **Step 4: Run tests and observe the current singleton/fallback design fail**

Run: `node --test ts-api-agent/src/infrastructure/database/*.test.ts tests/integration/remote_parquet_pruning.test.ts`

Expected: FAIL because the current singleton uses `genomic_data.duckdb`, has no manifest/coordinate resolver, and silently falls back to fixtures.

- [ ] **Step 5: Implement strict manifest resolution and candidate-file selection**

Validate the manifest before building any SQL. Select candidates in application code by chromosome and min/max position. Return an explicit immutable URI list; never use a wildcard. Verify candidate object ETag/version, size, and checksum metadata through bounded `HEAD` calls before querying. Reject empty candidates as `TargetNotPresent` rather than broadening the scan.

- [ ] **Step 6: Implement the versioned ClinVar coordinate resolver**

Build the small demo reference DuckDB once from `clinvar_coordinates_grch38.tsv`, record `demo-clinvar-grch38-v1` as its reference version, and open it read-only at serving time. Resolve targets before remote scan. The canonical match key is `(referenceBuild, normalizedChrom, pos, normalizedRef, normalizedAlt)`, with rsID retained as provenance rather than the only join key. Require the dataset manifest and ClinVar snapshot to use the same reference build and declared reference version.

- [ ] **Step 7: Implement constrained in-memory DuckDB sessions**

Open `:memory:`, load a DuckDB/httpfs extension version proven in Task 1, configure scoped read-only S3 credentials from trusted environment, and set:

```sql
SET memory_limit = '512MB';
SET threads = 4;
SET enable_http_metadata_cache = true;
```

Add a 10-second application query deadline and call the binding's interrupt/cancellation API when it expires. Always drop secrets and close the connection in `finally`. The runtime image must use a preinstalled compatible extension and must not require Internet access on the first `/ask`.

- [ ] **Step 8: Implement explicit-file remote Parquet querying**

Remove fixture initialization, vector JSON storage, exception-swallowing synthesis fallback, dataset-local path caching, and the global user-data repository. For each chromosome group, construct the `read_parquet([...], hive_partitioning = true)` file list exclusively from validated manifest descriptors, apply literal `chrom` plus parameterized position predicates directly above the scan, and join a small candidate relation on `pos`, `ref`, and `alt`. Do not rely on dynamic join filtering alone to prune S3 data.

- [ ] **Step 9: Inject the repository into agent tools**

Replace the exported singleton tool with:

```ts
export function createQueryGenotypeTool(repository: GenotypeRepository) {
  return tool({
    description: 'Queries the selected published genomic dataset.',
    parameters: z.object({ targetId: z.string().min(1) }),
    execute: ({ targetId }) => repository.synthesizeVariant(targetId),
  });
}
```

Return provenance containing dataset checksum, reference version, and files scanned. Keep the literature repository separate and global.

- [ ] **Step 10: Run repository, pruning, and tool tests**

Run: `node --test ts-api-agent/src/infrastructure/database/*.test.ts ts-api-agent/src/infrastructure/ai/tools.test.ts tests/integration/remote_parquet_pruning.test.ts`

Expected: PASS; no serving test creates, downloads, or reads a per-dataset `.duckdb` file, and a targeted query does not read unrelated chromosome data.

- [ ] **Step 11: Commit remote dataset isolation**

```bash
git add ts-api-agent/src/infrastructure/database ts-api-agent/src/infrastructure/ai tests/integration/remote_parquet_pruning.test.ts tests/fixtures/clinvar_coordinates_grch38.tsv scripts/generate_clinical_benchmark_vcf.ts
git commit -m "feat: query partitioned genomic Parquet through DuckDB"
```

### Task 8: Expose the Real Dataset Lifecycle in the API and UI

**Files:**
- Modify: `ts-api-agent/src/index.ts`
- Create: `ts-api-agent/src/index.test.ts`
- Modify: `ts-api-agent/public/index.html`
- Modify: `ts-api-agent/src/application/trigger_workflow.ts`

**Interfaces:**
- Produces: `GET /api/datasets/catalog`.
- Produces: `POST /api/ingestions` with `{ datasetKey }`.
- Produces: `GET /api/ingestions/:workflowId`.
- Modifies: `POST /ask` to require `{ datasetId, question }`.

- [ ] **Step 1: Write API validation tests**

Assert the catalog returns only two seeded entries; arbitrary S3/HTTP/path keys return `400`; ingestion returns `202` with fresh `datasetId` and `workflowId`; `/ask` without `datasetId` returns `400`; `/ask` for an unpublished dataset returns `409`; and API input cannot contain a Parquet URI or override manifest files. Cover `DatasetNotPublished`, `ReferenceBuildMismatch`, `TargetNotResolvable`, `RemoteDatasetUnavailable`, and `QueryBudgetExceeded` without substituting fixture results.

- [ ] **Step 2: Run API tests and observe current simulation behavior fail**

Run: `node --test ts-api-agent/src/index.test.ts`

Expected: FAIL because the current endpoint accepts `fileKey`, derives `userId` from filenames, and starts timer-based simulated completion when Temporal is unavailable.

- [ ] **Step 3: Implement explicit dependency construction**

Export `createApp(dependencies)` so tests inject Temporal client, catalog, Parquet dataset resolver, coordinate resolver, DuckDB session factory, and agent factory. Runtime startup builds real adapters. Remove module-load fixture initialization and global mutable fallback maps.

- [ ] **Step 4: Implement catalog and ingestion endpoints**

Generate `datasetId` and `workflowId` in the API before Workflow start. Start only the real Workflow. If Temporal is unavailable, return `503`; do not report simulated progress or completion.

- [ ] **Step 5: Implement dataset-scoped ask endpoint**

Resolve the published manifest first, construct the request-scoped remote-Parquet genotype repository and tools, then call the agent. Return provenance containing `datasetId`, `datasetChecksumSha256`, manifest/layout version, reference version, and the exact files scanned.

- [ ] **Step 6: Update the zero-build UI**

Rename the first step from upload to dataset selection, show `demo-small` and `na12878-full`, display returned `datasetId`, poll real Workflow status, enable chat only after `COMPLETED`, and reuse the same `datasetId` for all messages. Add a separate explicit `Run ingestion again` action.

- [ ] **Step 7: Run API tests and TypeScript build**

Run: `node --test ts-api-agent/src/index.test.ts`

Run: `npm run build --workspace=ts-api-agent`

Expected: PASS.

- [ ] **Step 8: Commit the truthful API lifecycle**

```bash
git add ts-api-agent/src/index.ts ts-api-agent/src/index.test.ts ts-api-agent/src/application/trigger_workflow.ts ts-api-agent/public/index.html
git commit -m "feat: expose published dataset lifecycle"
```

### Task 9: Containerize, Verify the Vertical Slice, and Document Its Boundaries

**Files:**
- Create: `rust-ingestion-worker/Dockerfile`
- Create: `ts-api-agent/Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `docker-compose.prod.yml`
- Create: `tests/integration/cross_language_ingestion.test.ts`
- Create: `tests/integration/dataset_isolation.test.ts`
- Modify: `tests/integration/remote_parquet_pruning.test.ts`
- Modify: `ts-api-agent/src/test_e2e.ts`
- Modify: `ts-api-agent/package.json`
- Modify: `package.json`
- Modify: `Makefile`
- Modify: `README.md`
- Modify: `GUIDE.md`

**Interfaces:**
- Produces: one-command local stack and one-command vertical-slice verification.

- [ ] **Step 1: Repair the test command before adding E2E assertions**

Add `test` to the workspace package using the Node test runner and make root `npm test` execute TS unit tests plus Rust unit tests. Keep MinIO/Temporal integration tests in an explicit `test:integration` script.

- [ ] **Step 2: Write the failing cross-language E2E test**

The test must:

1. seed `demo-small`;
2. call `POST /api/ingestions`;
3. wait for real Temporal completion;
4. read the manifest from MinIO and assert it was written only after all declared Parquet objects existed;
5. assert the dataset checksum, positive variant count, chromosome partition layout, sorted/canonical file inventory, and every object's ETag/size/checksum metadata;
6. call `/ask` with the returned `datasetId` and a known gene;
7. assert response provenance contains the same dataset ID, dataset checksum, reference version, and exact files scanned;
8. assert no local per-dataset `.duckdb` artifact was downloaded by the API.

- [ ] **Step 3: Write the dataset-isolation E2E test**

Ingest two small fixtures with opposite genotypes under two allowlisted test catalog entries injected only in the test process. Query both and assert no cross-dataset row or Parquet URI leakage. Corrupt or remove one declared partition and assert an explicit remote-dataset failure rather than partial evidence.

- [ ] **Step 4: Write the remote-pruning E2E test**

Ingest data containing at least chromosomes 1 and 12. Resolve a known chromosome-12 gene, then prove from the repository's selected-file provenance plus MinIO request accounting that chromosome-1 data is not read and that the full chromosome-12 object is not downloaded. Record selected files, S3 requests, bytes read, and query latency.

- [ ] **Step 5: Create production-style Worker images**

The Rust Dockerfile compiles `temporal_worker` in a builder stage and copies only the release binary plus CA certificates into runtime. The TS image installs locked production dependencies and a DuckDB `httpfs` extension matching the binding/engine version proven in Task 1, then runs either API or control-plane Worker by command. Verify the image can query MinIO with external network disabled; no runtime container invokes Cargo or downloads an extension from the Internet.

- [ ] **Step 6: Wire explicit services and health dependencies**

Compose services are `temporal`, `minio`, `qdrant`, `ts-api`, `ts-control-worker`, and `rust-ingestion-worker`. Both Workers use the same Temporal namespace but different queues. Give Rust bounded CPU/memory/temp-disk configuration. Give API scoped read-only access to `genomic-artifacts/datasets/`, S3 request timeouts, and query limits. Do not mount a shared user-data volume between TS and Rust. Add a lifecycle policy or documented cleanup command for orphan version/attempt prefixes.

- [ ] **Step 7: Run the full verification matrix**

Run: `npm test`

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml`

Run: `docker compose up -d --build`

Run: `npm run test:integration --workspace=ts-api-agent`

Run: `npm run build --workspace=ts-api-agent`

Expected: all commands PASS; Temporal UI shows `buildDatasetArtifact` on `genomic-ingestion-rust` with Rust worker identity and heartbeat details.

- [ ] **Step 8: Verify serving-path invariants**

Search production code and assert there is no per-dataset `.duckdb` download/cache, no `read_parquet` wildcard, no user-supplied S3 URI, no unresolved-target full scan, no runtime fixture fallback, and no production shell launch of Rust. Confirm metrics/log fields include selected file count, bytes read, S3 request count, query latency, dataset checksum, and reference version.

- [ ] **Step 9: Update documentation truthfully**

Document seeded dataset simulation, local DuckDB processing, immutable chromosome-partitioned Parquet in S3, in-memory DuckDB remote queries, global ClinVar/PubMed roles, coordinate/allele resolution, Public Preview Rust SDK status, task queues, failure/retry semantics, query limits, and exact demo commands. Remove claims about per-dataset serving DuckDB files/local checksum caches, `production-ready`, fabricated throughput without benchmark methodology, fake Temporal heartbeat behavior, and synthetic abstracts described as real abstracts.

- [ ] **Step 10: Commit the interview-ready vertical slice**

```bash
git add rust-ingestion-worker/Dockerfile ts-api-agent/Dockerfile docker-compose.yml docker-compose.prod.yml tests ts-api-agent/src/test_e2e.ts ts-api-agent/package.json package.json Makefile README.md GUIDE.md
git commit -m "feat: deliver cross-language genomic ingestion vertical slice"
```

---

## Recommended Execution Order and Parallelism

Do Task 1 first and alone. Task 2 has a single owner because every later agent depends on its wire names. After Task 2, Tasks 3 and the TypeScript half of Task 4 can run in parallel; the Rust half of Task 4 may run beside Task 3 if neither edits shared Rust module exports until integration. Tasks 5 and 6 can then run in parallel against frozen contracts, followed sequentially by Tasks 7, 8, and 9.

Suggested subagent ownership:

- Contract owner: Task 2 and cross-language fixture review.
- Rust data-plane agent: Tasks 3 and 5.
- Object-store agent: Task 4.
- TypeScript orchestration agent: Task 6.
- Query isolation agent: Task 7.
- API/UI agent: Task 8.
- Integration owner: Task 9 and final verification.

The recommended stopping point is Task 9. PubMed real-abstract ingestion, full ClinVar normalization/liftover, a general lakehouse table format, authentication, and multi-tenant authorization should be separate follow-up plans.

## Self-Review Result

- Every design requirement maps to at least one task.
- Cross-language names, task queues, object keys, failure types, and manifest fields are consistent across tasks.
- The Temporal SDK risk and remote DuckDB/Parquet pruning risk are tested before VCF/S3 refactoring.
- Runtime fixture substitution, shell execution, global DuckDB state, and false completion are explicitly removed.
- No Qdrant or LLM work blocks the deterministic ingestion/query vertical slice.
