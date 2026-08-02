# Cross-Language Genomic Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a truthful end-to-end path in which a TypeScript Temporal Workflow ingests an allowlisted S3 VCF through a real Rust Temporal Activity Worker, publishes an immutable DuckDB artifact, and queries that exact dataset from the agent API.

**Architecture:** TypeScript owns the control plane and schedules a Rust-only Activity queue through Temporal. Rust streams the selected S3 object into an attempt-local DuckDB file, validates it, and uploads a staging artifact; TypeScript publishes the artifact by writing its manifest last. Query requests resolve the manifest by `datasetId`, verify and cache the immutable artifact, and open a request-scoped read-only DuckDB repository.

**Tech Stack:** Node.js 25, TypeScript 5.7, Hono, Temporal TypeScript SDK 1.11, Rust 2021, Temporal Rust SDK 0.5.0 Public Preview, Tokio, Rayon, DuckDB, AWS SDK for JavaScript/Rust, MinIO/S3, Qdrant, Node test runner.

## Global Constraints

- Arbitrary uploads and arbitrary URL/path ingestion remain out of scope; only `demo-small` and `na12878-full` are accepted.
- Cross-worker payloads contain JSON-compatible primitives only and use camelCase field names.
- `genomic-control-plane` is the TypeScript Workflow/Activity queue; `genomic-ingestion-rust` is activity-only.
- S3 object references, not local paths or shared volumes, cross the language boundary.
- `temporalio-sdk`, `temporalio-client`, `temporalio-common`, and `temporalio-macros` are pinned to `0.5.0`; the Public Preview status is documented.
- Published DuckDB artifacts are immutable and queryable only when a matching manifest exists.
- Rust retries never append to an existing DuckDB file.
- Qdrant stores global literature only; no user genotype is written to Qdrant.
- Fixture fallback is allowed in automated tests only and is never silent in runtime paths.

---

## File Structure

### Shared contract and fixtures

- `contracts/ingestion-v1.md` — canonical wire names, payload examples, failure taxonomy.
- `contracts/fixtures/build-dataset-artifact.input.json` — golden input consumed by TS and Rust tests.
- `contracts/fixtures/build-dataset-artifact.result.json` — golden output consumed by TS and Rust tests.
- `contracts/fixtures/dataset-manifest.json` — golden published manifest.

### TypeScript control plane

- `ts-api-agent/src/domain/datasets.ts` — domain types and state names.
- `ts-api-agent/src/application/dataset-catalog.ts` — allowlisted seeded datasets.
- `ts-api-agent/src/application/ingestion-contracts.ts` — Zod wire schemas.
- `ts-api-agent/src/application/workflows.ts` — deterministic orchestration and progress query.
- `ts-api-agent/src/application/control-plane-activities.ts` — S3 inspect and manifest-last publish activities.
- `ts-api-agent/src/application/worker.ts` — TS Workflow plus control-plane Activity Worker.
- `ts-api-agent/src/infrastructure/object-store/s3-object-store.ts` — S3 adapter.
- `ts-api-agent/src/infrastructure/database/dataset-artifact-resolver.ts` — manifest resolution, checksum cache.
- `ts-api-agent/src/infrastructure/database/duckdb.ts` — dataset-scoped read-only repository only.
- `ts-api-agent/src/infrastructure/ai/tools.ts` — request-scoped genotype tool.
- `ts-api-agent/src/infrastructure/ai/agent.ts` — receives `datasetId`/repository dependencies.
- `ts-api-agent/src/index.ts` — catalog, ingestion/status, and dataset-scoped ask endpoints.

### Rust data plane

- `rust-ingestion-worker/src/contracts.rs` — serde wire structs and stable failure names.
- `rust-ingestion-worker/src/object_store.rs` — S3/MinIO download and upload adapter.
- `rust-ingestion-worker/src/vcf.rs` — streaming plain/gzip VCF reader and record parser.
- `rust-ingestion-worker/src/artifact.rs` — attempt-local DuckDB builder and validator.
- `rust-ingestion-worker/src/temporal_activities.rs` — thin Activity wrapper, heartbeat, cancellation, failure mapping.
- `rust-ingestion-worker/src/bin/temporal_worker.rs` — activity-only Worker bootstrap.
- `rust-ingestion-worker/src/main.rs` — retain only explicit CLI/debug behavior; do not masquerade as a Temporal worker.

### Integration and operations

- `tests/integration/cross_language_ingestion.test.ts` — TS Workflow to Rust Activity proof.
- `tests/integration/dataset_isolation.test.ts` — two datasets produce isolated query results.
- `docker-compose.yml` — local Temporal, MinIO, Qdrant, TS Worker, and Rust Worker topology.
- `docker-compose.prod.yml` — same worker separation without runtime Cargo compilation.
- `rust-ingestion-worker/Dockerfile` — multi-stage release image.
- `ts-api-agent/Dockerfile` — TS API/control-plane image.
- `scripts/seed_demo_s3.sh` — idempotent seeded-object bootstrap.

---

### Task 1: Feasibility Gate — TypeScript Workflow Calls a Real Rust Activity

**Files:**
- Modify: `rust-ingestion-worker/Cargo.toml`
- Create: `rust-ingestion-worker/src/bin/temporal_probe_worker.rs`
- Create: `ts-api-agent/src/application/temporal_probe_workflow.ts`
- Create: `ts-api-agent/src/application/temporal_probe_worker.ts`
- Create: `tests/integration/temporal_rust_probe.test.ts`
- Modify: `ts-api-agent/package.json`

**Interfaces:**
- Consumes: Temporal at `TEMPORAL_ADDRESS`, namespace `default`.
- Produces: Activity Type `rustActivityProbe` on `genomic-ingestion-rust`; payload `{ message: string, iterations: number }`; result `{ echoed: string, workerLanguage: "rust" }`.

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

- [ ] **Step 7: Run the feasibility gate**

Run: `node --test tests/integration/temporal_rust_probe.test.ts`

Expected: PASS; the Temporal UI pending Activity shows a Rust worker identity and heartbeat detail.

- [ ] **Step 8: Apply the gate decision**

Proceed to Task 2 only when payload compatibility, heartbeat, cancellation, and retry all pass. If any is unsupported by SDK 0.5.0, stop this plan and retain the production fallback `TS Activity -> spawn(shell: false) -> Rust processor`; do not build a partial custom Temporal Core integration.

- [ ] **Step 9: Commit the gate**

```bash
git add rust-ingestion-worker/Cargo.toml Cargo.lock rust-ingestion-worker/src/bin/temporal_probe_worker.rs ts-api-agent/src/application/temporal_probe_workflow.ts ts-api-agent/src/application/temporal_probe_worker.ts ts-api-agent/package.json tests/integration/temporal_rust_probe.test.ts
git commit -m "spike: prove TypeScript workflow to Rust activity"
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
    "contentLength": 1024
  },
  "staging": {
    "bucket": "genomic-artifacts",
    "key": "staging/ds-test-001/attempt-1.duckdb"
  }
}
```

The result fixture contains `stagingArtifactUri`, `checksumSha256`, `variantCount`, `rejectedRecordCount`, `referenceBuild`, and `processorVersion`.

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

### Task 3: Build and Test the Pure Rust Artifact Processor

**Files:**
- Create: `rust-ingestion-worker/src/vcf.rs`
- Create: `rust-ingestion-worker/src/artifact.rs`
- Create: `rust-ingestion-worker/tests/artifact_builder_test.rs`
- Modify: `rust-ingestion-worker/src/lib.rs`
- Modify: `rust-ingestion-worker/src/models.rs`
- Modify: `rust-ingestion-worker/Cargo.toml`
- Delete after migration: `rust-ingestion-worker/src/activities/mod.rs`

**Interfaces:**
- Consumes: `ArtifactBuildRequest { source_path, output_path, dataset_id, source_etag }`.
- Produces: `ArtifactStats { checksum_sha256, variant_count, rejected_record_count, reference_build }`.
- Produces progress through a `ProgressSink` trait independent of Temporal.

- [ ] **Step 1: Add processor dependencies**

Add `sha2 = "=0.11.0"`, `hex = "=0.4.3"`, and `tempfile = "=3.27.0"`; retain `flate2`, `duckdb`, `rayon`, and `tokio`. Do not add Temporal imports to `vcf.rs` or `artifact.rs`.

- [ ] **Step 2: Write failing parser tests**

Test the existing `demo_user.vcf`, a gzipped copy created in a test temp directory, header-only input, a missing `GT` field, invalid POS, phased `0|1`, and a multiallelic `1/2` record. Assert malformed records increment `rejectedRecordCount` instead of panicking.

- [ ] **Step 3: Write the bounded-memory acceptance test**

Generate 100,000 VCF records through a buffered writer, run the processor with `batch_size = 1_000`, and use an instrumented `ProgressSink` to assert no reported in-memory batch exceeds 1,000 records.

- [ ] **Step 4: Run tests and observe the current all-in-memory design fail**

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml --test artifact_builder_test`

Expected: FAIL because the streaming reader, progress trait, metadata table, and overwrite-safe builder do not exist.

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
CREATE INDEX user_variants_rsid_idx ON user_variants(rsid);
```

Append bounded batches, commit metadata after parsing, close the connection, reopen read-only, validate schema/counts, then calculate SHA-256.

- [ ] **Step 7: Replace fake Temporal context with `ProgressSink`**

Provide `NoopProgressSink` for CLI/tests. Delete the custom `ActivityContext` whose heartbeat only logs JSON. Temporal adaptation belongs in Task 5.

- [ ] **Step 8: Run processor tests**

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml artifact_builder`

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml vcf`

Expected: PASS, including gzip and bounded-batch cases.

- [ ] **Step 9: Commit the pure data plane**

```bash
git add rust-ingestion-worker/Cargo.toml Cargo.lock rust-ingestion-worker/src rust-ingestion-worker/tests/artifact_builder_test.rs
git commit -m "feat: stream VCF into immutable DuckDB artifacts"
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
- Produces TS `ObjectStore` methods `head`, `copy`, `putJson`, `getJson`, `downloadToFile`.
- Produces `inspectDatasetSource(datasetId, datasetKey)` and `publishDataset(input, result)`.
- Produces Rust `ObjectStore` methods `download_exact` and `upload_file`.

- [ ] **Step 1: Add S3 dependencies**

Add `@aws-sdk/client-s3` to TypeScript. Add `aws-config = "=1.10.0"` and `aws-sdk-s3 = "=1.140.0"` to Rust after confirming `rustc --version` is at least 1.94.1; if it is older, update the project toolchain explicitly before compiling rather than silently selecting a different dependency set.

- [ ] **Step 2: Write fake-object-store tests for source inspection**

Assert that catalog mapping controls bucket/key, the returned input includes ETag and content length, missing ETag fails, and the caller cannot inject a URL or alternate bucket.

- [ ] **Step 3: Write publication idempotency tests**

Use an in-memory `ObjectStore` fake and verify call order:

```text
HEAD staging artifact
COPY staging -> datasets/{datasetId}/variants.duckdb
HEAD final artifact
PUT datasets/{datasetId}/manifest.json
```

Assert a second identical call succeeds without changing identity; a conflicting existing manifest raises `DatasetPublicationConflict`; no manifest is written when copy/checksum verification fails.

- [ ] **Step 4: Implement TypeScript S3 adapter and activities**

Configure endpoint, region, credentials, and path-style mode from explicit environment variables. Never form public HTTP URLs from `s3://` strings. Escape no SQL or shell content in this adapter.

- [ ] **Step 5: Write Rust MinIO tests**

Test exact ETag matching, wrong ETag rejection as `SourceObjectChanged`, streaming download to a temp file, upload metadata containing SHA-256, and retrying an identical upload.

- [ ] **Step 6: Implement Rust S3 adapter**

Use the AWS SDK client configured for MinIO path-style access. Verify source ETag before and after download. Upload to the exact attempt staging key supplied by the Workflow.

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
  "processedVariants": 2500
}
```

- [ ] **Step 3: Implement the thin Temporal Activity**

The wrapper creates attempt-scoped temporary paths using Workflow ID, Activity ID, and attempt number; constructs the S3 adapter; calls the pure artifact processor; uploads the artifact; returns the wire result; and removes local temporary files on success, failure, or cancellation.

- [ ] **Step 4: Implement cancellation-aware progress**

At every heartbeat boundary, check Activity cancellation and stop reading/writing before cleanup. Never delete source objects, final artifacts, or another attempt's staging key during cancellation.

- [ ] **Step 5: Implement activity-only Worker bootstrap**

Connect using `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE`, register `buildDatasetArtifact`, configure `WorkerTaskTypes::activity_only()`, task queue `genomic-ingestion-rust`, and identity prefix `rust-ingestion-worker@`.

- [ ] **Step 6: Run Rust Activity tests**

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml temporal_activity`

Expected: PASS.

- [ ] **Step 7: Re-run the cross-language probe using the production Activity Type**

Replace the probe integration payload with the golden `buildDatasetArtifact` fixture backed by MinIO and assert the returned checksum, variant count, heartbeat presence, and Rust worker identity.

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

Cover the exact state sequence `RESOLVING -> BUILDING -> PUBLISHING -> COMPLETED`, Rust task queue selection, nonretryable invalid VCF behavior, retryable object-store failure, cancellation, and the invariant that `publishDataset` is never scheduled after build failure.

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

The Workflow receives a complete serializable input containing IDs and catalog key. It performs no filesystem, S3, UUID, date, DuckDB, or network operations directly. Return the published manifest as the Workflow result.

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

### Task 7: Make DuckDB Queries Dataset-Scoped and Remove Runtime Fixture Substitution

**Files:**
- Create: `ts-api-agent/src/infrastructure/database/dataset-artifact-resolver.ts`
- Create: `ts-api-agent/src/infrastructure/database/dataset-artifact-resolver.test.ts`
- Modify: `ts-api-agent/src/infrastructure/database/duckdb.ts`
- Create: `ts-api-agent/src/infrastructure/database/duckdb.test.ts`
- Modify: `ts-api-agent/src/infrastructure/ai/tools.ts`
- Modify: `ts-api-agent/src/infrastructure/ai/agent.ts`
- Create: `ts-api-agent/src/infrastructure/ai/tools.test.ts`

**Interfaces:**
- Produces: `DatasetArtifactResolver.resolve(datasetId): Promise<ResolvedDatasetArtifact>`.
- Produces: `GenotypeRepositoryFactory.open(datasetId): Promise<GenotypeRepository>`.
- Produces: `createQueryGenotypeTool(repository)`; no global repository import.

- [ ] **Step 1: Write resolver tests**

Use a fake object store and temp cache. Assert that missing manifest fails, checksum mismatch deletes the bad cache file and fails, matching checksum reuses the cache, and paths derive from checksum rather than untrusted dataset strings.

- [ ] **Step 2: Write the two-dataset isolation test first**

Create two minimal DuckDB files with opposing `rs762551` genotypes, publish fake manifests, query both repositories, and assert the results differ. Assert querying an unknown `datasetId` never returns rows from either fixture.

- [ ] **Step 3: Run tests and observe singleton/fallback failure**

Run: `node --test ts-api-agent/src/infrastructure/database/dataset-artifact-resolver.test.ts ts-api-agent/src/infrastructure/database/duckdb.test.ts`

Expected: FAIL because the current singleton uses `genomic_data.duckdb` and silently falls back to repository fixtures.

- [ ] **Step 4: Implement manifest resolution and checksum cache**

Download published artifacts into `<cacheRoot>/<checksumSha256>.duckdb.part`, verify SHA-256, rename to `<cacheRoot>/<checksumSha256>.duckdb`, and open only the final path. Ensure concurrent identical downloads converge safely.

- [ ] **Step 5: Reduce `DuckDbRepository` to read-only dataset queries**

Remove fixture initialization, vector JSON storage, and exception-swallowing synthesis fallback. The constructor requires `datasetDbPath` and `referenceDbPath`; `synthesizeVariant` opens the dataset read-only, attaches the reference read-only, executes a parameterized query, and always closes connections in `finally`.

- [ ] **Step 6: Inject the repository into agent tools**

Replace the exported singleton tool with:

```ts
export function createQueryGenotypeTool(repository: GenotypeRepository) {
  return tool({
    description: 'Queries the selected genomic dataset.',
    parameters: z.object({ targetId: z.string().min(1) }),
    execute: ({ targetId }) => repository.synthesizeVariant(targetId),
  });
}
```

Keep the literature repository separate and global.

- [ ] **Step 7: Run repository and tool tests**

Run: `node --test ts-api-agent/src/infrastructure/database/*.test.ts ts-api-agent/src/infrastructure/ai/tools.test.ts`

Expected: PASS; no test creates or reads `genomic_data.duckdb` implicitly.

- [ ] **Step 8: Commit dataset isolation**

```bash
git add ts-api-agent/src/infrastructure/database ts-api-agent/src/infrastructure/ai
git commit -m "fix: scope genotype queries to published datasets"
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

Assert the catalog returns only two seeded entries; arbitrary S3/HTTP/path keys return `400`; ingestion returns `202` with fresh `datasetId` and `workflowId`; `/ask` without `datasetId` returns `400`; `/ask` for an unpublished dataset returns `409`; a published dataset reaches the injected agent.

- [ ] **Step 2: Run API tests and observe current simulation behavior fail**

Run: `node --test ts-api-agent/src/index.test.ts`

Expected: FAIL because the current endpoint accepts `fileKey`, derives `userId` from filenames, and starts timer-based simulated completion when Temporal is unavailable.

- [ ] **Step 3: Implement explicit dependency construction**

Export `createApp(dependencies)` so tests inject Temporal client, catalog, artifact resolver, and agent factory. Runtime startup builds real adapters. Remove module-load fixture initialization and global mutable fallback maps.

- [ ] **Step 4: Implement catalog and ingestion endpoints**

Generate `datasetId` and `workflowId` in the API before Workflow start. Start only the real Workflow. If Temporal is unavailable, return `503`; do not report simulated progress or completion.

- [ ] **Step 5: Implement dataset-scoped ask endpoint**

Resolve the published artifact first, construct the request-scoped genotype repository and tools, then call the agent. Return provenance containing `datasetId`, artifact checksum, and reference version.

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
4. read the manifest from MinIO;
5. assert the artifact checksum and positive variant count;
6. call `/ask` with the returned `datasetId` and a known gene;
7. assert response provenance contains the same dataset ID and checksum.

- [ ] **Step 3: Write the dataset-isolation E2E test**

Ingest two small fixtures with opposite genotypes under two allowlisted test catalog entries injected only in the test process. Query both and assert no cross-dataset row or cache reuse by dataset path.

- [ ] **Step 4: Create production-style Worker images**

The Rust Dockerfile compiles `temporal_worker` in a builder stage and copies only the release binary plus CA certificates into runtime. The TS image installs locked production dependencies and runs either API or control-plane Worker by command. No runtime container invokes Cargo.

- [ ] **Step 5: Wire explicit services and health dependencies**

Compose services are `temporal`, `minio`, `qdrant`, `ts-api`, `ts-control-worker`, and `rust-ingestion-worker`. Both Workers use the same Temporal namespace but different queues. Give Rust bounded CPU/memory configuration and a writable temp directory; do not mount a shared DuckDB volume between TS and Rust.

- [ ] **Step 6: Run the full verification matrix**

Run: `npm test`

Run: `cargo test --manifest-path rust-ingestion-worker/Cargo.toml`

Run: `docker compose up -d --build`

Run: `npm run test:integration --workspace=ts-api-agent`

Run: `npm run build --workspace=ts-api-agent`

Expected: all commands PASS; Temporal UI shows `buildDatasetArtifact` on `genomic-ingestion-rust` with Rust worker identity and heartbeat details.

- [ ] **Step 7: Update documentation truthfully**

Document seeded dataset simulation, per-dataset immutable DuckDB artifacts, global ClinVar/PubMed roles, Public Preview Rust SDK status, task queues, failure/retry semantics, and exact demo commands. Remove `production-ready`, fabricated throughput claims without benchmark methodology, fake Tokio/Temporal heartbeat claims, and statements that synthetic abstracts are real PubMed abstracts.

- [ ] **Step 8: Commit the interview-ready vertical slice**

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

The recommended stopping point is Task 9. PubMed real-abstract ingestion, full ClinVar normalization, Parquet export, authentication, and multi-tenant authorization should be separate follow-up plans.

## Self-Review Result

- Every design requirement maps to at least one task.
- Cross-language names, task queues, object keys, failure types, and manifest fields are consistent across tasks.
- The main SDK risk is tested before VCF/S3 refactoring.
- Runtime fixture substitution, shell execution, global DuckDB state, and false completion are explicitly removed.
- No Qdrant or LLM work blocks the deterministic ingestion/query vertical slice.
