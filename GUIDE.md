# Operator and developer guide

Everything the [README](README.md) summarises, in the detail you need to run it, debug it or
change it. The normative cross-language contract is `contracts/ingestion-v1.md`; where this guide
and that document disagree, that document wins.

---

## 1. The two task queues

| Queue | Served by | Registers |
| --- | --- | --- |
| `genomic-control-plane` | `ts-api-agent/src/application/worker.ts` | `GenomicIngestionWorkflow`, `inspectDatasetSource`, `publishDataset` |
| `genomic-ingestion-rust` | `rust-ingestion-worker` (`temporal_worker`) | `buildDatasetArtifact` — **activity only** |

Both Workers share one Temporal namespace (`default`) and neither can serve the other's work.
The Rust worker is started with `WorkerTaskTypes::activity_only()`: it never polls for Workflow
tasks, because Workflow code exists only in TypeScript and a Rust worker polling this queue for
them would strand them silently. Its identity is prefixed `rust-ingestion-worker@`, so a pending
Activity in Temporal history is attributable to the data plane without cross-referencing
anything.

`buildDatasetArtifact` has **no TypeScript implementation** and is never registered in
TypeScript. The Workflow schedules it by name onto the other queue. Nothing in the TypeScript
process launches a Rust binary, shells out, or touches a local VCF — a unit test
(`ts-api-agent/src/serving-invariants.test.ts`) fails the build if a `child_process` import or
the word `cargo` appears anywhere under `ts-api-agent/src`. The sweep does not reach
`tests/integration/support/stack.ts`, which legitimately spawns `cargo` to drive the Rust worker
for the integration suites and is not request-path code.

### Progress, and what it is allowed to claim

`GET /api/ingestions/{workflowId}` forwards the Workflow's own `getProgress` query verbatim:

```json
{ "datasetId": "…", "datasetKey": "demo-small", "state": "VERIFYING_OBJECTS",
  "unobservedStates": ["PUBLISHING_MANIFEST"], "message": "…" }
```

`state` is the latest state the Workflow can *prove* was entered. `unobservedStates` carries
states a running Activity may already have passed through but which the Workflow has no way to
witness — verification and manifest publication happen inside one Activity, so the boundary
between them is real but unobservable from outside. Nothing interpolates or times a transition.
If Temporal is unreachable the answer is `503`, never a progress bar.

### Heartbeats

The Rust Activity heartbeats a structured payload throughout, with a `heartbeatTimeout` of 15
seconds. Phases, in order, exactly as frozen in the contract:

`DOWNLOADING_SOURCE` → `PARSING` → `WRITING_DUCKDB` → `EXPORTING_PARQUET` →
`UPLOADING_PARTITION` → `FINALIZING`

```json
{ "phase": "UPLOADING_PARTITION", "processedBytes": 126000000, "processedVariants": 3893341,
  "currentPartition": "12", "completedFiles": 3, "uploadedBytes": 1048576 }
```

`currentPartition` is `null` while no chromosome partition is being processed. The payload is
emitted when progress actually changes, plus a keepalive that re-emits the last observation so a
long single-partition upload is not mistaken for a dead worker; the keepalive stops re-emitting
a stalled observation rather than papering over a hang. The worker throttles recorded heartbeats
to 5-second intervals, which is what leaves ~10 s of margin against the 15 s timeout instead of
the ~3 s the SDK default would leave.

There is **no percentage** in the payload and none is computed. Total variant count is not known
until parsing finishes.

### Failure and retry semantics

Failure types are matched by name across the language boundary
(`contracts/ingestion-v1.md`, "Failure taxonomy"):

| Failure type | Retryable | Raised when |
| --- | --- | --- |
| `InvalidVcfFormat` | no | the source VCF is unparseable |
| `SourceObjectChanged` | no | the source ETag/version changed under the attempt |
| `ObjectStoreUnavailable` | yes | S3/MinIO is unreachable or returning transient errors |
| `ArtifactWriteFailed` | yes | a transient local disk or upload failure |
| `ArtifactValidationFailed` | no | the produced artifact fails a deterministic invariant |

The Rust Activity gets at most 3 attempts, `startToCloseTimeout` 30 minutes,
`scheduleToCloseTimeout` 45 minutes, and `WAIT_CANCELLATION_COMPLETED` — a cancellation blocks
the Workflow until the Activity actually reports back, so it is recorded in history as
`ActivityTaskCanceled` rather than abandoned mid-upload.

Each attempt writes under its **own** prefix, `…/versions/{v}/attempt-{n}/`, and its own local
workspace. A retry can therefore never append to or overwrite a previous attempt's objects, and
a failed attempt leaves an orphan prefix rather than a half-written dataset. Control-plane
activities retry up to 5 times with exponential backoff; contract violations, unknown catalog
keys and object-verification failures are non-retryable.

---

## 2. The published artifact

```
s3://genomic-artifacts/datasets/{datasetId}/versions/{v}/attempt-{n}/variants/chrom=<value>/part-NNN.parquet
s3://genomic-artifacts/datasets/{datasetId}/manifest.json
```

Physical Parquet columns are `pos UINTEGER NOT NULL`, `rsid VARCHAR NULL`,
`ref VARCHAR NOT NULL`, `alt VARCHAR NOT NULL`, `gt_raw VARCHAR NOT NULL`. **`chrom` is not a
physical column** — it is the `chrom=<value>` directory, restored by the reader.

Reading one back, anywhere, always uses both options:

```sql
read_parquet(['s3://…/chrom=12/part-000.parquet'],
             hive_partitioning = true, hive_types_autocast = 0)
```

`hive_partitioning = true` on its own is prohibited. With autocast left on, DuckDB infers the
Hive column's type from the partition values the scan happened to touch, so `chrom` comes back
as `BIGINT` for an autosome-only scan and `VARCHAR` for a scan that also touched `X` — the same
dataset, two types, no error. `contracts/ingestion-v1.md` ("Reading the dataset") states this
normatively and a test asserts every cell of its table against the real engine.

The manifest is validated before any SQL exists: layout/schema version, schema fingerprint,
bucket, prefix containment, partition agreement, canonical ordering, duplicates, and the dataset
content checksum. The allowed prefix is *derived* from the manifest's own
`datasetId`/`artifactVersion`, never read off the wire.

`datasetChecksumSha256` identifies dataset **content**, independent of the attempt that produced
it: it is computed from relative descriptors (`chrom=<value>/part-NNN.parquet`, content SHA-256,
size, row count, min/max position), so re-running the same source into a different attempt prefix
reproduces the same checksum. Its exact byte format is in the contract.

### Orphan attempt prefixes

A failed, retried or cancelled attempt leaves objects nothing will read. There is no S3
lifecycle rule for them, deliberately: a lifecycle expiration matches on prefix and age, and
"attempt-1 under this dataset" is indistinguishable by prefix from the attempt a live manifest
depends on. Deciding requires reading the manifest, so:

```bash
make cleanup-orphans                      # report only
make cleanup-orphans ARGS="--delete"      # remove exactly what was reported
scripts/cleanup_orphan_attempts.sh --min-age-hours 6
```

It only ever considers `datasets/*/versions/*/attempt-*/`, skips any dataset with no manifest
(an ingestion may be in flight), skips anything modified within `--min-age-hours` (default 24),
and does nothing at all without `--delete`.

---

## 3. Serving one question

1. **Resolve the manifest.** One `HEAD` distinguishes "not published" (`409
   DatasetNotPublished`) from "published but broken", and bounds the body before it is buffered.
2. **Resolve the target.** A gene symbol or rsID becomes one or more
   `(chrom, pos, ref, alt)` rows from the versioned ClinVar snapshot. An unplaceable target is
   `422 TargetNotResolvable` — thrown before anything has been headed, opened or read, so it can
   never widen into a scan.
3. **Select objects.** Only manifest-declared objects whose partition matches and whose
   `[minPos, maxPos]` contains a resolved coordinate. An empty selection is `404
   TargetNotPresent`, never a wider scan.
4. **Re-verify.** Every selected object is HEADed and compared against the manifest — existence,
   ETag, version id, size and the `x-amz-meta-sha256` content checksum — immediately before the
   scan. Publication proved they were right *then*; a manifest is long lived. A mismatch is
   `409 ObjectVerificationFailed`, and no partial evidence is returned.
5. **Scan.** An in-memory session, an explicit URI list, the partition literal and bound
   positions directly above the scan, then a join against a small candidate relation.

### Limits, and what enforces them

| Limit | Value | Where |
| --- | --- | --- |
| Rows returned per target | 256 | `MAX_VARIANT_ROWS`, a SQL `LIMIT` |
| Parquet objects a manifest may declare | 128 | `MAX_DATASET_PARQUET_OBJECTS` |
| Manifest body | 1 MiB | `MAX_MANIFEST_BYTES`, checked by `HEAD` before buffering |
| Session memory | 512 MB | `SET memory_limit` |
| Session threads | 4 | `SET threads` |
| Query deadline | 10 s (`DUCKDB_QUERY_DEADLINE_MS`) | `connection.interrupt()`, then `504 QueryBudgetExceeded` |
| S3 request / connect timeout | 10 s / 3 s (`S3_REQUEST_TIMEOUT_MS`, `S3_CONNECT_TIMEOUT_MS`) | the SDK's default is *no* request timeout |
| S3 attempts | 3 (`S3_MAX_ATTEMPTS`) | |

The query deadline is a real deadline: a timer arms `connection.interrupt()`, so an overrunning
scan is cancelled in the engine rather than abandoned by a promise while the work continues.

### Metrics

Each served question emits one structured record, with every number measured rather than
estimated — `s3RequestCount` and `bytesRead` are read out of DuckDB's own `duckdb_logs`, so they
describe what the engine actually put on the wire:

```
[serving-metrics] {"datasetId":"demo-small-…","datasetChecksumSha256":"…",
  "referenceVersion":"demo-clinvar-grch38-v1","selectedFileCount":1,"inventorySize":4,
  "s3RequestCount":6,"bytesRead":384647,"queryLatencyMs":29}
```

No gene, rsID, position or genotype is recorded. A metrics stream is not a place to accumulate
somebody's clinical profile.

### Failure codes

| Status | Error | Meaning |
| --- | --- | --- |
| 400 | `UnknownDatasetKey`, `MissingDatasetId`, `MissingQuestion`, `MalformedRequestBody`, `UnrecognizedRequestField`, `DatasetResolutionFailed` (`DATASET_ID_UNSAFE`) | the request named something the API does not accept — including a dataset id that is not a single safe path segment, which never named a dataset in the first place |
| 404 | `IngestionRunNotFound`, `TargetNotPresent` | no such run; or the dataset provably does not contain the target |
| 409 | `DatasetNotPublished`, `ObjectVerificationFailed`, `ReferenceSnapshotMismatch`, `ReferenceBuildMismatch`, `DatasetPublicationConflict`, `DatasetResolutionFailed` (any other code) | the dataset exists as an id but cannot be served as published |
| 422 | `TargetNotResolvable`, `TargetResolutionLimitExceeded` | the reference snapshot cannot place the gene or rsID, or resolves it to more coordinates than one query may return |
| 503 | `IngestionServiceUnavailable`, `RemoteDatasetUnavailable`, `ReferenceSnapshotUnavailable`, `HttpfsExtensionUnavailable` | an upstream gave out; retryable |
| 504 | `QueryBudgetExceeded`, `SessionConfigurationTimedOut` | the query deadline fired |

---

## 4. Running it

### With containers

```bash
make up      # docker compose up -d --build
make seed    # ./scripts/seed_demo_s3.sh
make demo    # ingest demo-small end to end and ask a question
make logs    # docker compose logs -f
make down
```

`docker-compose.yml` brings up `temporal`, `minio`, `qdrant`, `ts-api`, `ts-control-worker` and
`rust-ingestion-worker`, plus a one-shot `minio-provision` job that creates the two buckets and
the API's scoped identity. Health dependencies are explicit: the workers wait for Temporal and
MinIO to report healthy and for provisioning to complete. Ports bind to `127.0.0.1`.

It is deliberately small — the local stack, and nothing else. One property does hold in both
files and is not configurable: **no volume is ever shared between a TypeScript container and the
Rust one.** Only S3 object references cross the language boundary; a shared mount would let a
local path become the contract by accident.

### The deployment overlay

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

`docker-compose.prod.yml` overlays the same six services rather than redefining them, and adds
what should be true of a real deployment and only gets in the way on a laptop:

- **No credential defaults.** `${VAR:?…}` fails the deployment when a secret is unset instead of
  quietly starting with `admin`/`password123`.
- **Nothing published but the API.** MinIO, its console, the Temporal frontend and the Web UI
  stay on the compose network; reach them over an SSH tunnel.
- **The API reads, and reads narrowly.** It runs as the scoped MinIO identity whose policy allows
  `s3:GetObject` under `genomic-artifacts/datasets/` and nothing else — no writes, no other
  bucket, and no `ListBucket`, because the manifest *is* the inventory and the serving path never
  lists a prefix. Locally it uses the root credentials, which is one less moving part to explain.
- **Read-only root filesystems** on all three application containers, with a small `/tmp` tmpfs.
- **Bounded data plane.** `rust-ingestion-worker` gets 4 CPUs, 4 GB of memory, 512 pids and a
  2 GB tmpfs staging mount. The staging cap sits below the memory limit on purpose: a tmpfs is
  memory-backed, so staged bytes count against `mem_limit`, and a larger figure would be one that
  could never actually be filled. A source too large for those bounds fails that container rather
  than the host. Locally the staging directory is unbounded.
- **Request and query budgets** (`S3_*_TIMEOUT_MS`, `S3_MAX_ATTEMPTS`,
  `DUCKDB_QUERY_DEADLINE_MS`) and bounded log rotation.

The limitations it does *not* fix — a single-node Temporal dev server, single-node MinIO, no
authentication — are listed at the top of the file.

Neither runtime image invokes Cargo or downloads a DuckDB extension. The Rust image copies only
the release binary and a CA bundle out of its builder; the TypeScript image preinstalls `httpfs`
(engine `v1.5.5`, extension `827222f`) and builds the ClinVar snapshot at build time, and the
serving session forces `autoinstall_known_extensions`/`autoload_known_extensions` off. A cold
container on a Docker network with no route off the host still answers `/ask`; that is asserted
by `tests/integration/offline_container_serving.test.ts`.

### Without containers

Four terminals, in this order:

```bash
# 1. infrastructure
docker compose up -d minio qdrant
make temporal-dev                       # temporal server start-dev, UI on :8233

# 2. the reference snapshot (once) and the seeded sources (once)
make reference-snapshot
make seed

# 3. the two workers
export S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=admin S3_SECRET_KEY=password123
make worker                             # TypeScript control plane

export TEMPORAL_ADDRESS=localhost:7233 INGESTION_STAGING_ROOT=$(mktemp -d)
cargo run --release --bin temporal_worker

# 4. the API
make api
```

Then `make demo`, or by hand:

```bash
curl -sS -X POST localhost:3000/api/ingestions \
  -H 'content-type: application/json' -d '{"datasetKey":"demo-small"}'
# → {"datasetId":"demo-small-…","datasetKey":"demo-small","workflowId":"genomic-ingestion-…"}

curl -sS localhost:3000/api/ingestions/genomic-ingestion-demo-small-…
# → {"state":"COMPLETED", …}

curl -sS -X POST localhost:3000/ask -H 'content-type: application/json' \
  -d '{"datasetId":"demo-small-…","question":"Can I drink coffee?"}' | jq .
```

### Environment

| Variable | Default | Used by |
| --- | --- | --- |
| `PORT` | `3000` | API |
| `TEMPORAL_HOST` | `localhost:7233` | API, control-plane Worker |
| `TEMPORAL_ADDRESS` | `localhost:7233` | Rust Worker |
| `TEMPORAL_NAMESPACE` | `default` | Rust Worker, control-plane Worker (`worker.ts`) |
| `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` | — (required) | all three |
| `S3_REGION`, `S3_FORCE_PATH_STYLE` | `us-east-1`, `true` | all three |
| `S3_ARTIFACT_BUCKET` | `genomic-artifacts` | API, control-plane Worker |
| `S3_REQUEST_TIMEOUT_MS`, `S3_CONNECT_TIMEOUT_MS`, `S3_MAX_ATTEMPTS` | `10000`, `3000`, `3` | API, control-plane Worker |
| `DUCKDB_QUERY_DEADLINE_MS` | `10000` | API |
| `DUCKDB_ALLOW_EXTENSION_INSTALL` | `false` | API — leave off in an image that ships `httpfs` |
| `INGESTION_STAGING_ROOT` | a temp directory | Rust Worker |
| `CLINVAR_COORDINATES_TSV`, `CLINVAR_SNAPSHOT_DB` | the committed TSV, `data/reference/` | reference snapshot |
| `CEREBRAS_API_KEY`, `CEREBRAS_MODEL` | unset, `llama-3.3-70b` | API — unset means the deterministic local answer path |
| `QDRANT_HOST`, `OLLAMA_HOST` | `http://localhost:6333`, `http://localhost:11434` | optional literature search |

---

## 5. Tests

```bash
npm test                  # TS unit tests + Rust unit tests + typecheck of tests/integration/**; no infrastructure
npm run test:integration  # MinIO + a Temporal dev server + the real Rust worker + Docker
```

`npm test` needs `cargo` on `PATH`; rustup installs it outside a login shell's path on some
machines (`export PATH="$(rustup which cargo | xargs dirname):$PATH"`).

The 12 MinIO-backed Rust object-store adapter tests in
`rust-ingestion-worker/tests/minio_object_store_test.rs` are `#[ignore]`d by `cargo test` for
hermeticity; run them with `make test-rust-integration` (brings up MinIO itself) — `make
test-integration` runs this before the suites below.

| Suite | Proves |
| --- | --- |
| `cross_language_ingestion` | the whole slice from `POST /api/ingestions` to `/ask`; that the manifest was written only after every declared object existed; that every partition of the `na12878-full` ingest — the real 3,893,341-variant GIAB VCF when `data/na12878_hg001.vcf.gz` is present, otherwise a synthetic source of comparable volume (see the note below the suite table) — is *physically* sorted by `(pos, ref, alt)` |
| `dataset_isolation` | two datasets with opposite genotypes never leak a row, a URI or an S3 request into each other; a corrupted or removed partition is an explicit refusal, not partial evidence |
| `remote_parquet_probe` | feasibility gate: an in-memory DuckDB session can query an explicit remote S3 Parquet URI with `chrom`/`pos` predicates, pruned, without downloading the whole object |
| `remote_parquet_pruning` | from an HTTP proxy's request log: a chromosome-12 target reads one object, part of it, and never touches chromosome 1 or 15 |
| `offline_container_serving` | a cold container with no route off the host answers `/ask` from remote Parquet |
| `temporal_rust_probe` | retry and cancellation semantics against real Temporal history |
| `serving-invariants` (unit) | the *absence* of a second way to answer across `ts-api-agent/src` and `rust-ingestion-worker/src`: no local database, no glob, no fixture fallback, no subprocess, no bare `hive_partitioning`. `tests/integration/**` is outside both collections and is not swept — it is typechecked instead (`tsconfig.integration.json`, run by `npm test`). |

Every suite creates its own buckets under a per-run name and removes exactly those. None of them
deletes anything it did not create.

The `na12878-full` scenario uses the real GIAB VCF when `data/na12878_hg001.vcf.gz` is present
(`make download-real-data`) and otherwise substitutes a synthetic source of comparable volume,
saying so in the log. That substitution happens only inside the test process; no runtime path
may fall back to a fixture, and a unit test enforces it.

---

## 6. Known limitations

Repeated from the README because they matter more than the feature list:

- Single-node Temporal dev server on SQLite, single-node MinIO, no TLS anywhere.
- No authentication or authorization on `/ask`. Any caller that can reach the port can read any
  published dataset by id.
- The Temporal Rust SDK is **Public Preview**, pinned `=0.5.0`, and outside Temporal's API
  stability guarantees.
- The ClinVar snapshot is a 15-row demo extract, not ClinVar. No liftover; GRCh38 only.
- PubMed "abstracts" are paragraphs synthesized from real NCBI metadata, not real abstract text.
- No benchmark harness, and therefore no throughput claim.
