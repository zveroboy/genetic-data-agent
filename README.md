# Genomic VCF Ingestion & Insight Engine

A cross-language vertical slice: a **TypeScript Temporal Workflow** orchestrates a **Rust
Temporal Activity** that turns a genomic VCF into immutable, chromosome-partitioned Parquet in
object storage, and an HTTP API answers questions about one published dataset by querying that
Parquet **remotely**, through an in-memory DuckDB with `httpfs`.

This is an educational MVP built to demonstrate a specific set of engineering decisions. It is
not production software: see [Scope and limits](#scope-and-limits) for what it deliberately does
not do.

---

## What actually happens

```
POST /api/ingestions {"datasetKey":"demo-small"}
        │
        ▼
GenomicIngestionWorkflow                                   queue: genomic-control-plane   (TS)
        │
        ├─ inspectDatasetSource   pins the seeded catalog key to an immutable S3 identity (TS)
        │
        ├─ buildDatasetArtifact   queue: genomic-ingestion-rust                         (Rust)
        │     streams the S3 object → local DuckDB staging → chromosome-partitioned
        │     Zstandard Parquet, sorted by (pos, ref, alt) → uploads to an attempt-unique,
        │     immutable prefix. Heartbeats its phase throughout.
        │
        └─ publishDataset         HEAD-verifies every uploaded object, then writes
                                  datasets/{datasetId}/manifest.json LAST                 (TS)
        ▼
POST /ask {"datasetId":"…","question":"Can I drink coffee?"}
        resolve the manifest → resolve the gene/rsID to (chrom, pos, ref, alt) against a
        versioned ClinVar snapshot → select only the manifest-declared objects that can
        contain those coordinates → read them over ranged HTTP with DuckDB httpfs
```

**The manifest is the only readiness signal.** Parquet objects exist under an attempt prefix
before anything is published; nothing queries them until a manifest names them. A run that fails
anywhere leaves an orphan attempt prefix that no query path will ever look at.

### Datasets are seeded, not uploaded

There is no upload endpoint. The API accepts exactly two catalog keys — `demo-small` and
`na12878-full` — and resolves each to a bucket and object key from a server-side allowlist
(`ts-api-agent/src/application/dataset-catalog.ts`). URLs, `s3://` URIs, bucket names and
filesystem paths are rejected at the edge rather than sanitised downstream. This simulates "a
user's genome arrived in object storage" without exposing the ingestion path to caller-supplied
locations.

- **`demo-small`** — `tests/fixtures/demo_user.vcf`, four synthetic clinical sites. Ingests in about a
  second; used by the demo below.
- **`na12878-full`** — the public [GIAB NA12878/HG001](https://www.nist.gov/programs-projects/genome-bottle)
  GRCh38 benchmark VCF (~126 MB gzipped, 3,893,341 variants). Fetched by
  `make download-real-data`; not tracked in this repository.

### Rust does the data plane, locally, then hands over S3 objects

The Rust Activity streams the source object, parses VCF records, stages them in a **local**
DuckDB database in its own per-attempt workspace, and exports one Zstandard Parquet file per
chromosome with `COPY … ORDER BY pos, ref, alt` — one `COPY` per partition, because a single
`COPY … PARTITION_BY` does not preserve the query's order inside the files it writes. The
staging database and the export directory are removed when the attempt ends.

Nothing local crosses the language boundary. The Activity result is a list of S3 object
descriptors — bucket, key, ETag, content SHA-256, byte size, row count, `minPos`/`maxPos` — and
the TypeScript side never sees a path.

### Serving reads remote Parquet, and keeps nothing

`/ask` opens a fresh in-memory DuckDB session per request, loads `httpfs`, attaches
bucket-scoped S3 credentials, reads an explicit list of `s3://` URIs built from validated
manifest descriptors, and closes. There is **no per-dataset `.duckdb` file, no download and no
local cache** of anybody's variants — the only thing on local disk is the versioned ClinVar
coordinate snapshot, which is reference data derived from a TSV tracked in this repository.

Pruning is by construction rather than by hope: the partition value is a literal directly above
the scan, positions are bound parameters, and only objects whose declared `[minPos, maxPos]`
can contain a resolved coordinate are listed at all. Measured on a real ingestion
(`tests/integration/remote_parquet_pruning.test.ts`, chromosome-12 target in a four-partition
dataset) through an HTTP proxy in front of MinIO that counts every request the engine makes, and
pinned by hard assertions in the test itself, not just logged: **zero bytes** read from either
non-matching row group in the selected object, **zero requests** against the chromosome-1 or
chromosome-15 objects, and **exactly one** EOF-terminating GET (httpfs's single speculative
footer probe), bounded under 64 KiB. Total bytes read is also checked, more loosely, at under
50% of the selected object's size. See that file for the exact figures on any given run.

### Reference data is global; user data is per-dataset

- **ClinVar (coordinates and clinical annotation).** A versioned snapshot,
  `demo-clinvar-grch38-v1`, built from `tests/fixtures/clinvar_coordinates_grch38.tsv` (15
  targets). It maps a gene symbol or rsID to `(chrom, pos, ref, alt)` plus phenotype, clinical
  significance and an evidence note. Every manifest records which snapshot version its dataset
  was ingested against, and a repository refuses to open when the snapshot on disk is a
  different version or genome build — coordinates from one build against Parquet written for
  another would silently answer the right question about the wrong position.
- **PubMed (literature).** Optional, and not part of the deterministic answer.
  `ts-api-agent/scripts/ingest_pubmed.ts` queries the NCBI E-utilities API for real PMIDs, titles, journals
  and years, and then **synthesizes a short descriptive paragraph from that metadata** — it does
  not download the real abstract text. Those synthesized paragraphs are embedded with Ollama
  (`nomic-embed-text`) into Qdrant. Treat a literature hit as "a real paper exists, here is its
  PMID", not as a quotation from it. Without Ollama and Qdrant the genotype answer stands and
  the API logs that literature search was unavailable.

### Answers are deterministic unless you configure a model

With no `CEREBRAS_API_KEY` set, `/ask` maps the question to a target gene, queries the genotype,
and composes the answer from the reference annotation. That is a real code path, not a stub: it
is what the demo below exercises. The mapping is five keyword families (caffeine, lactose,
statins, warfarin, SSRIs); a question outside all five still gets a real, evidenced answer, but
against the default target, `rs762551`/CYP1A2 — the answer names the rsID and gene it actually
queried, so this is never presented as an answer to a question it wasn't. With a key set, Cerebras
(`llama-3.3-70b`) drives the same `query_genotype` tool and writes the prose; the genotype and its
provenance are produced the same way either way.

Every answer carries provenance: dataset id, dataset content checksum, artifact/layout/schema
versions, schema fingerprint, reference build and version, and **the exact object URIs scanned**.

---

## Run it

Requires Docker, the [`temporal` CLI](https://docs.temporal.io/cli), a Rust toolchain, and the
AWS CLI — `scripts/seed_demo_s3.sh` (step 2 below) hard-exits without it, and
`tests/integration/remote_parquet_probe.test.ts`, part of `test:integration`, shells out to `aws`
directly.

```bash
make up      # temporal, minio, qdrant, ts-api, ts-control-worker, rust-ingestion-worker
make seed    # the two allowlisted source objects into s3://genomic-data/samples/
make demo    # ingest demo-small, wait for the manifest, ask a question
```

`make seed` is idempotent — it re-uploads an object only when its stored `sha256` metadata does
not match the local file — but the first run downloads the ~126 MB NA12878/HG001 VCF from NCBI
into `data/`. `make demo DATASET_KEY=na12878-full` then ingests the real genome instead.

`make demo` prints the ingestion response, polls the Workflow's own `getProgress` query until it
reports `COMPLETED`, and then asks about caffeine metabolism. The expected answer names
`rs762551` (CYP1A2) with genotype `C/C`, with provenance listing exactly one scanned object.

Watch the run in the Temporal Web UI at <http://localhost:8233>: `buildDatasetArtifact` appears
on the `genomic-ingestion-rust` task queue, with a `rust-ingestion-worker@…` worker identity and
heartbeat details carrying the current phase.

| Endpoint | |
| --- | --- |
| API | <http://localhost:3000> (a zero-build page at `/`) |
| Temporal Web UI | <http://localhost:8233> |
| MinIO console | <http://localhost:9001> (`admin` / `password123`) |

The whole thing by hand, without containers, is in **[GUIDE.md](GUIDE.md)**, along with the
environment variables, failure semantics and operational commands.

### Tests

```bash
npm test                 # 317 TypeScript unit tests + the Rust unit tests. No infrastructure.
npm run test:integration # end to end: MinIO, a Temporal dev server, the real Rust worker
```

The integration suite starts its own Temporal dev server per file and creates its own buckets,
so it does not collide with a running `docker compose` stack. It covers the whole slice
(`cross_language_ingestion`), cross-dataset isolation and corruption refusal
(`dataset_isolation`), request-level pruning (`remote_parquet_pruning`), and a cold container on
a network with no route off the host answering `/ask` (`offline_container_serving`).

---

## Scope and limits

Stated plainly, because a demo that overstates itself is worse than a small one.

- **Not production software.** Temporal runs as a single-node dev server on SQLite; MinIO is one
  node with no replication; there is no authentication, no authorization and no multi-tenancy in
  front of `/ask` — any caller who can reach the port can read any published dataset by id.
- **No performance claims.** This repository publishes no throughput figure, because it contains
  no benchmark harness to produce one. The only timings quoted anywhere are wall-clock
  observations printed by the integration tests on one developer machine (for scale: the full
  NA12878/HG001 ingest — 3,893,341 variants across 22 partitions — took ~39 s end to end,
  including upload, on an M-series laptop against local MinIO). Treat those as anecdotes, not
  measurements.
- **The reference snapshot is a 15-row demo extract**, not ClinVar. A question about a gene
  outside it is answered with an explicit "not present in reference snapshot", never a guess.
- **PubMed abstracts are synthesized from metadata** (see above).
- **The Temporal Rust SDK is Public Preview** and pinned to `=0.5.0`. It is not covered by
  Temporal's API stability guarantees; a minor release may break the Activity registration
  surface. The pin is deliberate.
- **Reference/liftover work is out of scope.** Only GRCh38 is supported, and the producer
  normalises contigs to `1`–`22`, `X`, `Y`, `MT`; anything else is a rejected record.

Deliberate follow-ups, each its own piece of work rather than a flag: real PubMed abstract
ingestion, full ClinVar normalization and liftover, a general lakehouse table format,
authentication, and multi-tenant authorization.

## Where things are

| | |
| --- | --- |
| `contracts/ingestion-v1.md` | The frozen cross-language wire contract. Normative. |
| `ts-api-agent/src/application/` | Workflow, control-plane activities, contracts, catalog |
| `ts-api-agent/src/infrastructure/` | S3, DuckDB sessions, manifest resolution, reference, agent |
| `rust-ingestion-worker/src/` | VCF parsing, DuckDB staging, Parquet export, S3 upload, Activity |
| `tests/integration/` | The end-to-end suites described above |
| `SPEC.md` / `docs/superpowers/` | Original specification and the plan/design records |
