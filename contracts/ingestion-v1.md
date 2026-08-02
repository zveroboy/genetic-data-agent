# Ingestion contract v1

Documentation of the frozen cross-language wire contract. It is **not** a second source of
truth: the executable definitions are

| Language   | File                                                |
| ---------- | --------------------------------------------------- |
| TypeScript | `ts-api-agent/src/application/ingestion-contracts.ts` |
| Rust       | `rust-ingestion-worker/src/contracts.rs`              |
| Both       | `contracts/fixtures/*.json` (golden payloads)         |

Both languages parse the same golden fixtures in their test suites, so a change on one side
that is not mirrored on the other fails immediately:

```bash
node --test ts-api-agent/src/application/ingestion-contracts.test.ts
cargo test --manifest-path rust-ingestion-worker/Cargo.toml contracts
```

## Rules

- Payloads carry JSON-compatible primitives only, with **camelCase** field names. Dates,
  BigInt, buffers, class instances and serialized language-native errors are prohibited.
- Every wire object is closed: `.strict()` in Zod, `deny_unknown_fields` in serde. An
  unexpected field is an error, never a silently ignored one.
- A field documented as `T | null` is **nullable, not optional**: the key must be present and
  may carry `null`. TypeScript spells this `.nullable()`; Rust must spell it
  `#[serde(deserialize_with = "required_nullable")]` on the `Option<T>`, because a bare
  `Option<T>` silently accepts an omitted key even under `deny_unknown_fields`.
- Integers are plain JSON numbers. Rust narrows some of them (see the type column); a value
  outside the Rust range is a contract violation, not a truncation.
- Only the seeded catalog keys `demo-small` and `na12878-full` are accepted. Arbitrary
  uploads, URLs, `s3://` URIs and filesystem paths are rejected by
  `ts-api-agent/src/application/dataset-catalog.ts`; nothing downstream sanitises them.
- Published Parquet objects are immutable and become queryable only once a matching
  `manifest.json` exists.
- `ts-api-agent/src/application/ingestion-contracts.ts` is free of Node built-ins at import
  and evaluation time, so Temporal **workflow** code may import its constants, schemas and
  types by value. Everything needing `node:crypto` lives in
  `ts-api-agent/src/application/dataset-checksum.ts`, which workflow code must not import; a
  test enforces the boundary.

## Versioning

| Constant           | Value | Meaning                                                        |
| ------------------ | ----- | -------------------------------------------------------------- |
| `contractVersion`  | `1`   | Activity input envelope. Rejected during deserialization if not 1, on both sides: `z.literal(1)` in TypeScript, the validating `ContractVersion` newtype in Rust. |
| `layoutVersion`    | `1`   | Physical prefix shape and partition directories.                |
| `schemaVersion`    | `1`   | Logical Parquet column set.                                     |
| `artifactFormat`   | `"parquet-dataset"` | The only supported artifact format at layout 1.    |

## Object layout

```text
s3://<target.bucket>/datasets/{datasetId}/versions/{artifactVersion}/attempt-{attempt}/variants/chrom=<value>/part-NNN.parquet
s3://<target.bucket>/datasets/{datasetId}/manifest.json
```

### Key composition

An object key is composed of exactly three parts:

```text
key          = attemptPrefix + "variants/" + relativePath
attemptPrefix = allowedPrefix + "attempt-{attempt}/"
allowedPrefix = "datasets/{datasetId}/versions/{artifactVersion}/"
relativePath  = "chrom=<value>/part-NNN.parquet"
```

**`relativePath` does not contain the `variants/` segment.** That is deliberate and load
bearing: `relativePath` is the unit the dataset checksum is computed from, and the Rust
processor emits it relative to its *local* Parquet export directory, with no S3 knowledge.
The `variants/` segment is contributed only by the S3 mapping layer. Consequently the same
dataset content yields the same `datasetChecksumSha256` whatever prefix it is uploaded under.

Validation strips `{attemptPrefix}variants/` — not merely `{attemptPrefix}` — to recover
`relativePath`, and rejects with `KEY_OUTSIDE_ALLOWED_PREFIX` any key that does not sit under
that exact prefix, including one that omits `variants/`. The segment is named
`VARIANTS_SEGMENT` in both languages.

- `target.allowedPrefix` is `datasets/{datasetId}/versions/{artifactVersion}/`. It is
  **derived and re-checked**, never trusted as sent: a widened value such as `datasets/`
  would otherwise satisfy every containment check. See `ALLOWED_PREFIX_MISMATCH`.
- `attemptPrefix` is strictly below `allowedPrefix` and unique per activity attempt, so a
  retry can never append to or overwrite a previous attempt's objects.
- Dataset, version and attempt segments must not contain `=`; `=` appears only in the
  `chrom=<value>` partition directory. `variants` is a plain segment for the same reason the
  attempt segment is `attempt-{attempt}`: it must not become an accidental Hive column.
- `partitionSpec` is `["chrom"]`; `sortOrder` is `["chrom", "pos", "ref", "alt"]`.
- The physical Parquet schema is `pos`, `rsid`, `ref`, `alt`, `gt_raw`. `chrom` is not a
  physical column: it is restored from the partition directory via
  `read_parquet(..., hive_partitioning = true)`.

`schemaFingerprint` is the SHA-256 of the canonical column description

```text
pos:UINTEGER:NOT NULL;rsid:VARCHAR:NULL;ref:VARCHAR:NOT NULL;alt:VARCHAR:NOT NULL;gt_raw:VARCHAR:NOT NULL
```

which is `89e4e0a61728e9776376f7550d09426acba14bd486c68a918e66fb11d437d7de`.

## Payloads

### `BuildDatasetArtifactInput`

Activity `buildDatasetArtifact` on task queue `genomic-ingestion-rust`. Golden payload:
`contracts/fixtures/build-dataset-artifact.input.json`.

| Field                  | JSON type       | Notes                                            |
| ---------------------- | --------------- | ------------------------------------------------ |
| `contractVersion`      | number          | Exactly `1`.                                      |
| `datasetId`            | string          | Single safe path segment.                         |
| `datasetKey`           | string          | `"demo-small"` \| `"na12878-full"`.               |
| `source.bucket`        | string          | From the catalog, never from API input.           |
| `source.key`           | string          | From the catalog, never from API input.           |
| `source.etag`          | string          | Verified before and after download.               |
| `source.versionId`     | string \| null  | `null` when the bucket is unversioned.            |
| `source.contentLength` | number          | Bytes.                                            |
| `reference.build`      | string          | `"GRCh38"` for both seeded datasets.              |
| `reference.version`    | string          | `"demo-clinvar-grch38-v1"`.                       |
| `target.bucket`        | string          | Artifact bucket.                                  |
| `target.artifactVersion` | string        | Single safe path segment.                         |
| `target.allowedPrefix` | string          | Ends with `/`; the only writable prefix.          |

### `BuildDatasetArtifactResult`

Golden payload: `contracts/fixtures/build-dataset-artifact.result.json`.

| Field                   | JSON type | Notes                                           |
| ----------------------- | --------- | ----------------------------------------------- |
| `attemptPrefix`         | string    | Ends with `/`, strictly below `allowedPrefix`.   |
| `datasetChecksumSha256` | string    | 64 lowercase hex chars; see below.               |
| `variantCount`          | number    | Accepted records.                                |
| `rejectedRecordCount`   | number    | Malformed records skipped, not fatal.            |
| `referenceBuild`        | string    | Must equal `reference.build`.                    |
| `processorVersion`      | string    | Provenance of the Rust processor.                |
| `parquetObjects`        | array     | Canonically ordered; at least one entry.         |

Each `parquetObjects` entry:

| Field            | JSON type      | Notes                                          |
| ---------------- | -------------- | ---------------------------------------------- |
| `bucket`         | string         | Same bucket for every entry.                    |
| `key`            | string         | `{attemptPrefix}variants/chrom=<value>/part-NNN.parquet`. |
| `etag`           | string         | Returned by the upload.                         |
| `versionId`      | string \| null | `null` when the bucket is unversioned.          |
| `chrom`          | string         | Must equal the `chrom=<value>` directory.       |
| `checksumSha256` | string         | SHA-256 of the file content, lowercase hex.     |
| `byteSize`       | number         | Object size in bytes.                           |
| `rowCount`       | number         | Rows in the file.                               |
| `minPos`         | number         | Minimum `pos`, used for row-group pruning. Bounded by `u32` — see below. |
| `maxPos`         | number         | Maximum `pos`, used for row-group pruning. Bounded by `u32` — see below. |

`minPos`/`maxPos` are `u32` in Rust, matching the DuckDB `UINTEGER` `pos` column and
`models::UserVariant::pos`. TypeScript validates them only as non-negative safe integers, so
TypeScript is the wider of the two: a value above `4294967295` parses in TypeScript and fails
to deserialize in Rust. That bound is a deliberate part of the contract, not an oversight —
no human chromosome exceeds it. Producers must stay within `u32`.

### `DatasetManifest`

Written last, by TypeScript, only after every declared object has been verified. Golden
payload: `contracts/fixtures/dataset-manifest.json`. It contains every
`BuildDatasetArtifactResult` field with the same name and type, plus:

| Field               | JSON type | Notes                                             |
| ------------------- | --------- | ------------------------------------------------- |
| `datasetId`         | string    | Binds the manifest to its dataset prefix.          |
| `artifactFormat`    | string    | `"parquet-dataset"`.                               |
| `layoutVersion`     | number    | Exactly `1`.                                       |
| `schemaVersion`     | number    | Exactly `1`.                                       |
| `schemaFingerprint` | string    | SHA-256 of the canonical column description.       |
| `artifactVersion`   | string    | Single safe path segment.                          |
| `referenceVersion`  | string    | Versioned annotation snapshot.                     |
| `partitionSpec`     | string[]  | Exactly `["chrom"]`.                               |
| `sortOrder`         | string[]  | Exactly `["chrom", "pos", "ref", "alt"]`.          |

### `IngestionHeartbeat`

Activity heartbeat detail. Phases, in order: `DOWNLOADING_SOURCE`, `PARSING`,
`WRITING_DUCKDB`, `EXPORTING_PARQUET`, `UPLOADING_PARTITION`, `FINALIZING`.

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

`currentPartition` is `null` while no chromosome partition is being processed.

## Dataset content checksum

`datasetChecksumSha256` identifies dataset *content*, independent of which activity attempt
produced it. It is computed from **relative** descriptors:

1. For each Parquet object, take `relativePath = key` with the `{attemptPrefix}variants/`
   prefix removed — so `relativePath` is `chrom=<value>/part-NNN.parquet`, never carrying the
   `variants/` segment. A key not under that prefix is a validation failure, not input to the
   hash.
2. Emit one line per object, tab separated, terminated by `\n`:

   ```text
   {chrom}\t{relativePath}\t{checksumSha256}\t{byteSize}\t{rowCount}\t{minPos}\t{maxPos}\n
   ```

   The four integer fields are rendered as **unpadded base-10 ASCII digits**: no leading
   zeros, no `+`/`-` sign, no thousands separators, no exponent, no decimal point. `0`
   renders as the single character `0`. This is what JavaScript's
   `Number.prototype.toString()` and Rust's `Display for u32`/`u64` both produce for the
   non-negative integers these fields hold, so neither implementation needs a format string.
3. Sort the lines byte-wise ascending by `(chrom, relativePath)` — a UTF-8 byte comparison,
   not a numeric chromosome order, so `"1" < "10" < "12" < "2"`.
4. The checksum is the lowercase hex SHA-256 of the concatenated lines.

Bucket, key prefix, the `variants/` segment, ETag and version ID are deliberately excluded:
re-running the same source into a different attempt prefix must reproduce the same checksum,
and the Rust processor must be able to compute it from local descriptors before any upload.

## Canonical inventory invariants

Enforced by `assertCanonicalArtifactInventory` before anything is published or queried:

| Code                                    | Rejects                                                     |
| --------------------------------------- | ----------------------------------------------------------- |
| `EMPTY_INVENTORY`                       | A dataset declaring no Parquet object.                       |
| `ALLOWED_PREFIX_MISMATCH`               | An input `allowedPrefix` that is not the prefix derived from its own `datasetId`/`artifactVersion`. |
| `ATTEMPT_PREFIX_OUTSIDE_ALLOWED_PREFIX` | An attempt prefix not strictly below the allowed prefix.     |
| `KEY_OUTSIDE_ALLOWED_PREFIX`            | An object key outside `{attemptPrefix}variants/`, including one omitting `variants/`. |
| `BUCKET_MISMATCH`                       | A descriptor in an unexpected bucket.                        |
| `DUPLICATE_KEY`                         | The same key declared more than once.                        |
| `NONCANONICAL_ORDER`                    | Descriptors not ordered by `(chrom, relativePath)`.          |
| `PARTITION_MISMATCH`                    | `chrom` contradicting the `chrom=<value>` directory.         |
| `DATASET_CHECKSUM_MISMATCH`             | A checksum that the descriptor list does not reproduce.      |
| `SCHEMA_FINGERPRINT_MISMATCH`           | A manifest not describing the frozen Parquet schema.         |
| `REFERENCE_BUILD_MISMATCH`              | A result whose build contradicts the requested reference.    |

Both an activity result and a published manifest derive their allowed prefix from a
`datasetId`/`artifactVersion` pair rather than reading `target.allowedPrefix` off the wire, so
neither can claim objects belonging to another dataset or artifact version.

## Failure taxonomy

Stable Temporal application failure type names raised by the Rust activity and matched by
name in the TypeScript workflow's retry policy.

| Failure type               | Retryable | Raised when                                                     |
| -------------------------- | --------- | ---------------------------------------------------------------- |
| `InvalidVcfFormat`         | no        | The source VCF is unparseable; retrying cannot help.              |
| `SourceObjectChanged`      | no        | The source ETag/version changed under the attempt.                |
| `ObjectStoreUnavailable`   | yes       | S3/MinIO is unreachable or returning transient errors.            |
| `ArtifactWriteFailed`      | yes       | A transient local disk or upload failure.                         |
| `ArtifactValidationFailed` | no        | The produced artifact fails a deterministic invariant above.      |

## Fixture fallback

Fixture data may only be substituted inside automated tests. No runtime path may silently
fall back to a fixture.
