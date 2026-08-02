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
- Only the seeded catalog keys `demo-small` and `na12878-full` are accepted. Arbitrary
  uploads, URLs, `s3://` URIs and filesystem paths are rejected by
  `ts-api-agent/src/application/dataset-catalog.ts`; nothing downstream sanitises them.
- Published Parquet objects are immutable and become queryable only once a matching
  `manifest.json` exists.
- Workflow code must import the TypeScript module with `import type` only. The checksum
  helper uses `node:crypto`, which is unavailable inside the Temporal workflow sandbox.

## Versioning

| Constant           | Value | Meaning                                                        |
| ------------------ | ----- | -------------------------------------------------------------- |
| `contractVersion`  | `1`   | Activity input envelope. Rejected by the input schema if not 1. |
| `layoutVersion`    | `1`   | Physical prefix shape and partition directories.                |
| `schemaVersion`    | `1`   | Logical Parquet column set.                                     |
| `artifactFormat`   | `"parquet-dataset"` | The only supported artifact format at layout 1.    |

## Object layout

```text
s3://<target.bucket>/datasets/{datasetId}/versions/{artifactVersion}/attempt-{attempt}/chrom=<value>/<file>.parquet
s3://<target.bucket>/datasets/{datasetId}/manifest.json
```

- `target.allowedPrefix` is `datasets/{datasetId}/versions/{artifactVersion}/`.
- `attemptPrefix` is strictly below `allowedPrefix` and unique per activity attempt, so a
  retry can never append to or overwrite a previous attempt's objects.
- Dataset, version and attempt segments must not contain `=`; `=` appears only in the
  `chrom=<value>` partition directory.
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
| `key`            | string         | Below `attemptPrefix`.                          |
| `etag`           | string         | Returned by the upload.                         |
| `versionId`      | string \| null | `null` when the bucket is unversioned.          |
| `chrom`          | string         | Must equal the `chrom=<value>` directory.       |
| `checksumSha256` | string         | SHA-256 of the file content, lowercase hex.     |
| `byteSize`       | number         | Object size in bytes.                           |
| `rowCount`       | number         | Rows in the file.                               |
| `minPos`         | number         | Minimum `pos`, used for row-group pruning.      |
| `maxPos`         | number         | Maximum `pos`, used for row-group pruning.      |

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

1. For each Parquet object, take `relativePath = key` with `attemptPrefix` removed.
2. Emit one line per object, tab separated, terminated by `\n`:

   ```text
   {chrom}\t{relativePath}\t{checksumSha256}\t{byteSize}\t{rowCount}\t{minPos}\t{maxPos}\n
   ```

3. Sort the lines byte-wise ascending by `(chrom, relativePath)` — a UTF-8 byte comparison,
   not a numeric chromosome order, so `"1" < "10" < "12" < "2"`.
4. The checksum is the lowercase hex SHA-256 of the concatenated lines.

Bucket, key prefix, ETag and version ID are deliberately excluded: re-running the same
source into a different attempt prefix must reproduce the same checksum.

## Canonical inventory invariants

Enforced by `assertCanonicalArtifactInventory` before anything is published or queried:

| Code                                    | Rejects                                                     |
| --------------------------------------- | ----------------------------------------------------------- |
| `EMPTY_INVENTORY`                       | A dataset declaring no Parquet object.                       |
| `ATTEMPT_PREFIX_OUTSIDE_ALLOWED_PREFIX` | An attempt prefix not strictly below the allowed prefix.     |
| `KEY_OUTSIDE_ALLOWED_PREFIX`            | An object key outside the attempt prefix.                    |
| `BUCKET_MISMATCH`                       | A descriptor in an unexpected bucket.                        |
| `DUPLICATE_KEY`                         | The same key declared more than once.                        |
| `NONCANONICAL_ORDER`                    | Descriptors not ordered by `(chrom, relativePath)`.          |
| `PARTITION_MISMATCH`                    | `chrom` contradicting the `chrom=<value>` directory.         |
| `DATASET_CHECKSUM_MISMATCH`             | A checksum that the descriptor list does not reproduce.      |
| `SCHEMA_FINGERPRINT_MISMATCH`           | A manifest not describing the frozen Parquet schema.         |
| `REFERENCE_BUILD_MISMATCH`              | A result whose build contradicts the requested reference.    |

A manifest derives its allowed prefix from its own `datasetId` and `artifactVersion`, so a
manifest cannot claim objects belonging to another dataset or artifact version.

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
