//! The local → wire inventory mapping: the S3 key one exported file gets, the [`ParquetObject`]
//! list built from the local descriptors and the upload receipts, and the two checks that must
//! hold before that list may be returned to the Workflow.
//!
//! This is where the `variants/` segment enters, and the only place it does. `relativePath` — the
//! unit the dataset checksum is computed from — never carries it, which is what makes the
//! checksum reproducible under any attempt prefix.

use super::IngestionFailure;
use crate::artifact::{dataset_checksum_sha256, LocalParquetFile};
use crate::contracts::{ParquetObject, PARQUET_SCHEMA_FINGERPRINT, VARIANTS_SEGMENT};
use crate::object_store::UploadedObject;

/// Composes `{attemptPrefix}variants/{relativePath}`.
///
/// The `variants/` segment lives only in the S3 key. `relativePath` — the unit the dataset
/// checksum is computed from — never carries it, which is what makes the checksum reproducible
/// under any attempt prefix.
pub fn object_key_for(attempt_prefix: &str, relative_path: &str) -> String {
    format!("{attempt_prefix}{VARIANTS_SEGMENT}{relative_path}")
}

/// Pairs the processor's local descriptors with the identities S3 returned.
///
/// `byteSize` comes from the *local* descriptor, because [`UploadedObject`] does not carry one
/// and the TypeScript verifier compares `head.contentLength` against it before it will publish.
/// Everything else that identifies the object — bucket, key, ETag, version ID — comes from the
/// upload receipt, and the receipt's key must be exactly the one this layer composed.
pub fn published_inventory(
    attempt_prefix: &str,
    files: &[LocalParquetFile],
    receipts: &[UploadedObject],
) -> Result<Vec<ParquetObject>, IngestionFailure> {
    if files.len() != receipts.len() {
        return Err(IngestionFailure::validation(format!(
            "the export produced {} Parquet files but {} were uploaded; a published inventory \
             must describe every one of them",
            files.len(),
            receipts.len()
        )));
    }

    let mut inventory = Vec::with_capacity(files.len());
    for (file, receipt) in files.iter().zip(receipts) {
        let expected_key = object_key_for(attempt_prefix, &file.relative_path);
        if receipt.key != expected_key {
            return Err(IngestionFailure::validation(format!(
                "'{}' was uploaded to '{}' but its descriptor composes '{expected_key}'; a \
                 descriptor and an upload from different attempts must never be published together",
                file.relative_path, receipt.key
            )));
        }
        if receipt.bucket != receipts[0].bucket {
            return Err(IngestionFailure::validation(format!(
                "'{}' is in bucket '{}' but the inventory started in '{}'",
                receipt.key, receipt.bucket, receipts[0].bucket
            )));
        }
        inventory.push(ParquetObject {
            bucket: receipt.bucket.clone(),
            key: receipt.key.clone(),
            etag: receipt.etag.clone(),
            version_id: receipt.version_id.clone(),
            chrom: file.chrom.clone(),
            checksum_sha256: file.checksum_sha256.clone(),
            byte_size: file.byte_size,
            row_count: file.row_count,
            min_pos: file.min_pos,
            max_pos: file.max_pos,
        });
    }
    Ok(inventory)
}

/// Re-derives the dataset checksum from the *published* inventory and compares it to the one the
/// processor computed locally.
///
/// The relative path is recovered by stripping `{attemptPrefix}variants/`, exactly as
/// `dataset-checksum.ts` does before it will accept a publish. Running the processor's own
/// canonicalisation over the mapped keys is what proves the mapping layer did not disturb the
/// content identity — a dropped `variants/` segment, a descriptor paired with the wrong object,
/// a statistic transcribed wrongly.
pub fn assert_inventory_checksum(
    attempt_prefix: &str,
    objects: &[ParquetObject],
    expected: &str,
) -> Result<(), IngestionFailure> {
    let variants_prefix = format!("{attempt_prefix}{VARIANTS_SEGMENT}");
    let mut files = Vec::with_capacity(objects.len());
    for object in objects {
        let relative_path = object.key.strip_prefix(&variants_prefix).ok_or_else(|| {
            IngestionFailure::validation(format!(
                "object key '{}' is not below '{variants_prefix}'",
                object.key
            ))
        })?;
        files.push(LocalParquetFile {
            relative_path: relative_path.to_string(),
            chrom: object.chrom.clone(),
            checksum_sha256: object.checksum_sha256.clone(),
            byte_size: object.byte_size,
            row_count: object.row_count,
            min_pos: object.min_pos,
            max_pos: object.max_pos,
            schema_fingerprint: PARQUET_SCHEMA_FINGERPRINT.to_string(),
        });
    }

    let computed = dataset_checksum_sha256(&files);
    if computed != expected {
        return Err(IngestionFailure::validation(format!(
            "the published inventory hashes to '{computed}' but the processor computed \
             '{expected}'"
        )));
    }
    Ok(())
}

/// Every published object must be in the bucket the input targets — the artifact bucket is
/// deployment configuration, and an inventory that names another one would fail the TypeScript
/// verifier's `BUCKET_MISMATCH` after the objects had already been written.
pub(super) fn assert_published_bucket(
    objects: &[ParquetObject],
    expected_bucket: &str,
) -> Result<(), IngestionFailure> {
    for object in objects {
        if object.bucket != expected_bucket {
            return Err(IngestionFailure::validation(format!(
                "'{}' was published to bucket '{}', the input targets '{expected_bucket}'",
                object.key, object.bucket
            )));
        }
    }
    Ok(())
}
