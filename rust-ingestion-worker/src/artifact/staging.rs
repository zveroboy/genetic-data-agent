//! DuckDB staging: the source is read one record at a time and appended in bounded batches
//! into an attempt-local database, which is what keeps memory independent of the source size.

use std::io;
use std::path::Path;

use duckdb::{params, Connection};

use super::{report, ArtifactBuildRequest, ArtifactError, Stopped};
use crate::contracts::IngestionPhase;
use crate::models::{ProgressEvent, ProgressSink, UserVariant};
use crate::vcf::{open_vcf, VcfRecord};

/// The attempt-local staging schema. `chrom` is a real column here; it becomes a partition
/// directory only on export.
pub(super) const STAGING_SCHEMA: &str = "\
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
);";

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct StagingCounts {
    pub(super) accepted: u64,
    pub(super) rejected: u64,
    pub(super) bytes: u64,
}

/// Reads the source one record at a time and appends it in bounded batches.
pub(super) fn stage_variants(
    staging: &Connection,
    request: &ArtifactBuildRequest,
    batch_size: usize,
    progress: &dyn ProgressSink,
) -> Result<StagingCounts, Stopped> {
    let mut reader = open_vcf(&request.source_path, request.concurrency.bgzf_blocks)
        .map_err(|error| classify_source_error(&request.source_path, &error))?;

    report(progress, ProgressEvent::phase(IngestionPhase::Parsing))?;

    let mut counts = StagingCounts::default();
    let mut batch: Vec<UserVariant> = Vec::with_capacity(batch_size);

    while let Some(record) = reader.next() {
        match record.map_err(|error| classify_source_error(&request.source_path, &error))? {
            VcfRecord::Variant(variant) => batch.push(variant),
            VcfRecord::Rejected { .. } => counts.rejected += 1,
        }

        if batch.len() >= batch_size {
            counts.bytes = reader.bytes_read();
            flush_batch(staging, &mut batch, &mut counts, progress)?;
        }
    }

    counts.bytes = reader.bytes_read();
    if !batch.is_empty() {
        flush_batch(staging, &mut batch, &mut counts, progress)?;
    }
    Ok(counts)
}

/// Decides whether an I/O failure while reading the source is a property of the *bytes* or of
/// the *environment*, because the two get opposite retry treatment.
///
/// A corrupt gzip member, a truncated one and a stream that is not valid UTF-8 are
/// deterministic: the same source reproduces them exactly, so they are genuine
/// [`crate::contracts::FailureType::InvalidVcfFormat`] and must not be retried. Everything
/// else — the scratch file is missing, the directory is not readable, the disk the download
/// landed on returned `EIO` — says nothing at all about the user's VCF. Reporting those as
/// `InvalidVcfFormat` would permanently fail the workflow on a transient fault *and* misdiagnose
/// it as malformed input, so they map onto the retryable
/// [`crate::contracts::FailureType::ArtifactWriteFailed`] instead.
///
/// The kinds are enumerated positively: an unrecognised kind is treated as transient, which is
/// the safe direction (a retry that fails again is cheap, a wrongly permanent failure is not).
fn classify_source_error(path: &Path, error: &io::Error) -> ArtifactError {
    match error.kind() {
        // `flate2` reports a corrupt deflate stream or a bad gzip header as `InvalidInput` and
        // a member that ends early as `UnexpectedEof`; `BufRead::read_line` reports non-UTF-8
        // input as `InvalidData`. All three are content, not environment.
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => {
            ArtifactError::InvalidVcf(format!("cannot decode {}: {error}", path.display()))
        }
        _ => ArtifactError::WriteFailed(format!(
            "cannot read the local source {}: {error}",
            path.display()
        )),
    }
}

/// Appends one bounded batch through the DuckDB appender and clears it.
fn flush_batch(
    staging: &Connection,
    batch: &mut Vec<UserVariant>,
    counts: &mut StagingCounts,
    progress: &dyn ProgressSink,
) -> Result<(), Stopped> {
    let batch_records = batch.len();
    {
        let mut appender = staging
            .appender("user_variants")
            .map_err(|error| ArtifactError::WriteFailed(format!("cannot open appender: {error}")))?;
        for variant in batch.iter() {
            appender
                .append_row(params![
                    variant.chrom,
                    variant.pos,
                    variant.rsid,
                    variant.ref_allele,
                    variant.alt_allele,
                    variant.gt_raw
                ])
                .map_err(|error| ArtifactError::WriteFailed(format!("cannot append variant: {error}")))?;
        }
        appender
            .flush()
            .map_err(|error| ArtifactError::WriteFailed(format!("cannot flush appender: {error}")))?;
    }
    batch.clear();
    counts.accepted += batch_records as u64;

    report(
        progress,
        ProgressEvent {
            processed_bytes: counts.bytes,
            processed_variants: counts.accepted,
            batch_records,
            ..ProgressEvent::phase(IngestionPhase::WritingDuckdb)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::contracts::FailureType;

    /// Transient local I/O must stay retryable; only deterministic content problems are
    /// `InvalidVcfFormat`.
    #[test]
    fn classifies_local_io_failures_as_retryable_and_content_failures_as_not() {
        let path = Path::new("/x/source.vcf");
        for kind in [
            io::ErrorKind::NotFound,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::Interrupted,
            io::ErrorKind::TimedOut,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::Other,
        ] {
            let error = classify_source_error(path, &io::Error::new(kind, "boom"));
            assert_eq!(
                error.failure_type(),
                FailureType::ArtifactWriteFailed,
                "{kind:?} is environmental and must be retryable"
            );
            assert!(error.failure_type().is_retryable(), "{kind:?} must be retryable");
        }

        for kind in [
            io::ErrorKind::InvalidInput,   // flate2: corrupt deflate stream / bad gzip header
            io::ErrorKind::InvalidData,    // read_line: stream is not valid UTF-8
            io::ErrorKind::UnexpectedEof,  // a gzip member that ends early
        ] {
            let error = classify_source_error(path, &io::Error::new(kind, "boom"));
            assert_eq!(
                error.failure_type(),
                FailureType::InvalidVcfFormat,
                "{kind:?} is deterministic and must not be retried"
            );
            assert!(!error.failure_type().is_retryable(), "{kind:?} must not be retryable");
        }
    }
}
