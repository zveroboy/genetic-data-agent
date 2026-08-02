//! The pure Parquet dataset processor: stage a VCF in an attempt-local DuckDB, export a
//! sorted chromosome-partitioned Zstandard Parquet dataset, validate it and checksum it.
//!
//! Everything here is expressed in paths *relative to the local export directory*. There is
//! no bucket, no key, no ETag and no version ID at this layer, and deliberately so: the
//! dataset content checksum is computed from relative descriptors, so re-running the same
//! source into a different attempt prefix reproduces the same checksum. The S3 mapping and
//! the Temporal activity wrapper are separate concerns layered on top of this module.
//!
//! The four stages are one submodule each, and this file is only their sequence:
//!
//! - [`staging`] reads the source and appends it into the attempt-local DuckDB in bounded
//!   batches.
//! - [`export`] copies the staging table out as one sorted Parquet file per chromosome, then
//!   brings the written files to their contract names.
//! - [`validate`] re-reads the export through a fresh connection and produces the canonical
//!   descriptor list.
//! - [`checksum`] canonicalises that list and hashes it.
//!
//! [`layout`] holds what the export and the validation must agree on: the frozen partition and
//! file-name shapes, and the SQL literals a path becomes.
//!
//! The canonicalisation implemented by [`canonical_descriptor_block`] is specified in
//! `contracts/ingestion-v1.md` and is verified against the golden cross-language fixture.

mod checksum;
mod export;
mod layout;
mod staging;
mod validate;

pub use checksum::{canonical_descriptor_block, dataset_checksum_sha256, LocalParquetFile};

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use duckdb::{params, Connection};

use crate::contracts::FailureType;
use crate::models::{ProgressEvent, ProgressSink};

use crate::contracts::IngestionPhase;

use self::export::{export_parquet, rename_partition_files};
use self::staging::{stage_variants, STAGING_SCHEMA};
use self::validate::validate_export;

/// Provenance string recorded in the staging database and published in the manifest.
pub const PROCESSOR_VERSION: &str = concat!("rust-ingestion-worker/", env!("CARGO_PKG_VERSION"));

/// Rows per Parquet row group. Small enough that a point lookup reads a fraction of a
/// partition, large enough that the footer stays cheap.
pub const ROW_GROUP_SIZE: usize = 100_000;

/// Records buffered before a flush into DuckDB when the caller does not choose.
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// What the processor needs to build one dataset. Purely local paths and provenance.
#[derive(Debug, Clone)]
pub struct ArtifactBuildRequest {
    /// The VCF to read. Plain or gzipped; detected from the content.
    pub source_path: PathBuf,
    /// Where to create the attempt-local staging database. Must not already exist.
    pub staging_db_path: PathBuf,
    /// Where to write the partitioned Parquet dataset. Must not already exist.
    pub parquet_output_dir: PathBuf,
    /// Recorded in `dataset_metadata` for provenance.
    pub dataset_id: String,
    /// The source object's ETag, recorded in `dataset_metadata` so a staged database can be
    /// tied back to the exact bytes it came from.
    pub source_etag: String,
    /// The reference genome build the dataset is pinned to, echoed into [`ArtifactStats`].
    pub reference_build: String,
    /// Records held in memory between flushes into DuckDB. This is the memory bound.
    pub batch_size: usize,
}

/// The result of a successful build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactStats {
    /// SHA-256 over the canonical relative descriptor block. Independent of any S3 prefix.
    pub dataset_checksum_sha256: String,
    /// Canonically ordered by `(chrom, relativePath)`, byte-wise.
    pub local_parquet_files: Vec<LocalParquetFile>,
    pub variant_count: u64,
    pub rejected_record_count: u64,
    pub reference_build: String,
    pub processor_version: String,
}

/// Failures the processor can produce, each mapped onto a frozen contract failure type so a
/// caller can decide retryability without re-deriving it.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    /// The source cannot yield a usable dataset. Retrying the same bytes cannot help.
    #[error("invalid VCF source: {0}")]
    InvalidVcf(String),
    /// A local filesystem or DuckDB write failed.
    #[error("artifact write failed: {0}")]
    WriteFailed(String),
    /// The produced artifact violates an invariant it must satisfy before publication.
    #[error("artifact validation failed: {0}")]
    ValidationFailed(String),
}

impl ArtifactError {
    pub fn failure_type(&self) -> FailureType {
        match self {
            Self::InvalidVcf(_) => FailureType::InvalidVcfFormat,
            Self::WriteFailed(_) => FailureType::ArtifactWriteFailed,
            Self::ValidationFailed(_) => FailureType::ArtifactValidationFailed,
        }
    }
}

/// Why a build stopped before it produced statistics: a failure, or the sink asking it to stop.
///
/// Internal to this module. A caller sees the distinction as `Ok(None)` versus `Err(_)`, because
/// stopping on request is not a failure and must never be reported as one.
enum Stopped {
    Interrupted,
    Failed(ArtifactError),
}

impl From<ArtifactError> for Stopped {
    fn from(error: ArtifactError) -> Self {
        Self::Failed(error)
    }
}

/// Publishes one progress event and turns a [`ControlFlow::Break`] into [`Stopped::Interrupted`],
/// so every reporting site in this module is one `?`.
fn report(progress: &dyn ProgressSink, event: ProgressEvent) -> Result<(), Stopped> {
    match progress.report(&event) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(()) => Err(Stopped::Interrupted),
    }
}

/// Streams `request.source_path` into a partitioned Parquet dataset under
/// `request.parquet_output_dir`, reporting progress to `progress`.
///
/// Memory is bounded by `request.batch_size` records: the source is read one line at a time
/// and flushed into DuckDB in batches, and the sort and partitioning happen inside DuckDB
/// rather than in Rust memory.
///
/// Returns `Ok(None)` when `progress` asked the build to stop — see [`ProgressSink`]. That is
/// not a failure: it is the caller's own request coming back, and the caller is the only one who
/// knows what it means (for the Temporal adapter, a cancellation). Everything the abandoned run
/// created is below `request.staging_db_path` and `request.parquet_output_dir`, which the caller
/// owns and removes.
pub fn build_artifact(
    request: &ArtifactBuildRequest,
    progress: &dyn ProgressSink,
) -> Result<Option<ArtifactStats>, ArtifactError> {
    match build_or_stop(request, progress) {
        Ok(stats) => Ok(Some(stats)),
        Err(Stopped::Interrupted) => Ok(None),
        Err(Stopped::Failed(error)) => Err(error),
    }
}

fn build_or_stop(
    request: &ArtifactBuildRequest,
    progress: &dyn ProgressSink,
) -> Result<ArtifactStats, Stopped> {
    let batch_size = request.batch_size.max(1);
    refuse_existing_path(&request.staging_db_path, "staging database")?;
    refuse_existing_path(&request.parquet_output_dir, "Parquet export directory")?;

    let staging = Connection::open(&request.staging_db_path)
        .map_err(|error| ArtifactError::WriteFailed(format!("cannot create staging database: {error}")))?;
    staging
        .execute_batch(STAGING_SCHEMA)
        .map_err(|error| ArtifactError::WriteFailed(format!("cannot create staging tables: {error}")))?;

    let counts = stage_variants(&staging, request, batch_size, progress)?;

    if counts.accepted == 0 {
        return Err(ArtifactError::InvalidVcf(format!(
            "{} yielded no parseable variant records ({} rejected)",
            request.source_path.display(),
            counts.rejected
        ))
        .into());
    }

    staging
        .execute(
            "INSERT INTO dataset_metadata
             (dataset_id, source_etag, reference_build, variant_count, rejected_record_count, processor_version)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![
                request.dataset_id,
                request.source_etag,
                request.reference_build,
                counts.accepted,
                counts.rejected,
                PROCESSOR_VERSION
            ],
        )
        .map_err(|error| ArtifactError::WriteFailed(format!("cannot record dataset metadata: {error}")))?;

    report(
        progress,
        ProgressEvent {
            processed_bytes: counts.bytes,
            processed_variants: counts.accepted,
            ..ProgressEvent::phase(IngestionPhase::ExportingParquet)
        },
    )?;
    export_parquet(&staging, &request.parquet_output_dir)?;

    // The staging database is closed before the export is inspected, so validation reads the
    // files exactly as a consumer would.
    staging
        .close()
        .map_err(|(_, error)| ArtifactError::WriteFailed(format!("cannot close staging database: {error}")))?;

    rename_partition_files(&request.parquet_output_dir)?;
    let local_parquet_files = validate_export(&request.parquet_output_dir, &counts, progress)?;

    let stats = ArtifactStats {
        dataset_checksum_sha256: dataset_checksum_sha256(&local_parquet_files),
        local_parquet_files,
        variant_count: counts.accepted,
        rejected_record_count: counts.rejected,
        reference_build: request.reference_build.clone(),
        processor_version: PROCESSOR_VERSION.to_string(),
    };

    // The processor's own last observation. It says "the local build is complete", which is the
    // end of *this* layer's work and not the end of the ingestion: uploading is still to come.
    // The phase is `EXPORTING_PARQUET` rather than `FINALIZING` for that reason — `FINALIZING`
    // belongs to whoever publishes the dataset, and the contract's phase list is ordered.
    report(
        progress,
        ProgressEvent {
            processed_bytes: counts.bytes,
            processed_variants: stats.variant_count,
            completed_files: stats.local_parquet_files.len() as u64,
            ..ProgressEvent::phase(IngestionPhase::ExportingParquet)
        },
    )?;
    Ok(stats)
}

/// An attempt writes only to paths it created, so an existing one is refused rather than
/// appended to or overwritten.
fn refuse_existing_path(path: &Path, what: &str) -> Result<(), ArtifactError> {
    match path.try_exists() {
        Ok(false) => Ok(()),
        Ok(true) => Err(ArtifactError::WriteFailed(format!(
            "{what} '{}' already exists; an attempt never reuses a path",
            path.display()
        ))),
        Err(error) => Err(ArtifactError::WriteFailed(format!(
            "cannot inspect '{}': {error}",
            path.display()
        ))),
    }
}
