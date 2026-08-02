//! The pure Parquet dataset processor: stage a VCF in an attempt-local DuckDB, export a
//! sorted chromosome-partitioned Zstandard Parquet dataset, validate it and checksum it.
//!
//! Everything here is expressed in paths *relative to the local export directory*. There is
//! no bucket, no key, no ETag and no version ID at this layer, and deliberately so: the
//! dataset content checksum is computed from relative descriptors, so re-running the same
//! source into a different attempt prefix reproduces the same checksum. The S3 mapping and
//! the Temporal activity wrapper are separate concerns layered on top of this module.
//!
//! The canonicalisation implemented by [`canonical_descriptor_block`] is specified in
//! `contracts/ingestion-v1.md` and is verified against the golden cross-language fixture.

use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use duckdb::{params, Connection};
use sha2::{Digest, Sha256};

use crate::contracts::{FailureType, PARQUET_SCHEMA_COLUMNS, PARQUET_SCHEMA_FINGERPRINT};
use crate::models::{ProgressEvent, ProgressSink, UserVariant};
use crate::vcf::{normalize_chromosome, open_vcf, VcfRecord};

use crate::contracts::IngestionPhase;

/// Provenance string recorded in the staging database and published in the manifest.
pub const PROCESSOR_VERSION: &str = concat!("rust-ingestion-worker/", env!("CARGO_PKG_VERSION"));

/// Rows per Parquet row group. Small enough that a point lookup reads a fraction of a
/// partition, large enough that the footer stays cheap.
pub const ROW_GROUP_SIZE: usize = 100_000;

/// DuckDB flushes a row group once it has *reached* `ROW_GROUP_SIZE`, and it only checks on
/// vector boundaries, so a group may overshoot by up to one standard vector.
const ROW_GROUP_OVERSHOOT: u64 = 2_048;

/// Records buffered before a flush into DuckDB when the caller does not choose.
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// The partition directory prefix. `=` appears in an object key only here.
const PARTITION_PREFIX: &str = "chrom=";

/// The contract's Parquet file name is `part-NNN.parquet`: this prefix, then exactly
/// [`PART_FILE_DIGITS`] ASCII digits, then this extension.
const PART_FILE_PREFIX: &str = "part-";
const PART_FILE_SUFFIX: &str = ".parquet";
const PART_FILE_DIGITS: usize = 3;

/// One past the largest index `part-NNN` can express. A partition holding more files than this
/// cannot be named under the contract, so it is refused rather than silently widened to
/// `part-1000.parquet`.
const MAX_PARTITION_FILES: usize = 1_000;

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

/// One exported Parquet file, described relative to the export directory.
///
/// This is intentionally *not* a wire type: it has no bucket, key, ETag or version ID.
/// Mapping it onto an S3 object is a later layer's job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalParquetFile {
    /// `chrom=<value>/part-NNN.parquet`, relative to the export directory and never carrying
    /// the `variants/` segment that only exists in S3 keys.
    pub relative_path: String,
    pub chrom: String,
    /// Lowercase hex SHA-256 of the file's bytes.
    pub checksum_sha256: String,
    pub byte_size: u64,
    pub row_count: u64,
    pub min_pos: u32,
    pub max_pos: u32,
    /// Always [`PARQUET_SCHEMA_FINGERPRINT`]; recorded per file because it is verified per
    /// file rather than assumed.
    pub schema_fingerprint: String,
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

/// Streams `request.source_path` into a partitioned Parquet dataset under
/// `request.parquet_output_dir`, reporting progress to `progress`.
///
/// Memory is bounded by `request.batch_size` records: the source is read one line at a time
/// and flushed into DuckDB in batches, and the sort and partitioning happen inside DuckDB
/// rather than in Rust memory.
pub fn build_artifact(
    request: &ArtifactBuildRequest,
    progress: &dyn ProgressSink,
) -> Result<ArtifactStats, ArtifactError> {
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
        )));
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

    progress.report(&ProgressEvent {
        processed_bytes: counts.bytes,
        processed_variants: counts.accepted,
        ..ProgressEvent::phase(IngestionPhase::ExportingParquet)
    });
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

    progress.report(&ProgressEvent {
        processed_bytes: counts.bytes,
        processed_variants: stats.variant_count,
        completed_files: stats.local_parquet_files.len() as u64,
        ..ProgressEvent::phase(IngestionPhase::Finalizing)
    });
    Ok(stats)
}

/// The attempt-local staging schema. `chrom` is a real column here; it becomes a partition
/// directory only on export.
const STAGING_SCHEMA: &str = "\
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
struct StagingCounts {
    accepted: u64,
    rejected: u64,
    bytes: u64,
}

/// Reads the source one record at a time and appends it in bounded batches.
fn stage_variants(
    staging: &Connection,
    request: &ArtifactBuildRequest,
    batch_size: usize,
    progress: &dyn ProgressSink,
) -> Result<StagingCounts, ArtifactError> {
    let mut reader = open_vcf(&request.source_path)
        .map_err(|error| classify_source_error(&request.source_path, &error))?;

    progress.report(&ProgressEvent::phase(IngestionPhase::Parsing));

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
/// [`FailureType::InvalidVcfFormat`] and must not be retried. Everything else — the scratch
/// file is missing, the directory is not readable, the disk the download landed on returned
/// `EIO` — says nothing at all about the user's VCF. Reporting those as `InvalidVcfFormat`
/// would permanently fail the workflow on a transient fault *and* misdiagnose it as malformed
/// input, so they map onto the retryable [`FailureType::ArtifactWriteFailed`] instead.
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
) -> Result<(), ArtifactError> {
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

    progress.report(&ProgressEvent {
        processed_bytes: counts.bytes,
        processed_variants: counts.accepted,
        batch_records,
        ..ProgressEvent::phase(IngestionPhase::WritingDuckdb)
    });
    Ok(())
}

/// Exports the staging table as a sorted, chromosome-partitioned Zstandard Parquet dataset.
/// DuckDB performs the sort and the partitioning; Rust never holds the dataset.
fn export_parquet(staging: &Connection, output_dir: &Path) -> Result<(), ArtifactError> {
    let statement = format!(
        "COPY (
  SELECT chrom, pos, rsid, ref, alt, gt_raw
  FROM user_variants
  ORDER BY chrom, pos, ref, alt
)
TO {} (
  FORMAT PARQUET,
  PARTITION_BY (chrom),
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE {ROW_GROUP_SIZE}
);",
        sql_string_literal(output_dir)?
    );
    staging
        .execute_batch(&statement)
        .map_err(|error| ArtifactError::WriteFailed(format!("Parquet export failed: {error}")))
}

/// Renames DuckDB's generated `data_N.parquet` files to the contract's `part-NNN.parquet`.
///
/// The contract fixes the file name shape because it is part of `relativePath`, which the
/// dataset checksum is computed from; DuckDB's own naming is an implementation detail.
fn rename_partition_files(output_dir: &Path) -> Result<(), ArtifactError> {
    for partition in read_sorted_dir(output_dir)? {
        if !partition.is_dir() {
            return Err(ArtifactError::ValidationFailed(format!(
                "unexpected file '{}' at the export root",
                partition.display()
            )));
        }
        let mut files: Vec<PathBuf> = read_sorted_dir(&partition)?
            .into_iter()
            .filter(|path| path.extension().is_some_and(|extension| extension == "parquet"))
            .collect();
        // `data_2` must not sort after `data_10`: order by the trailing index, not the name.
        files.sort_by_key(|path| (trailing_index(path), path.clone()));

        for (index, file) in files.iter().enumerate() {
            let renamed = partition.join(part_file_name(index, &partition)?);
            if *file != renamed {
                std::fs::rename(file, &renamed).map_err(|error| {
                    ArtifactError::WriteFailed(format!(
                        "cannot rename '{}' to '{}': {error}",
                        file.display(),
                        renamed.display()
                    ))
                })?;
            }
        }
    }
    Ok(())
}

/// The contract's `part-NNN.parquet` name for the `index`-th file of a partition.
///
/// `{index:03}` pads but does not truncate, so index 1000 would silently produce
/// `part-1000.parquet` and break the frozen `NNN` shape — and with it `relativePath`, which the
/// cross-language dataset checksum is computed from. DuckDB writes one file per partition
/// today, so this is unreachable; it fails loudly rather than degrading quietly if that ever
/// changes.
fn part_file_name(index: usize, partition: &Path) -> Result<String, ArtifactError> {
    if index >= MAX_PARTITION_FILES {
        return Err(ArtifactError::ValidationFailed(format!(
            "partition '{}' produced more than {MAX_PARTITION_FILES} Parquet files; the \
             contract's '{PART_FILE_PREFIX}NNN{PART_FILE_SUFFIX}' name cannot express index {index}",
            partition.display()
        )));
    }
    Ok(format!(
        "{PART_FILE_PREFIX}{index:0width$}{PART_FILE_SUFFIX}",
        width = PART_FILE_DIGITS
    ))
}

/// Whether a file name is exactly the contract's `part-NNN.parquet` shape.
fn is_canonical_part_name(name: &str) -> bool {
    let Some(digits) = name
        .strip_prefix(PART_FILE_PREFIX)
        .and_then(|rest| rest.strip_suffix(PART_FILE_SUFFIX))
    else {
        return false;
    };
    digits.len() == PART_FILE_DIGITS && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// The `chrom=<value>/part-NNN.parquet` descriptor for one exported file.
///
/// The name is taken from disk but not trusted: `relative_path` is the single string the
/// cross-language dataset checksum is most sensitive to, so its shape is asserted here rather
/// than assumed from the fact that [`rename_partition_files`] just ran.
fn canonical_relative_path(directory_name: &str, name: &str) -> Result<String, ArtifactError> {
    if !is_canonical_part_name(name) {
        return Err(ArtifactError::ValidationFailed(format!(
            "'{directory_name}/{name}' is not a '{PART_FILE_PREFIX}NNN{PART_FILE_SUFFIX}' file; \
             the relative path shape is frozen because the dataset checksum is computed from it"
        )));
    }
    Ok(format!("{directory_name}/{name}"))
}

/// The integer at the end of a file stem, used only to order DuckDB's `data_N` output.
fn trailing_index(path: &Path) -> u64 {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.rsplit(['_', '-']).next())
        .and_then(|suffix| suffix.parse::<u64>().ok())
        .unwrap_or(u64::MAX)
}

fn read_sorted_dir(directory: &Path) -> Result<Vec<PathBuf>, ArtifactError> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(directory)
        .map_err(|error| {
            ArtifactError::WriteFailed(format!("cannot list '{}': {error}", directory.display()))
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|error| ArtifactError::WriteFailed(format!("cannot list '{}': {error}", directory.display())))
        })
        .collect::<Result<_, _>>()?;
    entries.sort();
    Ok(entries)
}

/// Inspects every exported file through a fresh DuckDB connection and produces the canonical
/// descriptor list.
fn validate_export(
    output_dir: &Path,
    counts: &StagingCounts,
    progress: &dyn ProgressSink,
) -> Result<Vec<LocalParquetFile>, ArtifactError> {
    let connection = Connection::open_in_memory().map_err(|error| {
        ArtifactError::ValidationFailed(format!("cannot open a validation connection: {error}"))
    })?;

    let mut files = Vec::new();
    for partition in read_sorted_dir(output_dir)? {
        let directory_name = partition
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ArtifactError::ValidationFailed("non-UTF-8 partition directory".to_string()))?;

        let chrom = directory_name.strip_prefix(PARTITION_PREFIX).ok_or_else(|| {
            ArtifactError::ValidationFailed(format!("'{directory_name}' is not a chrom= partition"))
        })?;
        // Defence in depth: the value was normalised on the way in, so it must survive a
        // second normalisation unchanged.
        if normalize_chromosome(chrom).as_deref() != Some(chrom) {
            return Err(ArtifactError::ValidationFailed(format!(
                "partition value '{chrom}' is not a canonical chromosome"
            )));
        }

        for file in read_sorted_dir(&partition)? {
            let name = file
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| ArtifactError::ValidationFailed("non-UTF-8 Parquet file".to_string()))?
                .to_string();
            let relative_path = canonical_relative_path(directory_name, &name)?;
            files.push(describe_parquet_file(&connection, &file, chrom, relative_path)?);

            progress.report(&ProgressEvent {
                processed_bytes: counts.bytes,
                processed_variants: counts.accepted,
                current_partition: Some(chrom.to_string()),
                completed_files: files.len() as u64,
                ..ProgressEvent::phase(IngestionPhase::ExportingParquet)
            });
        }
    }

    if files.is_empty() {
        return Err(ArtifactError::ValidationFailed(
            "the export produced no Parquet files".to_string(),
        ));
    }

    let exported: u64 = files.iter().map(|file| file.row_count).sum();
    if exported != counts.accepted {
        return Err(ArtifactError::ValidationFailed(format!(
            "exported {exported} rows but staged {}",
            counts.accepted
        )));
    }

    sort_canonically(&mut files);
    Ok(files)
}

/// The sort key *inside* one partition file: the frozen `sortOrder` without `chrom`, which is
/// the partition directory rather than a physical column.
const PHYSICAL_SORT_KEY: [&str; 3] = ["pos", "ref", "alt"];

/// The sortedness check, as `(lag columns, comparison)`, built from [`PHYSICAL_SORT_KEY`] so it
/// can never cover fewer columns than the contract's sort order.
///
/// The comparison is a row value over the *whole* key: comparing `pos` alone would accept an
/// export whose `COPY` had dropped `ref, alt` from its `ORDER BY`, since any fixture with
/// distinct positions passes either way.
fn sortedness_expressions() -> (String, String) {
    let lag = PHYSICAL_SORT_KEY
        .iter()
        .map(|column| {
            format!(
                "lag({column}, 1, {}) OVER () AS previous_{column}",
                sort_key_minimum(column)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let previous = PHYSICAL_SORT_KEY
        .iter()
        .map(|column| format!("previous_{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    (lag, format!("({}) >= ({previous})", PHYSICAL_SORT_KEY.join(", ")))
}

/// The minimum of a sort-key column's domain, used as the `lag` default so the first row of a
/// file always compares as in order: `pos` is a non-negative `UINTEGER`, and `ref`/`alt` are
/// `NOT NULL` strings the parser never lets be empty.
fn sort_key_minimum(column: &str) -> &'static str {
    if column == "pos" {
        "0::UINTEGER"
    } else {
        "''"
    }
}

/// Validates one Parquet file against the frozen schema and collects its statistics.
fn describe_parquet_file(
    connection: &Connection,
    path: &Path,
    chrom: &str,
    relative_path: String,
) -> Result<LocalParquetFile, ArtifactError> {
    let quoted = sql_string_literal(path)?;
    assert_physical_schema(connection, &quoted, &relative_path)?;
    assert_zstandard(connection, &quoted, &relative_path)?;
    assert_row_groups(connection, &quoted, &relative_path)?;

    let (lag_columns, sorted_predicate) = sortedness_expressions();
    let (row_count, min_pos, max_pos, sorted, nulls) = connection
        .query_row(
            &format!(
                "SELECT count(*), min(pos), max(pos), coalesce(bool_and({sorted_predicate}), true), {}
                 FROM (SELECT *, {lag_columns} FROM read_parquet({quoted}))",
                not_null_violation_expression()
            ),
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<u32>>(1)?,
                    row.get::<_, Option<u32>>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .map_err(|error| {
            ArtifactError::ValidationFailed(format!("cannot read statistics of '{relative_path}': {error}"))
        })?;

    if row_count <= 0 {
        return Err(ArtifactError::ValidationFailed(format!("'{relative_path}' is empty")));
    }
    if nulls != 0 {
        return Err(ArtifactError::ValidationFailed(format!(
            "'{relative_path}' has {nulls} rows with a null in a NOT NULL column"
        )));
    }
    if !sorted {
        return Err(ArtifactError::ValidationFailed(format!(
            "'{relative_path}' is not ordered by (pos, ref, alt)"
        )));
    }
    let (Some(min_pos), Some(max_pos)) = (min_pos, max_pos) else {
        return Err(ArtifactError::ValidationFailed(format!(
            "'{relative_path}' has no position statistics"
        )));
    };

    let (byte_size, checksum_sha256) = hash_file(path)?;
    Ok(LocalParquetFile {
        relative_path,
        chrom: chrom.to_string(),
        checksum_sha256,
        byte_size,
        row_count: row_count as u64,
        min_pos,
        max_pos,
        schema_fingerprint: PARQUET_SCHEMA_FINGERPRINT.to_string(),
    })
}

/// The physical file must carry exactly the columns of [`PARQUET_SCHEMA_COLUMNS`], in order.
/// `chrom` is encoded by the directory and must not appear.
fn assert_physical_schema(
    connection: &Connection,
    quoted_path: &str,
    relative_path: &str,
) -> Result<(), ArtifactError> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT name, duckdb_type FROM parquet_schema({quoted_path}) WHERE column_id > 0 ORDER BY column_id"
        ))
        .map_err(|error| ArtifactError::ValidationFailed(format!("cannot read the schema of '{relative_path}': {error}")))?;
    let actual: Vec<(String, String)> = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .and_then(|rows| rows.collect())
        .map_err(|error| ArtifactError::ValidationFailed(format!("cannot read the schema of '{relative_path}': {error}")))?;

    let expected: Vec<(String, String)> = parquet_schema_spec()
        .into_iter()
        .map(|(name, column_type, _)| (name.to_string(), column_type.to_string()))
        .collect();
    if actual != expected {
        return Err(ArtifactError::ValidationFailed(format!(
            "'{relative_path}' has columns {actual:?}, expected {expected:?}"
        )));
    }
    Ok(())
}

fn assert_zstandard(
    connection: &Connection,
    quoted_path: &str,
    relative_path: &str,
) -> Result<(), ArtifactError> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT DISTINCT compression FROM parquet_metadata({quoted_path})"
        ))
        .map_err(|error| ArtifactError::ValidationFailed(format!("cannot read '{relative_path}': {error}")))?;
    let codecs: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .and_then(|rows| rows.collect())
        .map_err(|error| ArtifactError::ValidationFailed(format!("cannot read '{relative_path}': {error}")))?;

    if codecs.iter().any(|codec| codec != "ZSTD") {
        return Err(ArtifactError::ValidationFailed(format!(
            "'{relative_path}' uses {codecs:?}, expected Zstandard only"
        )));
    }
    Ok(())
}

/// Every row group but the last must be a full one, and none may overshoot by more than a
/// single DuckDB vector.
fn assert_row_groups(
    connection: &Connection,
    quoted_path: &str,
    relative_path: &str,
) -> Result<(), ArtifactError> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata({quoted_path}) ORDER BY row_group_id"
        ))
        .map_err(|error| ArtifactError::ValidationFailed(format!("cannot read '{relative_path}': {error}")))?;
    let sizes: Vec<i64> = statement
        .query_map([], |row| row.get::<_, i64>(1))
        .and_then(|rows| rows.collect())
        .map_err(|error| ArtifactError::ValidationFailed(format!("cannot read '{relative_path}': {error}")))?;

    let maximum = ROW_GROUP_SIZE as i64 + ROW_GROUP_OVERSHOOT as i64;
    for (index, rows) in sizes.iter().enumerate() {
        let is_last = index + 1 == sizes.len();
        if *rows > maximum || (!is_last && *rows < ROW_GROUP_SIZE as i64) {
            return Err(ArtifactError::ValidationFailed(format!(
                "'{relative_path}' row group {index} holds {rows} rows, expected close to {ROW_GROUP_SIZE}"
            )));
        }
    }
    Ok(())
}

/// `pos IS NULL OR ref IS NULL OR ...` counted as rows, built from the frozen schema so the
/// check follows the contract rather than a second hand-written list.
fn not_null_violation_expression() -> String {
    let predicate = parquet_schema_spec()
        .into_iter()
        .filter(|(_, _, nullable)| !nullable)
        .map(|(name, _, _)| format!("{name} IS NULL"))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!("count(*) FILTER (WHERE {predicate})")
}

/// Parses [`PARQUET_SCHEMA_COLUMNS`] into `(name, duckdb type, nullable)` triples, so the
/// validation and the published fingerprint cannot drift apart.
fn parquet_schema_spec() -> Vec<(&'static str, &'static str, bool)> {
    PARQUET_SCHEMA_COLUMNS
        .split(';')
        .map(|column| {
            let mut parts = column.split(':');
            let name = parts.next().expect("column name");
            let column_type = parts.next().expect("column type");
            let nullability = parts.next().expect("column nullability");
            (name, column_type, nullability == "NULL")
        })
        .collect()
}

/// SHA-256 of a file's bytes, read in fixed-size chunks so file size does not bound memory.
fn hash_file(path: &Path) -> Result<(u64, String), ArtifactError> {
    let mut file = File::open(path)
        .map_err(|error| ArtifactError::ValidationFailed(format!("cannot open '{}': {error}", path.display())))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| ArtifactError::ValidationFailed(format!("cannot read '{}': {error}", path.display())))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }
    Ok((total, hex::encode(hasher.finalize())))
}

/// Byte-wise ascending by `(chrom, relativePath)`, matching `Ord for str`.
fn sort_canonically(files: &mut [LocalParquetFile]) {
    files.sort_by(|left, right| {
        left.chrom
            .as_bytes()
            .cmp(right.chrom.as_bytes())
            .then_with(|| left.relative_path.as_bytes().cmp(right.relative_path.as_bytes()))
    });
}

/// The canonical descriptor block the dataset checksum is taken over.
///
/// One tab-separated line per file, terminated by `\n`, byte-wise sorted by
/// `(chrom, relativePath)`, integers rendered as unpadded base-10. Specified in
/// `contracts/ingestion-v1.md`; `ts-api-agent/src/application/dataset-checksum.ts` is the
/// other implementation.
pub fn canonical_descriptor_block(files: &[LocalParquetFile]) -> String {
    let mut ordered = files.to_vec();
    sort_canonically(&mut ordered);
    ordered
        .iter()
        .map(|file| {
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                file.chrom,
                file.relative_path,
                file.checksum_sha256,
                file.byte_size,
                file.row_count,
                file.min_pos,
                file.max_pos
            )
        })
        .collect()
}

/// Deterministic content checksum of a Parquet dataset, independent of any S3 prefix.
pub fn dataset_checksum_sha256(files: &[LocalParquetFile]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical_descriptor_block(files).as_bytes());
    hex::encode(hasher.finalize())
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

/// Renders a path as a single-quoted SQL literal, doubling embedded quotes.
fn sql_string_literal(path: &Path) -> Result<String, ArtifactError> {
    let text = path
        .to_str()
        .ok_or_else(|| ArtifactError::WriteFailed(format!("non-UTF-8 path '{}'", path.display())))?;
    Ok(format!("'{}'", text.replace('\'', "''")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The validation reads its column list out of the frozen contract constant.
    #[test]
    fn parses_the_frozen_parquet_schema_columns() {
        assert_eq!(
            parquet_schema_spec(),
            [
                ("pos", "UINTEGER", false),
                ("rsid", "VARCHAR", true),
                ("ref", "VARCHAR", false),
                ("alt", "VARCHAR", false),
                ("gt_raw", "VARCHAR", false),
            ]
        );
        assert_eq!(
            not_null_violation_expression(),
            "count(*) FILTER (WHERE pos IS NULL OR ref IS NULL OR alt IS NULL OR gt_raw IS NULL)"
        );
    }

    #[test]
    fn quotes_paths_containing_a_single_quote() {
        assert_eq!(
            sql_string_literal(Path::new("/tmp/it's here/out")).unwrap(),
            "'/tmp/it''s here/out'"
        );
    }

    #[test]
    fn orders_duckdb_partition_files_numerically() {
        assert_eq!(trailing_index(Path::new("/x/data_2.parquet")), 2);
        assert_eq!(trailing_index(Path::new("/x/data_10.parquet")), 10);
        assert_eq!(trailing_index(Path::new("/x/part-007.parquet")), 7);
    }

    /// The in-file sort key is the frozen `sortOrder` minus the partition column, so the
    /// validation predicate cannot drift away from the contract.
    #[test]
    fn the_physical_sort_key_is_the_frozen_sort_order_without_chrom() {
        let expected: Vec<&str> = crate::contracts::SORT_ORDER
            .iter()
            .copied()
            .filter(|column| *column != "chrom")
            .collect();
        assert_eq!(PHYSICAL_SORT_KEY.to_vec(), expected);

        let (lag_columns, sorted_predicate) = sortedness_expressions();
        assert_eq!(
            lag_columns,
            "lag(pos, 1, 0::UINTEGER) OVER () AS previous_pos, \
             lag(ref, 1, '') OVER () AS previous_ref, \
             lag(alt, 1, '') OVER () AS previous_alt"
        );
        assert_eq!(
            sorted_predicate,
            "(pos, ref, alt) >= (previous_pos, previous_ref, previous_alt)",
            "the sortedness check must compare the whole sort key, not pos alone"
        );
    }

    /// `part-NNN` cannot express a four-digit index, so it must refuse rather than widen.
    #[test]
    fn part_file_names_are_three_digits_and_refuse_to_widen() {
        let partition = Path::new("/x/chrom=1");
        assert_eq!(part_file_name(0, partition).unwrap(), "part-000.parquet");
        assert_eq!(part_file_name(7, partition).unwrap(), "part-007.parquet");
        assert_eq!(part_file_name(999, partition).unwrap(), "part-999.parquet");

        let error = part_file_name(1_000, partition).expect_err("part-1000 breaks the NNN shape");
        assert_eq!(error.failure_type(), FailureType::ArtifactValidationFailed);
    }

    #[test]
    fn only_the_contract_file_name_shape_becomes_a_relative_path() {
        assert_eq!(
            canonical_relative_path("chrom=X", "part-000.parquet").unwrap(),
            "chrom=X/part-000.parquet"
        );

        for rejected in [
            "data_0.parquet",          // DuckDB's own name, if the rename were ever skipped
            "part-0.parquet",          // unpadded
            "part-0000.parquet",       // widened past NNN
            "part-00a.parquet",        // not digits
            "part-000.parquet.tmp",    // a staging leftover
            "part-000.PARQUET",        // wrong case
            "part-000",                // no extension
            ".part-000.parquet",       // hidden file
            "",
        ] {
            let Err(error) = canonical_relative_path("chrom=1", rejected) else {
                panic!("'{rejected}' must not reach a relativePath");
            };
            assert_eq!(
                error.failure_type(),
                FailureType::ArtifactValidationFailed,
                "'{rejected}' must be a validation failure"
            );
        }
    }

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
