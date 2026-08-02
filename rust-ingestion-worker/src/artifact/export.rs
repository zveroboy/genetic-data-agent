//! Parquet export: one `COPY` per chromosome partition, then the rename that brings every
//! written file to the contract's `part-NNN.parquet` name.
//!
//! This module writes; it does not judge what it wrote. Everything the export must satisfy
//! before it may be published is checked in [`super::validate`], through a fresh connection and
//! against the files as a consumer sees them.

use std::path::{Path, PathBuf};

use duckdb::Connection;

use super::layout::{
    part_file_name, read_sorted_dir, sql_string_literal, sql_text_literal, trailing_index,
    PARTITION_PREFIX,
};
use super::{ArtifactError, ROW_GROUP_SIZE};
use crate::vcf::normalize_chromosome;

/// Exports the staging table as a sorted, chromosome-partitioned Zstandard Parquet dataset:
/// one `COPY` per partition, each sorted by the contract's in-file sort key. DuckDB performs
/// every sort and write; Rust never holds the dataset.
///
/// **Why not one `COPY … PARTITION_BY (chrom)`.** A single partitioned `COPY` does *not*
/// preserve its `ORDER BY` inside the partition files it writes: the partition writer buffers
/// and flushes each partition's chunks independently of the sort, so rows land out of order
/// once a partition spans more than one chunk. It is size dependent and not reproducible from
/// one run to the next — a 2 000-row partition was observed to restart its `pos` sequence part
/// way through the file. Physical order is not cosmetic here: `sortOrder` is a frozen promise
/// in `contracts/ingestion-v1.md`, and the query path's row-group pruning is only correct
/// because the rows inside a row group are contiguous in that order.
///
/// Copying one partition at a time removes the partition writer from the picture entirely, and
/// costs nothing: the sorts are smaller, and each output file is written exactly once, already
/// under its contract name.
pub(super) fn export_parquet(
    staging: &Connection,
    output_dir: &Path,
) -> Result<(), ArtifactError> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        ArtifactError::WriteFailed(format!(
            "cannot create the export directory '{}': {error}",
            output_dir.display()
        ))
    })?;

    for chrom in staged_chromosomes(staging)? {
        // The value was normalised on the way in and is about to become a directory name and a
        // SQL literal, so it is re-checked rather than trusted a second time.
        if normalize_chromosome(&chrom).as_deref() != Some(chrom.as_str()) {
            return Err(ArtifactError::ValidationFailed(format!(
                "staged chromosome '{chrom}' is not a canonical chromosome"
            )));
        }

        let partition = output_dir.join(format!("{PARTITION_PREFIX}{chrom}"));
        std::fs::create_dir_all(&partition).map_err(|error| {
            ArtifactError::WriteFailed(format!(
                "cannot create the partition directory '{}': {error}",
                partition.display()
            ))
        })?;

        let statement = format!(
            "COPY (
  SELECT pos, rsid, ref, alt, gt_raw
  FROM user_variants
  WHERE chrom = {}
  ORDER BY pos, ref, alt
)
TO {} (
  FORMAT PARQUET,
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE {ROW_GROUP_SIZE}
);",
            sql_text_literal(&chrom),
            sql_string_literal(&partition.join(part_file_name(0, &partition)?))?
        );
        staging.execute_batch(&statement).map_err(|error| {
            ArtifactError::WriteFailed(format!("Parquet export of '{chrom}' failed: {error}"))
        })?;
    }
    Ok(())
}

/// The distinct chromosomes staged, in the contract's byte-wise partition order.
fn staged_chromosomes(staging: &Connection) -> Result<Vec<String>, ArtifactError> {
    let mut statement = staging
        .prepare("SELECT DISTINCT chrom FROM user_variants ORDER BY chrom")
        .map_err(|error| {
            ArtifactError::WriteFailed(format!("cannot list staged chromosomes: {error}"))
        })?;
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .and_then(|rows| rows.collect())
        .map_err(|error| {
            ArtifactError::WriteFailed(format!("cannot list staged chromosomes: {error}"))
        })
}

/// Brings every exported file to the contract's `part-NNN.parquet` name.
///
/// The contract fixes the file name shape because it is part of `relativePath`, which the
/// dataset checksum is computed from. [`export_parquet`] already writes each partition's single
/// file under that name, so this is normally a no-op; it stays because the naming is the
/// contract's to guarantee rather than the writer's, and because it is also where a stray
/// non-Parquet entry at the export root is caught.
pub(super) fn rename_partition_files(output_dir: &Path) -> Result<(), ArtifactError> {
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
