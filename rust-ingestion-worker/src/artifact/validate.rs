//! Everything the export must satisfy before it may be published: the frozen physical schema,
//! Zstandard everywhere, row groups of the contracted size, physical sortedness, no null in a
//! `NOT NULL` column, a row count that matches what was staged, and a `part-NNN.parquet` name.
//!
//! It runs through a *fresh* DuckDB connection over the files on disk, so it reads them exactly
//! as a consumer would rather than through the writer that produced them. Its output is the
//! canonical descriptor list the dataset checksum is taken over.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use duckdb::Connection;
use sha2::{Digest, Sha256};

use super::checksum::{sort_canonically, LocalParquetFile};
use super::layout::{canonical_relative_path, read_sorted_dir, sql_string_literal, PARTITION_PREFIX};
use super::staging::StagingCounts;
use super::{report, ArtifactError, Stopped, ROW_GROUP_SIZE};
use crate::contracts::{IngestionPhase, PARQUET_SCHEMA_COLUMNS, PARQUET_SCHEMA_FINGERPRINT};
use crate::models::{ProgressEvent, ProgressSink};
use crate::vcf::normalize_chromosome;

/// DuckDB flushes a row group once it has *reached* [`ROW_GROUP_SIZE`], and it only checks on
/// vector boundaries, so a group may overshoot by up to one standard vector.
const ROW_GROUP_OVERSHOOT: u64 = 2_048;

/// Inspects every exported file through a fresh DuckDB connection and produces the canonical
/// descriptor list.
pub(super) fn validate_export(
    output_dir: &Path,
    counts: &StagingCounts,
    progress: &dyn ProgressSink,
) -> Result<Vec<LocalParquetFile>, Stopped> {
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
            ))
            .into());
        }

        for file in read_sorted_dir(&partition)? {
            let name = file
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| ArtifactError::ValidationFailed("non-UTF-8 Parquet file".to_string()))?
                .to_string();
            let relative_path = canonical_relative_path(directory_name, &name)?;
            files.push(describe_parquet_file(&connection, &file, chrom, relative_path)?);

            report(
                progress,
                ProgressEvent {
                    processed_bytes: counts.bytes,
                    processed_variants: counts.accepted,
                    current_partition: Some(chrom.to_string()),
                    completed_files: files.len() as u64,
                    ..ProgressEvent::phase(IngestionPhase::ExportingParquet)
                },
            )?;
        }
    }

    if files.is_empty() {
        return Err(ArtifactError::ValidationFailed(
            "the export produced no Parquet files".to_string(),
        )
        .into());
    }

    let exported: u64 = files.iter().map(|file| file.row_count).sum();
    if exported != counts.accepted {
        return Err(ArtifactError::ValidationFailed(format!(
            "exported {exported} rows but staged {}",
            counts.accepted
        ))
        .into());
    }

    sort_canonically(&mut files);
    Ok(files)
}

/// The sort key *inside* one partition file: the frozen `sortOrder` without `chrom`, which is
/// the partition directory rather than a physical column.
const PHYSICAL_SORT_KEY: [&str; 3] = ["pos", "ref", "alt"];

/// The reader-side column that carries a Parquet file's *physical* row index.
///
/// `read_parquet(…, file_row_number = true)` projects it. Ordering the window by it is what makes
/// the check below a statement about the bytes on disk rather than about whatever order the scan
/// happened to emit — which is the only question worth asking, because
/// [`super::export::export_parquet`]'s contract is physical order and the query path's row-group
/// pruning depends on it.
const PHYSICAL_ROW_ORDER: &str = "file_row_number";

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
                "lag({column}, 1, {}) OVER (ORDER BY {PHYSICAL_ROW_ORDER}) AS previous_{column}",
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
                 FROM (SELECT *, {lag_columns}
                       FROM read_parquet({quoted}, {PHYSICAL_ROW_ORDER} = true))",
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
        // Every window is ordered by the reader's physical row index. `OVER ()` with no ordering
        // clause describes scan order, not the order of the bytes on disk, and physical order is
        // exactly what `export_parquet` promises.
        assert_eq!(
            lag_columns,
            "lag(pos, 1, 0::UINTEGER) OVER (ORDER BY file_row_number) AS previous_pos, \
             lag(ref, 1, '') OVER (ORDER BY file_row_number) AS previous_ref, \
             lag(alt, 1, '') OVER (ORDER BY file_row_number) AS previous_alt"
        );
        assert_eq!(
            sorted_predicate,
            "(pos, ref, alt) >= (previous_pos, previous_ref, previous_alt)",
            "the sortedness check must compare the whole sort key, not pos alone"
        );
    }
}
