//! Everything the export must satisfy before it may be published: the frozen physical schema,
//! Zstandard everywhere, row groups of the contracted size, physical sortedness, no null in a
//! `NOT NULL` column, a row count that matches what was staged, and a `part-NNN.parquet` name.
//!
//! It runs through a *fresh* DuckDB connection over the files on disk, so it reads them exactly
//! as a consumer would rather than through the writer that produced them. Its output is the
//! canonical descriptor list the dataset checksum is taken over.
//!
//! **Why this stage is parallel.** Each exported file is inspected and hashed entirely on its
//! own: nothing one file's descriptor says depends on another's. The directory walk that decides
//! *which* files exist stays sequential and deterministic, and only the per-file work fans out —
//! see [`validate_export`] for the ordering, error and cancellation rules that keeps intact.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use duckdb::Connection;
use sha2::{Digest, Sha256};

use super::cancel::CancelGate;
use super::checksum::{sort_canonically, LocalParquetFile};
use super::layout::{canonical_relative_path, read_sorted_dir, sql_string_literal, PARTITION_PREFIX};
use super::staging::StagingCounts;
use super::{ArtifactError, Stopped, ROW_GROUP_SIZE};
use crate::concurrency::{map_bounded_with, workers_for, ConcurrencyLimits};
use crate::contracts::{IngestionPhase, PARQUET_SCHEMA_COLUMNS, PARQUET_SCHEMA_FINGERPRINT};
use crate::models::{ProgressEvent, ProgressSink};
use crate::vcf::normalize_chromosome;

/// DuckDB flushes a row group once it has *reached* [`ROW_GROUP_SIZE`], and it only checks on
/// vector boundaries, so a group may overshoot by up to one standard vector.
const ROW_GROUP_OVERSHOOT: u64 = 2_048;

/// One file the export produced, as the sequential directory walk found it. Everything the
/// per-file work needs, so that work can run off the walking thread.
struct ExportedFile {
    path: PathBuf,
    chrom: String,
    relative_path: String,
}

/// Inspects every exported file through a fresh DuckDB connection and produces the canonical
/// descriptor list.
///
/// The per-file inspection runs on up to `limits.validate_files` threads. Four things had to
/// survive that, and each is handled here rather than left to the scheduler:
///
/// - **Ordering.** The descriptor list feeds the cross-language dataset checksum, which is taken
///   over descriptors sorted by `(chrom, relativePath)`. The walk that produces the input list is
///   sequential and sorted, [`map_bounded_with`] returns results in input order, and
///   [`sort_canonically`] still runs at the end — so the checksum cannot depend on which worker
///   finished first. The test `parallel_validation_reproduces_the_sequential_descriptor_list`
///   pins that against a real export.
/// - **Errors.** Every worker's outcome is kept, and failures are resolved *by position*, so the
///   error a caller sees is the one the sequential walk would have reported: the first failure in
///   canonical order, with its own message. No failure is reduced to "one of them failed", and
///   none is dropped because another thread finished first.
/// - **Cancellation.** Reports go through [`CancelGate`], so the sink's first `Break` is the last
///   event it is handed, and workers that have not started yet see the flag and stop. An
///   interruption outranks a failure: once the caller has asked the build to stop, a validation
///   error found afterwards by a still-running worker must not be reported as the reason the
///   build ended — the caller is discarding the attempt either way, and re-labelling its own
///   cancellation as a failure would make it look retryable.
/// - **The published `completedFiles` count.** Assigned by the gate, inside the same lock that
///   publishes the event, so the sequence a heartbeat consumer reads is 1, 2, 3, … and not a
///   permutation of it. See [`CancelGate::report_completion`].
/// - **Worker lifetime.** [`map_bounded_with`] blocks until every task has finished and drops the
///   pool, so no thread outlives this call and the activity cannot return with work still going.
///
/// **One change in error precedence, deliberate and worth naming.** The shape walk
/// ([`enumerate_exported_files`]) now runs to completion before any file is inspected, where the
/// old sequential loop interleaved the two. So a malformed partition name *later* in the
/// directory order now outranks a corrupt-file error *earlier* in it, which is the opposite of
/// what the loop reported. This is the better order — the shape of the export is a stronger
/// statement about it than one file's contents, and it is the order that is stable across bounds
/// — but it is a change, not a preservation.
pub(super) fn validate_export(
    output_dir: &Path,
    counts: &StagingCounts,
    progress: &dyn ProgressSink,
    limits: ConcurrencyLimits,
) -> Result<Vec<LocalParquetFile>, Stopped> {
    let exported = enumerate_exported_files(output_dir)?;
    if exported.is_empty() {
        return Err(ArtifactError::ValidationFailed(
            "the export produced no Parquet files".to_string(),
        )
        .into());
    }

    let gate = CancelGate::new(progress);
    let workers = workers_for(limits.validate_files, exported.len());

    let outcomes = map_bounded_with(
        &exported,
        workers,
        // One DuckDB connection per worker, never one per file and never one shared: `Connection`
        // is `Send` but not `Sync`, so a shared `&Connection` would not compile, and re-opening
        // per file would pay the engine's start-up cost once per partition.
        Connection::open_in_memory,
        |connection, file| -> Result<LocalParquetFile, Stopped> {
            if gate.is_cancelled() {
                return Err(Stopped::Interrupted);
            }
            let connection = connection.as_ref().map_err(|error| {
                ArtifactError::ValidationFailed(format!(
                    "cannot open a validation connection: {error}"
                ))
            })?;
            let described = describe_parquet_file(
                connection,
                &file.path,
                &file.chrom,
                file.relative_path.clone(),
            )?;

            gate.report_completion(|completed_files| ProgressEvent {
                processed_bytes: counts.bytes,
                processed_variants: counts.accepted,
                current_partition: Some(file.chrom.clone()),
                // A count of files finished, not this file's index: with several workers in
                // flight the two differ. The gate assigns it inside the same lock that
                // publishes the event, so the sequence a consumer reads really is 1, 2, 3, …
                // — see `CancelGate::report_completion` for why doing it here would not be.
                completed_files,
                ..ProgressEvent::phase(IngestionPhase::ExportingParquet)
            })?;
            Ok(described)
        },
    )
    .map_err(ArtifactError::WriteFailed)?;

    if outcomes
        .iter()
        .any(|outcome| matches!(outcome, Err(Stopped::Interrupted)))
    {
        return Err(Stopped::Interrupted);
    }
    let mut files = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        files.push(outcome?);
    }

    let exported_rows: u64 = files.iter().map(|file| file.row_count).sum();
    if exported_rows != counts.accepted {
        return Err(ArtifactError::ValidationFailed(format!(
            "exported {exported_rows} rows but staged {}",
            counts.accepted
        ))
        .into());
    }

    sort_canonically(&mut files);
    Ok(files)
}

/// Walks the export root and names every file that must be inspected, in the directory order
/// [`read_sorted_dir`] fixes.
///
/// Deliberately sequential and deliberately separate from the inspection: it is cheap (two
/// `readdir`s per partition), it is where the *shape* of the export is judged, and keeping it on
/// one thread is what makes the shape errors below deterministic — a malformed partition name
/// always aborts before any file is inspected, exactly as it did when the whole stage was a loop.
fn enumerate_exported_files(output_dir: &Path) -> Result<Vec<ExportedFile>, ArtifactError> {
    let mut exported = Vec::new();
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
            exported.push(ExportedFile {
                relative_path: canonical_relative_path(directory_name, &name)?,
                chrom: chrom.to_string(),
                path: file,
            });
        }
    }
    Ok(exported)
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

    use std::io::Write;
    use std::ops::ControlFlow;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use tempfile::TempDir;

    use crate::artifact::dataset_checksum_sha256;
    use crate::contracts::FailureType;

    // -----------------------------------------------------------------------------------
    // A hand-built export, so the parallel stage can be driven directly
    // -----------------------------------------------------------------------------------

    /// Rows written per partition by [`write_export`]. Small: these tests are about ordering,
    /// error resolution and cancellation, not about throughput.
    const ROWS_PER_PARTITION: u32 = 50;

    /// Writes a valid export — one `chrom=<value>/part-000.parquet` per name, in the frozen
    /// schema, Zstandard, sorted — using DuckDB directly rather than the exporter, so a failure
    /// here is a failure of the validation and not of the code that produced its input.
    fn write_export(root: &Path, chroms: &[&str]) -> StagingCounts {
        let connection = Connection::open_in_memory().expect("writer connection");
        std::fs::create_dir_all(root).expect("export root");
        for chrom in chroms {
            let partition = root.join(format!("{PARTITION_PREFIX}{chrom}"));
            std::fs::create_dir_all(&partition).expect("partition directory");
            let target = sql_string_literal(&partition.join("part-000.parquet")).expect("literal");
            connection
                .execute_batch(&format!(
                    "COPY (
                       SELECT (10000 + i * 100)::UINTEGER AS pos,
                              'rs' || i AS rsid,
                              'A' AS ref, 'C' AS alt, '0/1' AS gt_raw
                       FROM range(0, {ROWS_PER_PARTITION}) t(i)
                       ORDER BY pos, ref, alt
                     ) TO {target} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_SIZE});"
                ))
                .expect("write partition");
        }
        StagingCounts {
            accepted: u64::from(ROWS_PER_PARTITION) * chroms.len() as u64,
            rejected: 0,
            bytes: 4_096,
        }
    }

    /// Replaces a partition's Parquet file with bytes that are not Parquet at all, so
    /// [`describe_parquet_file`] fails on exactly that file and on no other.
    fn corrupt(root: &Path, chrom: &str) {
        let path = root
            .join(format!("{PARTITION_PREFIX}{chrom}"))
            .join("part-000.parquet");
        let mut file = File::create(&path).expect("truncate");
        file.write_all(b"this is not a parquet file").expect("corrupt");
    }

    /// Bounds every parallel test runs at: the sequential path, and enough workers that every
    /// file is in flight at once.
    const BOUNDS: [usize; 3] = [1, 4, 16];

    fn limits(validate_files: usize) -> ConcurrencyLimits {
        ConcurrencyLimits {
            validate_files,
            ..ConcurrencyLimits::SEQUENTIAL
        }
    }

    #[derive(Default)]
    struct Collecting {
        events: Mutex<Vec<ProgressEvent>>,
    }

    impl ProgressSink for Collecting {
        fn report(&self, event: &ProgressEvent) -> ControlFlow<()> {
            self.events.lock().expect("sink lock").push(event.clone());
            ControlFlow::Continue(())
        }
    }

    /// Breaks at its n-th event and counts everything it was handed.
    struct BreakAt {
        stop_after: usize,
        seen: AtomicUsize,
    }

    impl BreakAt {
        fn new(stop_after: usize) -> Self {
            Self {
                stop_after,
                seen: AtomicUsize::new(0),
            }
        }
    }

    impl ProgressSink for BreakAt {
        fn report(&self, _event: &ProgressEvent) -> ControlFlow<()> {
            if self.seen.fetch_add(1, Ordering::SeqCst) + 1 >= self.stop_after {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    // -----------------------------------------------------------------------------------
    // Ordering under parallel execution
    // -----------------------------------------------------------------------------------

    /// The invariant the frozen dataset checksum rests on: the descriptor list, and therefore
    /// the checksum, is identical however many workers produced it.
    ///
    /// The partition names are chosen so byte-wise order (`1`, `12`, `2`, `MT`, `X`) differs
    /// from numeric order — a list accidentally ordered by completion, or by a numeric sort,
    /// fails here.
    #[test]
    fn parallel_validation_reproduces_the_sequential_descriptor_list() {
        let chroms = ["1", "12", "2", "22", "3", "MT", "X", "Y"];
        let directory = TempDir::new().expect("temp dir");
        let root = directory.path().join("export");
        let counts = write_export(&root, &chroms);

        let baseline = validate_export(&root, &counts, &Collecting::default(), limits(1))
            .unwrap_or_else(|_| panic!("the sequential validation must succeed"));

        for workers in BOUNDS {
            let parallel = validate_export(&root, &counts, &Collecting::default(), limits(workers))
                .unwrap_or_else(|_| panic!("validation at {workers} workers must succeed"));
            assert_eq!(
                parallel, baseline,
                "the descriptor list changed at {workers} workers"
            );
            assert_eq!(
                dataset_checksum_sha256(&parallel),
                dataset_checksum_sha256(&baseline),
                "the dataset checksum changed at {workers} workers"
            );
        }

        let paths: Vec<&str> = baseline.iter().map(|file| file.relative_path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "chrom=1/part-000.parquet",
                "chrom=12/part-000.parquet",
                "chrom=2/part-000.parquet",
                "chrom=22/part-000.parquet",
                "chrom=3/part-000.parquet",
                "chrom=MT/part-000.parquet",
                "chrom=X/part-000.parquet",
                "chrom=Y/part-000.parquet",
            ],
            "descriptors must be byte-wise ordered by (chrom, relativePath)"
        );
    }

    /// Every file is reported exactly once, and the running count a heartbeat projects is
    /// monotonic even though the reports come from different threads.
    #[test]
    fn every_file_is_reported_once_and_the_completed_count_is_monotonic() {
        let chroms = ["1", "2", "3", "4", "5", "6", "7", "8"];
        let directory = TempDir::new().expect("temp dir");
        let root = directory.path().join("export");
        let counts = write_export(&root, &chroms);

        for workers in BOUNDS {
            let sink = Collecting::default();
            validate_export(&root, &counts, &sink, limits(workers)).expect("validation succeeds");

            let events = sink.events.lock().expect("sink lock").clone();
            assert_eq!(events.len(), chroms.len(), "one event per file at {workers} workers");

            let completed: Vec<u64> = events.iter().map(|event| event.completed_files).collect();
            assert_eq!(
                completed,
                (1..=chroms.len() as u64).collect::<Vec<_>>(),
                "the completed-file count must advance by one per event at {workers} workers"
            );

            let mut reported: Vec<String> = events
                .iter()
                .map(|event| event.current_partition.clone().expect("a partition"))
                .collect();
            reported.sort();
            let mut expected: Vec<String> = chroms.iter().map(|chrom| chrom.to_string()).collect();
            expected.sort();
            assert_eq!(reported, expected, "every partition must be reported once");
        }
    }

    // -----------------------------------------------------------------------------------
    // Errors from any worker
    // -----------------------------------------------------------------------------------

    /// A failure in one worker must surface with its own message — not be swallowed because
    /// another worker finished first, and not be reduced to "one of them failed".
    ///
    /// Two files are corrupt, so the test also pins *which* error is reported: the first in
    /// canonical order, which is what the sequential walk would have produced. Byte-wise that
    /// is `chrom=1`, even though `chrom=2` is numerically smaller than `chrom=12`.
    #[test]
    fn a_failure_in_any_worker_surfaces_as_the_first_failure_in_canonical_order() {
        for workers in BOUNDS {
            let directory = TempDir::new().expect("temp dir");
            let root = directory.path().join("export");
            let counts = write_export(&root, &["1", "12", "2", "3"]);
            corrupt(&root, "2");
            corrupt(&root, "12");

            let Err(Stopped::Failed(error)) =
                validate_export(&root, &counts, &Collecting::default(), limits(workers))
            else {
                panic!("a corrupt Parquet file must fail the validation at {workers} workers");
            };
            assert_eq!(error.failure_type(), FailureType::ArtifactValidationFailed);
            assert!(
                error.to_string().contains("chrom=12/part-000.parquet"),
                "at {workers} workers the reported failure was '{error}', not the first in \
                 canonical order"
            );
        }
    }

    /// A single corrupt file among many good ones is still found, whichever worker gets it.
    #[test]
    fn one_corrupt_file_among_many_is_never_missed() {
        let chroms = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"];
        for victim in ["1", "7", "12"] {
            for workers in BOUNDS {
                let directory = TempDir::new().expect("temp dir");
                let root = directory.path().join("export");
                let counts = write_export(&root, &chroms);
                corrupt(&root, victim);

                let Err(Stopped::Failed(error)) =
                    validate_export(&root, &counts, &Collecting::default(), limits(workers))
                else {
                    panic!("corrupting chrom={victim} must fail at {workers} workers");
                };
                assert!(
                    error.to_string().contains(&format!("chrom={victim}/part-000.parquet")),
                    "corrupting chrom={victim} reported '{error}' at {workers} workers"
                );
            }
        }
    }

    // -----------------------------------------------------------------------------------
    // Cancellation mid-stage
    // -----------------------------------------------------------------------------------

    /// A sink that breaks stops the parallel stage, and — the property the gate exists for —
    /// is handed no further event, however many workers were already running.
    #[test]
    fn a_break_stops_the_parallel_stage_and_ends_the_sink_s_events() {
        let chroms = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"];
        for workers in BOUNDS {
            let directory = TempDir::new().expect("temp dir");
            let root = directory.path().join("export");
            let counts = write_export(&root, &chroms);

            let sink = BreakAt::new(2);
            let outcome = validate_export(&root, &counts, &sink, limits(workers));

            assert!(
                matches!(outcome, Err(Stopped::Interrupted)),
                "a break must interrupt rather than fail, at {workers} workers"
            );
            assert_eq!(
                sink.seen.load(Ordering::SeqCst),
                2,
                "at {workers} workers the sink was handed events after it asked to stop"
            );
        }
    }

    /// Once the caller has asked the build to stop, that is the answer — a validation failure a
    /// still-running worker finds afterwards must not turn the caller's own cancellation into a
    /// (retryable) failure.
    #[test]
    fn an_interruption_outranks_a_failure_found_by_another_worker() {
        for workers in BOUNDS {
            let directory = TempDir::new().expect("temp dir");
            let root = directory.path().join("export");
            let counts = write_export(&root, &["1", "2", "3", "4", "5", "6", "7", "8"]);
            corrupt(&root, "8");

            // Breaks on the very first event, so the interruption is certain to precede any
            // resolution of the corrupt file.
            let outcome = validate_export(&root, &counts, &BreakAt::new(1), limits(workers));
            assert!(
                matches!(outcome, Err(Stopped::Interrupted)),
                "at {workers} workers a cancelled run reported {outcome:?} instead of stopping"
            );
        }
    }

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
