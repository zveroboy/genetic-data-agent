//! Parquet export: one `COPY` per chromosome partition, then the rename that brings every
//! written file to the contract's `part-NNN.parquet` name.
//!
//! This module writes; it does not judge what it wrote. Everything the export must satisfy
//! before it may be published is checked in [`super::validate`], through a fresh connection and
//! against the files as a consumer sees them.

use std::path::{Path, PathBuf};

use duckdb::Connection;

use super::cancel::CancelGate;
use super::layout::{
    part_file_name, read_sorted_dir, sql_string_literal, sql_text_literal, trailing_index,
    PARTITION_PREFIX,
};
use super::{ArtifactError, Stopped, ROW_GROUP_SIZE};
use crate::concurrency::{map_bounded_with, workers_for, ConcurrencyLimits};
use crate::contracts::IngestionPhase;
use crate::models::{ProgressEvent, ProgressSink};
use crate::vcf::normalize_chromosome;

/// One partition, resolved before anything is copied: which chromosome, which directory, and the
/// exact statement that produces its file.
struct PartitionCopy {
    chrom: String,
    statement: String,
}

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
///
/// **Why the partitions may run concurrently.** Each `COPY` reads a disjoint slice of one
/// table and writes one file nobody else touches, so the outputs cannot interfere. What they do
/// share is DuckDB's *internal* thread pool, which belongs to the database instance rather than
/// to a connection: `limits.export_partitions` concurrent statements do not get a scheduler each,
/// they subdivide the one that already exists. Measured on a 22-partition genome the stage went
/// 0.31 s sequential -> 0.09 s at four workers -> 0.06 s at eight, and then stopped moving, so the
/// default is capped below the core count: past the plateau the only thing more workers add is
/// more simultaneous sort buffers inside DuckDB. The feared collapse from oversubscription did
/// not appear at any bound up to one worker per partition — see
/// `.superpowers/sdd/parallelism-report.md`.
///
/// A `Connection` is `Send` but not `Sync`, so the concurrency model is one cloned connection per
/// worker — [`Connection::try_clone`], which attaches another connection to the *same* in-process
/// database rather than opening the file twice — created on this thread before any worker starts.
pub(super) fn export_parquet(
    staging: &Connection,
    output_dir: &Path,
    progress: &dyn ProgressSink,
    limits: ConcurrencyLimits,
) -> Result<(), Stopped> {
    std::fs::create_dir_all(output_dir).map_err(|error| {
        ArtifactError::WriteFailed(format!(
            "cannot create the export directory '{}': {error}",
            output_dir.display()
        ))
    })?;

    let partitions = plan_partitions(staging, output_dir)?;
    if partitions.is_empty() {
        return Ok(());
    }

    let gate = CancelGate::new(progress);
    let workers = workers_for(limits.export_partitions, partitions.len());

    let outcomes = map_bounded_with(
        &partitions,
        workers,
        // One connection per worker onto the same database. Cloning happens here, on the calling
        // thread, because `try_clone` borrows a `!Sync` `Connection`.
        || staging.try_clone(),
        |connection, partition| -> Result<(), Stopped> {
            if gate.is_cancelled() {
                return Err(Stopped::Interrupted);
            }
            let connection = connection.as_ref().map_err(|error| {
                ArtifactError::WriteFailed(format!("cannot open an export connection: {error}"))
            })?;
            connection.execute_batch(&partition.statement).map_err(|error| {
                ArtifactError::WriteFailed(format!(
                    "Parquet export of '{}' failed: {error}",
                    partition.chrom
                ))
            })?;

            // The export's only interruption point. Without it a cancelled whole-genome build
            // would still copy all 22 partitions before anyone asked whether it should.
            gate.report(ProgressEvent {
                current_partition: Some(partition.chrom.clone()),
                ..ProgressEvent::phase(IngestionPhase::ExportingParquet)
            })
        },
    )
    .map_err(ArtifactError::WriteFailed)?;

    // Resolved exactly as the validation resolves its outcomes, and for the same reasons: an
    // interruption is the caller's own decision and outranks anything a still-running worker
    // found afterwards; otherwise the first failure in partition order is reported, so the
    // message does not depend on which worker lost the race.
    if outcomes
        .iter()
        .any(|outcome| matches!(outcome, Err(Stopped::Interrupted)))
    {
        return Err(Stopped::Interrupted);
    }
    outcomes.into_iter().collect()
}

/// Resolves every partition before any of them is copied: validates the chromosome value,
/// creates the directory, and builds the statement.
///
/// Sequential and separate on purpose. It is the part that *judges* — a non-canonical
/// chromosome must abort the export before a single byte is written, and it must abort with the
/// same message whatever the concurrency bound is — while the part that fans out only executes
/// statements that were already approved.
fn plan_partitions(
    staging: &Connection,
    output_dir: &Path,
) -> Result<Vec<PartitionCopy>, ArtifactError> {
    let mut partitions = Vec::new();
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
        partitions.push(PartitionCopy { chrom, statement });
    }
    Ok(partitions)
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::ControlFlow;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use tempfile::TempDir;

    use crate::contracts::FailureType;

    const BOUNDS: [usize; 3] = [1, 4, 22];

    fn limits(export_partitions: usize) -> ConcurrencyLimits {
        ConcurrencyLimits {
            export_partitions,
            ..ConcurrencyLimits::SEQUENTIAL
        }
    }

    /// A staging database holding `rows` rows for each of `chroms`, deliberately inserted in an
    /// order that is *not* the sort order, so an export that failed to sort would be visible.
    fn staged(chroms: &[&str], rows: u32) -> Connection {
        let connection = Connection::open_in_memory().expect("staging connection");
        connection
            .execute_batch(super::super::staging::STAGING_SCHEMA)
            .expect("schema");
        for chrom in chroms {
            connection
                .execute_batch(&format!(
                    "INSERT INTO user_variants
                     SELECT '{chrom}', ((({rows} - i) * 7919) % {rows} * 100 + 10000)::UINTEGER,
                            'rs' || i, 'A', 'C', '0/1'
                     FROM range(0, {rows}) t(i);"
                ))
                .expect("stage rows");
        }
        connection
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

    struct BreakAt {
        stop_after: usize,
        seen: AtomicUsize,
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

    /// Reads a partition file back and returns its rows in *physical* order.
    fn physical_rows(path: &Path) -> Vec<u32> {
        let reader = Connection::open_in_memory().expect("reader");
        let quoted = sql_string_literal(path).expect("literal");
        let mut statement = reader
            .prepare(&format!(
                "SELECT pos FROM read_parquet({quoted}, file_row_number = true) \
                 ORDER BY file_row_number"
            ))
            .expect("prepare");
        statement
            .query_map([], |row| row.get::<_, u32>(0))
            .and_then(|rows| rows.collect())
            .expect("rows")
    }

    /// The invariant a concurrent export could plausibly break: every partition file is still
    /// physically sorted by `(pos, ref, alt)`, and holds exactly its own partition's rows.
    ///
    /// The rows are staged in a scrambled order and each partition is large enough to span
    /// several DuckDB chunks, which is the condition under which the `PARTITION_BY` form lost
    /// its ordering in the first place.
    #[test]
    fn every_partition_stays_physically_sorted_at_every_bound() {
        const ROWS: u32 = 30_000;
        let chroms = ["1", "12", "2", "X", "MT"];

        for workers in BOUNDS {
            let directory = TempDir::new().expect("temp dir");
            let root = directory.path().join("export");
            let staging = staged(&chroms, ROWS);

            export_parquet(&staging, &root, &Collecting::default(), limits(workers))
                .unwrap_or_else(|error| panic!("export at {workers} workers failed: {error:?}"));

            for chrom in chroms {
                let file = root
                    .join(format!("{PARTITION_PREFIX}{chrom}"))
                    .join("part-000.parquet");
                let rows = physical_rows(&file);
                assert_eq!(rows.len(), ROWS as usize, "chrom={chrom} at {workers} workers");
                assert!(
                    rows.windows(2).all(|pair| pair[0] <= pair[1]),
                    "chrom={chrom} is not physically sorted at {workers} workers"
                );
            }
        }
    }

    /// The bytes themselves, not just the ordering: a concurrent export must produce the same
    /// files a sequential one does, or the frozen dataset checksum would depend on the bound.
    #[test]
    fn concurrent_export_writes_byte_identical_files() {
        let chroms = ["1", "12", "2", "22", "X", "Y", "MT"];
        let read_all = |root: &Path| -> Vec<Vec<u8>> {
            chroms
                .iter()
                .map(|chrom| {
                    std::fs::read(
                        root.join(format!("{PARTITION_PREFIX}{chrom}"))
                            .join("part-000.parquet"),
                    )
                    .expect("read partition")
                })
                .collect()
        };

        let baseline_dir = TempDir::new().expect("temp dir");
        let baseline_root = baseline_dir.path().join("export");
        export_parquet(
            &staged(&chroms, 5_000),
            &baseline_root,
            &Collecting::default(),
            limits(1),
        )
        .expect("sequential export");
        let baseline = read_all(&baseline_root);

        for workers in [2, 7, 22] {
            let directory = TempDir::new().expect("temp dir");
            let root = directory.path().join("export");
            export_parquet(
                &staged(&chroms, 5_000),
                &root,
                &Collecting::default(),
                limits(workers),
            )
            .expect("concurrent export");
            assert_eq!(
                read_all(&root),
                baseline,
                "the exported bytes changed at {workers} workers"
            );
        }
    }

    /// Every partition is reported exactly once, whatever the bound.
    #[test]
    fn each_partition_is_reported_once() {
        let chroms = ["1", "2", "3", "4", "5", "6"];
        for workers in BOUNDS {
            let directory = TempDir::new().expect("temp dir");
            let sink = Collecting::default();
            export_parquet(
                &staged(&chroms, 100),
                &directory.path().join("export"),
                &sink,
                limits(workers),
            )
            .expect("export succeeds");

            let mut reported: Vec<String> = sink
                .events
                .lock()
                .expect("sink lock")
                .iter()
                .map(|event| event.current_partition.clone().expect("a partition"))
                .collect();
            reported.sort();
            assert_eq!(
                reported,
                chroms.iter().map(|chrom| chrom.to_string()).collect::<Vec<_>>(),
                "at {workers} workers the export did not report each partition once"
            );
        }
    }

    /// A cancelled export stops, reports nothing further, and does not run to completion — the
    /// property the per-partition report was added for.
    #[test]
    fn a_break_stops_the_export_and_ends_the_sink_s_events() {
        let chroms = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"];
        for workers in BOUNDS {
            let directory = TempDir::new().expect("temp dir");
            let root = directory.path().join("export");
            let sink = BreakAt {
                stop_after: 1,
                seen: AtomicUsize::new(0),
            };

            let outcome = export_parquet(&staged(&chroms, 2_000), &root, &sink, limits(workers));

            assert!(
                matches!(outcome, Err(Stopped::Interrupted)),
                "a break must interrupt rather than fail, at {workers} workers"
            );
            assert_eq!(
                sink.seen.load(Ordering::SeqCst),
                1,
                "at {workers} workers the sink was handed events after it asked to stop"
            );
        }
    }

    /// A failure in one worker surfaces with its own message rather than being lost to a
    /// concurrently succeeding partition. The write fails because the partition's destination
    /// path is occupied by a directory, which is a real `COPY` failure rather than a stub.
    #[test]
    fn a_failure_in_one_partition_surfaces_from_any_worker() {
        let chroms = ["1", "2", "3", "4", "5", "6", "7", "8"];
        for workers in BOUNDS {
            let directory = TempDir::new().expect("temp dir");
            let root = directory.path().join("export");
            // Occupy chrom=5's output path with a directory, so only that `COPY` can fail.
            std::fs::create_dir_all(root.join(format!("{PARTITION_PREFIX}5")).join("part-000.parquet"))
                .expect("occupy the output path");

            let Err(Stopped::Failed(error)) =
                export_parquet(&staged(&chroms, 100), &root, &Collecting::default(), limits(workers))
            else {
                panic!("an unwritable partition must fail the export at {workers} workers");
            };
            assert_eq!(error.failure_type(), FailureType::ArtifactWriteFailed);
            assert!(
                error.to_string().contains("'5'"),
                "at {workers} workers the failure was '{error}' and did not name the partition"
            );
        }
    }

    /// A non-canonical chromosome aborts before anything is written, at every bound: the
    /// judging half of the stage stays sequential precisely so this cannot race.
    #[test]
    fn a_non_canonical_chromosome_aborts_before_any_file_is_written() {
        for workers in BOUNDS {
            let directory = TempDir::new().expect("temp dir");
            let root = directory.path().join("export");
            let staging = Connection::open_in_memory().expect("connection");
            staging
                .execute_batch(super::super::staging::STAGING_SCHEMA)
                .expect("schema");
            staging
                .execute_batch(
                    "INSERT INTO user_variants VALUES ('../escape', 1, NULL, 'A', 'C', '0/1');",
                )
                .expect("stage");

            let Err(Stopped::Failed(error)) =
                export_parquet(&staging, &root, &Collecting::default(), limits(workers))
            else {
                panic!("a non-canonical chromosome must fail the export at {workers} workers");
            };
            assert_eq!(error.failure_type(), FailureType::ArtifactValidationFailed);
            assert_eq!(
                std::fs::read_dir(&root).expect("export root").count(),
                0,
                "nothing may have been written at {workers} workers"
            );
        }
    }
}
