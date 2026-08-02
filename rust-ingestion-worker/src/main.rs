//! Local debug CLI for the ingestion data plane.
//!
//! This binary is a developer convenience, not a worker: it runs the processor once against
//! a local file, or queries a staged DuckDB database. It deliberately does not poll a task
//! queue and must not grow into a second Temporal entry point.
//!
//! ```text
//! rust-ingestion-worker [--force-clean] <vcf-path> [staging-db-path] [parquet-output-dir]
//! rust-ingestion-worker --query <gene-or-rsid> [staging-db-path]
//! ```
//!
//! Output paths the CLI derived itself are cleared before a run. A path typed on the command
//! line is never cleared implicitly — see [`clear_previous_run`].

use anyhow::{bail, Context, Result};
use duckdb::{params, Connection};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;
use tracing_subscriber::EnvFilter;

use rust_ingestion_worker::artifact::{build_artifact, ArtifactBuildRequest, DEFAULT_BATCH_SIZE};
use rust_ingestion_worker::concurrency::ConcurrencyLimits;
use rust_ingestion_worker::models::NoopProgressSink;

const DEFAULT_STAGING_DB: &str = "genomic_data.duckdb";
const DEFAULT_VCF: &str = "tests/fixtures/demo_user.vcf";

/// Opt-in to clearing an output path the operator typed rather than one the CLI derived.
const FORCE_CLEAN_FLAG: &str = "--force-clean";

#[derive(Serialize)]
struct QueryResultRow {
    rsid: String,
    gene: String,
    user_genotype: String,
    phenotype: String,
    clinical_significance: String,
    evidence_note: String,
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() >= 3 && args[1] == "--query" {
        let staging_db = args.get(3).cloned().unwrap_or_else(|| DEFAULT_STAGING_DB.to_string());
        return run_query(&args[2], Path::new(&staging_db));
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let force_clean = args.iter().skip(1).any(|argument| argument == FORCE_CLEAN_FLAG);
    let positional: Vec<&String> =
        args.iter().skip(1).filter(|argument| *argument != FORCE_CLEAN_FLAG).collect();

    let source_path =
        PathBuf::from(positional.first().map_or(DEFAULT_VCF, |value| value.as_str()));
    let supplied_staging_db = positional.get(1).map(|value| PathBuf::from(value.as_str()));
    let staging_db_path = supplied_staging_db
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_STAGING_DB));
    let supplied_output_dir = positional.get(2).map(|value| PathBuf::from(value.as_str()));
    let parquet_output_dir = supplied_output_dir
        .clone()
        .unwrap_or_else(|| staging_db_path.with_extension("parquet.d"));

    // The processor refuses to reuse a path, because a Temporal attempt must never append to a
    // previous attempt's output. A debug run is allowed to clear the previous run first — but
    // only for the paths this CLI derived. See `clear_previous_run`.
    clear_previous_run(&staging_db_path, supplied_staging_db.is_some(), force_clean)?;
    clear_previous_run(&parquet_output_dir, supplied_output_dir.is_some(), force_clean)?;

    let request = ArtifactBuildRequest {
        source_path,
        staging_db_path,
        parquet_output_dir,
        dataset_id: "local-debug".to_string(),
        source_etag: "local-debug".to_string(),
        reference_build: "GRCh38".to_string(),
        batch_size: DEFAULT_BATCH_SIZE,
        concurrency: cli_concurrency_limits()?,
    };

    // `NoopProgressSink` never asks the build to stop, so `None` is unreachable here; the debug
    // CLI has no cancellation to express.
    let stats = build_artifact(&request, &NoopProgressSink)
        .with_context(|| format!("failed to build a dataset from {}", request.source_path.display()))?
        .expect("the debug CLI's progress sink never interrupts a build");

    info!(
        "Ingested {} variants ({} rejected) into {} Parquet files under '{}'; dataset checksum {}",
        stats.variant_count,
        stats.rejected_record_count,
        stats.local_parquet_files.len(),
        request.parquet_output_dir.display(),
        stats.dataset_checksum_sha256
    );
    for file in &stats.local_parquet_files {
        info!(
            "  {} — {} rows, pos {}..{}, {} bytes",
            file.relative_path, file.row_count, file.min_pos, file.max_pos, file.byte_size
        );
    }
    Ok(())
}

/// The debug CLI's concurrency bounds, taken from the environment so a before/after measurement
/// can be run against one binary instead of one build per setting.
///
/// Defaults to [`ConcurrencyLimits::default`] — the same bounds the Temporal worker uses — so a
/// plain `rust-ingestion-worker file.vcf.gz` is representative of production. `INGEST_SEQUENTIAL=1`
/// selects [`ConcurrencyLimits::SEQUENTIAL`], which is the pre-parallel pipeline and the baseline
/// every measurement in `.superpowers/sdd/parallelism-report.md` is taken against. Individual
/// stages can be overridden on top of that.
///
/// Deliberately confined to this binary: the library takes its bounds as a parameter and reads no
/// environment of its own, so the worker's behaviour cannot be changed by an ambient variable.
///
/// **Every one of these variables fails loudly on a value it does not understand.** They exist to
/// select the configuration a measurement is attributed to, so a typo that silently selects the
/// *other* configuration does not produce a slower run, it produces a wrong number in a report —
/// the one failure mode that cannot be caught by looking at the output. `INGEST_SEQUENTIAL=2`
/// used to mean "parallel"; it is now an error.
fn cli_concurrency_limits() -> Result<ConcurrencyLimits> {
    let flag = |name: &str| -> Result<Option<usize>> {
        match env::var(name) {
            Err(_) => Ok(None),
            Ok(value) => value
                .trim()
                .parse::<usize>()
                .map(Some)
                .with_context(|| format!("{name} is set to '{value}', which is not a whole number")),
        }
    };

    let mut limits = match flag("INGEST_SEQUENTIAL")? {
        Some(1) => ConcurrencyLimits::SEQUENTIAL,
        None | Some(0) => ConcurrencyLimits::default(),
        Some(other) => bail!(
            "INGEST_SEQUENTIAL is set to '{other}'. It selects the fully sequential baseline and \
             takes 1 (sequential) or 0 (the default bounds) only — a value it ignored would mean \
             the opposite of what was typed. Use INGEST_VALIDATE_FILES, INGEST_EXPORT_PARTITIONS \
             or INGEST_BGZF_BLOCKS to set a bound."
        ),
    };
    if let Some(value) = flag("INGEST_VALIDATE_FILES")? {
        limits.validate_files = value;
    }
    if let Some(value) = flag("INGEST_EXPORT_PARTITIONS")? {
        limits.export_partitions = value;
    }
    if let Some(value) = flag("INGEST_BGZF_BLOCKS")? {
        limits.bgzf_blocks = value;
    }
    Ok(limits)
}

/// Clears one output path from a previous debug run.
///
/// This calls `remove_dir_all`, so it is the one genuinely destructive thing the CLI does. A
/// path the CLI *derived* is safe to clear: it is `<staging-db>.parquet.d` next to a database
/// this binary also owns. A path the *operator typed* is not — `rust-ingestion-worker in.vcf
/// out.duckdb .` would otherwise recursively erase the working directory. So a supplied path is
/// only ever cleared with an explicit `--force-clean`, and never when it contains the process's
/// current directory, which no debug run has any reason to delete.
fn clear_previous_run(path: &Path, operator_supplied: bool, force_clean: bool) -> Result<()> {
    if !path.try_exists().with_context(|| format!("cannot inspect {}", path.display()))? {
        return Ok(());
    }
    if operator_supplied && !force_clean {
        bail!(
            "{} already exists. It was given on the command line, so it is not deleted \
             implicitly; remove it yourself or re-run with {FORCE_CLEAN_FLAG}.",
            path.display()
        );
    }
    if contains_current_directory(path) {
        bail!(
            "refusing to recursively delete {}: it is, or contains, the current directory",
            path.display()
        );
    }
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
    .with_context(|| format!("cannot clear the previous run's {}", path.display()))
}

/// Whether `path` is the current directory or an ancestor of it. Unresolvable paths are
/// treated as dangerous, because that is the safe direction for a recursive delete.
fn contains_current_directory(path: &Path) -> bool {
    let (Ok(resolved), Ok(current)) = (fs::canonicalize(path), env::current_dir()) else {
        return true;
    };
    let Ok(current) = fs::canonicalize(current) else {
        return true;
    };
    current.starts_with(resolved)
}

/// Joins staged variants against the ClinVar annotation fixture. Debug-only.
fn run_query(target_id: &str, staging_db: &Path) -> Result<()> {
    if !staging_db.try_exists()? {
        bail!("no staged database at {}", staging_db.display());
    }
    let conn = Connection::open(staging_db)
        .with_context(|| format!("failed to open DuckDB at {}", staging_db.display()))?;

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS clinvar_annotations (
            rsid VARCHAR,
            gene VARCHAR,
            phenotype VARCHAR,
            clinical_significance VARCHAR,
            evidence_note VARCHAR
        );",
    )?;

    let annotations: i64 = conn
        .query_row("SELECT count(*) FROM clinvar_annotations;", [], |row| row.get(0))
        .unwrap_or(0);
    if annotations == 0 {
        load_annotation_fixture(&conn);
    }

    let sql = "
        SELECT
            v.rsid, c.gene,
            CASE
                WHEN v.gt_raw LIKE '%0/0%' THEN v.ref || '/' || v.ref
                WHEN v.gt_raw LIKE '%0/1%' OR v.gt_raw LIKE '%1/0%' THEN v.ref || '/' || v.alt
                WHEN v.gt_raw LIKE '%1/1%' THEN v.alt || '/' || v.alt
                ELSE v.gt_raw
            END AS user_genotype,
            c.phenotype, c.clinical_significance, c.evidence_note
        FROM user_variants v
        JOIN clinvar_annotations c ON v.rsid = c.rsid
        WHERE (c.gene = ? OR c.rsid = ?)
        LIMIT 10;
    ";

    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params![target_id, target_id], |row| {
        Ok(QueryResultRow {
            rsid: row.get(0)?,
            gene: row.get(1)?,
            user_genotype: row.get(2)?,
            phenotype: row.get(3)?,
            clinical_significance: row.get(4)?,
            evidence_note: row.get(5)?,
        })
    })?;

    let results: Vec<QueryResultRow> = rows.filter_map(Result::ok).collect();
    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}

fn load_annotation_fixture(conn: &Connection) {
    let candidates = [
        "tests/fixtures/clinvar_benchmark.tsv",
        "tests/fixtures/annotations_mock.tsv",
        "../tests/fixtures/clinvar_benchmark.tsv",
        "../tests/fixtures/annotations_mock.tsv",
    ];
    for candidate in candidates {
        let Ok(path) = fs::canonicalize(candidate) else {
            continue;
        };
        let _ = conn.execute(
            &format!(
                "COPY clinvar_annotations FROM '{}' (DELIMITER '\t', HEADER true);",
                path.to_string_lossy().replace('\'', "''")
            ),
            [],
        );
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

    /// Serialises the two tests below that touch the process's current directory.
    ///
    /// `std::env::set_current_dir` mutates global process state, and this test binary runs
    /// tests on multiple threads by default. Every test that reads or writes the process cwd
    /// takes this lock first, so the mutating test below can never interleave with the test
    /// that reads `env::current_dir()`.
    static CURRENT_DIRECTORY_MUTEX: Mutex<()> = Mutex::new(());

    /// The regression this guard exists for: `rust-ingestion-worker in.vcf out.duckdb .` must
    /// not recursively delete the operator's working directory.
    #[test]
    fn refuses_to_clear_an_operator_supplied_path_without_the_flag() {
        let directory = TempDir::new().expect("temp dir");
        let victim = directory.path().join("precious");
        fs::create_dir(&victim).expect("create");
        fs::write(victim.join("keep.txt"), b"keep me").expect("seed");

        let error = clear_previous_run(&victim, true, false)
            .expect_err("a supplied path must not be deleted implicitly");
        assert!(
            error.to_string().contains(FORCE_CLEAN_FLAG),
            "the error must point at the opt-in flag: {error}"
        );
        assert!(victim.join("keep.txt").exists(), "nothing may have been deleted");
    }

    /// A path the CLI derived itself is still cleared, so the debug workflow is unchanged.
    #[test]
    fn clears_a_derived_path_without_the_flag() {
        let directory = TempDir::new().expect("temp dir");
        let derived = directory.path().join("genomic_data.parquet.d");
        fs::create_dir(&derived).expect("create");
        fs::write(derived.join("part-000.parquet"), b"stale").expect("seed");

        clear_previous_run(&derived, false, false).expect("a derived path is cleared");
        assert!(!derived.exists());
    }

    #[test]
    fn clears_an_operator_supplied_path_once_the_flag_is_given() {
        let directory = TempDir::new().expect("temp dir");
        let supplied = directory.path().join("explicit.d");
        fs::create_dir(&supplied).expect("create");

        clear_previous_run(&supplied, true, true).expect("--force-clean clears it");
        assert!(!supplied.exists());
    }

    /// Checks the pure predicate `clear_previous_run` relies on against the real process cwd,
    /// rather than calling `clear_previous_run` itself against `env::current_dir()`: a
    /// `clear_previous_run` call against the actual current directory (or its parent) would
    /// recursively `remove_dir_all` the crate — or the repository's parent — the day this guard
    /// regresses. See `clear_previous_run_refuses_to_delete_an_ancestor_of_the_process_cwd`
    /// below for the test that exercises `clear_previous_run`'s guard branch itself, safely,
    /// against a temp directory instead.
    ///
    /// Takes `CURRENT_DIRECTORY_MUTEX` because it reads the live process cwd, and the test below
    /// mutates it; without the lock the two could interleave and this test could observe a cwd
    /// that is mid-move.
    #[test]
    fn contains_current_directory_recognises_the_current_directory_and_its_ancestors() {
        let _serialize_cwd_access = CURRENT_DIRECTORY_MUTEX
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let current = env::current_dir().expect("current directory");
        assert!(
            contains_current_directory(&current),
            "the current directory must be recognised as itself"
        );

        let parent = current.parent().expect("a parent directory").to_path_buf();
        assert!(
            contains_current_directory(&parent),
            "an ancestor of the current directory must also be recognised"
        );
    }

    /// Exercises `clear_previous_run`'s guard branch itself — not just the pure predicate —
    /// without ever pointing a deletion at a real path outside a `TempDir`.
    ///
    /// `std::env::set_current_dir` is process-global and this test binary runs tests in
    /// parallel by default, so a test that moves the cwd could corrupt any concurrently running
    /// test that resolves a relative path. Two things make this safe:
    /// 1. `CURRENT_DIRECTORY_MUTEX` is held for the whole test, and the only other test in this
    ///    module that reads the process cwd (`contains_current_directory_recognises_the_current_directory_and_its_ancestors`)
    ///    takes the same lock, so the two can never interleave. No other test in this module
    ///    reads or writes the process cwd.
    /// 2. The original cwd is restored on every exit path: explicitly right after the call under
    ///    test (so the temp directory's own `Drop` never runs while the cwd still points inside
    ///    it), and via an RAII guard whose `Drop` restores it too, in case anything above panics
    ///    first.
    #[test]
    fn clear_previous_run_refuses_to_delete_an_ancestor_of_the_process_cwd() {
        let _serialize_cwd_access = CURRENT_DIRECTORY_MUTEX
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        struct RestoreCurrentDirectory(PathBuf);
        impl Drop for RestoreCurrentDirectory {
            fn drop(&mut self) {
                // Best-effort: if this fails during an unwind there is nothing more to do, and
                // panicking again from a `Drop` during a panic would abort the process.
                let _ = env::set_current_dir(&self.0);
            }
        }

        let original_cwd = env::current_dir().expect("read the current directory");
        // Held for the rest of the test so a panic before the explicit restore below still
        // restores the cwd on unwind.
        let _restore_on_unwind = RestoreCurrentDirectory(original_cwd.clone());

        let root = TempDir::new().expect("temp dir");
        let nested = root.path().join("nested").join("deeper");
        fs::create_dir_all(&nested).expect("create nested directories");

        env::set_current_dir(&nested).expect("move the process cwd into the temp tree");

        // `root` is an ancestor of the new cwd — entirely inside this temp tree, never a real
        // checkout — so this proves the guard fires even with `force_clean = true`.
        let result = clear_previous_run(root.path(), true, true);

        env::set_current_dir(&original_cwd).expect("restore the current directory");

        let error = result.expect_err(
            "clear_previous_run must refuse to delete an ancestor of the current directory",
        );
        assert!(
            error.to_string().contains("current directory"),
            "the error must name the current-directory guard: {error}"
        );
        assert!(root.path().exists(), "nothing may have been deleted");
    }

    #[test]
    fn a_missing_path_is_nothing_to_clear() {
        let directory = TempDir::new().expect("temp dir");
        clear_previous_run(&directory.path().join("absent"), true, false).expect("no-op");
    }
}
