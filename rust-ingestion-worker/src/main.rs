//! Local debug CLI for the ingestion data plane.
//!
//! This binary is a developer convenience, not a worker: it runs the processor once against
//! a local file, or queries a staged DuckDB database. It deliberately does not poll a task
//! queue and must not grow into a second Temporal entry point.
//!
//! ```text
//! rust-ingestion-worker <vcf-path> [staging-db-path] [parquet-output-dir]
//! rust-ingestion-worker --query <gene-or-rsid> [staging-db-path]
//! ```

use anyhow::{bail, Context, Result};
use duckdb::{params, Connection};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;
use tracing_subscriber::EnvFilter;

use rust_ingestion_worker::artifact::{build_artifact, ArtifactBuildRequest, DEFAULT_BATCH_SIZE};
use rust_ingestion_worker::models::NoopProgressSink;

const DEFAULT_STAGING_DB: &str = "genomic_data.duckdb";
const DEFAULT_VCF: &str = "tests/fixtures/demo_user.vcf";

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

    let source_path = PathBuf::from(args.get(1).cloned().unwrap_or_else(|| DEFAULT_VCF.to_string()));
    let staging_db_path =
        PathBuf::from(args.get(2).cloned().unwrap_or_else(|| DEFAULT_STAGING_DB.to_string()));
    let parquet_output_dir = args
        .get(3)
        .map(PathBuf::from)
        .unwrap_or_else(|| staging_db_path.with_extension("parquet.d"));

    // The processor refuses to reuse a path, because a Temporal attempt must never append to
    // a previous attempt's output. A debug run against explicit paths is different: the
    // operator asked for these paths, so the CLI clears them first.
    remove_if_present(&staging_db_path)?;
    remove_if_present(&parquet_output_dir)?;

    let request = ArtifactBuildRequest {
        source_path,
        staging_db_path,
        parquet_output_dir,
        dataset_id: "local-debug".to_string(),
        source_etag: "local-debug".to_string(),
        reference_build: "GRCh38".to_string(),
        batch_size: DEFAULT_BATCH_SIZE,
    };

    let stats = build_artifact(&request, &NoopProgressSink)
        .with_context(|| format!("failed to build a dataset from {}", request.source_path.display()))?;

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

fn remove_if_present(path: &Path) -> Result<()> {
    if !path.try_exists().with_context(|| format!("cannot inspect {}", path.display()))? {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
    .with_context(|| format!("cannot clear the previous run's {}", path.display()))
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
