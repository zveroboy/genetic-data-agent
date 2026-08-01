use anyhow::{Context, Result};
use duckdb::{params, Connection};
use serde::Serialize;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader};
use tracing::info;
use tracing_subscriber::EnvFilter;

use rust_ingestion_worker::activities::{parse_vcf_activity, ActivityContext};

#[derive(Serialize)]
struct QueryResultRow {
    rsid: String,
    gene: String,
    user_genotype: String,
    phenotype: String,
    clinical_significance: String,
    evidence_note: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    // Check if invoked in --query mode: rust-ingestion-worker --query <target_id> [db_path]
    if args.len() >= 3 && args[1] == "--query" {
        let target_id = &args[2];
        let db_path = args.get(3).cloned().unwrap_or_else(|| "genomic_data.duckdb".to_string());
        
        let mut conn = Connection::open(&db_path)
            .with_context(|| format!("Failed to open DuckDB at {}", db_path))?;

        // Ensure clinvar_annotations exists and has data
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS clinvar_annotations (
                rsid VARCHAR,
                gene VARCHAR,
                phenotype VARCHAR,
                clinical_significance VARCHAR,
                evidence_note VARCHAR
            );"
        )?;

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM clinvar_annotations;", [], |row| row.get(0))
            .unwrap_or(0);

        if count == 0 {
            // Load clinical annotations
            let tsv_candidates = [
                "tests/fixtures/clinvar_benchmark.tsv",
                "tests/fixtures/annotations_mock.tsv",
                "../tests/fixtures/clinvar_benchmark.tsv",
                "../tests/fixtures/annotations_mock.tsv",
            ];
            for cand in tsv_candidates {
                if fs::metadata(cand).is_ok() {
                    let abs_path = std::fs::canonicalize(cand).unwrap_or_else(|_| cand.into());
                    let abs_str = abs_path.to_string_lossy();
                    let _ = conn.execute(
                        &format!("COPY clinvar_annotations FROM '{}' (DELIMITER '\t', HEADER true);", abs_str),
                        [],
                    );
                    break;
                }
            }
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
        let rows_iter = stmt.query_map(params![target_id, target_id], |row| {
            Ok(QueryResultRow {
                rsid: row.get(0)?,
                gene: row.get(1)?,
                user_genotype: row.get(2)?,
                phenotype: row.get(3)?,
                clinical_significance: row.get(4)?,
                evidence_note: row.get(5)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows_iter {
            if let Ok(r) = row {
                results.push(r);
            }
        }

        let json = serde_json::to_string_pretty(&results)?;
        println!("{}", json);
        return Ok(());
    }

    // Default ingestion mode
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    info!("Starting Genomic VCF Ingestion Worker (GSD Mode with .vcf.gz MultiGzDecoder support)");

    let vcf_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "tests/fixtures/demo_user.vcf".to_string());
    let db_path = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "genomic_data.duckdb".to_string());

    let file = fs::File::open(&vcf_path)
        .with_context(|| format!("Failed to open VCF file at {}", vcf_path))?;

    let reader: Box<dyn BufRead> = if vcf_path.ends_with(".gz") {
        info!("Detected BGZF/GZIP compressed genomic VCF file: {} (using MultiGzDecoder)", vcf_path);
        Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };

    let lines: Vec<String> = reader.lines().filter_map(|l| l.ok()).collect();

    info!("Read {} lines from {}", lines.len(), vcf_path);

    let ctx = ActivityContext::new();
    let variants = parse_vcf_activity(ctx, lines, &db_path).await?;

    info!(
        "Successfully ingested {} variants into DuckDB at '{}'",
        variants.len(),
        db_path
    );

    Ok(())
}
