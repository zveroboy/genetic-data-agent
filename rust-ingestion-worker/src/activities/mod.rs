use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context, Result};
use duckdb::{params, Connection};
use rayon::prelude::*;
use serde::Serialize;
use tokio::time::sleep;
use tracing::info;

use crate::models::{ProgressPayload, UserVariant};

#[derive(Debug, thiserror::Error)]
pub enum ActivityError {
    #[error("Rayon parse task error: {0}")]
    ParseTaskError(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("IO error: {0}")]
    IoError(String),
}

impl From<tokio::task::JoinError> for ActivityError {
    fn from(err: tokio::task::JoinError) -> Self {
        ActivityError::ParseTaskError(err.to_string())
    }
}

impl From<duckdb::Error> for ActivityError {
    fn from(err: duckdb::Error) -> Self {
        ActivityError::DatabaseError(err.to_string())
    }
}

#[derive(Clone, Default)]
pub struct ActivityContext {
    pub task_token: Option<Vec<u8>>,
}

impl ActivityContext {
    pub fn new() -> Self {
        Self { task_token: None }
    }

    pub fn heartbeat<T: Serialize>(&self, payload: T) {
        if let Ok(json_str) = serde_json::to_string(&payload) {
            info!("[Temporal Heartbeat] {}", json_str);
        }
    }
}

pub async fn parse_vcf_activity(
    ctx: ActivityContext,
    vcf_lines: Vec<String>,
    db_path: &str,
) -> Result<Vec<UserVariant>, ActivityError> {
    let total = vcf_lines.len();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_for_rayon = Arc::clone(&counter);
    let ctx_clone = ctx.clone();

    let parse_task = tokio::task::spawn_blocking(move || {
        let variants: Vec<UserVariant> = vcf_lines
            .into_par_iter()
            .filter_map(|line| {
                let parsed = UserVariant::from_vcf_line(&line);
                counter_for_rayon.fetch_add(1, Ordering::Relaxed);
                parsed
            })
            .collect();
        variants
    });

    let heartbeat_task = async move {
        loop {
            sleep(Duration::from_millis(500)).await;
            let current = counter.load(Ordering::Relaxed);
            let percentage = if total > 0 {
                (current as f64 / total as f64) * 100.0
            } else {
                100.0
            };

            ctx_clone.heartbeat(ProgressPayload {
                stage: "RAYON_PARSING".into(),
                processed: current,
                total,
                percentage: format!("{:.1}%", percentage),
            });

            if current >= total {
                break;
            }
        }
    };

    let (parse_res, _) = tokio::join!(parse_task, heartbeat_task);
    let variants = parse_res?;

    info!("Rayon parsing completed. Parsed {} valid variants", variants.len());

    save_variants_to_duckdb(db_path, &variants)
        .map_err(|e| ActivityError::DatabaseError(e.to_string()))?;

    ctx.heartbeat(ProgressPayload {
        stage: "RAYON_PARSING_COMPLETE".into(),
        processed: total,
        total,
        percentage: "100.0%".into(),
    });

    Ok(variants)
}

pub async fn parse_and_index_activity(
    ctx: ActivityContext,
    vcf_lines: Vec<String>,
) -> Result<(), ActivityError> {
    parse_vcf_activity(ctx, vcf_lines, "genomic_data.duckdb").await?;
    Ok(())
}

pub fn save_variants_to_duckdb(db_path: &str, variants: &[UserVariant]) -> Result<()> {
    let conn = Connection::open(db_path).context("Failed to open DuckDB connection")?;

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS user_variants (
            chrom VARCHAR,
            pos UINTEGER,
            rsid VARCHAR,
            ref VARCHAR,
            alt VARCHAR,
            gt_raw VARCHAR
        );",
    )
    .context("Failed to create user_variants table")?;

    // Use Appender for maximum C-API speed when ingesting millions of rows
    let mut appender = conn.appender("user_variants").context("Failed to create DuckDB appender")?;
    for v in variants {
        appender
            .append_row(params![v.chrom, v.pos, v.rsid, v.ref_allele, v.alt_allele, v.gt_raw])
            .context("Failed to append row")?;
    }
    appender.flush().context("Failed to flush appender")?;
    info!("Saved {} variants to DuckDB table user_variants at {} via Appender", variants.len(), db_path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_vcf_activity_hybrid_parallelism() {
        let lines = vec![
            "15\t74749576\trs762551\tA\tC\t99\tPASS\tGENE=CYP1A2\tGT\t1/1".to_string(),
            "2\t135851076\trs4988235\tC\tT\t99\tPASS\tGENE=LCT\tGT\t0/0".to_string(),
            "12\t21282148\trs4149056\tT\tC\t99\tPASS\tGENE=SLCO1B1\tGT\t0/1".to_string(),
        ];
        let ctx = ActivityContext::new();
        let variants = parse_vcf_activity(ctx, lines, ":memory:")
            .await
            .expect("parse_vcf_activity should succeed");
        assert_eq!(variants.len(), 3);
        assert_eq!(variants[0].rsid, "rs762551");
        assert_eq!(variants[1].rsid, "rs4988235");
        assert_eq!(variants[2].rsid, "rs4149056");
    }
}
