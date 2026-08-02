//! Feasibility probe: activity-only Temporal Rust worker.
//!
//! Throwaway scaffolding for Task 1's cross-language gate. It registers a single
//! Activity Type `rustActivityProbe` on the `genomic-ingestion-rust` task queue so a
//! TypeScript Workflow can schedule genuine Rust Activities. A later task deletes this
//! and replaces it with the production `buildDatasetArtifact` Activity.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

/// Task queue polled exclusively by the Rust worker.
const TASK_QUEUE: &str = "genomic-ingestion-rust";

/// Sentinel message that makes the first Activity attempt fail so the retry path is
/// exercised end to end across the language boundary.
const FAIL_ONCE_MESSAGE: &str = "fail-once";

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProbeInput {
    message: String,
    iterations: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProbeResult {
    echoed: String,
    worker_language: String,
}

struct ProbeActivities;

#[activities]
impl ProbeActivities {
    #[activity(name = "rustActivityProbe")]
    async fn rust_activity_probe(
        ctx: ActivityContext,
        input: ProbeInput,
    ) -> Result<ProbeResult, ActivityError> {
        let attempt = ctx.info().attempt;
        tracing::info!(
            activity_type = %ctx.info().activity_type,
            task_queue = %ctx.info().task_queue,
            attempt,
            iterations = input.iterations,
            "rustActivityProbe started"
        );

        if input.message == FAIL_ONCE_MESSAGE && attempt == 1 {
            return Err(ActivityError::from(anyhow::anyhow!(
                "probe failing deliberately on attempt 1"
            )));
        }

        for iteration in 0..input.iterations {
            if ctx.is_cancelled() {
                tracing::info!(iteration, "rustActivityProbe cancelled");
                return Err(ActivityError::cancelled());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            ctx.record_heartbeat(vec![
                serde_json::json!({ "iteration": iteration })
                    .as_json_payload()
                    .expect("heartbeat detail must serialize"),
            ]);
        }

        if ctx.is_cancelled() {
            return Err(ActivityError::cancelled());
        }

        Ok(ProbeResult {
            echoed: input.message,
            worker_language: "rust".to_string(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let address =
        std::env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| "localhost:7233".to_string());
    let target = if address.contains("://") {
        address.clone()
    } else {
        format!("http://{address}")
    };
    let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_string());
    let identity = format!("rust-ingestion-worker@{}", std::process::id());

    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()?,
    )?;
    let connection =
        Connection::connect(ConnectionOptions::new(Url::parse(&target)?).build()).await?;
    let client = Client::new(connection, ClientOptions::new(namespace.clone()).build())?;

    let worker_options = WorkerOptions::new(TASK_QUEUE)
        .task_types(WorkerTaskTypes::activity_only())
        .client_identity_override(identity.clone())
        .register_activities(ProbeActivities)
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    println!(
        "[rust-probe-worker] ready task_queue={TASK_QUEUE} identity={identity} activity={}",
        ProbeActivities::rust_activity_probe.name()
    );
    worker.run().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_probe_contract_round_trips_camel_case_payloads() {
        // Exactly what the TS workflow sends for `rustActivityProbe`.
        let ts_payload = r#"{"message":"hello","iterations":20}"#;
        let input: ProbeInput =
            serde_json::from_str(ts_payload).expect("TS payload must deserialize");
        assert_eq!(input.message, "hello");
        assert_eq!(input.iterations, 20);

        let result = ProbeResult {
            echoed: "hello".to_string(),
            worker_language: "rust".to_string(),
        };
        let json: serde_json::Value = serde_json::to_value(&result).expect("result must serialize");
        assert_eq!(json["echoed"], "hello");
        assert_eq!(json["workerLanguage"], "rust");
        assert!(
            json.get("worker_language").is_none(),
            "result must use camelCase `workerLanguage`, not snake_case"
        );
    }
}
