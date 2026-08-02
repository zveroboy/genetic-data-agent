//! The genomic ingestion data plane's Temporal worker.
//!
//! A bootstrap and nothing more: it connects, builds the shared S3 client **once**, registers
//! the single `buildDatasetArtifact` Activity and polls. Every decision about ingestion lives in
//! [`rust_ingestion_worker::temporal_activities`].
//!
//! Two properties of this worker are contractual rather than incidental:
//!
//! - It is **activity-only** ([`WorkerTaskTypes::activity_only`]). `genomic-ingestion-rust` never
//!   serves a Workflow task; Workflow code lives exclusively in the TypeScript control plane, and
//!   a Rust worker that polled for Workflow tasks on this queue would silently strand them.
//! - Its identity is prefixed `rust-ingestion-worker@`, so a pending Activity in Temporal history
//!   is attributable to the Rust data plane without cross-referencing anything.
//!
//! ```text
//! TEMPORAL_ADDRESS=localhost:7233 TEMPORAL_NAMESPACE=default \
//! S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=… S3_SECRET_KEY=… \
//!   cargo run --bin temporal_worker
//! ```

use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use rust_ingestion_worker::temporal_activities::{
    install_quiet_cancellation_panic_hook, staging_root_from_env, IngestionActivities,
    ACTIVITY_TYPE, TASK_QUEUE, WORKER_IDENTITY_PREFIX,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        // A worker's output is read by log collectors and by the integration test, not by a
        // terminal; colour escapes only make structured fields harder to parse.
        .with_ansi(false)
        .init();
    // Installed before any Activity runs, so an ordinary cancellation never prints a panic.
    install_quiet_cancellation_panic_hook();

    let address =
        std::env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| "localhost:7233".to_string());
    let target = if address.contains("://") {
        address.clone()
    } else {
        format!("http://{address}")
    };
    let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_string());
    let identity = format!("{WORKER_IDENTITY_PREFIX}{}", std::process::id());

    // Built once, here: `aws_config`'s loader is async and does real work, so paying for it on
    // every Activity attempt would tax exactly the retries that are already going badly.
    let staging_root = staging_root_from_env();
    let activities = IngestionActivities::from_env().await?;

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
        .register_activities(activities)
        .build();
    let mut worker = Worker::new(&runtime, client, worker_options)?;

    let registered = IngestionActivities::build_dataset_artifact.name();
    assert_eq!(
        registered, ACTIVITY_TYPE,
        "the registered activity type must be the name the TypeScript workflow schedules"
    );
    // The readiness line the integration test waits for. Printed rather than logged so it cannot
    // be filtered out by `RUST_LOG`.
    println!(
        "[rust-ingestion-worker] ready task_queue={TASK_QUEUE} identity={identity} \
         activity={registered} namespace={namespace} staging_root={}",
        staging_root.display()
    );

    worker.run().await?;
    Ok(())
}
