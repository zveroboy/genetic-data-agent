//! The Temporal adapter: the thin layer that joins the pure processor to the object store and
//! presents the pair as the `buildDatasetArtifact` Activity.
//!
//! Everything genuinely *about* ingestion lives elsewhere — [`crate::vcf`] parses,
//! [`crate::artifact`] stages and exports, [`crate::object_store`] speaks S3. This module owns
//! only what those three deliberately do not, and each of those concerns is one submodule:
//!
//! - **Attempt scoping** ([`attempt`]). Each attempt gets its own local workspace (staging
//!   database, Parquet export directory, downloaded source) and its own S3 prefix, so no retry
//!   can append to a previous attempt's output. See [`AttemptWorkspace`] and
//!   [`validated_attempt_prefix`].
//! - **The S3 key mapping** ([`inventory`]). `relativePath` deliberately excludes the `variants/`
//!   segment so the dataset checksum stays independent of any attempt's prefix; the segment is
//!   contributed there, by [`object_key_for`], and nowhere else. No part of a key is ever derived
//!   from file content.
//! - **Heartbeats and cancellation** ([`heartbeat`]). [`HeartbeatReporter`] projects the
//!   processor's Temporal-free [`crate::models::ProgressEvent`]s onto the frozen
//!   [`crate::contracts::IngestionHeartbeat`] payload and is the point at which a cancellation
//!   stops the run.
//! - **Failure mapping** (this file). Every error becomes an [`IngestionFailure`], which carries a
//!   frozen [`FailureType`] and therefore its retryability, before it is turned into a Temporal
//!   `ApplicationFailure`. The workflow's `nonRetryableErrorTypes` list matches those names by
//!   value.
//!
//! What is left here is the Activity itself: download, build, upload, map the result onto the
//! wire type.
//!
//! The Activity never writes `manifest.json`. Publication — and with it the only readiness
//! signal a dataset has — belongs to the TypeScript `publishDataset` Activity.

mod attempt;
mod heartbeat;
mod inventory;

pub use attempt::{
    attempt_prefix_for, attempt_workspace_name, derived_allowed_prefix, validated_attempt_prefix,
    AttemptWorkspace,
};
pub use heartbeat::{
    phase_rank, HeartbeatChannel, HeartbeatReporter, KeepaliveTick, KEEPALIVE_BUDGET_TICKS,
    PHASE_ORDER,
};
pub use inventory::{assert_inventory_checksum, object_key_for, published_inventory};

use std::path::PathBuf;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use serde_json::Value;
use temporalio_common::error::ApplicationFailure;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::artifact::{
    build_artifact, ArtifactBuildRequest, ArtifactError, ArtifactStats, DEFAULT_BATCH_SIZE,
};
use crate::contracts::{
    BuildDatasetArtifactInput, BuildDatasetArtifactResult, FailureType, IngestionPhase,
    CONTRACT_VERSION,
};
use crate::object_store::{
    ObjectStoreConfig, ObjectStoreError, S3ObjectStore, UploadRequest, UploadedObject,
};

use self::heartbeat::{keepalive_period, ActivityHeartbeatChannel, Keepalive};
use self::inventory::assert_published_bucket;

/// The Activity Type the TypeScript Workflow schedules by name.
pub const ACTIVITY_TYPE: &str = "buildDatasetArtifact";

/// The activity-only task queue this worker polls. No Workflow is ever hosted on it.
pub const TASK_QUEUE: &str = "genomic-ingestion-rust";

/// Prefix of the worker identity, so a pending activity in Temporal history is attributable to
/// the Rust data plane at a glance.
pub const WORKER_IDENTITY_PREFIX: &str = "rust-ingestion-worker@";

/// How many Parquet objects are uploaded at once. Bounded because a partition is streamed from
/// disk per upload: the ceiling on concurrent transfers is the ceiling on socket and file
/// handles the activity holds.
pub const UPLOAD_CONCURRENCY: usize = 4;

/// Environment variable naming the directory attempt workspaces are created under.
pub const STAGING_ROOT_VARIABLE: &str = "INGESTION_STAGING_ROOT";

// -----------------------------------------------------------------------------------------
// Failure mapping
// -----------------------------------------------------------------------------------------

/// A failure on its way out of the Activity, already reduced to the frozen taxonomy.
///
/// Retryability is *never* decided here: it is read back off [`FailureType::is_retryable`], so
/// the adapter cannot drift away from `contracts/ingestion-v1.md` or from the workflow's
/// `nonRetryableErrorTypes` list.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct IngestionFailure {
    failure_type: FailureType,
    message: String,
}

impl IngestionFailure {
    pub fn new(failure_type: FailureType, message: impl Into<String>) -> Self {
        Self {
            failure_type,
            message: message.into(),
        }
    }

    /// A deterministic refusal by the adapter itself — a widened allowed prefix, a receipt that
    /// does not match the key it was asked to write. Retrying reproduces it exactly.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(FailureType::ArtifactValidationFailed, message)
    }

    pub fn failure_type(&self) -> FailureType {
        self.failure_type
    }

    /// Converts to the Temporal failure the workflow sees. The *type name* is the contract
    /// string, and `non_retryable` is set from the same taxonomy rather than left to the
    /// scheduling side: a retry policy that forgot to list a deterministic failure must not be
    /// able to resurrect an attempt that can only fail again.
    pub fn into_application_failure(self) -> ApplicationFailure {
        let non_retryable = !self.failure_type.is_retryable();
        let type_name = self.failure_type.as_str().to_string();
        ApplicationFailure::builder(self)
            .type_name(type_name)
            .non_retryable(non_retryable)
            .build()
    }
}

impl From<ArtifactError> for IngestionFailure {
    fn from(error: ArtifactError) -> Self {
        Self::new(error.failure_type(), error.to_string())
    }
}

impl From<ObjectStoreError> for IngestionFailure {
    fn from(error: ObjectStoreError) -> Self {
        Self::new(error.failure_type(), error.to_string())
    }
}

/// Why the Activity stopped early. A cancellation is not a failure and must not be reported as
/// one: Temporal records it as `ActivityTaskCanceled`, and the workflow's
/// `WAIT_CANCELLATION_COMPLETED` is waiting for exactly that.
#[derive(Debug)]
enum Interrupted {
    Cancelled,
    Failed(IngestionFailure),
}

impl<E: Into<IngestionFailure>> From<E> for Interrupted {
    fn from(error: E) -> Self {
        Self::Failed(error.into())
    }
}

// -----------------------------------------------------------------------------------------
// The Activity
// -----------------------------------------------------------------------------------------

/// The registered Activity implementation.
///
/// The S3 client is built once, at worker start, and shared by every attempt: `aws_config`'s
/// loader is `async` and does real work, so constructing it per attempt would pay that cost on
/// every retry. There is no other state — the struct holds nothing an attempt can mutate, and
/// everything an attempt needs is derived from its own `ActivityContext`.
pub struct IngestionActivities {
    store: Arc<S3ObjectStore>,
    staging_root: PathBuf,
    batch_size: usize,
}

impl IngestionActivities {
    pub fn new(store: S3ObjectStore, staging_root: PathBuf) -> Self {
        Self {
            store: Arc::new(store),
            staging_root,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Builds the shared S3 client from the same environment variables the TypeScript adapter
    /// reads. Called once, from the worker binary.
    pub async fn from_env() -> Result<Self, ObjectStoreError> {
        let config = ObjectStoreConfig::from_env()?;
        Ok(Self::new(S3ObjectStore::new(&config).await, staging_root_from_env()))
    }
}

/// Where attempt workspaces are created. Overridable so a deployment can point staging at a
/// volume sized for a whole genome rather than at the system temporary directory.
pub fn staging_root_from_env() -> PathBuf {
    std::env::var(STAGING_ROOT_VARIABLE)
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("genomic-ingestion"))
}

#[activities]
impl IngestionActivities {
    /// Streams the pinned source object into a chromosome-partitioned Parquet dataset and
    /// publishes it under this attempt's own immutable prefix.
    ///
    /// The input arrives as raw JSON rather than as a typed parameter so that a payload which
    /// violates the frozen contract — an unknown field, a `contractVersion` this worker does not
    /// implement — fails as a deterministic, non-retryable `ArtifactValidationFailed` instead of
    /// as whatever the SDK's payload converter would produce by default.
    #[activity(name = "buildDatasetArtifact")]
    pub async fn build_dataset_artifact(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: Value,
    ) -> Result<BuildDatasetArtifactResult, ActivityError> {
        let info = ctx.info();
        let attempt = info.attempt;
        let workspace_name = attempt_workspace_name(
            info.workflow_execution
                .as_ref()
                .map(|execution| execution.workflow_id.as_str())
                .unwrap_or_default(),
            &info.activity_id,
            attempt,
        );
        tracing::info!(
            activity_type = %info.activity_type,
            task_queue = %info.task_queue,
            attempt,
            workspace = %workspace_name,
            "buildDatasetArtifact started"
        );

        let reporter = Arc::new(HeartbeatReporter::new(Arc::new(
            ActivityHeartbeatChannel::new(ctx.clone()),
        )));
        let keepalive = Keepalive::spawn(reporter.clone(), keepalive_period(&ctx));

        let outcome = self
            .run(&ctx, &reporter, input, attempt, &workspace_name)
            .await;
        drop(keepalive);

        match outcome {
            Ok(result) => Ok(result),
            Err(Interrupted::Cancelled) => {
                tracing::info!(attempt, "buildDatasetArtifact cancelled");
                Err(ActivityError::cancelled())
            }
            Err(Interrupted::Failed(failure)) => {
                tracing::warn!(
                    attempt,
                    failure_type = %failure.failure_type(),
                    retryable = failure.failure_type().is_retryable(),
                    error = %failure,
                    "buildDatasetArtifact failed"
                );
                Err(ActivityError::application(failure.into_application_failure()))
            }
        }
    }
}

impl IngestionActivities {
    /// The Activity body, expressed in this crate's own error vocabulary so the Temporal-facing
    /// wrapper above stays a translation and nothing more.
    async fn run(
        &self,
        ctx: &ActivityContext,
        reporter: &Arc<HeartbeatReporter>,
        raw_input: Value,
        attempt: u32,
        workspace_name: &str,
    ) -> Result<BuildDatasetArtifactResult, Interrupted> {
        let input: BuildDatasetArtifactInput =
            serde_json::from_value(raw_input).map_err(|error| {
                IngestionFailure::validation(format!(
                    "the activity input does not satisfy ingestion contract v{CONTRACT_VERSION}: \
                     {error}"
                ))
            })?;
        let attempt_prefix = validated_attempt_prefix(&input, attempt)?;

        // Dropped at the end of this function — on success, on failure and on the cancellation
        // path alike — which removes the downloaded source, the staging database and the local
        // Parquet export. Nothing outside it is ever deleted.
        let workspace = AttemptWorkspace::create(&self.staging_root, workspace_name)?;

        reporter.emit(IngestionPhase::DownloadingSource, None);
        stop_if_cancelled(ctx)?;
        let downloaded = self
            .store
            .download_exact(&input.source, &workspace.source_path())
            .await
            .map_err(IngestionFailure::from)?;
        stop_if_cancelled(ctx)?;

        let stats = self
            .build_locally(reporter, &input, &workspace, &downloaded.etag)
            .await?;
        stop_if_cancelled(ctx)?;

        let receipts = self
            .publish_parquet(ctx, reporter, &input, &workspace, &attempt_prefix, &stats)
            .await?;

        let parquet_objects = published_inventory(
            &attempt_prefix,
            &stats.local_parquet_files,
            &receipts,
        )?;
        assert_published_bucket(&parquet_objects, &input.target.bucket)?;
        assert_inventory_checksum(
            &attempt_prefix,
            &parquet_objects,
            &stats.dataset_checksum_sha256,
        )?;
        if stats.reference_build != input.reference.build {
            return Err(IngestionFailure::validation(format!(
                "the processor reports reference build '{}' but '{}' was requested",
                stats.reference_build, input.reference.build
            ))
            .into());
        }

        reporter.emit(IngestionPhase::Finalizing, None);
        Ok(BuildDatasetArtifactResult {
            attempt_prefix,
            dataset_checksum_sha256: stats.dataset_checksum_sha256,
            variant_count: stats.variant_count,
            rejected_record_count: stats.rejected_record_count,
            reference_build: stats.reference_build,
            processor_version: stats.processor_version,
            parquet_objects,
        })
    }

    /// Runs the pure processor on the blocking pool, with the heartbeat reporter as its progress
    /// sink and therefore as its cancellation checkpoint.
    ///
    /// `spawn_blocking`, not `block_in_place`: the processor holds its thread for the whole of a
    /// parse-and-export, and the runtime only has one worker thread per core. Two or three
    /// concurrent attempts taking those threads would starve the gRPC poller and the keepalive
    /// that this very Activity depends on to stay alive.
    async fn build_locally(
        &self,
        reporter: &Arc<HeartbeatReporter>,
        input: &BuildDatasetArtifactInput,
        workspace: &AttemptWorkspace,
        source_etag: &str,
    ) -> Result<ArtifactStats, Interrupted> {
        let request = ArtifactBuildRequest {
            source_path: workspace.source_path(),
            staging_db_path: workspace.staging_db_path(),
            parquet_output_dir: workspace.parquet_dir(),
            dataset_id: input.dataset_id.clone(),
            source_etag: source_etag.to_string(),
            reference_build: input.reference.build.clone(),
            batch_size: self.batch_size,
        };
        let sink = reporter.clone();

        // `Ok(None)` is the processor answering the `ControlFlow::Break` the reporter returns
        // once the Activity has been cancelled — not a failure, and never reported as one.
        let outcome = tokio::task::spawn_blocking(move || build_artifact(&request, sink.as_ref()))
            .await
            .map_err(|error| {
                // The processor thread panicked or was aborted. It says nothing about the input,
                // so it stays retryable; the local workspace is removed either way.
                IngestionFailure::new(
                    FailureType::ArtifactWriteFailed,
                    format!("the ingestion processor thread did not finish: {error}"),
                )
            })?;

        match outcome.map_err(IngestionFailure::from)? {
            None => Err(Interrupted::Cancelled),
            Some(stats) => Ok(stats),
        }
    }

    /// Uploads every exported Parquet file to this attempt's prefix, with bounded concurrency
    /// and in canonical order.
    async fn publish_parquet(
        &self,
        ctx: &ActivityContext,
        reporter: &Arc<HeartbeatReporter>,
        input: &BuildDatasetArtifactInput,
        workspace: &AttemptWorkspace,
        attempt_prefix: &str,
        stats: &ArtifactStats,
    ) -> Result<Vec<UploadedObject>, Interrupted> {
        let parquet_dir = workspace.parquet_dir();
        let prepared: Vec<PreparedUpload> = stats
            .local_parquet_files
            .iter()
            .map(|file| PreparedUpload {
                key: object_key_for(attempt_prefix, &file.relative_path),
                local_path: parquet_dir.join(&file.relative_path),
                checksum_sha256: file.checksum_sha256.clone(),
                chrom: file.chrom.clone(),
                byte_size: file.byte_size,
            })
            .collect();

        let store = self.store.as_ref();
        let bucket = input.target.bucket.as_str();
        let mut transfers = Vec::with_capacity(prepared.len());
        for upload in &prepared {
            transfers.push(async move {
                let request = UploadRequest {
                    bucket,
                    attempt_prefix,
                    key: &upload.key,
                    local_path: &upload.local_path,
                    checksum_sha256: &upload.checksum_sha256,
                };
                store
                    .upload_file(&request)
                    .await
                    .map(|receipt| (upload, receipt))
            });
        }
        let mut uploads = stream::iter(transfers).buffered(UPLOAD_CONCURRENCY);

        let mut receipts = Vec::with_capacity(prepared.len());
        while let Some(result) = uploads.next().await {
            let (upload, receipt) = result.map_err(IngestionFailure::from)?;
            reporter.note_uploaded_bytes(upload.byte_size);
            reporter.emit(IngestionPhase::UploadingPartition, Some(&upload.chrom));
            receipts.push(receipt);
            // Dropping the stream here abandons the transfers still in flight. Nothing already
            // uploaded is deleted: the attempt prefix is simply orphaned, and no query path can
            // reach it because `publishDataset` never wrote a manifest naming it.
            if ctx.is_cancelled() {
                return Err(Interrupted::Cancelled);
            }
        }
        Ok(receipts)
    }
}

/// One Parquet file, resolved to everything the upload needs before any transfer starts.
struct PreparedUpload {
    key: String,
    local_path: PathBuf,
    checksum_sha256: String,
    chrom: String,
    byte_size: u64,
}

fn stop_if_cancelled(ctx: &ActivityContext) -> Result<(), Interrupted> {
    if ctx.is_cancelled() {
        Err(Interrupted::Cancelled)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The activity type registered with the worker must be the exact name the TypeScript
    /// workflow schedules; it is matched by string, not by any shared declaration.
    #[test]
    fn registers_the_contracted_activity_type() {
        assert_eq!(
            IngestionActivities::build_dataset_artifact.name(),
            ACTIVITY_TYPE
        );
    }

    #[test]
    fn the_staging_root_defaults_below_the_system_temporary_directory() {
        let root = staging_root_from_env();
        assert!(
            root.starts_with(std::env::temp_dir())
                || std::env::var(STAGING_ROOT_VARIABLE).is_ok(),
            "{}",
            root.display()
        );
    }
}
