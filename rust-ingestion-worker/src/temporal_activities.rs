//! The Temporal adapter: the thin layer that joins the pure processor to the object store and
//! presents the pair as the `buildDatasetArtifact` Activity.
//!
//! Everything genuinely *about* ingestion lives elsewhere — [`crate::vcf`] parses,
//! [`crate::artifact`] stages and exports, [`crate::object_store`] speaks S3. This module owns
//! only what those three deliberately do not:
//!
//! - **Attempt scoping.** Each attempt gets its own local workspace (staging database, Parquet
//!   export directory, downloaded source) and its own S3 prefix, so no retry can append to a
//!   previous attempt's output. See [`AttemptWorkspace`] and [`validated_attempt_prefix`].
//! - **The S3 key mapping.** `relativePath` deliberately excludes the `variants/` segment so the
//!   dataset checksum stays independent of any attempt's prefix; the segment is contributed
//!   here, by [`object_key_for`], and nowhere else. No part of a key is ever derived from file
//!   content.
//! - **Heartbeats and cancellation.** [`HeartbeatReporter`] projects the processor's
//!   Temporal-free [`ProgressEvent`]s onto the frozen [`IngestionHeartbeat`] payload and is the
//!   point at which a cancellation stops the run.
//! - **Failure mapping.** Every error becomes an [`IngestionFailure`], which carries a frozen
//!   [`FailureType`] and therefore its retryability, before it is turned into a Temporal
//!   `ApplicationFailure`. The workflow's `nonRetryableErrorTypes` list matches those names by
//!   value.
//!
//! The Activity never writes `manifest.json`. Publication — and with it the only readiness
//! signal a dataset has — belongs to the TypeScript `publishDataset` Activity.

use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use futures::stream::{self, StreamExt};
use serde_json::Value;
use temporalio_common::error::ApplicationFailure;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::artifact::{
    build_artifact, dataset_checksum_sha256, ArtifactBuildRequest, ArtifactError, ArtifactStats,
    LocalParquetFile, DEFAULT_BATCH_SIZE,
};
use crate::contracts::{
    BuildDatasetArtifactInput, BuildDatasetArtifactResult, FailureType, IngestionHeartbeat,
    IngestionPhase, ParquetObject, CONTRACT_VERSION, PARQUET_SCHEMA_FINGERPRINT, VARIANTS_SEGMENT,
};
use crate::models::{ProgressEvent, ProgressSink};
use crate::object_store::{
    ObjectStoreConfig, ObjectStoreError, S3ObjectStore, UploadRequest, UploadedObject,
};

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

/// Keepalive period when the Workflow scheduled the Activity without a `heartbeatTimeout`.
const DEFAULT_KEEPALIVE_PERIOD: Duration = Duration::from_secs(5);

/// The keepalive re-reports this often relative to the negotiated `heartbeatTimeout`, leaving
/// room for two missed ticks before the server would time the Activity out.
const KEEPALIVE_DIVISOR: u32 = 3;

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
// Heartbeats
// -----------------------------------------------------------------------------------------

/// Where a heartbeat goes and where a cancellation comes from.
///
/// This exists so the adapter can be driven in a unit test: `ActivityContext` cannot be
/// constructed outside a running worker, and a heartbeat projection that is only exercised
/// end to end is a projection nobody can assert the shape of.
pub trait HeartbeatChannel: Send + Sync + 'static {
    /// Publishes one heartbeat. The frozen struct is handed over rather than a pre-serialized
    /// payload so the wire encoding stays the contract type's own — field order included.
    fn record(&self, heartbeat: &IngestionHeartbeat);
    /// Whether the Activity has been cancelled.
    fn is_cancelled(&self) -> bool;
}

/// Projects the processor's progress onto the frozen [`IngestionHeartbeat`] payload.
///
/// The projection is cumulative: an event updates the fields it knows about and leaves the rest
/// standing, so a heartbeat published during the upload stage still carries the variant count
/// the parse established. `completedFiles` is therefore monotone — it counts Parquet files
/// completely written and validated, which does not become untrue once uploading starts —
/// while `uploadedBytes` is what tracks the upload's own progress.
pub struct HeartbeatReporter {
    channel: Arc<dyn HeartbeatChannel>,
    state: Mutex<IngestionHeartbeat>,
}

impl HeartbeatReporter {
    pub fn new<C: HeartbeatChannel>(channel: Arc<C>) -> Self {
        Self {
            channel,
            state: Mutex::new(IngestionHeartbeat {
                phase: IngestionPhase::DownloadingSource,
                processed_bytes: 0,
                processed_variants: 0,
                current_partition: None,
                completed_files: 0,
                uploaded_bytes: 0,
            }),
        }
    }

    /// Folds a processor event into the running picture without publishing anything.
    pub fn absorb(&self, event: &ProgressEvent) {
        let mut state = self.locked();
        state.phase = event.phase;
        state.processed_bytes = event.processed_bytes;
        state.processed_variants = event.processed_variants;
        state.current_partition = event.current_partition.clone();
        state.completed_files = event.completed_files;
    }

    /// Adds one uploaded object's bytes to the running total.
    pub fn note_uploaded_bytes(&self, bytes: u64) {
        self.locked().uploaded_bytes += bytes;
    }

    /// Publishes the current picture under `phase`, working on `current_partition`.
    pub fn emit(&self, phase: IngestionPhase, current_partition: Option<&str>) {
        let heartbeat = {
            let mut state = self.locked();
            state.phase = phase;
            state.current_partition = current_partition.map(str::to_string);
            state.clone()
        };
        self.publish(&heartbeat);
    }

    /// Re-publishes the last observation unchanged.
    ///
    /// Two stages of the pipeline are a single uninterruptible call with no progress callback —
    /// streaming a multi-gigabyte source onto disk, and DuckDB's one `COPY … TO` that exports
    /// the whole dataset. Without this the Activity would look silent to the server and be
    /// killed by the Workflow's 15-second `heartbeatTimeout` long before it was actually stuck.
    pub fn reemit(&self) {
        let heartbeat = self.locked().clone();
        self.publish(&heartbeat);
    }

    pub fn is_cancelled(&self) -> bool {
        self.channel.is_cancelled()
    }

    fn publish(&self, heartbeat: &IngestionHeartbeat) {
        self.channel.record(heartbeat);
    }

    /// A poisoned heartbeat mutex means another thread panicked mid-update. The recorded numbers
    /// are only ever whole-field writes, so the worst case is one stale count in one heartbeat —
    /// not a reason to fail an ingestion that is otherwise healthy.
    fn locked(&self) -> std::sync::MutexGuard<'_, IngestionHeartbeat> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The processor reports through this. Each report is a heartbeat *and* a cancellation
/// checkpoint: see [`run_until_cancelled`].
impl ProgressSink for HeartbeatReporter {
    fn report(&self, event: &ProgressEvent) {
        self.absorb(event);
        self.reemit();
        if self.is_cancelled() {
            abort_processing();
        }
    }
}

// -----------------------------------------------------------------------------------------
// Cancellation
// -----------------------------------------------------------------------------------------

/// The panic payload used to unwind out of the blocking processor.
struct CancellationRequested;

/// Abandons the current processor run at a heartbeat boundary.
///
/// [`ProgressSink::report`] returns `()`: the processor is a synchronous, uninterruptible call
/// that cannot be handed an error, and its signature is a frozen seam shared with the CLI and
/// the acceptance tests. Unwinding from the callback is therefore the only way to *actually*
/// stop reading the source and writing the staging database when a cancellation arrives, rather
/// than merely noting it and letting a full-genome build run to completion first.
///
/// The unwind is contained: it is raised only from the sink, only in this module, and is caught
/// by [`run_until_cancelled`] immediately around the processor call. Everything it passes
/// through — the VCF reader, the DuckDB connection and appender — releases its resources in
/// `Drop`.
fn abort_processing() -> ! {
    std::panic::panic_any(CancellationRequested)
}

/// Runs the blocking processor, returning `None` if a cancellation stopped it.
///
/// A panic that is *not* the cancellation sentinel is re-raised unchanged: a genuine bug must
/// never be laundered into "the user cancelled".
pub fn run_until_cancelled<T>(body: impl FnOnce() -> T) -> Option<T> {
    match std::panic::catch_unwind(AssertUnwindSafe(body)) {
        Ok(value) => Some(value),
        Err(payload) if payload.is::<CancellationRequested>() => None,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// Keeps the cancellation unwind out of the log.
///
/// The default panic hook prints before the unwind reaches [`run_until_cancelled`], which would
/// make every ordinary cancellation look like a crash. This installs one hook, once, that stays
/// silent for the sentinel and delegates every other panic to the hook that was already in
/// place.
pub fn install_quiet_cancellation_panic_hook() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if info.payload().is::<CancellationRequested>() {
                return;
            }
            previous(info);
        }));
    });
}

// -----------------------------------------------------------------------------------------
// Attempt scoping
// -----------------------------------------------------------------------------------------

/// The single immutable prefix a dataset's artifact version may be written under.
///
/// Derived from the identifiers, never taken from the wire: `target.allowedPrefix` arrives as
/// data, and a widened value such as `datasets/` would satisfy every containment check below.
/// Mirrors `allowedPrefixFor` in `ingestion-contracts.ts`.
pub fn derived_allowed_prefix(dataset_id: &str, artifact_version: &str) -> String {
    format!("datasets/{dataset_id}/versions/{artifact_version}/")
}

/// The prefix one Activity attempt owns, strictly below `allowed_prefix`.
pub fn attempt_prefix_for(allowed_prefix: &str, attempt: u32) -> String {
    format!("{allowed_prefix}attempt-{attempt}/")
}

/// Derives and re-checks this attempt's writable prefix.
///
/// Three refusals, all deterministic:
///
/// 1. `datasetId` and `artifactVersion` must be single safe path segments — they flow verbatim
///    into the prefix, and `..` or a `/` would climb straight out of the dataset's namespace.
/// 2. The declared `target.allowedPrefix` must be exactly the derived one
///    (`ALLOWED_PREFIX_MISMATCH` on the TypeScript side).
/// 3. The attempt prefix must be strictly below it. The object-store adapter validates only that
///    a *key* sits below the prefix it is handed; this is the check that the prefix itself is
///    contained, which TypeScript enforces as `ATTEMPT_PREFIX_OUTSIDE_ALLOWED_PREFIX`.
pub fn validated_attempt_prefix(
    input: &BuildDatasetArtifactInput,
    attempt: u32,
) -> Result<String, IngestionFailure> {
    if !is_safe_path_segment(&input.dataset_id) {
        return Err(IngestionFailure::validation(format!(
            "dataset id '{}' is not a single safe path segment",
            input.dataset_id
        )));
    }
    if !is_safe_path_segment(&input.target.artifact_version) {
        return Err(IngestionFailure::validation(format!(
            "artifact version '{}' is not a single safe path segment",
            input.target.artifact_version
        )));
    }

    let allowed = derived_allowed_prefix(&input.dataset_id, &input.target.artifact_version);
    if input.target.allowed_prefix != allowed {
        return Err(IngestionFailure::validation(format!(
            "the input declares allowed prefix '{}' but '{}'/'{}' derives '{allowed}'",
            input.target.allowed_prefix, input.dataset_id, input.target.artifact_version
        )));
    }

    let prefix = attempt_prefix_for(&allowed, attempt);
    if !prefix.starts_with(&allowed) || prefix.len() == allowed.len() {
        return Err(IngestionFailure::validation(format!(
            "attempt prefix '{prefix}' is not strictly below '{allowed}'"
        )));
    }
    Ok(prefix)
}

/// `^[A-Za-z0-9][A-Za-z0-9._-]*$`, matching `pathSegmentSchema` in `ingestion-contracts.ts`.
/// `=` is excluded on purpose: it appears in a key only in a `chrom=<value>` partition
/// directory, and must not be smuggled into a dataset or version segment.
fn is_safe_path_segment(segment: &str) -> bool {
    let mut characters = segment.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && characters.all(|character| {
            character.is_ascii_alphanumeric()
                || character == '.'
                || character == '_'
                || character == '-'
        })
}

/// Composes `{attemptPrefix}variants/{relativePath}`.
///
/// The `variants/` segment lives only in the S3 key. `relativePath` — the unit the dataset
/// checksum is computed from — never carries it, which is what makes the checksum reproducible
/// under any attempt prefix.
pub fn object_key_for(attempt_prefix: &str, relative_path: &str) -> String {
    format!("{attempt_prefix}{VARIANTS_SEGMENT}{relative_path}")
}

/// A file name for one attempt's workspace, unique per (Workflow, Activity, attempt).
///
/// The Workflow ID is caller-influenced data, so it is reduced to alphanumerics, `-` and `_`
/// before it can become a directory name: a `../` in it must not be able to point the workspace
/// — and therefore the recursive cleanup — anywhere but below the staging root.
pub fn attempt_workspace_name(workflow_id: &str, activity_id: &str, attempt: u32) -> String {
    format!(
        "{}-{}-attempt-{attempt}",
        sanitize_identifier(workflow_id),
        sanitize_identifier(activity_id)
    )
}

/// Longest identifier fragment kept in a workspace name, so a pathological Workflow ID cannot
/// push the path past the filesystem's limit.
const MAX_IDENTIFIER_FRAGMENT: usize = 64;

fn sanitize_identifier(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .take(MAX_IDENTIFIER_FRAGMENT)
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "unnamed".to_string()
    } else {
        sanitized
    }
}

/// One attempt's private scratch directory: the downloaded source, the staging database and the
/// Parquet export all live inside it, and it is removed on every exit path.
///
/// It is created with `create_dir`, never `create_dir_all`, so an existing directory is a hard
/// error rather than something to reuse. That is both the "an attempt never reuses a path" rule
/// and the guarantee that [`Drop`] can only ever delete a tree this run created.
#[derive(Debug)]
pub struct AttemptWorkspace {
    root: PathBuf,
}

impl AttemptWorkspace {
    pub fn create(staging_root: &Path, name: &str) -> Result<Self, IngestionFailure> {
        std::fs::create_dir_all(staging_root).map_err(|error| {
            IngestionFailure::new(
                FailureType::ArtifactWriteFailed,
                format!(
                    "cannot create the staging root '{}': {error}",
                    staging_root.display()
                ),
            )
        })?;

        let root = staging_root.join(name);
        std::fs::create_dir(&root).map_err(|error| {
            IngestionFailure::new(
                FailureType::ArtifactWriteFailed,
                format!(
                    "cannot create the attempt workspace '{}': {error}; an attempt never reuses a path",
                    root.display()
                ),
            )
        })?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn source_path(&self) -> PathBuf {
        self.root.join("source.vcf")
    }

    pub fn staging_db_path(&self) -> PathBuf {
        self.root.join("staging.duckdb")
    }

    pub fn parquet_dir(&self) -> PathBuf {
        self.root.join("parquet")
    }
}

impl Drop for AttemptWorkspace {
    fn drop(&mut self) {
        // Scoped to this attempt's own tree, which `create` proved did not exist beforehand.
        // Nothing in S3 is ever removed: an abandoned attempt prefix stays orphaned and
        // unqueryable, because no manifest names it.
        if let Err(error) = std::fs::remove_dir_all(&self.root) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %self.root.display(),
                    %error,
                    "could not remove the attempt workspace"
                );
            }
        }
    }
}

// -----------------------------------------------------------------------------------------
// Local → wire inventory mapping
// -----------------------------------------------------------------------------------------

/// Pairs the processor's local descriptors with the identities S3 returned.
///
/// `byteSize` comes from the *local* descriptor, because [`UploadedObject`] does not carry one
/// and the TypeScript verifier compares `head.contentLength` against it before it will publish.
/// Everything else that identifies the object — bucket, key, ETag, version ID — comes from the
/// upload receipt, and the receipt's key must be exactly the one this layer composed.
pub fn published_inventory(
    attempt_prefix: &str,
    files: &[LocalParquetFile],
    receipts: &[UploadedObject],
) -> Result<Vec<ParquetObject>, IngestionFailure> {
    if files.len() != receipts.len() {
        return Err(IngestionFailure::validation(format!(
            "the export produced {} Parquet files but {} were uploaded; a published inventory \
             must describe every one of them",
            files.len(),
            receipts.len()
        )));
    }

    let mut inventory = Vec::with_capacity(files.len());
    for (file, receipt) in files.iter().zip(receipts) {
        let expected_key = object_key_for(attempt_prefix, &file.relative_path);
        if receipt.key != expected_key {
            return Err(IngestionFailure::validation(format!(
                "'{}' was uploaded to '{}' but its descriptor composes '{expected_key}'; a \
                 descriptor and an upload from different attempts must never be published together",
                file.relative_path, receipt.key
            )));
        }
        if receipt.bucket != receipts[0].bucket {
            return Err(IngestionFailure::validation(format!(
                "'{}' is in bucket '{}' but the inventory started in '{}'",
                receipt.key, receipt.bucket, receipts[0].bucket
            )));
        }
        inventory.push(ParquetObject {
            bucket: receipt.bucket.clone(),
            key: receipt.key.clone(),
            etag: receipt.etag.clone(),
            version_id: receipt.version_id.clone(),
            chrom: file.chrom.clone(),
            checksum_sha256: file.checksum_sha256.clone(),
            byte_size: file.byte_size,
            row_count: file.row_count,
            min_pos: file.min_pos,
            max_pos: file.max_pos,
        });
    }
    Ok(inventory)
}

/// Re-derives the dataset checksum from the *published* inventory and compares it to the one the
/// processor computed locally.
///
/// The relative path is recovered by stripping `{attemptPrefix}variants/`, exactly as
/// `dataset-checksum.ts` does before it will accept a publish. Running the processor's own
/// canonicalisation over the mapped keys is what proves the mapping layer did not disturb the
/// content identity — a dropped `variants/` segment, a descriptor paired with the wrong object,
/// a statistic transcribed wrongly.
pub fn assert_inventory_checksum(
    attempt_prefix: &str,
    objects: &[ParquetObject],
    expected: &str,
) -> Result<(), IngestionFailure> {
    let variants_prefix = format!("{attempt_prefix}{VARIANTS_SEGMENT}");
    let mut files = Vec::with_capacity(objects.len());
    for object in objects {
        let relative_path = object.key.strip_prefix(&variants_prefix).ok_or_else(|| {
            IngestionFailure::validation(format!(
                "object key '{}' is not below '{variants_prefix}'",
                object.key
            ))
        })?;
        files.push(LocalParquetFile {
            relative_path: relative_path.to_string(),
            chrom: object.chrom.clone(),
            checksum_sha256: object.checksum_sha256.clone(),
            byte_size: object.byte_size,
            row_count: object.row_count,
            min_pos: object.min_pos,
            max_pos: object.max_pos,
            schema_fingerprint: PARQUET_SCHEMA_FINGERPRINT.to_string(),
        });
    }

    let computed = dataset_checksum_sha256(&files);
    if computed != expected {
        return Err(IngestionFailure::validation(format!(
            "the published inventory hashes to '{computed}' but the processor computed \
             '{expected}'"
        )));
    }
    Ok(())
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
        install_quiet_cancellation_panic_hook();

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

        let outcome = tokio::task::spawn_blocking(move || {
            run_until_cancelled(|| build_artifact(&request, sink.as_ref()))
        })
        .await
        .map_err(|error| {
            // The processor thread panicked or was aborted. It says nothing about the input, so
            // it stays retryable; the local workspace is removed either way.
            IngestionFailure::new(
                FailureType::ArtifactWriteFailed,
                format!("the ingestion processor thread did not finish: {error}"),
            )
        })?;

        match outcome {
            None => Err(Interrupted::Cancelled),
            Some(result) => Ok(result.map_err(IngestionFailure::from)?),
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

/// Every published object must be in the bucket the input targets — the artifact bucket is
/// deployment configuration, and an inventory that names another one would fail the TypeScript
/// verifier's `BUCKET_MISMATCH` after the objects had already been written.
fn assert_published_bucket(
    objects: &[ParquetObject],
    expected_bucket: &str,
) -> Result<(), IngestionFailure> {
    for object in objects {
        if object.bucket != expected_bucket {
            return Err(IngestionFailure::validation(format!(
                "'{}' was published to bucket '{}', the input targets '{expected_bucket}'",
                object.key, object.bucket
            )));
        }
    }
    Ok(())
}

fn stop_if_cancelled(ctx: &ActivityContext) -> Result<(), Interrupted> {
    if ctx.is_cancelled() {
        Err(Interrupted::Cancelled)
    } else {
        Ok(())
    }
}

/// Publishes heartbeats through the real `ActivityContext`.
struct ActivityHeartbeatChannel {
    context: ActivityContext,
}

impl ActivityHeartbeatChannel {
    fn new(context: ActivityContext) -> Self {
        Self { context }
    }
}

impl HeartbeatChannel for ActivityHeartbeatChannel {
    fn record(&self, heartbeat: &IngestionHeartbeat) {
        // A struct of primitives and a `String`; serializing it cannot fail, and a panic here
        // would be a genuine bug in the frozen contract type rather than something to swallow.
        let payload = heartbeat
            .as_json_payload()
            .expect("an ingestion heartbeat is JSON by construction");
        let rendered = serde_json::to_string(heartbeat)
            .expect("an ingestion heartbeat is JSON by construction");
        // Logged as well as recorded, from the one call site that records: the SDK throttles
        // heartbeats before they reach the server, so the log is the only complete, ordered
        // account of the phases an attempt went through.
        tracing::info!(heartbeat = %rendered, "ingestion heartbeat");
        self.context.record_heartbeat(vec![payload]);
    }

    fn is_cancelled(&self) -> bool {
        self.context.is_cancelled()
    }
}

/// Re-reports the last observation on a timer for as long as it lives.
struct Keepalive {
    handle: tokio::task::JoinHandle<()>,
}

impl Keepalive {
    fn spawn(reporter: Arc<HeartbeatReporter>, period: Duration) -> Self {
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            // `interval` fires immediately; the first real heartbeat is the caller's job.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                reporter.reemit();
            }
        });
        Self { handle }
    }
}

impl Drop for Keepalive {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// A third of the negotiated `heartbeatTimeout`, so two ticks can be missed before the server
/// would consider the Activity dead.
fn keepalive_period(ctx: &ActivityContext) -> Duration {
    ctx.info()
        .heartbeat_timeout
        .map(|timeout| timeout / KEEPALIVE_DIVISOR)
        .unwrap_or(DEFAULT_KEEPALIVE_PERIOD)
        .max(Duration::from_secs(1))
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
