//! Heartbeats and cancellation: the projection of the processor's Temporal-free
//! [`ProgressEvent`]s onto the frozen [`IngestionHeartbeat`] payload, the channel that payload
//! goes out on, and the keepalive that keeps an Activity alive across the two stages that have
//! no progress callback of their own.
//!
//! Two properties are this module's alone, and both are tested here:
//!
//! - **The published phase sequence never regresses.** [`PHASE_ORDER`] is the contract's ordered
//!   phase list, and [`projected_phase`] is what keeps the processor from reaching the terminal
//!   phase the adapter owns.
//! - **Publishing is ordered.** [`HeartbeatReporter::publish_locked`] hands the payload to the
//!   channel with the state lock still held, so wire order matches the order of the mutations
//!   that produced it even when the main task and the keepalive race.

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_sdk::activities::ActivityContext;

use crate::contracts::{IngestionHeartbeat, IngestionPhase};
use crate::models::{ProgressEvent, ProgressSink};

/// Keepalive period when the Workflow scheduled the Activity without a `heartbeatTimeout`.
const DEFAULT_KEEPALIVE_PERIOD: Duration = Duration::from_secs(5);

/// The keepalive re-reports this often relative to the negotiated `heartbeatTimeout`.
///
/// Reporting more often than the timeout is all this buys: sdk-core aggregates recorded
/// heartbeats and only sends one to the server every `min(heartbeatTimeout × 0.8,
/// max_heartbeat_throttle_interval)`, so ticking faster than that does not put more heartbeats on
/// the wire — it only makes sure there is always a fresh observation waiting when core's throttle
/// window opens. `temporal_worker.rs` sets `max_heartbeat_throttle_interval` (and
/// `default_heartbeat_throttle_interval`) to 5 seconds, matching this period exactly: against the
/// production 15-second `heartbeatTimeout`, `heartbeatTimeout × 0.8` is 12 seconds, so the
/// 5-second worker-local cap is what actually governs. That makes the two spare ticks this
/// constant implies real — the server-side margin is 10 seconds, not 3, and worst-case
/// cancellation observation is bounded at ~5 seconds rather than ~12.
const KEEPALIVE_DIVISOR: u32 = 3;

/// How long the keepalive will hold an Activity open on an observation that has not changed,
/// as a multiple of the negotiated `heartbeatTimeout`.
///
/// The keepalive exists because two stages of the pipeline have no progress callback: streaming
/// the pinned source object to disk, and DuckDB's per-partition `COPY`. Left unbounded it also
/// disables the thing `heartbeatTimeout` is *for* — a worker wedged in a hung socket read or a
/// DuckDB deadlock would keep reporting the last observation until `startToCloseTimeout`, which
/// production sets to 30 minutes. So the re-emits are budgeted, and once the budget is spent the
/// Activity falls silent and the server's own `heartbeatTimeout` retries it.
///
/// 40 × the 15-second production `heartbeatTimeout` is 10 minutes: one third of
/// `startToCloseTimeout`, so a genuine hang is detected three times sooner than before, and
/// comfortably more than the longest legitimate callback-free stretch. The longest is the
/// download of the largest catalogued source (`na12878-full`, a gzipped whole-genome VCF of a
/// couple of gigabytes); 10 minutes only becomes tight below a sustained few MB/s, an order of
/// magnitude under what the deployment's MinIO delivers. A partition `COPY` is seconds.
const KEEPALIVE_BUDGET_TIMEOUTS: u32 = 40;

/// The budget expressed in keepalive ticks, which is what the keepalive counts. The period is
/// `heartbeatTimeout / KEEPALIVE_DIVISOR`, so above a 3-second `heartbeatTimeout` the two
/// constants compose into a wall-clock bound that scales with the negotiated timeout and needs no
/// separate justification. Below 3 seconds, `keepalive_period` clamps the period to a 1-second
/// floor, so the wall-clock bound stops scaling with `heartbeatTimeout` and starts depending on
/// its actual value — this is only reachable with a pathologically short `heartbeatTimeout`, well
/// below anything this deployment negotiates.
pub const KEEPALIVE_BUDGET_TICKS: u32 = KEEPALIVE_BUDGET_TIMEOUTS * KEEPALIVE_DIVISOR;

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

/// The frozen phase list of `contracts/ingestion-v1.md`, **in order**. A consumer polling
/// heartbeats is entitled to read a phase as progress, so the published sequence must never
/// regress through it.
pub const PHASE_ORDER: [IngestionPhase; 6] = [
    IngestionPhase::DownloadingSource,
    IngestionPhase::Parsing,
    IngestionPhase::WritingDuckdb,
    IngestionPhase::ExportingParquet,
    IngestionPhase::UploadingPartition,
    IngestionPhase::Finalizing,
];

/// A phase's position in [`PHASE_ORDER`].
///
/// A `match` rather than a lookup into [`PHASE_ORDER`]: the array is a plain literal with no
/// compile-time link to the enum, so a seventh `IngestionPhase` variant added without updating it
/// would have silently fallen through to the `.expect()` this used to end in — a *runtime* panic,
/// on the very phase the omission was about, discovered only by whichever attempt happened to
/// reach it. `match` is exhaustive by construction: the compiler refuses to build until every
/// variant — old and new — has an arm, so an added variant is a compile error here instead.
pub fn phase_rank(phase: IngestionPhase) -> usize {
    match phase {
        IngestionPhase::DownloadingSource => 0,
        IngestionPhase::Parsing => 1,
        IngestionPhase::WritingDuckdb => 2,
        IngestionPhase::ExportingParquet => 3,
        IngestionPhase::UploadingPartition => 4,
        IngestionPhase::Finalizing => 5,
    }
}

/// Projects a *processor* phase onto the phase the adapter may publish.
///
/// `FINALIZING` is the last entry of an ordered list, and in this pipeline it belongs to the
/// adapter: it is published once, after every partition has been uploaded. The processor's own
/// work finishes with the local Parquet export, which is `EXPORTING_PARQUET` — uploads are still
/// to come. Letting a processor event through as `FINALIZING` would make the published sequence
/// reach the terminal phase, regress to `UPLOADING_PARTITION`, and reach it again.
///
/// The processor no longer emits `FINALIZING`, so this is a guard rather than a correction: the
/// ordering of the published sequence is the adapter's to guarantee, not something it inherits.
fn projected_phase(phase: IngestionPhase) -> IngestionPhase {
    match phase {
        IngestionPhase::Finalizing => IngestionPhase::ExportingParquet,
        other => other,
    }
}

/// Whether a keepalive tick published anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepaliveTick {
    /// The observation was re-sent (or had changed, which resets the budget).
    Published,
    /// The budget for an unchanged observation is spent. Nothing was sent, and nothing more will
    /// be until real progress arrives.
    BudgetSpent,
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
    state: Mutex<ReporterState>,
    /// How many times the keepalive may re-send an observation that has not changed.
    keepalive_budget: u32,
}

struct ReporterState {
    heartbeat: IngestionHeartbeat,
    /// The payload published last, and how many consecutive keepalive ticks have re-sent it
    /// unchanged. Real progress resets the count; see [`KEEPALIVE_BUDGET_TICKS`].
    published: Option<IngestionHeartbeat>,
    unchanged_reemits: u32,
}

impl HeartbeatReporter {
    pub fn new<C: HeartbeatChannel>(channel: Arc<C>) -> Self {
        Self::with_keepalive_budget(channel, KEEPALIVE_BUDGET_TICKS)
    }

    pub fn with_keepalive_budget<C: HeartbeatChannel>(channel: Arc<C>, budget: u32) -> Self {
        Self {
            channel,
            state: Mutex::new(ReporterState {
                heartbeat: IngestionHeartbeat {
                    phase: IngestionPhase::DownloadingSource,
                    processed_bytes: 0,
                    processed_variants: 0,
                    current_partition: None,
                    completed_files: 0,
                    uploaded_bytes: 0,
                },
                published: None,
                unchanged_reemits: 0,
            }),
            keepalive_budget: budget,
        }
    }

    /// Folds a processor event into the running picture without publishing anything.
    pub fn absorb(&self, event: &ProgressEvent) {
        let mut state = self.locked();
        state.heartbeat.phase = projected_phase(event.phase);
        state.heartbeat.processed_bytes = event.processed_bytes;
        state.heartbeat.processed_variants = event.processed_variants;
        state.heartbeat.current_partition = event.current_partition.clone();
        state.heartbeat.completed_files = event.completed_files;
    }

    /// Adds one uploaded object's bytes to the running total.
    pub fn note_uploaded_bytes(&self, bytes: u64) {
        self.locked().heartbeat.uploaded_bytes += bytes;
    }

    /// Publishes the current picture under `phase`, working on `current_partition`.
    ///
    /// This is the adapter's own progress, so `phase` is taken at face value: the adapter is the
    /// layer that owns `UPLOADING_PARTITION` and `FINALIZING`.
    pub fn emit(&self, phase: IngestionPhase, current_partition: Option<&str>) {
        let mut state = self.locked();
        state.heartbeat.phase = phase;
        state.heartbeat.current_partition = current_partition.map(str::to_string);
        state.unchanged_reemits = 0;
        self.publish_locked(&mut state);
    }

    /// Re-publishes the last observation, while the budget for doing so lasts.
    ///
    /// Two stages of the pipeline are a single uninterruptible call with no progress callback —
    /// streaming a multi-gigabyte source onto disk, and DuckDB's per-partition `COPY`. Without
    /// this the Activity would look silent to the server and be killed by the Workflow's
    /// 15-second `heartbeatTimeout` long before it was actually stuck.
    ///
    /// The budget is what keeps the cure from being worse than the disease: see
    /// [`KEEPALIVE_BUDGET_TIMEOUTS`]. An observation that *has* changed since the last publish is
    /// progress, is published, and resets the count.
    pub fn reemit(&self) -> KeepaliveTick {
        let mut state = self.locked();
        let unchanged = state.published.as_ref() == Some(&state.heartbeat);
        if unchanged {
            if state.unchanged_reemits >= self.keepalive_budget {
                return KeepaliveTick::BudgetSpent;
            }
            state.unchanged_reemits += 1;
        } else {
            state.unchanged_reemits = 0;
        }
        self.publish_locked(&mut state);
        KeepaliveTick::Published
    }

    pub fn is_cancelled(&self) -> bool {
        self.channel.is_cancelled()
    }

    /// Marks `state.heartbeat` as published and hands it to the channel, all while `state`'s lock
    /// guard stays held.
    ///
    /// This closes a publish-ordering window that used to exist here: `emit` and `reemit` each
    /// snapshotted the heartbeat under the lock, released it, and only *then* called
    /// `channel.record`. Two callers racing that way — the main task's `emit` and the keepalive's
    /// `reemit` are exactly such a pair — can have their `channel.record` calls land in the
    /// opposite order from the mutations that produced them: the keepalive snapshots
    /// `EXPORTING_PARQUET`, is overtaken by `emit(Finalizing)`, and then records its stale
    /// snapshot *after* the real one, publishing precisely the `FINALIZING → UPLOADING_PARTITION`
    /// regression `assertPhasesNeverRegress` (the integration test) exists to catch. Holding the
    /// guard from the snapshot straight through the call to `channel.record` makes wire order
    /// match lock-acquisition order, which matches the order the underlying mutations actually
    /// happened in. `channel.record` (`ActivityContext::record_heartbeat` in production, a plain
    /// counter in tests) does not call back into this reporter, so holding the lock across it
    /// cannot deadlock.
    fn publish_locked(&self, state: &mut ReporterState) {
        state.published = Some(state.heartbeat.clone());
        self.channel.record(&state.heartbeat);
    }

    /// A poisoned heartbeat mutex means another thread panicked mid-update. The recorded numbers
    /// are only ever whole-field writes, so the worst case is one stale count in one heartbeat —
    /// not a reason to fail an ingestion that is otherwise healthy.
    fn locked(&self) -> std::sync::MutexGuard<'_, ReporterState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The processor reports through this. Each report is a heartbeat *and* the point at which a
/// cancellation stops the run: [`ControlFlow::Break`] tells the processor to abandon the build
/// at this boundary, and [`crate::artifact::build_artifact`] answers with `Ok(None)`.
impl ProgressSink for HeartbeatReporter {
    fn report(&self, event: &ProgressEvent) -> ControlFlow<()> {
        {
            let mut state = self.locked();
            state.heartbeat.phase = projected_phase(event.phase);
            state.heartbeat.processed_bytes = event.processed_bytes;
            state.heartbeat.processed_variants = event.processed_variants;
            state.heartbeat.current_partition = event.current_partition.clone();
            state.heartbeat.completed_files = event.completed_files;
            state.unchanged_reemits = 0;
            self.publish_locked(&mut state);
        }
        if self.is_cancelled() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

/// Publishes heartbeats through the real `ActivityContext`.
pub(super) struct ActivityHeartbeatChannel {
    context: ActivityContext,
}

impl ActivityHeartbeatChannel {
    pub(super) fn new(context: ActivityContext) -> Self {
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

/// Re-reports the last observation on a timer, for a bounded stretch.
///
/// It keeps ticking for the Activity's whole life — a stage that resumes reporting restores the
/// budget — but it will not carry an *unchanged* observation past
/// [`KEEPALIVE_BUDGET_TICKS`] ticks. Past that the Activity goes quiet on purpose and the
/// server's `heartbeatTimeout` does the job it exists for.
pub(super) struct Keepalive {
    handle: tokio::task::JoinHandle<()>,
}

impl Keepalive {
    pub(super) fn spawn(reporter: Arc<HeartbeatReporter>, period: Duration) -> Self {
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            // `interval` fires immediately; the first real heartbeat is the caller's job.
            ticker.tick().await;
            let mut warned = false;
            loop {
                ticker.tick().await;
                if reporter.reemit() == KeepaliveTick::BudgetSpent && !warned {
                    warned = true;
                    tracing::warn!(
                        budget_ticks = KEEPALIVE_BUDGET_TICKS,
                        period_ms = period.as_millis() as u64,
                        "the ingestion activity has reported no new progress for the whole \
                         keepalive budget; heartbeats stop here so the server's heartbeat \
                         timeout can retry it"
                    );
                }
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
pub(super) fn keepalive_period(ctx: &ActivityContext) -> Duration {
    ctx.info()
        .heartbeat_timeout
        .map(|timeout| timeout / KEEPALIVE_DIVISOR)
        .unwrap_or(DEFAULT_KEEPALIVE_PERIOD)
        .max(Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Counts heartbeats. `Keepalive` is private, so the test that drives the real spawned task
    /// has to live in the crate.
    #[derive(Default)]
    struct CountingChannel(AtomicUsize);

    impl CountingChannel {
        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl HeartbeatChannel for CountingChannel {
        fn record(&self, _heartbeat: &IngestionHeartbeat) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn is_cancelled(&self) -> bool {
            false
        }
    }

    /// The regression this bound exists for: an always-on keepalive turns the Workflow's
    /// 15-second `heartbeatTimeout` into the 30-minute `startToCloseTimeout`, because a worker
    /// wedged in a hung S3 read or a DuckDB deadlock keeps re-sending its last observation
    /// forever. After the budget the spawned task must stop reaching the channel, so the server
    /// stops seeing heartbeats and can time the Activity out.
    #[tokio::test(start_paused = true)]
    async fn the_keepalive_stops_re_emitting_an_unchanged_observation_after_its_budget() {
        const BUDGET: u32 = 6;
        let period = Duration::from_millis(100);

        let channel = Arc::new(CountingChannel::default());
        let reporter = Arc::new(HeartbeatReporter::with_keepalive_budget(
            channel.clone(),
            BUDGET,
        ));
        reporter.emit(IngestionPhase::DownloadingSource, None);
        assert_eq!(channel.count(), 1, "the caller's own first heartbeat");

        let keepalive = Keepalive::spawn(reporter.clone(), period);

        /// Lets the spawned keepalive observe `ticks` periods of the paused clock. Stepping one
        /// period at a time is what gives the task a chance to be polled per tick.
        async fn tick(period: Duration, ticks: u32) {
            for _ in 0..ticks {
                tokio::time::advance(period).await;
                tokio::task::yield_now().await;
            }
        }

        // Far more ticks than the budget: the count must stop at the budget, not track the clock.
        tick(period, BUDGET * 10).await;
        assert_eq!(
            channel.count(),
            1 + BUDGET as usize,
            "the keepalive must re-send an unchanged observation at most {BUDGET} times"
        );

        // Real progress is a new observation, so the budget is restored and the keepalive
        // resumes: the bound detects a *stall*, it does not cap a long healthy activity.
        reporter.emit(IngestionPhase::Parsing, None);
        tick(period, BUDGET * 10).await;
        assert_eq!(
            channel.count(),
            2 + 2 * BUDGET as usize,
            "progress must restore the keepalive budget"
        );

        drop(keepalive);
    }

    /// The processor's terminal observation must not be published as the contract's terminal
    /// phase: `FINALIZING` is published once, by the adapter, after the uploads.
    #[test]
    fn the_processor_can_never_publish_the_terminal_phase() {
        for phase in PHASE_ORDER {
            let projected = projected_phase(phase);
            assert_ne!(
                projected,
                IngestionPhase::Finalizing,
                "{phase:?} must not reach the wire as FINALIZING"
            );
            assert!(
                phase_rank(projected) <= phase_rank(phase),
                "the projection must never advance a phase"
            );
        }
    }
}
