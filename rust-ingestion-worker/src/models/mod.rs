//! Value types shared by the streaming parser and the artifact builder, plus the progress
//! channel they report through.
//!
//! Nothing here knows about Temporal or S3. [`ProgressSink`] is the seam: the processor
//! reports [`ProgressEvent`]s, and a later task adapts them into Temporal heartbeats.

use serde::{Deserialize, Serialize};

use crate::contracts::IngestionPhase;

/// One accepted VCF record, in the shape the DuckDB `user_variants` staging table stores.
///
/// `rsid` is the only nullable column of the frozen Parquet schema; VCF spells a missing ID
/// `.`, which the parser turns into `None` rather than storing the placeholder verbatim.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserVariant {
    pub chrom: String,
    pub pos: u32,
    pub rsid: Option<String>,
    pub ref_allele: String,
    pub alt_allele: String,
    pub gt_raw: String,
}

/// A single progress observation from the processor.
///
/// The field set mirrors [`crate::contracts::IngestionHeartbeat`] so the adapter is a plain
/// projection, minus `uploadedBytes` (the processor never uploads) and plus
/// [`ProgressEvent::batch_records`], which exists so a caller — in particular the
/// bounded-memory acceptance test — can observe how much data is held in memory at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressEvent {
    pub phase: IngestionPhase,
    /// Uncompressed source bytes consumed so far.
    pub processed_bytes: u64,
    /// Accepted records staged so far.
    pub processed_variants: u64,
    /// The `chrom` partition currently being worked on, if any.
    pub current_partition: Option<String>,
    /// Parquet files completely written and validated so far.
    pub completed_files: u64,
    /// Records held in memory by the batch this event describes. Zero for events that do not
    /// describe a batch, so a reader can filter on it.
    pub batch_records: usize,
}

impl ProgressEvent {
    /// An event carrying only a phase, for the boundaries between processing stages.
    pub fn phase(phase: IngestionPhase) -> Self {
        Self {
            phase,
            processed_bytes: 0,
            processed_variants: 0,
            current_partition: None,
            completed_files: 0,
            batch_records: 0,
        }
    }
}

/// Where the processor reports progress. Implemented by tests, the CLI and — in a later
/// task — a Temporal activity heartbeat adapter.
pub trait ProgressSink: Send + Sync {
    fn report(&self, event: &ProgressEvent);
}

/// Discards every event. The default for the debug CLI and for tests that do not inspect
/// progress.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopProgressSink;

impl ProgressSink for NoopProgressSink {
    fn report(&self, _event: &ProgressEvent) {}
}
