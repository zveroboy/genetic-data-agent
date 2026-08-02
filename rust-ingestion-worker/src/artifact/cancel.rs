//! The progress-and-cancellation gate a *parallel* stage reports through.
//!
//! [`super::report`] is the sequential form: report, and turn a [`ControlFlow::Break`] into
//! [`Stopped::Interrupted`]. It is correct exactly because there is one caller. Once several
//! workers report concurrently, two things stop being true, and this type restores both:
//!
//! - **"The build stops reporting the moment it is asked to."** Without coordination, every
//!   worker already past its own cancellation check would still report, so a sink that breaks
//!   at its n-th event could be handed several more. The gate serialises the check and the
//!   report under one lock, so the first `Break` is the last event the sink ever sees.
//! - **`ProgressSink::report` is called from one thread at a time.** The trait is `Sync`, so
//!   concurrent calls would be sound — but the heartbeat adapter's projection reads and writes
//!   a shared picture, and a sink author should not have to reason about interleaving to
//!   implement six lines of bookkeeping.
//!
//! The lock is held only for the duration of one `report` call, and a stage reports once per
//! completed file or partition — tens of times per build, not per record.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use super::Stopped;
use crate::models::{ProgressEvent, ProgressSink};

pub(super) struct CancelGate<'a> {
    sink: &'a dyn ProgressSink,
    /// Serialises "is it cancelled?" with "report this event", which is what makes the first
    /// `Break` the last event.
    reporting: Mutex<()>,
    /// A lock-free mirror of the same fact, so a worker can skip expensive work without
    /// contending for the lock. It is only ever set inside the lock.
    cancelled: AtomicBool,
}

impl<'a> CancelGate<'a> {
    pub(super) fn new(sink: &'a dyn ProgressSink) -> Self {
        Self {
            sink,
            reporting: Mutex::new(()),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Publishes one event unless the build has already been asked to stop.
    ///
    /// Returns [`Stopped::Interrupted`] both when this event's own report broke and when an
    /// earlier one did, so a worker's `?` abandons its item either way.
    pub(super) fn report(&self, event: ProgressEvent) -> Result<(), Stopped> {
        // Poisoning can only mean another worker panicked mid-report. The flag is a plain
        // `bool` behind an atomic and cannot be left half-written, so the lock is recovered
        // rather than turning one worker's panic into a second panic in every other worker.
        let _serialised = self
            .reporting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if self.cancelled.load(Ordering::Acquire) {
            return Err(Stopped::Interrupted);
        }
        match self.sink.report(&event) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(()) => {
                self.cancelled.store(true, Ordering::Release);
                Err(Stopped::Interrupted)
            }
        }
    }

    /// Whether the build has been asked to stop. Checked before starting an item's work so a
    /// cancelled stage drains quickly instead of finishing every item it had already queued.
    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicUsize;

    use crate::contracts::IngestionPhase;

    /// Counts events and breaks at the n-th, like the acceptance tests' sink.
    struct BreakAt {
        stop_after: usize,
        seen: AtomicUsize,
    }

    impl ProgressSink for BreakAt {
        fn report(&self, _event: &ProgressEvent) -> ControlFlow<()> {
            if self.seen.fetch_add(1, Ordering::SeqCst) + 1 >= self.stop_after {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    fn event() -> ProgressEvent {
        ProgressEvent::phase(IngestionPhase::ExportingParquet)
    }

    /// The invariant the sequential path had and this type has to keep: a sink that breaks at
    /// its n-th event is never handed an (n+1)-th, however many workers were in flight.
    #[test]
    fn the_first_break_is_the_last_event_the_sink_sees() {
        for workers in [1, 2, 4, 16] {
            let sink = BreakAt {
                stop_after: 5,
                seen: AtomicUsize::new(0),
            };
            let gate = CancelGate::new(&sink);

            std::thread::scope(|scope| {
                for _ in 0..workers {
                    scope.spawn(|| {
                        for _ in 0..50 {
                            if gate.report(event()).is_err() {
                                break;
                            }
                        }
                    });
                }
            });

            assert_eq!(
                sink.seen.load(Ordering::SeqCst),
                5,
                "with {workers} workers the sink saw events after it asked to stop"
            );
            assert!(gate.is_cancelled());
        }
    }

    #[test]
    fn a_gate_that_was_never_broken_reports_everything() {
        let sink = BreakAt {
            stop_after: usize::MAX,
            seen: AtomicUsize::new(0),
        };
        let gate = CancelGate::new(&sink);
        for _ in 0..10 {
            gate.report(event()).expect("nothing asked the build to stop");
        }
        assert_eq!(sink.seen.load(Ordering::SeqCst), 10);
        assert!(!gate.is_cancelled());
    }
}
