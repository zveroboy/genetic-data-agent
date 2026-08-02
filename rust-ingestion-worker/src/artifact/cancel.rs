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
//! - **A running count published alongside an event is monotonic on the wire.** A counter
//!   incremented *outside* this lock does not give that: two workers can claim 1 and 2 and then
//!   reach the lock in the opposite order, publishing `[2, 1, …]`. The gate therefore owns the
//!   count and assigns it in the same critical section that publishes it — see
//!   [`CancelGate::report_completion`].
//!
//! The lock is held only for the duration of one `report` call, and a stage reports once per
//! completed file or partition — tens of times per build, not per record.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use super::Stopped;
use crate::models::{ProgressEvent, ProgressSink};

pub(super) struct CancelGate<'a> {
    sink: &'a dyn ProgressSink,
    /// Serialises "is it cancelled?" with "report this event", which is what makes the first
    /// `Break` the last event — and holds the count of items completed so far, so that a
    /// sequence number cannot be assigned outside the section that publishes it.
    completed: Mutex<u64>,
    /// A lock-free mirror of the cancellation, so a worker can skip expensive work without
    /// contending for the lock. It is only ever set inside the lock.
    cancelled: AtomicBool,
}

impl<'a> CancelGate<'a> {
    pub(super) fn new(sink: &'a dyn ProgressSink) -> Self {
        Self {
            sink,
            completed: Mutex::new(0),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Publishes one event unless the build has already been asked to stop.
    ///
    /// Returns [`Stopped::Interrupted`] both when this event's own report broke and when an
    /// earlier one did, so a worker's `?` abandons its item either way.
    pub(super) fn report(&self, event: ProgressEvent) -> Result<(), Stopped> {
        let _serialised = self.locked();
        self.publish_locked(&event)
    }

    /// Publishes one event built from the number of items completed *including this one*.
    ///
    /// The number is assigned under the same lock that publishes the event, which is the whole
    /// point of the method. The obvious alternative — `counter.fetch_add(1, Relaxed) + 1`
    /// evaluated while building the argument — is wrong, and wrong in a way that only shows up
    /// under load: nothing orders an atomic increment against a `lock()` that happens after it,
    /// so two workers routinely claim 1 and 2 and then publish in the opposite order. Here the
    /// increment *is* part of the critical section, so the n-th event published always carries
    /// the number n.
    ///
    /// A report the gate refuses does not consume a number, so the published sequence has no
    /// gaps either.
    pub(super) fn report_completion(
        &self,
        event: impl FnOnce(u64) -> ProgressEvent,
    ) -> Result<(), Stopped> {
        let mut completed = self.locked();
        if self.cancelled.load(Ordering::Acquire) {
            return Err(Stopped::Interrupted);
        }
        *completed += 1;
        self.publish_locked(&event(*completed))
    }

    /// Poisoning can only mean another worker panicked mid-report. The state behind the lock is
    /// a single `u64` and cannot be left half-written, so the lock is recovered rather than
    /// turning one worker's panic into a second panic in every other worker.
    fn locked(&self) -> MutexGuard<'_, u64> {
        self.completed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The report itself, with the gate's lock already held by the caller.
    fn publish_locked(&self, event: &ProgressEvent) -> Result<(), Stopped> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(Stopped::Interrupted);
        }
        match self.sink.report(event) {
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

    /// Records the `completed_files` of every event, in the order the sink was handed them.
    #[derive(Default)]
    struct Recording(Mutex<Vec<u64>>);

    impl ProgressSink for Recording {
        fn report(&self, event: &ProgressEvent) -> ControlFlow<()> {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event.completed_files);
            ControlFlow::Continue(())
        }
    }

    /// The sequence a heartbeat consumer reads must be 1, 2, 3, … — not merely a permutation of
    /// it. A counter incremented while *building* the argument, before `report` takes the lock,
    /// produces `[2, 1, 3, …]` a few times in a hundred runs; assigning it inside the lock
    /// cannot.
    ///
    /// Every worker hammers the gate, so there is real contention on every bound above one.
    #[test]
    fn the_published_completion_sequence_is_monotonic_under_contention() {
        const PER_WORKER: u64 = 250;
        for workers in [1, 2, 4, 16] {
            let sink = Recording::default();
            let gate = CancelGate::new(&sink);

            std::thread::scope(|scope| {
                for _ in 0..workers {
                    scope.spawn(|| {
                        for _ in 0..PER_WORKER {
                            gate.report_completion(|completed_files| ProgressEvent {
                                completed_files,
                                ..event()
                            })
                            .expect("nothing asked the build to stop");
                        }
                    });
                }
            });

            let published = sink.0.lock().expect("sink lock").clone();
            assert_eq!(
                published,
                (1..=workers * PER_WORKER).collect::<Vec<_>>(),
                "with {workers} workers the published completion count was not 1, 2, 3, …"
            );
        }
    }

    /// A refused report must not burn a sequence number, or the count a consumer sees would
    /// jump over the events the gate suppressed.
    #[test]
    fn a_cancelled_report_consumes_no_completion_number() {
        let sink = BreakAt {
            stop_after: 3,
            seen: AtomicUsize::new(0),
        };
        let gate = CancelGate::new(&sink);

        let mut published = Vec::new();
        for _ in 0..10 {
            let outcome = gate.report_completion(|completed_files| {
                published.push(completed_files);
                ProgressEvent {
                    completed_files,
                    ..event()
                }
            });
            if outcome.is_err() && gate.is_cancelled() {
                // The third report is the one that broke; it was still published.
                assert_eq!(published, [1, 2, 3]);
            }
        }
        assert_eq!(published, [1, 2, 3], "a suppressed report took a number");
        assert_eq!(sink.seen.load(Ordering::SeqCst), 3);
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
