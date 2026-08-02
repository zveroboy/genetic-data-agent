//! How much of each independently parallelisable stage may run at once, and the one bounded
//! `map` every such stage goes through.
//!
//! Two rules hold everywhere in this module, because the pipeline's correctness depends on
//! them rather than on its speed:
//!
//! 1. **Every degree of parallelism is an explicit bound.** Nothing here reads the global rayon
//!    pool: each stage builds a pool of exactly the size it was given, uses it, and drops it.
//!    An unbounded stage would be an unbounded amount of memory in flight and an unbounded
//!    amount of contention with DuckDB's own thread pool.
//! 2. **A bound of 1 is the sequential code path, not a pool of one thread.** That keeps
//!    [`ConcurrencyLimits::SEQUENTIAL`] an honest before/after baseline — it measures the
//!    pipeline without the parallel machinery, not the parallel machinery throttled to one
//!    worker — and it gives an operator a way to switch a stage off.
//!
//! [`map_bounded_with`] preserves input order in its output whatever the bound is, so a caller
//! can resolve outcomes in a deterministic order regardless of the order the work finished in.

use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::thread::available_parallelism;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

/// How many items of each independently parallelisable stage may be worked on at once.
///
/// The stages are bounded separately because they contend with different things: the export
/// competes with DuckDB's internal thread pool for the same cores, the validation is mostly
/// SHA-256 over already-written bytes, and BGZF decompression sits in front of a single-threaded
/// parser. One number for all three would tune none of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyLimits {
    /// Exported Parquet files hashed and validated at once. Each worker holds its own DuckDB
    /// connection and one 64 KiB read buffer.
    pub validate_files: usize,
}

impl ConcurrencyLimits {
    /// Every stage sequential: the behaviour the pipeline had before any stage was parallelised.
    /// Kept as a measurable baseline and as a per-deployment escape hatch.
    pub const SEQUENTIAL: Self = Self { validate_files: 1 };

    /// One worker per core the process may actually use. `available_parallelism` respects
    /// cgroup CPU limits, so a container with a fractional CPU quota does not get a pool sized
    /// for the host.
    pub fn from_available_parallelism() -> Self {
        let cores = available_parallelism().map_or(1, NonZeroUsize::get);
        Self {
            validate_files: cores,
        }
    }
}

impl Default for ConcurrencyLimits {
    fn default() -> Self {
        Self::from_available_parallelism()
    }
}

/// Clamps a configured bound to something usable: never zero, and never more workers than there
/// is work, so a two-file export does not build a sixteen-thread pool.
pub(crate) fn workers_for(limit: usize, items: usize) -> usize {
    limit.clamp(1, items.max(1))
}

/// Maps `items` with at most `workers` threads, giving each worker its own `S` state, and
/// returns the results **in the order of `items`** regardless of the order they completed in.
///
/// `workers == 1` runs the plain sequential loop on the calling thread: no pool is built, one
/// state is created, and nothing is shared. That is what makes a sequential run a real baseline
/// rather than the parallel machinery throttled to one worker.
///
/// **Exactly `workers` states are created**, up front, and handed out by checkout. `rayon`'s own
/// `map_init` is deliberately not used: it initialises once per *split*, not once per thread, so
/// a 22-item job on a 16-thread pool can call the initialiser 22 times. When `S` is a DuckDB
/// connection that is the difference between sixteen engine instances and one per partition.
/// The checkout can never block: at most `workers` closures run at once, so a state is always
/// free.
///
/// **Ordering is not incidental.** The dataset checksum is taken over descriptors sorted by
/// `(chrom, relativePath)`, and every caller here resolves failures by *position*, so the output
/// order has to be a property of the input rather than of the scheduler. `rayon`'s `collect`
/// into a `Vec` guarantees exactly that, and `results_follow_the_input_order_at_every_bound`
/// pins it.
///
/// Returns `Err` only when the thread pool itself cannot be built — an environment failure
/// (thread limit, resource exhaustion), never a failure of the work.
pub(crate) fn map_bounded_with<T, S, R, I, F>(
    items: &[T],
    workers: usize,
    init: I,
    map: F,
) -> Result<Vec<R>, String>
where
    T: Sync,
    R: Send,
    S: Send,
    I: Fn() -> S + Sync + Send,
    F: Fn(&mut S, &T) -> R + Sync + Send,
{
    if workers <= 1 {
        let mut state = init();
        return Ok(items.iter().map(|item| map(&mut state, item)).collect());
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .map_err(|error| format!("cannot build a {workers}-thread pool: {error}"))?;

    let free: Mutex<Vec<S>> = Mutex::new((0..workers).map(|_| init()).collect());
    let checkout = || {
        // A poisoned lock means some worker panicked while holding it. The vector itself is
        // never left inconsistent — a state is popped, used, and pushed back — so recovering is
        // strictly better than turning one panic into a panic in every other worker.
        let mut free = free.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // `pop` cannot be `None`: the pool runs at most `workers` closures at once and there are
        // `workers` states. `init()` is the defensive branch, not a fallback that is expected to
        // run, and it is still bounded by the number of concurrent closures.
        free.pop().unwrap_or_else(&init)
    };

    // `install` blocks until every task has finished, so no worker outlives this call, and the
    // pool is dropped on return, which releases its threads.
    Ok(pool.install(|| {
        items
            .par_iter()
            .map(|item| {
                let mut state = checkout();
                let result = map(&mut state, item);
                free.lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(state);
                result
            })
            .collect()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn a_bound_is_never_zero_and_never_exceeds_the_work() {
        assert_eq!(workers_for(0, 10), 1, "a zero bound must not disable the stage");
        assert_eq!(workers_for(16, 3), 3, "never more workers than items");
        assert_eq!(workers_for(4, 0), 1, "no work still needs a usable bound");
        assert_eq!(workers_for(4, 100), 4);
    }

    /// The property every caller depends on: the output is ordered by the input, not by
    /// completion. The map below deliberately makes early items slow so a completion-ordered
    /// implementation would visibly scramble the result.
    #[test]
    fn results_follow_the_input_order_at_every_bound() {
        let items: Vec<usize> = (0..64).collect();
        for workers in [1, 2, 8, 64] {
            let mapped = map_bounded_with(
                &items,
                workers,
                || (),
                |(), item| {
                    std::thread::sleep(std::time::Duration::from_micros((64 - *item as u64) * 20));
                    item * 2
                },
            )
            .expect("pool builds");
            assert_eq!(
                mapped,
                items.iter().map(|item| item * 2).collect::<Vec<_>>(),
                "bound {workers} scrambled the output order"
            );
        }
    }

    /// The per-worker state is created exactly `workers` times, never once per item.
    ///
    /// This is the reason `rayon`'s own `map_init` is not used: it initialises per *split*, and
    /// on this very input it produced eight states for four workers. With a DuckDB connection as
    /// the state, "per split" would mean an engine instance per partition.
    #[test]
    fn per_worker_state_is_created_exactly_once_per_worker() {
        let items: Vec<usize> = (0..500).collect();
        for workers in [1, 4, 16] {
            let initialised = AtomicUsize::new(0);
            let mapped = map_bounded_with(
                &items,
                workers,
                || {
                    initialised.fetch_add(1, Ordering::Relaxed);
                },
                |(), item| *item,
            )
            .expect("pool builds");
            assert_eq!(mapped, items);
            assert_eq!(
                initialised.load(Ordering::Relaxed),
                workers,
                "{workers} workers must create {workers} states, one per worker"
            );
        }
    }

    /// A state is never used by two closures at once, so a `!Sync` state — a DuckDB connection —
    /// is safe to hand out this way.
    #[test]
    fn a_checked_out_state_is_exclusive_to_one_closure() {
        let items: Vec<usize> = (0..2_000).collect();
        let live = AtomicUsize::new(0);
        map_bounded_with(
            &items,
            8,
            || AtomicUsize::new(0),
            |state, _| {
                // Two closures sharing one state would see this counter at 2.
                assert_eq!(state.fetch_add(1, Ordering::SeqCst), 0, "state was shared");
                live.fetch_add(1, Ordering::Relaxed);
                state.store(0, Ordering::SeqCst);
            },
        )
        .expect("pool builds");
        assert_eq!(live.load(Ordering::Relaxed), items.len());
    }

    #[test]
    fn a_sequential_bound_runs_on_the_calling_thread() {
        let items = vec![1, 2, 3];
        let caller = std::thread::current().id();
        let observed = map_bounded_with(&items, 1, || (), |(), _| std::thread::current().id())
            .expect("sequential needs no pool");
        assert!(observed.iter().all(|id| *id == caller));
    }
}
