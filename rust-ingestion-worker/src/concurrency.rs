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
    ///
    /// Capped below the core count, for the same reason as `export_partitions` and on the same
    /// evidence: each file's validation is itself a DuckDB query that already uses several
    /// cores, and in the per-item sweep sixteen workers on 22 files came out *slower* than
    /// eight. The interleaved session measured the stage at 0.16 s sequential → 0.10 s; it is 3%
    /// of the run either way, so the cap is about not provisioning threads that measurably do
    /// not help rather than about wall clock. Measurements in
    /// `.superpowers/sdd/parallelism-report.md`.
    pub validate_files: usize,
    /// Partitions whose `COPY … ORDER BY` may be in flight at once, each on its own connection
    /// to the same staging database.
    ///
    /// Capped below the core count. These statements share DuckDB's own thread pool rather than
    /// getting one each, and on a 22-partition genome the stage stopped improving at eight
    /// workers: the interleaved session measured 0.30 s sequential → 0.06 s at eight, and the
    /// per-item sweep — shape only, not comparable in absolute terms across sessions — put the
    /// plateau at that same eight. Past it the only thing more workers add is more concurrent
    /// sort buffers inside DuckDB, which is what the cap is protecting. Measurements in
    /// `.superpowers/sdd/parallelism-report.md`.
    pub export_partitions: usize,
    /// BGZF blocks inflated at once, when the source is `bgzip` output.
    ///
    /// `1` means the sequential `MultiGzDecoder`, which is also what a plain-gzip or uncompressed
    /// source always gets whatever this says. Above 1 the decompression moves off the parsing
    /// thread entirely, which is where most of the win comes from — see
    /// `.superpowers/sdd/parallelism-report.md`.
    pub bgzf_blocks: usize,
}

impl ConcurrencyLimits {
    /// Every stage sequential: the behaviour the pipeline had before any stage was parallelised.
    /// Kept as a measurable baseline and as a per-deployment escape hatch.
    pub const SEQUENTIAL: Self = Self {
        validate_files: 1,
        export_partitions: 1,
        bgzf_blocks: 1,
    };

    /// Capped low on purpose. On a 2 GB-uncompressed genome the interleaved session measured the
    /// staging stage at 4.34 s sequential → 3.27 s at four workers, and the per-item sweep — shape
    /// only — put the knee at *two*, with four, eight and sixteen indistinguishable from it. Most
    /// of that win is pipelining rather than parallelism: inflating 2 GB costs ~1.3 s and the
    /// parse-and-append it feeds costs ~3.3 s, so one dedicated inflate thread already stays ahead
    /// of the consumer. Four leaves a little headroom above the measured knee for hardware where
    /// inflation is slower relative to parsing, without provisioning threads that would only ever
    /// block.
    const MAX_BGZF_BLOCKS: usize = 4;

    /// Where concurrent `COPY` statements stopped paying when measured: they subdivide DuckDB's
    /// one internal thread pool rather than adding to it, so beyond this the stage does not get
    /// faster and only the number of simultaneous sort buffers grows.
    const MAX_EXPORT_PARTITIONS: usize = 8;

    /// Where per-file validation stopped paying when measured, and the same number for the same
    /// underlying reason: each file's checks are a DuckDB query that is already internally
    /// parallel, so past this the workers contend with the engine instead of adding to it. The
    /// sweep had sixteen workers behind eight on 22 files, so the core count is not the honest
    /// default even though nothing about this stage is unsafe at it.
    const MAX_VALIDATE_FILES: usize = 8;

    /// One worker per core the process may actually use, capped per stage at that stage's
    /// measured knee. `available_parallelism` respects cgroup CPU limits, so a container with a
    /// fractional CPU quota does not get a pool sized for the host.
    pub fn from_available_parallelism() -> Self {
        let cores = available_parallelism().map_or(1, NonZeroUsize::get);
        Self {
            validate_files: cores.min(Self::MAX_VALIDATE_FILES),
            export_partitions: cores.min(Self::MAX_EXPORT_PARTITIONS),
            bgzf_blocks: cores.min(Self::MAX_BGZF_BLOCKS),
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
/// **Exactly `workers` states are created**, up front and *on the calling thread*, and handed out
/// by checkout. Two consequences, both load-bearing:
///
/// - `rayon`'s own `map_init` is not used. It initialises once per *split*, not once per thread,
///   so a 22-item job on a 16-thread pool can call the initialiser 22 times. When `S` is a DuckDB
///   connection that is the difference between sixteen engine instances and one per partition.
/// - `init` is `FnMut` and needs no `Send`/`Sync` bound, because it never crosses a thread. That
///   is what lets the export stage create its states with `Connection::try_clone`, which borrows
///   a `Connection` — a type that is `Send` but deliberately not `Sync`.
///
/// The checkout can never block or fail: at most `workers` closures run at once, so a state is
/// always free.
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
    mut init: I,
    map: F,
) -> Result<Vec<R>, String>
where
    T: Sync,
    R: Send,
    S: Send,
    I: FnMut() -> S,
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

    // On the calling thread, before any worker exists.
    let free: Mutex<Vec<S>> = Mutex::new((0..workers).map(|_| init()).collect());
    // A poisoned lock means some worker panicked while holding it. The vector itself is never
    // left inconsistent — a state is popped, used, and pushed back — so recovering is strictly
    // better than turning one worker's panic into a panic in every other worker.
    let locked = || free.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    // `install` blocks until every task has finished, so no worker outlives this call, and the
    // pool is dropped on return, which releases its threads.
    Ok(pool.install(|| {
        items
            .par_iter()
            .map(|item| {
                // Cannot be `None`: there are `workers` states and the pool runs at most
                // `workers` of these closures at once.
                let mut state = locked().pop().expect("a free state per concurrent worker");
                let result = map(&mut state, item);
                locked().push(state);
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
