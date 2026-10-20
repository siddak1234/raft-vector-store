//! Runs independent trials on every CPU core.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Runs `trial` on every job in parallel and returns the results in job order. Prints
/// progress to standard error every `report_every` completed jobs.
pub fn run_parallel<J, R, F>(jobs: &[J], report_every: usize, trial: F) -> Vec<R>
where
    J: Sync,
    R: Send,
    F: Fn(&J) -> R + Sync,
{
    let workers = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(jobs.len().max(1));
    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<R>>> = Mutex::new((0..jobs.len()).map(|_| None).collect());

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(job) = jobs.get(index) else {
                        break;
                    };
                    let result = trial(job);
                    results.lock().expect("no worker panicked")[index] = Some(result);
                    let finished = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if report_every > 0 && finished.is_multiple_of(report_every) {
                        eprintln!("  {finished} / {} trials", jobs.len());
                    }
                }
            });
        }
    });

    results
        .into_inner()
        .expect("no worker panicked")
        .into_iter()
        .map(|result| result.expect("every job ran"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::run_parallel;

    #[test]
    fn keeps_job_order() {
        let jobs: Vec<u64> = (0..200).collect();
        let results = run_parallel(&jobs, 0, |n| n * n);
        assert_eq!(results, jobs.iter().map(|n| n * n).collect::<Vec<_>>());
    }
}
