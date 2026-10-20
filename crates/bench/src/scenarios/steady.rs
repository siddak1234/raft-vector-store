//! E1: steady-state performance (`docs/RESEARCH.md` §6).
//!
//! 1. Start the cluster and wait until a leader's no-op is committed.
//! 2. Run 2 s of warm-up, which is discarded.
//! 3. Measure for 10 s with closed-loop clients.

use std::time::Duration;

use super::{RUN_HEADER, median, ms, optional, percentile, run_columns, sim_config, start_ready};
use crate::algorithm::Algorithm;
use crate::config::RunConfig;
use crate::results::{Row, number};

const WARMUP: Duration = Duration::from_secs(2);
const MEASURE: Duration = Duration::from_secs(10);

/// One E1 trial.
#[derive(Debug, Clone)]
pub struct SteadyRow {
    run: String,
    seed: u64,
    ready: bool,
    /// Commit replies received during the measurement window.
    commits: u64,
    /// Median and 99th-percentile commit latency in milliseconds (metric M7).
    median_ms: Option<f64>,
    p99_ms: Option<f64>,
    /// Commits per second (metric M8).
    throughput: f64,
    /// Messages and bytes between servers per commit (metric M9).
    messages_per_commit: Option<f64>,
    bytes_per_commit: Option<f64>,
    violations: u64,
}

impl Row for SteadyRow {
    fn header() -> &'static str {
        concat!(
            "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,",
            "election_timeout_max_ms,uncommitted,",
            "seed,ready,commits,median_latency_ms,p99_latency_ms,throughput_per_s,",
            "messages_per_commit,bytes_per_commit,violations"
        )
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{}",
            self.run,
            self.seed,
            self.ready,
            self.commits,
            optional(self.median_ms),
            optional(self.p99_ms),
            number(self.throughput),
            optional(self.messages_per_commit),
            optional(self.bytes_per_commit),
            self.violations
        )
    }
}

/// Runs one E1 trial.
pub fn trial<A: Algorithm>(run: &RunConfig, seed: u64) -> SteadyRow {
    debug_assert!(SteadyRow::header().starts_with(RUN_HEADER));
    let mut row = SteadyRow {
        run: run_columns(run),
        seed,
        ready: false,
        commits: 0,
        median_ms: None,
        p99_ms: None,
        throughput: 0.0,
        messages_per_commit: None,
        bytes_per_commit: None,
        violations: 0,
    };
    let Some(mut sim) = start_ready::<A>(sim_config::<A>(run, run.clients, true), seed) else {
        return row;
    };
    row.ready = true;

    let window_start = sim.now() + WARMUP;
    sim.run_until(window_start);
    let before = sim.metrics().clone();
    let first_reply = sim.replies().len();
    sim.run_until(window_start + MEASURE);

    let mut latencies: Vec<f64> = sim.replies()[first_reply..]
        .iter()
        .map(|reply| ms(reply.latency()))
        .collect();
    latencies.sort_by(f64::total_cmp);
    let window = sim.metrics().since(&before);
    let commits = latencies.len() as u64;

    row.commits = commits;
    row.throughput = commits as f64 / MEASURE.as_secs_f64();
    if commits > 0 {
        row.median_ms = Some(median(&latencies));
        row.p99_ms = Some(percentile(&latencies, 99.0));
        row.messages_per_commit = Some(window.sent.messages as f64 / commits as f64);
        row.bytes_per_commit = Some(window.sent.bytes as f64 / commits as f64);
    }
    row.violations = sim.violation_count();
    row
}
