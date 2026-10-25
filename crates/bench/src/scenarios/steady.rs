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
    pub(crate) run: String,
    pub(crate) seed: u64,
    pub(crate) ready: bool,
    /// Commit replies received during the measurement window.
    pub(crate) commits: u64,
    /// Median and 99th-percentile commit latency in milliseconds (metric M7).
    pub(crate) median_ms: Option<f64>,
    pub(crate) p99_ms: Option<f64>,
    /// The 99.9th percentile and the largest commit latency (descriptive; amendment A8).
    pub(crate) p999_ms: Option<f64>,
    pub(crate) max_ms: Option<f64>,
    /// Commits per second (metric M8).
    pub(crate) throughput: f64,
    /// Messages and bytes between servers per commit (metric M9).
    pub(crate) messages_per_commit: Option<f64>,
    pub(crate) bytes_per_commit: Option<f64>,
    /// Storage syncs and bytes they wrote, all servers, per commit (descriptive; A8).
    pub(crate) fsyncs_per_commit: Option<f64>,
    pub(crate) storage_bytes_per_commit: Option<f64>,
    /// Real cluster only: CPU time of all servers per commit, and the largest peak resident
    /// memory of any server (descriptive; A8).
    pub(crate) cpu_ms_per_commit: Option<f64>,
    pub(crate) max_rss_mb: Option<f64>,
    pub(crate) violations: u64,
}

impl SteadyRow {
    /// A row with no measurements yet.
    pub(crate) fn new(run: &RunConfig, seed: u64) -> SteadyRow {
        SteadyRow {
            run: run_columns(run),
            seed,
            ready: false,
            commits: 0,
            median_ms: None,
            p99_ms: None,
            p999_ms: None,
            max_ms: None,
            throughput: 0.0,
            messages_per_commit: None,
            bytes_per_commit: None,
            fsyncs_per_commit: None,
            storage_bytes_per_commit: None,
            cpu_ms_per_commit: None,
            max_rss_mb: None,
            violations: 0,
        }
    }

    /// Fills in the latency columns from the window's sorted latencies, in milliseconds.
    pub(crate) fn set_latencies(&mut self, sorted: &[f64]) {
        if sorted.is_empty() {
            return;
        }
        self.median_ms = Some(median(sorted));
        self.p99_ms = Some(percentile(sorted, 99.0));
        self.p999_ms = Some(percentile(sorted, 99.9));
        self.max_ms = sorted.last().copied();
    }
}

impl Row for SteadyRow {
    fn header() -> &'static str {
        concat!(
            "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,",
            "election_timeout_max_ms,uncommitted,",
            "seed,ready,commits,median_latency_ms,p99_latency_ms,p999_latency_ms,",
            "max_latency_ms,throughput_per_s,messages_per_commit,bytes_per_commit,",
            "fsyncs_per_commit,storage_bytes_per_commit,cpu_ms_per_commit,max_rss_mb,violations"
        )
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.run,
            self.seed,
            self.ready,
            self.commits,
            optional(self.median_ms),
            optional(self.p99_ms),
            optional(self.p999_ms),
            optional(self.max_ms),
            number(self.throughput),
            optional(self.messages_per_commit),
            optional(self.bytes_per_commit),
            optional(self.fsyncs_per_commit),
            optional(self.storage_bytes_per_commit),
            optional(self.cpu_ms_per_commit),
            optional(self.max_rss_mb),
            self.violations
        )
    }
}

/// Runs one E1 trial.
pub fn trial<A: Algorithm>(run: &RunConfig, seed: u64) -> SteadyRow {
    debug_assert!(SteadyRow::header().starts_with(RUN_HEADER));
    let mut row = SteadyRow::new(run, seed);
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
    row.set_latencies(&latencies);
    if commits > 0 {
        let per_commit = |total: u64| Some(total as f64 / commits as f64);
        row.messages_per_commit = per_commit(window.sent.messages);
        row.bytes_per_commit = per_commit(window.sent.bytes);
        row.fsyncs_per_commit = per_commit(window.fsyncs);
        row.storage_bytes_per_commit = per_commit(window.storage_bytes);
    }
    row.violations = sim.violation_count();
    row
}
