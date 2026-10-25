//! E1-load: commit latency against offered load on the real cluster (`docs/RESEARCH.md`
//! amendment A8; exploratory).
//!
//! Each trial starts the cluster as E1 does, then sends commands at a fixed rate whatever
//! the replies: command `i` is due `i / rate` seconds after the start, and each goes out in
//! a session of its own, so a slow reply never holds back the next command (an open-loop
//! load). Latency runs from the moment a command was due, not from when it went out, so a
//! backlog in the client counts against the cluster.
//!
//! As in E1, the first 2 s are warm-up and the next 10 s are measured. Throughput counts the
//! replies received in the window; latency covers the commands due in the window. A command
//! without a reply within [`LATE`] of when it was due is late: it counts as slower than
//! every reply, so a percentile that falls among the late commands is left empty. Clients
//! follow amendment A1.1 but give up after [`LATE`], and their injected delays are a timed
//! sleep without the final spin, so the load does not take CPU from the servers.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use controlplane::driver::bench_request;

use crate::config::RunConfig;
use crate::real::{MEASURE, Trial, WARMUP, sends_between, usage_per_commit};
use crate::results::{Row, number};
use crate::scenarios::{RUN_HEADER, optional, run_columns};

/// How long after it is due a command may take before it counts as late.
pub const LATE: Duration = Duration::from_secs(2);
/// How long to wait for each server's measurement log after the load stops, while the
/// servers work off any backlog.
const EVENTS_TIMEOUT: Duration = Duration::from_secs(60);
/// Session IDs of the load's commands start above those of the trial's other clients.
const FIRST_SESSION: u64 = 1 << 32;

/// One E1-load trial.
#[derive(Debug, Clone)]
pub struct LoadRow {
    pub(crate) run: String,
    /// Offered load, in commands per second.
    pub(crate) rate: u32,
    pub(crate) seed: u64,
    pub(crate) ready: bool,
    /// Commands due in the window.
    pub(crate) offered: u64,
    /// Replies received in the window.
    pub(crate) commits: u64,
    /// Replies received per second.
    pub(crate) throughput: f64,
    /// Commands due in the window without a reply within [`LATE`].
    pub(crate) late: u64,
    /// Latency percentiles of the commands due in the window, in milliseconds.
    pub(crate) median_ms: Option<f64>,
    pub(crate) p99_ms: Option<f64>,
    pub(crate) p999_ms: Option<f64>,
    pub(crate) max_ms: Option<f64>,
    /// As in E1: messages, bytes, storage syncs, bytes synced, and CPU time per commit, all
    /// servers together, and the largest peak resident memory of any server.
    pub(crate) messages_per_commit: Option<f64>,
    pub(crate) bytes_per_commit: Option<f64>,
    pub(crate) fsyncs_per_commit: Option<f64>,
    pub(crate) storage_bytes_per_commit: Option<f64>,
    pub(crate) cpu_ms_per_commit: Option<f64>,
    pub(crate) max_rss_mb: Option<f64>,
}

impl LoadRow {
    fn new(run: &RunConfig, seed: u64) -> LoadRow {
        LoadRow {
            run: run_columns(run),
            rate: run.rate,
            seed,
            ready: false,
            offered: 0,
            commits: 0,
            throughput: 0.0,
            late: 0,
            median_ms: None,
            p99_ms: None,
            p999_ms: None,
            max_ms: None,
            messages_per_commit: None,
            bytes_per_commit: None,
            fsyncs_per_commit: None,
            storage_bytes_per_commit: None,
            cpu_ms_per_commit: None,
            max_rss_mb: None,
        }
    }
}

impl Row for LoadRow {
    fn header() -> &'static str {
        concat!(
            "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,",
            "election_timeout_max_ms,uncommitted,",
            "offered_per_s,seed,ready,offered,commits,throughput_per_s,late,",
            "median_latency_ms,p99_latency_ms,p999_latency_ms,max_latency_ms,",
            "messages_per_commit,bytes_per_commit,fsyncs_per_commit,storage_bytes_per_commit,",
            "cpu_ms_per_commit,max_rss_mb,violations"
        )
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},0",
            self.run,
            self.rate,
            self.seed,
            self.ready,
            self.offered,
            self.commits,
            number(self.throughput),
            self.late,
            optional(self.median_ms),
            optional(self.p99_ms),
            optional(self.p999_ms),
            optional(self.max_ms),
            optional(self.messages_per_commit),
            optional(self.bytes_per_commit),
            optional(self.fsyncs_per_commit),
            optional(self.storage_bytes_per_commit),
            optional(self.cpu_ms_per_commit),
            optional(self.max_rss_mb),
        )
    }
}

/// The `p`th percentile, by nearest rank, of `sorted` latencies together with `late`
/// commands that are slower than all of them; `None` if it falls among the late ones.
fn percentile_with_late(sorted: &[f64], late: u64, p: f64) -> Option<f64> {
    let n = sorted.len() as u64 + late;
    if n == 0 {
        return None;
    }
    let rank = ((p / 100.0 * n as f64).ceil() as u64).clamp(1, n);
    sorted.get(rank as usize - 1).copied()
}

/// The median of `sorted` latencies together with `late` slower commands, as E1 defines
/// it (the mean of the two middle values when there is an even number); `None` if a middle
/// value is late.
fn median_with_late(sorted: &[f64], late: u64) -> Option<f64> {
    let n = sorted.len() + late as usize;
    if n == 0 {
        return None;
    }
    if n % 2 == 1 {
        sorted.get(n / 2).copied()
    } else {
        Some((sorted.get(n / 2 - 1)? + sorted.get(n / 2)?) / 2.0)
    }
}

/// Runs one E1-load trial.
pub async fn trial(run: &RunConfig, seed: u64) -> LoadRow {
    debug_assert!(LoadRow::header().starts_with(RUN_HEADER));
    let mut row = LoadRow::new(run, seed);
    let Ok(trial) = Trial::start(run, seed, "load") else {
        return row;
    };
    if !trial.ready().await {
        return row;
    }
    // Thousands of commands are in flight, so their injected delays do not spin.
    let Ok(base) = trial.client_with(FIRST_SESSION, Some(LATE), false) else {
        return row;
    };
    row.ready = true;

    let rate = f64::from(run.rate);
    let started = Instant::now();
    let start = rpc::now_micros();
    let micros = |d: Duration| d.as_micros() as u64;
    let window = (start + micros(WARMUP), start + micros(WARMUP + MEASURE));
    let end = started + WARMUP + MEASURE;
    // (when the command was due, when its reply arrived), in microseconds.
    let replies: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

    let generator = {
        let replies = Arc::clone(&replies);
        tokio::spawn(async move {
            let mut tasks = Vec::new();
            for i in 0u64.. {
                let offset = Duration::from_secs_f64(i as f64 / rate);
                if started + offset >= end {
                    break;
                }
                tokio::time::sleep_until((started + offset).into()).await;
                let due = start + micros(offset);
                let mut client = base.session(FIRST_SESSION + 1 + i);
                let replies = Arc::clone(&replies);
                tasks.push(tokio::spawn(async move {
                    let command = bench_request(client.id(), 1).command;
                    if client.propose(command).await.is_ok() {
                        replies
                            .lock()
                            .expect("no task panicked")
                            .push((due, rpc::now_micros()));
                    }
                }));
            }
            tasks
        })
    };

    tokio::time::sleep_until((started + WARMUP).into()).await;
    let before = trial.usage().await;
    tokio::time::sleep_until(end.into()).await;
    let after = trial.usage().await;
    let tasks = generator.await.unwrap_or_default();
    tokio::time::sleep_until((end + LATE).into()).await;
    for task in &tasks {
        task.abort();
    }
    let replies = replies.lock().expect("no task panicked").clone();
    let events = trial.events_within(EVENTS_TIMEOUT).await;

    let in_window = |t: u64| t >= window.0 && t < window.1;
    let offered = (0u64..)
        .map(|i| start + micros(Duration::from_secs_f64(i as f64 / rate)))
        .take_while(|due| *due < window.1)
        .filter(|due| in_window(*due))
        .count() as u64;
    let mut latencies: Vec<f64> = replies
        .iter()
        .filter(|(due, received)| in_window(*due) && received.saturating_sub(*due) <= micros(LATE))
        .map(|(due, received)| received.saturating_sub(*due) as f64 / 1000.0)
        .collect();
    latencies.sort_by(f64::total_cmp);
    let commits = replies
        .iter()
        .filter(|(_, received)| in_window(*received))
        .count() as u64;

    row.offered = offered;
    row.commits = commits;
    row.throughput = commits as f64 / MEASURE.as_secs_f64();
    row.late = offered.saturating_sub(latencies.len() as u64);
    row.median_ms = median_with_late(&latencies, row.late);
    row.p99_ms = percentile_with_late(&latencies, row.late, 99.0);
    row.p999_ms = percentile_with_late(&latencies, row.late, 99.9);
    row.max_ms = percentile_with_late(&latencies, row.late, 100.0);
    if commits > 0 && events.len() == run.servers {
        let (messages, bytes) = sends_between(&events, window.0, window.1);
        row.messages_per_commit = Some(messages as f64 / commits as f64);
        row.bytes_per_commit = Some(bytes as f64 / commits as f64);
    }
    if let Some((fsyncs, storage_bytes, cpu_ms, rss_mb)) =
        usage_per_commit(before.as_deref(), after.as_deref(), commits)
    {
        row.fsyncs_per_commit = Some(fsyncs);
        row.storage_bytes_per_commit = Some(storage_bytes);
        row.cpu_ms_per_commit = Some(cpu_ms);
        row.max_rss_mb = Some(rss_mb);
    }
    row
}

#[cfg(test)]
mod tests {
    use super::{median_with_late, percentile_with_late};

    #[test]
    fn late_commands_count_as_slower_than_every_reply() {
        let on_time: Vec<f64> = (1..=98).map(f64::from).collect();
        // 98 replies and 2 late commands: the 98th percentile is a reply, the 99th is late.
        assert_eq!(percentile_with_late(&on_time, 2, 98.0), Some(98.0));
        assert_eq!(percentile_with_late(&on_time, 2, 99.0), None);
        assert_eq!(percentile_with_late(&on_time, 0, 100.0), Some(98.0));
        assert_eq!(percentile_with_late(&on_time, 1, 100.0), None);
        assert_eq!(percentile_with_late(&[], 0, 50.0), None);
    }

    #[test]
    fn the_median_matches_e1_without_late_commands() {
        assert_eq!(median_with_late(&[1.0, 2.0, 3.0, 4.0], 0), Some(2.5));
        assert_eq!(median_with_late(&[1.0, 2.0, 3.0], 1), Some(2.5));
        assert_eq!(median_with_late(&[1.0, 2.0], 1), Some(2.0));
        assert_eq!(median_with_late(&[1.0], 2), None);
    }
}
