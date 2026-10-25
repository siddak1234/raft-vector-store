//! Real-cluster trials (`docs/RESEARCH.md` §3, "Real cluster").
//!
//! Each trial starts fresh server processes on this machine, with the protocol's one-way
//! delay injected in the transport and on client traffic, real fsync, and fresh data
//! folders. Trials run one at a time, in a random order fixed by the experiment, so the
//! two algorithms (or the two arms of the A/A test) are interleaved and share any drift in
//! the machine's state (`docs/PLAN.md` §5, rule 6).
//!
//! The procedures match the simulator's (`crate::scenarios`), and the rows have the same
//! columns, so the same analysis code reads both. Times come from the measurement logs the
//! servers keep; every process reads the same clock.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use client::{ControlClient, RetryPolicy};
use controlplane::Algorithm;
use controlplane::driver::bench_request;
use controlplane::local::{LocalCluster, LocalOptions};
use rpc::proto::control_plane_client::ControlPlaneClient;
use rpc::proto::{CrashRequest, Empty, FaultsRequest};
use rpc::{Events, Outcome};
use tonic::transport::Channel;

use crate::algorithm::Kind;
use crate::cluster::{binary_dir, fresh_dir};
use crate::config::RunConfig;
use crate::scenarios::failover::FailoverRow;
use crate::scenarios::figure16::Figure16Row;
use crate::scenarios::steady::SteadyRow;
use crate::scenarios::{median, ms, percentile, run_columns};

const WARMUP: Duration = Duration::from_secs(2);
const MEASURE: Duration = Duration::from_secs(10);
const CAP: Duration = Duration::from_secs(60);

fn algorithm(run: &RunConfig) -> Algorithm {
    match Kind::parse(&run.algorithm).expect("checked before any trial runs") {
        Kind::Raft => Algorithm::Raft,
        Kind::Paxos => Algorithm::Paxos,
    }
}

/// The mean one-way delay of the run's network, in microseconds.
fn delay_us(run: &RunConfig) -> u64 {
    run.network().one_way_delay().as_micros() as u64
}

/// A cluster started for one trial, removed when dropped.
struct Trial {
    cluster: LocalCluster,
    servers: Vec<(u64, String)>,
    delay_us: u64,
}

impl Trial {
    fn start(run: &RunConfig, seed: u64, label: &str) -> Result<Trial, String> {
        let [min, max] = run.election_timeout_ms.unwrap_or([150, 300]);
        let options = LocalOptions {
            algorithm: algorithm(run),
            election_timeout_ms: (min, max),
            delay_us: delay_us(run),
            record: true,
            seed: Some(seed),
        };
        let binary = binary_dir()?.join("controlplane");
        let cluster = LocalCluster::start(&binary, &fresh_dir(label), run.servers as u64, options)?;
        let servers = cluster.servers().to_vec();
        Ok(Trial {
            cluster,
            servers,
            delay_us: delay_us(run),
        })
    }

    fn client(&self, id: u64, give_up: Option<Duration>) -> Result<ControlClient, String> {
        let policy = RetryPolicy {
            delay_us: self.delay_us,
            give_up,
            ..RetryPolicy::default()
        };
        ControlClient::new(&self.servers, policy, id)
    }

    fn raw(&self, id: u64) -> Option<ControlPlaneClient<Channel>> {
        self.client(0, None).ok()?.server(id)
    }

    /// Waits until a leader has committed a command from its own term, so its no-op too.
    async fn ready(&self) -> bool {
        let Ok(mut client) = self.client(1, Some(Duration::from_secs(30))) else {
            return false;
        };
        let request = bench_request(client.id(), 0);
        client
            .propose(request.command)
            .await
            .is_ok_and(|c| c.outcome == Outcome::Applied)
    }

    /// The server leading the highest term, with that term.
    async fn leader(&self) -> Option<(u64, u64)> {
        let mut best: Option<(u64, u64)> = None;
        for (id, _) in &self.servers {
            if !self.cluster.is_running(*id) {
                continue;
            }
            let mut raw = self.raw(*id)?;
            if let Ok(Ok(status)) =
                tokio::time::timeout(Duration::from_millis(200), raw.status(Empty {})).await
            {
                let status = status.into_inner();
                if status.role == "leader" && best.is_none_or(|(_, t)| status.term > t) {
                    best = Some((status.id, status.term));
                }
            }
        }
        best
    }

    /// The measurement logs of every running server.
    async fn events(&self) -> Vec<Events> {
        let mut logs = Vec::new();
        for (id, _) in &self.servers {
            if !self.cluster.is_running(*id) {
                continue;
            }
            let Some(mut raw) = self.raw(*id) else {
                continue;
            };
            if let Ok(Ok(reply)) =
                tokio::time::timeout(Duration::from_secs(5), raw.events(Empty {})).await
                && let Ok(events) = rpc::decode::<Events>(&reply.into_inner().events)
            {
                logs.push(events);
            }
        }
        logs
    }

    async fn set_faults(&self, id: u64, drop: Vec<u64>) {
        if let Some(mut raw) = self.raw(id) {
            let request = FaultsRequest {
                drop_to: drop.clone(),
                drop_from: drop,
                delay_us: self.delay_us,
                drop_entries_to: Vec::new(),
            };
            let _ = raw.set_faults(request).await;
        }
    }

    /// Runs the crash procedure at `leader` and returns the crash time.
    async fn crash(
        &mut self,
        leader: u64,
        drop_entries_to: Vec<u64>,
        commands: u32,
        heartbeat: bool,
    ) -> Option<u64> {
        let mut raw = self.raw(leader)?;
        let request = CrashRequest {
            drop_entries_to,
            commands,
            heartbeat,
        };
        let reply = tokio::time::timeout(Duration::from_secs(5), raw.crash(request)).await;
        self.cluster.reap(leader);
        match reply {
            Ok(Ok(reply)) => Some(reply.into_inner().time),
            // The process may have stopped before its reply left; its log has the time.
            _ => crash_time_from_log(self.cluster.dir(), leader),
        }
    }
}

fn crash_time_from_log(dir: &std::path::Path, id: u64) -> Option<u64> {
    let entries = std::fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(&format!("server-{id}.")) {
            continue;
        }
        let text = std::fs::read_to_string(entry.path()).ok()?;
        for line in text.lines() {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(line)
                && value["event"] == "crash"
            {
                return value["time"].as_u64();
            }
        }
    }
    None
}

impl Drop for Trial {
    fn drop(&mut self) {
        self.cluster.stop();
        let _ = std::fs::remove_dir_all(self.cluster.dir());
    }
}

/// Closed-loop clients that record when each reply arrived, and when its command was first
/// sent.
struct Clients {
    stop: Arc<AtomicBool>,
    replies: Arc<Mutex<Vec<(u64, u64)>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Clients {
    fn start(trial: &Trial, count: usize) -> Result<Clients, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let replies = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for n in 0..count {
            let mut client = trial.client(rpc::now_micros() + n as u64, None)?;
            let stop = Arc::clone(&stop);
            let replies = Arc::clone(&replies);
            tasks.push(tokio::spawn(async move {
                let mut seq = 0;
                while !stop.load(Ordering::Relaxed) {
                    seq += 1;
                    let sent = rpc::now_micros();
                    let command = bench_request(client.id(), seq).command;
                    let reply = tokio::select! {
                        reply = client.propose(command) => reply,
                        () = wait_for(&stop) => return,
                    };
                    if reply.is_ok() {
                        replies
                            .lock()
                            .expect("no task panicked")
                            .push((sent, rpc::now_micros()));
                    }
                }
            }));
        }
        Ok(Clients {
            stop,
            replies,
            tasks,
        })
    }

    async fn stop(self) -> Vec<(u64, u64)> {
        self.stop.store(true, Ordering::Relaxed);
        for task in self.tasks {
            let _ = task.await;
        }
        self.replies.lock().expect("no task panicked").clone()
    }

    fn replies(&self) -> Vec<(u64, u64)> {
        self.replies.lock().expect("no task panicked").clone()
    }
}

async fn wait_for(flag: &AtomicBool) {
    while !flag.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// E1 and the A/A test on a real cluster.
pub async fn steady(run: &RunConfig, seed: u64) -> SteadyRow {
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
    let Ok(trial) = Trial::start(run, seed, "steady") else {
        return row;
    };
    if !trial.ready().await {
        return row;
    }
    row.ready = true;
    let Ok(clients) = Clients::start(&trial, run.clients) else {
        return row;
    };
    let start = rpc::now_micros();
    let window = (
        start + WARMUP.as_micros() as u64,
        start + (WARMUP + MEASURE).as_micros() as u64,
    );
    tokio::time::sleep(WARMUP + MEASURE).await;
    let replies = clients.stop().await;
    let events = trial.events().await;

    let mut latencies: Vec<f64> = replies
        .iter()
        .filter(|(_, received)| *received >= window.0 && *received < window.1)
        .map(|(sent, received)| (received - sent) as f64 / 1000.0)
        .collect();
    latencies.sort_by(f64::total_cmp);
    let commits = latencies.len() as u64;
    let (messages, bytes) = sends_between(&events, window.0, window.1);
    row.commits = commits;
    row.throughput = commits as f64 / MEASURE.as_secs_f64();
    if commits > 0 {
        row.median_ms = Some(median(&latencies));
        row.p99_ms = Some(percentile(&latencies, 99.0));
        row.messages_per_commit = Some(messages as f64 / commits as f64);
        row.bytes_per_commit = Some(bytes as f64 / commits as f64);
    }
    row
}

/// Messages and bytes all servers sent in `[from, until)`.
fn sends_between(events: &[Events], from: u64, until: u64) -> (u64, u64) {
    let mut messages = 0;
    let mut bytes = 0;
    for log in events {
        for send in &log.sends {
            if send.time >= from && send.time < until {
                messages += 1;
                bytes += send.bytes;
            }
        }
    }
    (messages, bytes)
}

/// When some server first led a term above `term` after `after`, from the status logs.
fn elected_at(events: &[Events], term: u64, after: u64) -> Option<u64> {
    events
        .iter()
        .flat_map(|log| &log.status)
        .filter(|s| s.role == "leader" && s.term > term && s.time >= after)
        .map(|s| s.time)
        .min()
}

/// Terms in which some server became candidate between `from` and `until` (metric M3).
fn candidate_terms(events: &[Events], from: u64, until: u64) -> usize {
    events
        .iter()
        .flat_map(|log| &log.status)
        .filter(|s| s.role == "candidate" && s.time >= from && s.time <= until)
        .map(|s| s.term)
        .collect::<BTreeSet<_>>()
        .len()
}

fn seeded(seed: u64) -> rpc::delay::Jitter {
    rpc::delay::Jitter::new(seed ^ 0x5EED)
}

/// E2 on a real cluster: the Figure 16 procedure (`docs/RESEARCH.md` §6 and A1.6).
pub async fn figure16(run: &RunConfig, seed: u64) -> Figure16Row {
    let mut row = Figure16Row {
        run: run_columns(run),
        seed,
        ready: false,
        time_without_leader_ms: f64::NAN,
        censored: false,
        election_rounds: 0,
        followers_with_entry: 0,
        violations: 0,
    };
    let Ok(mut trial) = Trial::start(run, seed, "figure16") else {
        return row;
    };
    if !trial.ready().await {
        return row;
    }
    let mut rng = seeded(seed);
    let [min, _] = run.election_timeout_ms.unwrap_or([150, 300]);
    let heartbeat = Duration::from_millis(min) / 2;
    tokio::time::sleep(Duration::from_nanos(rng.below(heartbeat.as_nanos() as u64))).await;
    let Some((leader, term)) = trial.leader().await else {
        return row;
    };
    row.ready = true;
    let mut drop = Vec::new();
    for (id, _) in trial.servers.clone() {
        if id == leader {
            continue;
        }
        if rng.below(2) == 0 {
            row.followers_with_entry += 1;
        } else {
            drop.push(id);
        }
    }
    let Some(crashed) = trial.crash(leader, drop, 1, true).await else {
        row.ready = false;
        return row;
    };
    let elected = wait_for_leader(&trial, term).await;
    let events = trial.events().await;
    match elected
        .then(|| elected_at(&events, term, crashed))
        .flatten()
    {
        Some(at) => {
            row.time_without_leader_ms = (at.saturating_sub(crashed)) as f64 / 1000.0;
            row.election_rounds = candidate_terms(&events, crashed, at);
        }
        None => {
            row.censored = true;
            row.time_without_leader_ms = ms(CAP);
        }
    }
    row
}

/// Polls until a running server leads a term above `term`, up to the trial cap.
async fn wait_for_leader(trial: &Trial, term: u64) -> bool {
    let deadline = Instant::now() + CAP;
    while Instant::now() < deadline {
        if trial.leader().await.is_some_and(|(_, t)| t > term) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

/// E3 on a real cluster: failover with `k` uncommitted entries (`docs/RESEARCH.md` §6).
pub async fn failover(run: &RunConfig, seed: u64) -> FailoverRow {
    let mut row = FailoverRow {
        run: run_columns(run),
        seed,
        ready: false,
        censored: false,
        followers_with_entries: 0,
        time_without_leader_ms: f64::NAN,
        unavailability_ms: None,
        election_rounds: 0,
        election_messages: 0,
        election_bytes: 0,
        stale_log_rejections: None,
        resent_entries: None,
        max_log_gaps: 0,
        discarded_entries: 0,
        entries_to_leaders: 0,
        violations: 0,
    };
    let Ok(mut trial) = Trial::start(run, seed, "failover") else {
        return row;
    };
    if !trial.ready().await {
        return row;
    }
    let Ok(clients) = Clients::start(&trial, run.clients.max(1)) else {
        return row;
    };
    let mut rng = seeded(seed);
    tokio::time::sleep(Duration::from_secs(1) + Duration::from_nanos(rng.below(1_000_000_000)))
        .await;
    let Some((leader, term)) = trial.leader().await else {
        let _ = clients.stop().await;
        return row;
    };
    row.ready = true;
    let mut followers: Vec<u64> = trial
        .servers
        .iter()
        .map(|(id, _)| *id)
        .filter(|id| *id != leader)
        .collect();
    for i in (1..followers.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        followers.swap(i, j);
    }
    let reached = rng.below(trial.servers.len() as u64) as usize;
    row.followers_with_entries = reached;
    let drop = followers[reached..].to_vec();
    let before: u64 = trial
        .events()
        .await
        .iter()
        .map(|e| e.stale_log_rejections)
        .sum();
    let Some(crashed) = trial
        .crash(leader, drop, run.uncommitted as u32, false)
        .await
    else {
        row.ready = false;
        let _ = clients.stop().await;
        return row;
    };
    if !wait_for_leader(&trial, term).await {
        row.censored = true;
        row.time_without_leader_ms = ms(CAP);
        let _ = clients.stop().await;
        return row;
    }
    // Unavailability: the first reply to a command sent after the crash.
    let deadline = Instant::now() + CAP;
    let mut available = None;
    while Instant::now() < deadline {
        if let Some(at) = clients
            .replies()
            .iter()
            .filter(|(sent, _)| *sent >= crashed)
            .map(|(_, received)| *received)
            .min()
        {
            available = Some(at);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Let the new leader's no-op commit before reading the logs.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = clients.stop().await;
    let events = trial.events().await;
    let Some(elected) = elected_at(&events, term, crashed) else {
        row.censored = true;
        return row;
    };
    row.time_without_leader_ms = elected.saturating_sub(crashed) as f64 / 1000.0;
    row.election_rounds = candidate_terms(&events, crashed, elected);
    let (messages, bytes) = sends_between(&events, crashed, elected + 1);
    row.election_messages = messages;
    row.election_bytes = bytes;
    if run.algorithm == "raft" {
        let after: u64 = events.iter().map(|e| e.stale_log_rejections).sum();
        row.stale_log_rejections = Some(after.saturating_sub(before));
    }
    row.unavailability_ms = available.map(|at| at.saturating_sub(crashed) as f64 / 1000.0);
    // M10: entries the new leader sent from its election until its no-op committed. The
    // no-op is the last entry in its log at the moment of election.
    if let Some(new_leader) = events.iter().find(|log| {
        log.status
            .iter()
            .any(|s| s.role == "leader" && s.time == elected)
    }) {
        let at_election = new_leader.status.iter().find(|s| s.time == elected);
        if let Some(noop) = at_election.map(|s| s.last_index) {
            let committed = new_leader
                .status
                .iter()
                .filter(|s| s.time >= elected && s.commit_index >= noop)
                .map(|s| s.time)
                .min();
            if let Some(committed) = committed {
                let entries = new_leader
                    .sends
                    .iter()
                    .filter(|s| s.time >= elected && s.time <= committed)
                    .map(|s| s.entries)
                    .sum();
                row.resent_entries = Some(entries);
            }
        }
    }
    row.max_log_gaps = events.iter().map(|e| e.max_log_gaps).max().unwrap_or(0);
    row.discarded_entries = events.iter().map(|e| e.discarded_entries).sum();
    row.entries_to_leaders = events.iter().map(|e| e.entries_to_leaders).sum();
    row
}

/// Faults a real-cluster partition trial can inject on the leader's links.
#[derive(Debug, Clone, Copy)]
pub enum Partition {
    /// The leader alone.
    LeaderAlone,
    /// One follower alone.
    FollowerAlone,
}

/// A partition trial on a real cluster (`docs/RESEARCH.md` §6, E5: P1 and P3), recorded
/// as metric M2 (time until a command sent after the fault commits) and M6 (leader
/// changes while the leader kept a majority).
pub async fn partition(
    run: &RunConfig,
    seed: u64,
    kind: Partition,
) -> crate::scenarios::partition::PartitionRow {
    let mut row = crate::scenarios::partition::PartitionRow::new(run, seed);
    let Ok(trial) = Trial::start(run, seed, "partition") else {
        return row;
    };
    if !trial.ready().await {
        return row;
    }
    let Ok(clients) = Clients::start(&trial, run.clients.max(1)) else {
        return row;
    };
    let mut rng = seeded(seed);
    tokio::time::sleep(Duration::from_secs(1) + Duration::from_nanos(rng.below(1_000_000_000)))
        .await;
    let Some((leader, term)) = trial.leader().await else {
        let _ = clients.stop().await;
        return row;
    };
    row.ready = true;
    let target = match kind {
        Partition::LeaderAlone => leader,
        Partition::FollowerAlone => {
            let followers: Vec<u64> = trial
                .servers
                .iter()
                .map(|(id, _)| *id)
                .filter(|id| *id != leader)
                .collect();
            followers[rng.below(followers.len() as u64) as usize]
        }
    };
    let faulted = rpc::now_micros();
    let others: Vec<u64> = trial
        .servers
        .iter()
        .map(|(s, _)| *s)
        .filter(|s| *s != target)
        .collect();
    trial.set_faults(target, others.clone()).await;
    for other in &others {
        trial.set_faults(*other, vec![target]).await;
    }
    tokio::time::sleep(crate::scenarios::partition::DURATION).await;
    for (s, _) in trial.servers.clone() {
        trial.set_faults(s, Vec::new()).await;
    }
    tokio::time::sleep(crate::scenarios::partition::SETTLE).await;
    let replies = clients.stop().await;
    let events = trial.events().await;
    row.unavailability_ms = replies
        .iter()
        .filter(|(sent, _)| *sent >= faulted)
        .map(|(_, received)| received.saturating_sub(faulted) as f64 / 1000.0)
        .reduce(f64::min);
    // Leader changes: terms led after the fault, while the leader kept a majority.
    let led: BTreeSet<u64> = events
        .iter()
        .flat_map(|log| &log.status)
        .filter(|s| s.role == "leader" && s.time >= faulted && s.term > term)
        .map(|s| s.term)
        .collect();
    row.leader_changes = led.len() as u64;
    row.max_log_gaps = events.iter().map(|e| e.max_log_gaps).max().unwrap_or(0);
    row.discarded_entries = events.iter().map(|e| e.discarded_entries).sum();
    row.entries_to_leaders = events.iter().map(|e| e.entries_to_leaders).sum();
    row
}
