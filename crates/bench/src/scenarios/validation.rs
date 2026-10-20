//! The validation campaign (`docs/RESEARCH.md` §9): random faults, then a check that the
//! cluster recovers and lost nothing.
//!
//! Each seed runs 60 s of simulated time with three closed-loop clients:
//!
//! 1. From 1 s to 45 s, random faults: crashes (between inputs and during an input),
//!    partitions, one-way link failures, message loss up to 50%, duplication, and delays up
//!    to ten times normal. A new fault starts on average every second and lasts up to 5 s.
//! 2. At 45 s every fault ends. Within 10 s, a command sent after 45 s must commit
//!    (liveness).
//! 3. At 57 s the clients stop; at 60 s every server must have applied every acknowledged
//!    command (durability).
//!
//! Safety checks run throughout. The Raft-only log matching check also runs every 5 s.
//! Odd seeds use three servers and even seeds five.

use std::time::Duration;

use sim::{ChaosConfig, Check, SimConfig, SimTime, Simulation, Stream, chaos_schedule, stream_rng};

use super::COMMAND_BYTES;
use crate::algorithm::Algorithm;
use crate::results::Row;

const FAULTS_END: Duration = Duration::from_secs(45);
const LIVENESS_DEADLINE: Duration = Duration::from_secs(55);
const CLIENTS_STOP: Duration = Duration::from_secs(57);
const END: Duration = Duration::from_secs(60);
const CHECK_EVERY: Duration = Duration::from_secs(5);

/// One validation seed.
#[derive(Debug, Clone)]
pub struct ValidationRow {
    seed: u64,
    servers: usize,
    faults: usize,
    events: u64,
    replies: usize,
    /// The highest term reached, a rough count of elections.
    max_term: u64,
    /// The total number of violations; zero means the seed passed.
    pub violations: u64,
    /// The first violation, described, if any.
    pub first_violation: String,
}

impl Row for ValidationRow {
    fn header() -> &'static str {
        "seed,servers,faults,events,replies,max_term,violations,first_violation"
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},\"{}\"",
            self.seed,
            self.servers,
            self.faults,
            self.events,
            self.replies,
            self.max_term,
            self.violations,
            self.first_violation.replace('"', "'")
        )
    }
}

/// Simulated time per seed, in seconds (for metric M13).
pub const SECONDS_PER_SEED: u64 = 60;

/// Runs one validation seed.
pub fn trial<A: Algorithm>(seed: u64) -> ValidationRow {
    let servers = if seed % 2 == 1 { 3 } else { 5 };
    let config = SimConfig {
        servers,
        clients: 3,
        command_bytes: COMMAND_BYTES,
        count_bytes: false,
        raft_checks: A::RAFT_CHECKS,
        ..SimConfig::default()
    };
    let mut sim: Simulation<A, A::Observer> =
        Simulation::with_observer(config, seed, A::Observer::default());

    let chaos = ChaosConfig {
        start: Duration::from_secs(1),
        end: FAULTS_END,
        mean_gap: Duration::from_secs(1),
        max_duration: Duration::from_secs(5),
    };
    let ids = sim.ids().to_vec();
    let schedule = chaos_schedule(&ids, &chaos, &mut stream_rng(seed, Stream::Faults));
    let faults = schedule.len();
    for (time, fault) in schedule {
        sim.schedule(time, fault);
    }

    let mut checkpoint = CHECK_EVERY;
    while checkpoint <= FAULTS_END {
        sim.run_until(SimTime::from_duration(checkpoint));
        if A::RAFT_CHECKS {
            sim.check_log_matching();
        }
        checkpoint += CHECK_EVERY;
    }

    let quiet = SimTime::from_duration(FAULTS_END);
    let mut scanned = 0;
    let live = sim.run_until_condition(SimTime::from_duration(LIVENESS_DEADLINE), |s| {
        let replies = &s.replies()[scanned..];
        scanned = s.replies().len();
        replies.iter().any(|reply| reply.sent >= quiet)
    });
    if !live {
        sim.report(
            Check::Liveness,
            "no command sent after faults stopped committed within 10 s".into(),
        );
    }
    sim.run_until(SimTime::from_duration(CLIENTS_STOP));
    sim.stop_clients();
    sim.run_until(SimTime::from_duration(END));
    sim.check_durability();
    if A::RAFT_CHECKS {
        sim.check_log_matching();
    }

    let max_term = ids
        .iter()
        .filter_map(|&id| sim.status(id))
        .map(|status| status.term.0)
        .max()
        .unwrap_or(0);
    let first_violation = sim
        .violations()
        .first()
        .map(|v| format!("{} at {}: {}", v.check.name(), v.time, v.detail))
        .unwrap_or_default();
    ValidationRow {
        seed,
        servers,
        faults,
        events: sim.events(),
        replies: sim.replies().len(),
        max_term,
        violations: sim.violation_count(),
        first_violation,
    }
}
