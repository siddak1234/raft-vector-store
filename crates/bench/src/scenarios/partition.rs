//! E5: partitions (`docs/RESEARCH.md` §6, with details in amendment A5.3).
//!
//! 1. Run 4 clients in steady state until a leader's no-op is committed, then for a
//!    uniformly random time between 1 and 2 seconds.
//! 2. Inject the scenario's fault:
//!    - P1: the leader isolated alone;
//!    - P2: the leader isolated with one follower (a minority);
//!    - P3: one follower isolated, so the leader keeps its majority;
//!    - P4: messages from the leader to every follower dropped, messages to it delivered;
//!    - P5: the link between the leader and one follower alternating down and up, each
//!      phase uniform in 100–500 ms, for 10 s.
//!
//!    P1–P4 last 2 s and then heal.
//! 3. Measure M2, the time from the fault until a client receives the commit reply of a
//!    command sent after it, and M6, the leader changes from the fault until 3 s after it
//!    heals. In P3 and P5 the leader keeps a majority, so every leader change there is
//!    unnecessary. Metrics M15–M17 cover the whole trial.

use std::collections::BTreeSet;
use std::time::Duration;

use consensus::{NodeId, Role, Term};
use serde::Deserialize;
use sim::{Fault, SimTime, Stream, stream_rng};

use super::{RUN_HEADER, ms, optional, run_columns, shuffled_followers, sim_config, start_ready};
use crate::algorithm::Algorithm;
use crate::config::RunConfig;
use crate::results::Row;

/// How long P1–P4 last.
pub const DURATION: Duration = Duration::from_secs(2);
/// How long P5 flaps.
pub const FLAPPING: Duration = Duration::from_secs(10);
/// How long leader changes are still counted after the fault heals.
pub const SETTLE: Duration = Duration::from_secs(3);
const CAP: Duration = Duration::from_secs(60);

/// The partition scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionKind {
    /// The leader isolated alone.
    P1,
    /// The leader isolated with one follower.
    P2,
    /// One follower isolated.
    P3,
    /// Messages from the leader dropped; messages to it delivered.
    P4,
    /// A flapping link between the leader and one follower.
    P5,
}

/// One E5 trial.
#[derive(Debug, Clone)]
pub struct PartitionRow {
    pub(crate) run: String,
    pub(crate) seed: u64,
    pub(crate) ready: bool,
    pub(crate) unavailability_ms: Option<f64>,
    pub(crate) leader_changes: u64,
    pub(crate) max_log_gaps: u64,
    pub(crate) discarded_entries: u64,
    pub(crate) entries_to_leaders: u64,
    pub(crate) violations: u64,
}

impl PartitionRow {
    /// A row for a trial that has not run yet.
    pub fn new(run: &RunConfig, seed: u64) -> Self {
        PartitionRow {
            run: run_columns(run),
            seed,
            ready: false,
            unavailability_ms: None,
            leader_changes: 0,
            max_log_gaps: 0,
            discarded_entries: 0,
            entries_to_leaders: 0,
            violations: 0,
        }
    }
}

impl Row for PartitionRow {
    fn header() -> &'static str {
        concat!(
            "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,",
            "election_timeout_max_ms,uncommitted,",
            "seed,ready,unavailability_ms,leader_changes,max_log_gaps,discarded_entries,",
            "entries_to_leaders,violations"
        )
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{}",
            self.run,
            self.seed,
            self.ready,
            optional(self.unavailability_ms),
            self.leader_changes,
            self.max_log_gaps,
            self.discarded_entries,
            self.entries_to_leaders,
            self.violations
        )
    }
}

/// Runs one E5 trial in the simulator.
pub fn trial<A: Algorithm>(run: &RunConfig, seed: u64, kind: PartitionKind) -> PartitionRow {
    debug_assert!(PartitionRow::header().starts_with(RUN_HEADER));
    let mut row = PartitionRow::new(run, seed);
    let Some(mut sim) = start_ready::<A>(sim_config::<A>(run, run.clients, false), seed) else {
        return row;
    };
    let mut rng = stream_rng(seed, Stream::Scenario);
    let wait = Duration::from_secs(1) + Duration::from_nanos(rng.below(1_000_000_000));
    sim.run_until(sim.now() + wait);
    let Some((leader, term)) = sim.leader() else {
        row.violations = sim.violation_count();
        return row;
    };
    row.ready = true;
    let followers = shuffled_followers(sim.ids(), leader, &mut rng);
    let others: Vec<NodeId> = sim
        .ids()
        .iter()
        .copied()
        .filter(|&id| id != leader)
        .collect();
    let faulted = sim.now();
    let heals = match kind {
        PartitionKind::P1 => {
            sim.inject(Fault::Partition(vec![vec![leader], others]));
            faulted + DURATION
        }
        PartitionKind::P2 => {
            let minority = vec![leader, followers[0]];
            let rest: Vec<NodeId> = followers[1..].to_vec();
            sim.inject(Fault::Partition(vec![minority, rest]));
            faulted + DURATION
        }
        PartitionKind::P3 => {
            let rest: Vec<NodeId> = sim
                .ids()
                .iter()
                .copied()
                .filter(|&id| id != followers[0])
                .collect();
            sim.inject(Fault::Partition(vec![vec![followers[0]], rest]));
            faulted + DURATION
        }
        PartitionKind::P4 => {
            for &to in &followers {
                sim.inject(Fault::Block { from: leader, to });
            }
            faulted + DURATION
        }
        PartitionKind::P5 => {
            let peer = followers[0];
            let end = faulted + FLAPPING;
            let mut at = faulted;
            let mut down = true;
            while at < end {
                if down {
                    sim.schedule(
                        at,
                        Fault::Block {
                            from: leader,
                            to: peer,
                        },
                    );
                    sim.schedule(
                        at,
                        Fault::Block {
                            from: peer,
                            to: leader,
                        },
                    );
                } else {
                    sim.schedule(
                        at,
                        Fault::Unblock {
                            from: leader,
                            to: peer,
                        },
                    );
                    sim.schedule(
                        at,
                        Fault::Unblock {
                            from: peer,
                            to: leader,
                        },
                    );
                }
                down = !down;
                at = at + Duration::from_millis(100) + Duration::from_nanos(rng.below(400_000_001));
            }
            end
        }
    };
    sim.schedule(heals, Fault::HealLinks);

    // M6: terms with a leader, other than the one at the fault, until the window ends.
    let mut led: BTreeSet<Term> = BTreeSet::new();
    let window_end = heals + SETTLE;
    let mut scanned = 0;
    let mut available: Option<SimTime> = None;
    sim.run_until_condition(window_end.max(faulted + CAP), |s| {
        for &id in s.ids() {
            if let Some(status) = s.status(id)
                && status.role == Role::Leader
                && status.term > term
                && s.now() <= window_end
            {
                led.insert(status.term);
            }
        }
        if available.is_none() {
            let replies = &s.replies()[scanned..];
            scanned = s.replies().len();
            if replies.iter().any(|reply| reply.sent >= faulted) {
                available = Some(s.now());
            }
        }
        available.is_some() && s.now() >= window_end
    });
    row.unavailability_ms = available.map(|at| ms(at - faulted));
    row.leader_changes = led.len() as u64;
    row.max_log_gaps = sim.max_log_gaps();
    row.discarded_entries = sim.discarded_entries();
    row.entries_to_leaders = sim.metrics().entries_to_leaders;
    row.violations = sim.violation_count();
    row
}
