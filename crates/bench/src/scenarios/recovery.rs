//! E4: log recovery and catch-up (`docs/RESEARCH.md` §6, with details in amendment A5).
//!
//! - **E4a, consecutive crashes** (`recovery`). E3's procedure with `k` uncommitted entries
//!   crashes the first leader. As soon as the second leader is elected, the first one
//!   restarts with its log, and the second leader appends `k` more commands whose messages
//!   reach only a minority (counting itself) and crashes the same way. Metric M10 is
//!   measured for the third leader, which must handle entries from two earlier terms.
//! - **E4b, catch-up after a restart** (`catch_up`, `restart`). One follower stops, the
//!   leader commits `n` commands, and the follower restarts. Metric M11 runs from the
//!   restart until the follower's commit index reaches the leader's commit index at the
//!   moment of the restart.
//! - **E4c, catch-up after a partition** (`catch_up`, `partition`). As E4b, but the
//!   follower is partitioned instead of stopped, so it keeps its log; M11 runs from the
//!   moment the partition heals.

use std::time::Duration;

use consensus::{LogIndex, NodeId, Storage};
use serde::Deserialize;
use sim::{DropRule, Fault, SimTime, Simulation, Stream, stream_rng};

use super::{
    Elected, RUN_HEADER, elect, ms, optional, run_columns, shuffled_followers, sim_config,
    start_ready,
};
use crate::algorithm::Algorithm;
use crate::config::RunConfig;
use crate::results::Row;

const CAP: Duration = Duration::from_secs(60);

/// How a lagging follower is separated from the leader in E4b and E4c.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatchUp {
    /// E4b: the follower stops and restarts.
    Restart,
    /// E4c: the follower is partitioned and the partition heals.
    Partition,
}

/// One E4 trial.
#[derive(Debug, Clone)]
pub struct RecoveryRow {
    pub(crate) run: String,
    pub(crate) seed: u64,
    pub(crate) ready: bool,
    pub(crate) censored: bool,
    /// Metric M10 for the third leader (E4a).
    pub(crate) resent_entries: Option<u64>,
    /// Metric M11 (E4b, E4c).
    pub(crate) catch_up_ms: Option<f64>,
    pub(crate) max_log_gaps: u64,
    pub(crate) discarded_entries: u64,
    pub(crate) entries_to_leaders: u64,
    pub(crate) violations: u64,
}

impl RecoveryRow {
    fn new(run: &RunConfig, seed: u64) -> Self {
        RecoveryRow {
            run: run_columns(run),
            seed,
            ready: false,
            censored: false,
            resent_entries: None,
            catch_up_ms: None,
            max_log_gaps: 0,
            discarded_entries: 0,
            entries_to_leaders: 0,
            violations: 0,
        }
    }

    fn finish<A: Algorithm>(&mut self, sim: &Simulation<A, A::Observer>) {
        self.max_log_gaps = sim.max_log_gaps();
        self.discarded_entries = sim.discarded_entries();
        self.entries_to_leaders = sim.metrics().entries_to_leaders;
        self.violations = sim.violation_count();
    }
}

impl Row for RecoveryRow {
    fn header() -> &'static str {
        concat!(
            "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,",
            "election_timeout_max_ms,uncommitted,",
            "seed,ready,censored,resent_entries,catch_up_ms,max_log_gaps,",
            "discarded_entries,entries_to_leaders,violations"
        )
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{}",
            self.run,
            self.seed,
            self.ready,
            self.censored,
            self.resent_entries
                .map(|n| n.to_string())
                .unwrap_or_default(),
            optional(self.catch_up_ms),
            self.max_log_gaps,
            self.discarded_entries,
            self.entries_to_leaders,
            self.violations
        )
    }
}

/// Appends `k` commands at `leader` whose messages reach only the first `reached` of
/// `followers`, and crashes `leader` in the same instant.
fn crash_with_entries<A: Algorithm>(
    sim: &mut Simulation<A, A::Observer>,
    leader: NodeId,
    followers: &[NodeId],
    reached: usize,
    k: usize,
) {
    sim.clear_drop_rules();
    for &follower in &followers[reached.min(followers.len())..] {
        sim.add_drop_rule(DropRule {
            from: leader,
            to: follower,
            only_with_entries: true,
        });
    }
    for _ in 0..k {
        let _ = sim.propose(leader);
    }
    sim.inject(Fault::Crash(leader));
}

/// Runs until a server leads a term above `term`, or returns `None` at the cap.
fn next_leader<A: Algorithm>(
    sim: &mut Simulation<A, A::Observer>,
    term: consensus::Term,
) -> Option<Elected> {
    let deadline = sim.now() + CAP;
    elect(sim, deadline, term, |_| {})
}

/// E4a: consecutive crashes.
pub fn consecutive<A: Algorithm>(run: &RunConfig, seed: u64) -> RecoveryRow {
    debug_assert!(RecoveryRow::header().starts_with(RUN_HEADER));
    let mut row = RecoveryRow::new(run, seed);
    let Some(mut sim) = start_ready::<A>(sim_config::<A>(run, run.clients, false), seed) else {
        return row;
    };
    let mut rng = stream_rng(seed, Stream::Scenario);
    let wait = Duration::from_secs(1) + Duration::from_nanos(rng.below(1_000_000_000));
    sim.run_until(sim.now() + wait);
    let Some((first, term)) = sim.leader() else {
        row.finish(&sim);
        return row;
    };
    row.ready = true;
    let servers = sim.ids().len();
    let quorum = servers / 2 + 1;

    // The first crash, as in E3.
    let followers = shuffled_followers(sim.ids(), first, &mut rng);
    let reached = rng.below(servers as u64) as usize;
    crash_with_entries(&mut sim, first, &followers, reached, run.uncommitted);
    let Some(Elected {
        leader: second,
        term: second_term,
        ..
    }) = next_leader(&mut sim, term)
    else {
        row.censored = true;
        row.finish(&sim);
        return row;
    };

    // The first leader comes back with its log; the second appends k more commands that
    // reach a minority and crashes.
    sim.inject(Fault::Restart(first));
    let followers = shuffled_followers(sim.ids(), second, &mut rng);
    let reached = rng.below(quorum as u64 - 1) as usize;
    crash_with_entries(&mut sim, second, &followers, reached, run.uncommitted);
    sim.clear_drop_rules();
    let Some(Elected {
        leader: third,
        term: third_term,
        entries_before: before,
    }) = next_leader(&mut sim, second_term)
    else {
        row.censored = true;
        row.finish(&sim);
        return row;
    };

    // M10 for the third leader: entries sent from its election until its no-op, the last
    // entry in its log at election, is committed.
    let noop = sim.storage(third).last_index();
    let committed = sim.run_until_condition(sim.now() + CAP, |s| {
        s.status(third)
            .is_none_or(|st| st.term != third_term || st.commit_index >= noop)
    });
    if committed && sim.status(third).is_some_and(|st| st.term == third_term) {
        row.resent_entries = Some(sim.metrics().sent_by(third).entries - before);
    }
    row.finish(&sim);
    row
}

/// E4b and E4c: catch-up of a follower that missed `n` committed commands.
pub fn catch_up<A: Algorithm>(run: &RunConfig, seed: u64, how: CatchUp, n: usize) -> RecoveryRow {
    let mut row = RecoveryRow::new(run, seed);
    let Some(mut sim) = start_ready::<A>(sim_config::<A>(run, run.clients, false), seed) else {
        return row;
    };
    let mut rng = stream_rng(seed, Stream::Scenario);
    let Some((leader, term)) = sim.leader() else {
        row.finish(&sim);
        return row;
    };
    row.ready = true;
    let followers = shuffled_followers(sim.ids(), leader, &mut rng);
    let lagging = followers[0];
    match how {
        CatchUp::Restart => sim.inject(Fault::Crash(lagging)),
        CatchUp::Partition => {
            let rest: Vec<NodeId> = sim
                .ids()
                .iter()
                .copied()
                .filter(|&id| id != lagging)
                .collect();
            sim.inject(Fault::Partition(vec![vec![lagging], rest]));
        }
    }
    for _ in 0..n {
        if sim.propose(leader).is_err() {
            break;
        }
    }
    let last = sim.storage(leader).last_index();
    let committed = sim.run_until_condition(sim.now() + CAP, |s| {
        s.status(leader)
            .is_none_or(|st| st.term != term || st.commit_index >= last)
    });
    if !committed || sim.status(leader).is_none_or(|st| st.term != term) {
        row.censored = true;
        row.finish(&sim);
        return row;
    }
    let target: LogIndex = sim
        .status(leader)
        .map(|st| st.commit_index)
        .unwrap_or_default();
    let reconnected: SimTime = sim.now();
    match how {
        CatchUp::Restart => sim.inject(Fault::Restart(lagging)),
        CatchUp::Partition => sim.inject(Fault::HealLinks),
    }
    let caught_up = sim.run_until_condition(reconnected + CAP, |s| {
        s.status(lagging)
            .is_some_and(|st| st.commit_index >= target)
    });
    if caught_up {
        row.catch_up_ms = Some(ms(sim.now() - reconnected));
    } else {
        row.censored = true;
    }
    row.finish(&sim);
    row
}
