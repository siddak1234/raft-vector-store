//! E3: failover after a leader crash with uncommitted entries (`docs/RESEARCH.md` §6).
//!
//! 1. Run the clients in steady state until a leader's no-op is committed, then for a
//!    uniformly random time between 1 and 2 seconds.
//! 2. The leader appends `k` commands. The messages carrying them reach only a random
//!    subset of followers, whose size is uniform in `0..=N−1`.
//! 3. The leader crashes in the same instant, so no follower learns whether the entries
//!    were committed.
//! 4. Measure, from the crash:
//!    - M1: time until a server leads a higher term;
//!    - M3: election rounds;
//!    - M4: messages and bytes until the new leader is elected;
//!    - M5: votes refused for a stale log (Raft only);
//!    - M10: entries the new leader sends until its no-op is committed;
//!    - M2: time until a client receives a commit reply for a command sent after the crash.
//!
//!    Metrics M15–M17 cover the whole trial.
//!
//! The same seed makes the same choices (timing, subset) for every algorithm.

use std::collections::BTreeSet;
use std::time::Duration;

use consensus::Storage;
use sim::{DropRule, Fault, Stream, stream_rng};

use super::{
    RUN_HEADER, ms, optional, record_candidates, run_columns, shuffled_followers, sim_config,
    start_ready,
};
use crate::algorithm::Algorithm;
use crate::config::RunConfig;
use crate::results::{Row, number};

const CAP: Duration = Duration::from_secs(60);

/// One E3 trial.
#[derive(Debug, Clone)]
pub struct FailoverRow {
    run: String,
    seed: u64,
    /// Whether the procedure ran: a first leader was elected, and a leader existed at the
    /// moment of the crash. Trials that are not ready have no measurement.
    ready: bool,
    censored: bool,
    followers_with_entries: usize,
    time_without_leader_ms: f64,
    unavailability_ms: Option<f64>,
    election_rounds: usize,
    election_messages: u64,
    election_bytes: u64,
    stale_log_rejections: Option<u64>,
    resent_entries: Option<u64>,
    max_log_gaps: u64,
    discarded_entries: u64,
    entries_to_leaders: u64,
    violations: u64,
}

impl Row for FailoverRow {
    fn header() -> &'static str {
        concat!(
            "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,",
            "election_timeout_max_ms,uncommitted,",
            "seed,ready,censored,followers_with_entries,time_without_leader_ms,",
            "unavailability_ms,election_rounds,election_messages,election_bytes,",
            "stale_log_rejections,resent_entries,max_log_gaps,discarded_entries,",
            "entries_to_leaders,violations"
        )
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.run,
            self.seed,
            self.ready,
            self.censored,
            self.followers_with_entries,
            number(self.time_without_leader_ms),
            optional(self.unavailability_ms),
            self.election_rounds,
            self.election_messages,
            self.election_bytes,
            self.stale_log_rejections
                .map(|n| n.to_string())
                .unwrap_or_default(),
            self.resent_entries
                .map(|n| n.to_string())
                .unwrap_or_default(),
            self.max_log_gaps,
            self.discarded_entries,
            self.entries_to_leaders,
            self.violations
        )
    }
}

/// Runs one E3 trial.
pub fn trial<A: Algorithm>(run: &RunConfig, seed: u64) -> FailoverRow {
    debug_assert!(FailoverRow::header().starts_with(RUN_HEADER));
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
    let Some(mut sim) = start_ready::<A>(sim_config::<A>(run, run.clients, true), seed) else {
        return row;
    };
    let mut rng = stream_rng(seed, Stream::Scenario);

    let wait = Duration::from_secs(1) + Duration::from_nanos(rng.below(1_000_000_000));
    sim.run_until(sim.now() + wait);
    let Some((leader, term)) = sim.leader() else {
        // An election was in progress at the chosen instant, so there is no leader to crash.
        row.violations = sim.violation_count();
        return row;
    };
    row.ready = true;

    let followers = shuffled_followers(sim.ids(), leader, &mut rng);
    let reached = rng.below(sim.ids().len() as u64) as usize;
    row.followers_with_entries = reached;
    for &follower in &followers[reached..] {
        sim.add_drop_rule(DropRule {
            from: leader,
            to: follower,
            only_with_entries: true,
        });
    }
    for _ in 0..run.uncommitted {
        let _ = sim.propose(leader);
    }
    sim.inject(Fault::Crash(leader));
    let crashed = sim.now();
    let before = sim.metrics().clone();
    A::reset(sim.observer_mut());

    // M1, M3, M4, M5: until a server leads a higher term.
    let mut candidate_terms = BTreeSet::new();
    let elected = sim.run_until_condition(crashed + CAP, |s| {
        record_candidates(s, &mut candidate_terms);
        s.leader().is_some_and(|(_, t)| t > term)
    });
    row.election_rounds = candidate_terms.len();
    row.time_without_leader_ms = ms(sim.now() - crashed);
    if !elected {
        row.censored = true;
        row.violations = sim.violation_count();
        return row;
    }
    let election = sim.metrics().since(&before);
    row.election_messages = election.sent.messages;
    row.election_bytes = election.sent.bytes;
    row.stale_log_rejections = A::stale_log_rejections(sim.observer());

    // M10: entries the new leader sends until its no-op is committed. The no-op is the last
    // entry in the new leader's log at the moment it is elected, for both algorithms.
    let (new_leader, new_term) = sim.leader().expect("just elected");
    let noop = sim.storage(new_leader).last_index();
    let sent_before = sim.metrics().sent_by(new_leader).entries;
    let committed = sim.run_until_condition(crashed + CAP, |s| {
        s.status(new_leader)
            .is_none_or(|st| st.term != new_term || st.commit_index >= noop)
    });
    if committed && sim.status(new_leader).is_some_and(|st| st.term == new_term) {
        row.resent_entries = Some(sim.metrics().sent_by(new_leader).entries - sent_before);
    }

    // M2: until a client receives a commit reply for a command sent after the crash.
    let mut scanned = 0;
    let available = sim.run_until_condition(crashed + CAP, |s| {
        let replies = &s.replies()[scanned..];
        scanned = s.replies().len();
        replies.iter().any(|reply| reply.sent >= crashed)
    });
    if available {
        row.unavailability_ms = Some(ms(sim.now() - crashed));
    }

    row.max_log_gaps = sim.max_log_gaps();
    row.discarded_entries = sim.discarded_entries();
    row.entries_to_leaders = sim.metrics().entries_to_leaders;
    row.violations = sim.violation_count();
    row
}
