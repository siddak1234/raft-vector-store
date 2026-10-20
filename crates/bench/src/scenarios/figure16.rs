//! E2: reproduction of the Raft paper's Figure 16 (§9.3; `docs/RESEARCH.md` §6).
//!
//! 1. Wait for a leader whose no-op is committed.
//! 2. Wait a uniformly random time within one heartbeat interval.
//! 3. The leader appends one command, delivered only to a random subset of followers (each
//!    follower independently with probability 1/2), so log lengths differ and some
//!    candidates cannot win.
//! 4. In the same instant, the leader sends heartbeats to every follower and crashes. The
//!    heartbeats carry no entries, so they reach every follower and restart all of their
//!    election timers at about the same time, as in the paper's test.
//! 5. Measure the time until a server becomes leader in a higher term (metric M1). Trials
//!    are capped at 60 s and recorded as censored if no leader emerges.

use std::collections::BTreeSet;
use std::time::Duration;

use consensus::Timer;
use sim::{DropRule, Fault, Stream, stream_rng};

use super::{
    RUN_HEADER, ms, record_candidates, run_columns, shuffled_followers, sim_config, start_ready,
};
use crate::algorithm::Algorithm;
use crate::config::RunConfig;
use crate::results::{Row, number};

const CAP: Duration = Duration::from_secs(60);

/// Servers boot at times spread over this window, so the first election does not split
/// forever when timeouts have no randomness. Only the election after the crash is measured.
const START_SPREAD: Duration = Duration::from_millis(300);

/// One E2 trial.
#[derive(Debug, Clone)]
pub struct Figure16Row {
    run: String,
    seed: u64,
    /// Whether the procedure ran: a first leader was elected, and a leader existed at the
    /// moment of the crash. Trials that are not ready have no measurement.
    ready: bool,
    /// Time from the crash until a new leader is elected (metric M1), in milliseconds.
    time_without_leader_ms: f64,
    /// Whether the trial reached the cap without a new leader.
    censored: bool,
    /// Terms in which some server became candidate before the new leader (metric M3).
    election_rounds: usize,
    /// Followers that received the leader's last entry.
    followers_with_entry: usize,
    violations: u64,
}

impl Row for Figure16Row {
    fn header() -> &'static str {
        concat!(
            "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,",
            "election_timeout_max_ms,uncommitted,",
            "seed,ready,time_without_leader_ms,censored,election_rounds,",
            "followers_with_entry,violations"
        )
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{}",
            self.run,
            self.seed,
            self.ready,
            number(self.time_without_leader_ms),
            self.censored,
            self.election_rounds,
            self.followers_with_entry,
            self.violations
        )
    }
}

/// Runs one E2 trial.
pub fn trial<A: Algorithm>(run: &RunConfig, seed: u64) -> Figure16Row {
    debug_assert!(Figure16Row::header().starts_with(RUN_HEADER));
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
    let mut config = sim_config::<A>(run, 0, false);
    config.start_spread = START_SPREAD;
    let heartbeat = config.consensus.heartbeat_interval;
    let Some(mut sim) = start_ready::<A>(config, seed) else {
        return row;
    };
    let mut rng = stream_rng(seed, Stream::Scenario);

    let phase = Duration::from_nanos(rng.below(heartbeat.as_nanos() as u64));
    sim.run_until(sim.now() + phase);
    let Some((leader, term)) = sim.leader() else {
        // An election was in progress at the chosen instant, so there is no leader to crash.
        row.violations = sim.violation_count();
        return row;
    };
    row.ready = true;

    for follower in shuffled_followers(sim.ids(), leader, &mut rng) {
        if rng.below(2) == 0 {
            row.followers_with_entry += 1;
        } else {
            sim.add_drop_rule(DropRule {
                from: leader,
                to: follower,
                only_with_entries: true,
            });
        }
    }
    // The proposal cannot fail: `leader` was the leader at this instant.
    let _ = sim.propose(leader);
    sim.fire_timer(leader, Timer::Heartbeat);
    sim.inject(Fault::Crash(leader));
    let crashed = sim.now();

    let mut candidate_terms = BTreeSet::new();
    let elected = sim.run_until_condition(crashed + CAP, |s| {
        record_candidates(s, &mut candidate_terms);
        s.leader().is_some_and(|(_, t)| t > term)
    });
    row.censored = !elected;
    row.time_without_leader_ms = ms(sim.now() - crashed);
    row.election_rounds = candidate_terms.len();
    row.violations = sim.violation_count();
    row
}
