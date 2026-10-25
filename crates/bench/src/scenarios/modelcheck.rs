//! E6: exhaustive model checking (`docs/RESEARCH.md` §6, amended by A4).
//!
//! Each run explores every state of one algorithm reachable within its bounds, depth-first
//! with Stateright, and records metric M14: the number of distinct states of the compared
//! fields. It also records whether the safety properties held in every state and whether
//! some state commits a client command.
//!
//! A search counts as complete only if it ended before its time budget and before its
//! state cap; otherwise the row is marked incomplete and its counts are not comparable.

use std::time::{Duration, Instant};

use consensus::Config;
use modelcheck::{Bounds, Checkable, ConsensusModel};
use stateright::{Checker, Model};

use super::run_columns;
use crate::config::RunConfig;
use crate::results::{Row, number};

/// Time budget of one search.
pub const BUDGET: Duration = Duration::from_secs(6 * 3600);

/// Most states one search may generate, counting repeats. At about 45 bytes per unique
/// state and about six generated per unique one, this keeps a search within roughly 9 GB.
pub const STATE_CAP: usize = 1_200_000_000;

/// One E6 search.
#[derive(Debug, Clone)]
pub struct ModelcheckRow {
    run: String,
    max_commands: u8,
    max_elections: u8,
    max_heartbeats: u8,
    complete: bool,
    /// Distinct states of the compared fields (metric M14).
    states: usize,
    /// Distinct full search states, including algorithm internals.
    search_states: usize,
    /// States generated, counting repeats.
    generated: usize,
    max_depth: usize,
    seconds: f64,
    election_safety: bool,
    state_machine_safety: bool,
    command_committed: bool,
}

impl Row for ModelcheckRow {
    fn header() -> &'static str {
        concat!(
            "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,",
            "election_timeout_max_ms,uncommitted,",
            "max_commands,max_elections,max_heartbeats,complete,states,search_states,",
            "generated,max_depth,seconds,election_safety,state_machine_safety,",
            "command_committed"
        )
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.run,
            self.max_commands,
            self.max_elections,
            self.max_heartbeats,
            self.complete,
            self.states,
            self.search_states,
            self.generated,
            self.max_depth,
            number(self.seconds),
            self.election_safety,
            self.state_machine_safety,
            self.command_committed
        )
    }
}

/// Runs one search with every CPU core.
pub fn search<A: Checkable>(run: &RunConfig) -> ModelcheckRow {
    let bounds = Bounds {
        servers: run.servers,
        commands: run.max_commands.unwrap_or(0),
        elections: run.max_elections.unwrap_or(0),
        heartbeats: run.max_heartbeats.unwrap_or(0),
    };
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let model = ConsensusModel::<A>::new(bounds, Config::default());
    let projections = model.projections();
    let started = Instant::now();
    let checker = model
        .checker()
        .threads(threads)
        .timeout(BUDGET)
        .target_state_count(STATE_CAP)
        .spawn_dfs()
        .join();
    let elapsed = started.elapsed();
    let discoveries = checker.discoveries();
    ModelcheckRow {
        run: run_columns(run),
        max_commands: bounds.commands,
        max_elections: bounds.elections,
        max_heartbeats: bounds.heartbeats,
        complete: elapsed < BUDGET && checker.state_count() < STATE_CAP,
        states: projections.count(),
        search_states: checker.unique_state_count(),
        generated: checker.state_count(),
        max_depth: checker.max_depth(),
        seconds: elapsed.as_secs_f64(),
        election_safety: !discoveries.contains_key("election safety"),
        state_machine_safety: !discoveries.contains_key("state machine safety"),
        command_committed: discoveries.contains_key("a command is committed"),
    }
}
