//! The algorithms the runner can simulate, and their algorithm-specific measurements.

use consensus::{Consensus, NodeId, Storage};
use raft::{Message, Raft};
use sim::{Observer, ServerView, SimTime};

/// A consensus algorithm under test.
///
/// Shared metrics are measured by the simulator. This trait adds the few measurements that
/// need an algorithm's own message types, such as Raft's stale-log vote refusals (metric
/// M5, `docs/RESEARCH.md` §5).
pub trait Algorithm: Consensus {
    /// The name used in configs and result files.
    const NAME: &'static str;
    /// Whether the Raft-only checks (log matching, leader completeness) apply.
    const RAFT_CHECKS: bool;
    /// Counts algorithm-specific metrics during a simulation.
    type Observer: Observer<Self> + Default;

    /// Votes refused because the candidate's log was behind (metric M5), counted since the
    /// observer was last reset. `None` if the algorithm has no such votes.
    fn stale_log_rejections(observer: &Self::Observer) -> Option<u64>;

    /// Resets the observer's counters.
    fn reset(observer: &mut Self::Observer);
}

/// Counts Raft votes refused because the candidate's log was behind the voter's (§5.4.1).
#[derive(Debug, Default)]
pub struct StaleVotes {
    /// Refusals counted since the last reset.
    pub count: u64,
}

impl Observer<Raft> for StaleVotes {
    fn on_deliver(
        &mut self,
        _now: SimTime,
        _from: NodeId,
        _to: NodeId,
        message: &Message,
        receiver: ServerView<'_>,
    ) {
        let Message::RequestVote {
            term,
            last_log_index,
            last_log_term,
        } = message
        else {
            return;
        };
        let Some(status) = receiver.status else {
            return;
        };
        if *term < status.term {
            // Refused for its term, not its log.
            return;
        }
        let last = receiver.storage.last_index();
        let last_term = receiver
            .storage
            .entry(last)
            .map(|e| e.term)
            .unwrap_or_default();
        if (*last_log_term, *last_log_index) < (last_term, last) {
            self.count += 1;
        }
    }
}

impl Algorithm for Raft {
    const NAME: &'static str = "raft";
    const RAFT_CHECKS: bool = true;
    type Observer = StaleVotes;

    fn stale_log_rejections(observer: &StaleVotes) -> Option<u64> {
        Some(observer.count)
    }

    fn reset(observer: &mut StaleVotes) {
        observer.count = 0;
    }
}
