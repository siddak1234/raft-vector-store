//! The algorithms the runner can simulate, and their algorithm-specific measurements.

use consensus::{Consensus, NodeId, Storage};
use paxos::Paxos;
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

impl Algorithm for Paxos {
    const NAME: &'static str = "paxos";
    const RAFT_CHECKS: bool = false;
    type Observer = ();

    fn stale_log_rejections(_observer: &()) -> Option<u64> {
        None
    }

    fn reset(_observer: &mut ()) {}
}

/// The algorithms the runner can simulate, chosen by name in configs and on the command
/// line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// [`Raft`].
    Raft,
    /// [`Paxos`].
    Paxos,
}

impl Kind {
    /// Every algorithm.
    pub const ALL: [Kind; 2] = [Kind::Raft, Kind::Paxos];

    /// The algorithm's name.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Raft => Raft::NAME,
            Kind::Paxos => Paxos::NAME,
        }
    }

    /// Finds an algorithm by name.
    ///
    /// # Errors
    ///
    /// Returns a description listing the known names if `name` is not one of them.
    pub fn parse(name: &str) -> Result<Kind, String> {
        Kind::ALL
            .into_iter()
            .find(|kind| kind.name() == name)
            .ok_or_else(|| {
                let known: Vec<&str> = Kind::ALL.iter().map(|kind| kind.name()).collect();
                format!(
                    "unknown algorithm `{name}` (available: {})",
                    known.join(", ")
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::Kind;

    #[test]
    fn parses_every_name() {
        for kind in Kind::ALL {
            assert_eq!(Kind::parse(kind.name()), Ok(kind));
        }
        assert!(Kind::parse("zab").is_err());
    }
}
