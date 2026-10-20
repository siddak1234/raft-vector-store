//! Identifiers and log entries.

use serde::{Deserialize, Serialize};

/// Identifies a server in the consensus cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// Raft's election term, or Multi-Paxos's ballot number. Terms only increase.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Term(pub u64);

impl Term {
    /// The term before any election.
    pub const ZERO: Term = Term(0);
}

/// Position of an entry in the replicated log. The first entry has index 1.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct LogIndex(pub u64);

impl LogIndex {
    /// Means "no entry": the index before the first entry.
    pub const ZERO: LogIndex = LogIndex(0);

    /// Returns the index after this one.
    pub fn next(self) -> LogIndex {
        LogIndex(self.0 + 1)
    }
}

/// A client command. The consensus layer treats it as opaque bytes; the control-plane
/// state machine defines its contents.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Command(pub Vec<u8>);

/// The contents of a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Payload {
    /// An empty entry. A new leader appends one so that it can commit entries from earlier
    /// terms (Raft §8). Multi-Paxos also uses it to fill gaps. The state machine ignores it.
    Noop,
    /// A client command.
    Command(Command),
}

/// One entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Entry {
    /// The term in which the entry was created (Raft) or last accepted (Multi-Paxos).
    pub term: Term,
    /// The entry's contents.
    pub payload: Payload,
}
