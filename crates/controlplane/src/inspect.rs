//! The few measurements that need an algorithm's own message type, such as Raft's votes
//! refused for a stale log (metric M5). The driver calls them but never looks inside
//! messages itself, so the rest of the server treats both algorithms the same.

use consensus::{Consensus, Status, Storage};
use paxos::Paxos;
use raft::{Message, Raft};

/// An algorithm the server can run.
pub trait Inspect: Consensus + Send + 'static {
    /// Whether delivering `message` to a server in state `receiver` makes it refuse a vote
    /// because the candidate's log is behind its own (Raft §5.4.1).
    fn is_stale_vote<S: Storage>(message: &Self::Message, receiver: Status, storage: &S) -> bool;
}

impl Inspect for Raft {
    fn is_stale_vote<S: Storage>(message: &Message, receiver: Status, storage: &S) -> bool {
        let Message::RequestVote {
            term,
            last_log_index,
            last_log_term,
        } = message
        else {
            return false;
        };
        if *term < receiver.term {
            return false;
        }
        let last = storage.last_index();
        let last_term = storage.entry(last).map(|e| e.term).unwrap_or_default();
        (*last_log_term, *last_log_index) < (last_term, last)
    }
}

impl Inspect for Paxos {
    fn is_stale_vote<S: Storage>(_: &paxos::Message, _: Status, _: &S) -> bool {
        false
    }
}
