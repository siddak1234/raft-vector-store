//! The interface both algorithms implement.

use std::fmt::{self, Debug};
use std::hash::Hash;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Command, Config, LogIndex, NodeId, Payload, Storage, Term};

/// A consensus algorithm, written as a state machine that performs no I/O.
///
/// # Driver contract
///
/// Code that runs a server (the simulator, the model checker, or the networked server) is
/// called a driver. Every driver follows these rules, so both algorithms run under
/// identical conditions:
///
/// 1. Deliver one input at a time: [`start`](Self::start), [`step`](Self::step),
///    [`on_timer`](Self::on_timer), [`propose`](Self::propose), or
///    [`confirm_leadership`](Self::confirm_leadership).
/// 2. After each input, call [`Storage::sync`] before carrying out any output. A server
///    therefore never sends a message that depends on state it has not saved.
/// 3. Carry out outputs in the order returned.
/// 4. After a crash, discard the server and call [`start`](Self::start) again with the same
///    storage.
///
/// Implementations never read the clock, start threads, or perform I/O. They draw random
/// numbers only from an [`Rng`](crate::Rng) seeded with the `seed` passed to `start`.
pub trait Consensus: Sized {
    /// Messages exchanged between servers.
    type Message: Clone + Debug + Eq + Hash + Serialize + DeserializeOwned;

    /// Creates a server, restoring any state saved in `storage`. `cluster` lists every
    /// server, including this one. Returns the server and its first outputs, such as
    /// setting its election timer.
    fn start<S: Storage>(
        id: NodeId,
        cluster: &[NodeId],
        config: &Config,
        seed: u64,
        storage: &mut S,
    ) -> (Self, Vec<Output<Self::Message>>);

    /// Handles a message from another server.
    fn step<S: Storage>(
        &mut self,
        storage: &mut S,
        from: NodeId,
        message: Self::Message,
    ) -> Vec<Output<Self::Message>>;

    /// Handles a timer that has fired.
    fn on_timer<S: Storage>(&mut self, storage: &mut S, timer: Timer)
    -> Vec<Output<Self::Message>>;

    /// Submits a client command for replication.
    ///
    /// # Errors
    ///
    /// Returns [`NotLeader`] if this server is not the leader.
    fn propose<S: Storage>(
        &mut self,
        storage: &mut S,
        command: Command,
    ) -> Result<Vec<Output<Self::Message>>, NotLeader>;

    /// Asks the leader to confirm that it still leads by exchanging heartbeats with a
    /// majority (Raft §8). Storage is passed because the heartbeats carry log positions. The answer arrives later as [`Output::LeadershipConfirmed`] or
    /// [`Output::LeadershipLost`] with the same `request`. Used for leader-confirmed reads
    /// and for data-node lease renewals (`docs/DESIGN.md`).
    ///
    /// # Errors
    ///
    /// Returns [`NotLeader`] if this server is not the leader.
    fn confirm_leadership<S: Storage>(
        &mut self,
        storage: &mut S,
        request: RequestId,
    ) -> Result<Vec<Output<Self::Message>>, NotLeader>;

    /// Reports the server's current state, for checkers and metrics.
    fn status(&self) -> Status;

    /// Returns the name of a message's type, such as `"AppendEntries"`, so shared code can
    /// count messages by type (metrics M4, M9, and M18 in `docs/RESEARCH.md`).
    fn message_kind(message: &Self::Message) -> &'static str;

    /// Returns the number of log entries a message carries, so shared code can count
    /// entries in transit (metrics M10 and M16 in `docs/RESEARCH.md`).
    fn entry_count(message: &Self::Message) -> usize;
}

/// An action the driver carries out.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Output<M> {
    /// Send `message` to server `to`. The network may delay, reorder, duplicate, or lose
    /// it.
    Send {
        /// The receiving server.
        to: NodeId,
        /// The message.
        message: M,
    },
    /// Fire `timer` after `after`, replacing any pending timer of the same kind.
    SetTimer {
        /// The timer to set.
        timer: Timer,
        /// Delay until the timer fires.
        after: Duration,
    },
    /// Cancel `timer` if it is pending.
    CancelTimer {
        /// The timer to cancel.
        timer: Timer,
    },
    /// The entry at `index` is committed. Entries arrive in index order with no gaps.
    /// [`Payload::Noop`] entries are included so the driver can track progress; the state
    /// machine ignores them.
    Apply {
        /// The entry's index.
        index: LogIndex,
        /// The entry's contents.
        payload: Payload,
    },
    /// The leadership check `request` succeeded. A read is linearizable once the state
    /// machine has applied every entry up to `read_index`.
    LeadershipConfirmed {
        /// The request this answers.
        request: RequestId,
        /// The commit index when the request was made.
        read_index: LogIndex,
    },
    /// The leadership check `request` failed because this server stopped leading.
    LeadershipLost {
        /// The request this answers.
        request: RequestId,
    },
}

/// The timers an algorithm can set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Timer {
    /// Fires when a follower or candidate has not heard from a leader in time.
    Election,
    /// Fires when a leader should send heartbeats.
    Heartbeat,
}

/// Identifies a [`Consensus::confirm_leadership`] request. Chosen by the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(pub u64);

/// A server's role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Follows a leader.
    Follower,
    /// Trying to become leader.
    Candidate,
    /// Accepts client commands and replicates them.
    Leader,
}

/// A snapshot of a server's state, for checkers and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Status {
    /// The server's role.
    pub role: Role,
    /// The server's current term.
    pub term: Term,
    /// The leader this server follows, if known. A leader reports itself.
    pub leader: Option<NodeId>,
    /// The highest index this server knows to be committed.
    pub commit_index: LogIndex,
}

/// Returned when a request reaches a server that is not the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NotLeader {
    /// The leader this server knows of, so the client can retry there.
    pub leader: Option<NodeId>,
}

impl fmt::Display for NotLeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.leader {
            Some(NodeId(id)) => write!(f, "not the leader; try server {id}"),
            None => write!(f, "not the leader; leader unknown"),
        }
    }
}

impl std::error::Error for NotLeader {}
