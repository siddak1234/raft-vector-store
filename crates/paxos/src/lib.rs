//! Multi-Paxos consensus, implemented from the frozen specification in
//! `docs/PAXOS_SPEC.md`.
//!
//! The code is split by subproblem, as `crates/raft` is, so code complexity can be compared
//! per subproblem (`docs/RESEARCH.md` §5):
//!
//! - `election`: term ownership, `Prepare` and `Promise`, and becoming leader (spec §5,
//!   "Candidates" and "Becoming leader").
//! - `replication`: `Accept`, `Accepted`, and `Commit`, and follower progress (spec §5,
//!   "Followers" and "Leaders" rules 1–3).
//! - `commit`: choosing entries, applying them, and leadership confirmation (spec §5,
//!   "Leaders" rule 4).
//! - this file: the server's state, the messages, and the [`Consensus`] implementation.
//!
//! Details the specification leaves open are recorded in its Amendments section.
//!
//! See `docs/PLAN.md` §4.3.

mod commit;
mod election;
mod replication;

use std::collections::{BTreeMap, BTreeSet};

use consensus::{
    Command, Config, Consensus, Entry, HardState, LogIndex, NodeId, NotLeader, Output, Payload,
    RequestId, Rng, Role, Status, Storage, Term, Timer,
};
use serde::{Deserialize, Serialize};

/// The five Multi-Paxos messages (spec §4).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Message {
    /// Sent by a candidate to ask every server to promise its term.
    Prepare {
        /// The candidate's term.
        term: Term,
        /// The candidate's commit index. Voters return their entries above it.
        commit_index: LogIndex,
    },
    /// The answer to `Prepare`.
    Promise {
        /// The term promised, or the voter's current term if it refused.
        term: Term,
        /// Whether the voter promised the term.
        granted: bool,
        /// Every entry the voter holds above the candidate's commit index, with its index.
        entries: Vec<(LogIndex, Entry)>,
    },
    /// Sent by the leader to have followers accept entries. With no entries, it is a
    /// heartbeat.
    Accept {
        /// The leader's term. Every entry carries this term.
        term: Term,
        /// Index of the first entry.
        first_index: LogIndex,
        /// Entries for `first_index`, `first_index + 1`, and so on.
        entries: Vec<Entry>,
        /// The leader's commit index.
        commit_index: LogIndex,
        /// The leader's heartbeat round when it sent this message (spec amendment A1).
        round: u64,
    },
    /// The answer to `Accept` and to `Commit`.
    Accepted {
        /// The follower's current term.
        term: Term,
        /// Whether the follower accepted the message.
        success: bool,
        /// First index accepted. The range is empty when `last_index < first_index`.
        first_index: LogIndex,
        /// Last index accepted.
        last_index: LogIndex,
        /// The follower's commit index after handling the message.
        commit_index: LogIndex,
        /// The round of the `Accept` being answered, or 0 for a `Commit`.
        round: u64,
    },
    /// Sent by the leader to a follower that is missing chosen entries.
    Commit {
        /// The leader's term.
        term: Term,
        /// Index of the first entry.
        first_index: LogIndex,
        /// Chosen entries for `first_index`, `first_index + 1`, and so on.
        entries: Vec<Entry>,
        /// The leader's commit index.
        commit_index: LogIndex,
    },
}

impl Message {
    /// The sender's term.
    pub fn term(&self) -> Term {
        match self {
            Message::Prepare { term, .. }
            | Message::Promise { term, .. }
            | Message::Accept { term, .. }
            | Message::Accepted { term, .. }
            | Message::Commit { term, .. } => *term,
        }
    }
}

type Outputs = Vec<Output<Message>>;

/// What the leader knows about one follower.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Progress {
    /// The next index not yet sent in an `Accept`.
    next_index: LogIndex,
    /// The lowest index above `base_index` the follower has not accepted in this term.
    ack_floor: LogIndex,
    /// Indexes above `ack_floor` the follower has accepted in this term.
    acked: BTreeSet<LogIndex>,
    /// The follower's latest reported commit index (`followerCommit` in the spec).
    commit_index: LogIndex,
    /// The highest heartbeat round the follower has answered.
    acked_round: u64,
    /// The first index of an unanswered `Commit`, if one is outstanding.
    commit_waiting: Option<LogIndex>,
}

impl Progress {
    fn new(base_index: LogIndex) -> Self {
        Progress {
            next_index: base_index.next(),
            ack_floor: base_index.next(),
            acked: BTreeSet::new(),
            commit_index: LogIndex::ZERO,
            acked_round: 0,
            commit_waiting: None,
        }
    }

    /// Whether the follower has accepted `index` in the leader's term.
    fn has_accepted(&self, index: LogIndex) -> bool {
        index < self.ack_floor || self.acked.contains(&index)
    }
}

/// A leadership confirmation request waiting for a majority (spec §5, "Leaders" rule 4).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PendingRead {
    request: RequestId,
    /// The heartbeat round started for this request.
    round: u64,
    /// The commit index to report, set once the leader's no-op is committed.
    read_index: Option<LogIndex>,
}

/// One Multi-Paxos server.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Paxos {
    id: NodeId,
    /// This server's position in the sorted cluster, which decides the terms it owns.
    rank: u64,
    /// Number of servers in the cluster.
    servers: u64,
    /// Every other server, in ascending order.
    peers: Vec<NodeId>,
    config: Config,
    rng: Rng,
    role: Role,
    /// The leader of the current term, if known.
    leader: Option<NodeId>,
    /// Copy of the persistent `currentTerm`, which lives in storage.
    term: Term,
    commit_index: LogIndex,
    last_applied: LogIndex,
    /// Candidate only: the promises received in the current term, including its own.
    promises: BTreeMap<NodeId, Vec<(LogIndex, Entry)>>,
    /// Leader only: the commit index at election. The leader proposes every index above it.
    base_index: LogIndex,
    /// Leader only: for each index above the commit index, the servers that accepted it in
    /// the current term.
    accepted: BTreeMap<LogIndex, BTreeSet<NodeId>>,
    /// Leader only: progress of each follower.
    followers: BTreeMap<NodeId, Progress>,
    /// Leader only: index of the no-op appended on election.
    noop_index: LogIndex,
    /// Leader only: the current heartbeat round.
    round: u64,
    /// Leader only: leadership confirmations not yet answered.
    reads: Vec<PendingRead>,
}

impl Consensus for Paxos {
    type Message = Message;

    fn start<S: Storage>(
        id: NodeId,
        cluster: &[NodeId],
        config: &Config,
        seed: u64,
        storage: &mut S,
    ) -> (Self, Outputs) {
        let mut sorted: Vec<NodeId> = cluster.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let rank = sorted
            .iter()
            .position(|&s| s == id)
            .expect("the cluster includes this server") as u64;
        let peers: Vec<NodeId> = sorted.iter().copied().filter(|&p| p != id).collect();
        let mut paxos = Paxos {
            id,
            rank,
            servers: sorted.len() as u64,
            peers,
            config: config.clone(),
            rng: Rng::new(seed),
            role: Role::Follower,
            leader: None,
            term: storage.hard_state().term,
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
            promises: BTreeMap::new(),
            base_index: LogIndex::ZERO,
            accepted: BTreeMap::new(),
            followers: BTreeMap::new(),
            noop_index: LogIndex::ZERO,
            round: 0,
            reads: Vec::new(),
        };
        let mut out = Vec::new();
        paxos.reset_election_timer(&mut out);
        (paxos, out)
    }

    fn step<S: Storage>(&mut self, storage: &mut S, from: NodeId, message: Message) -> Outputs {
        let mut out = Vec::new();
        // Spec §5, all servers, rule 1: a higher term means this server is out of date.
        if message.term() > self.term {
            self.step_down(storage, message.term(), &mut out);
        }
        match message {
            Message::Prepare { term, commit_index } => {
                self.handle_prepare(storage, from, term, commit_index, &mut out);
            }
            Message::Promise {
                term,
                granted,
                entries,
            } => self.handle_promise(storage, from, term, granted, entries, &mut out),
            Message::Accept {
                term,
                first_index,
                entries,
                commit_index,
                round,
            } => self.handle_accept(
                storage,
                from,
                term,
                first_index,
                entries,
                commit_index,
                round,
                &mut out,
            ),
            Message::Accepted {
                term,
                success,
                first_index,
                last_index,
                commit_index,
                round,
            } => self.handle_accepted(
                storage,
                from,
                term,
                success,
                (first_index, last_index),
                commit_index,
                round,
                &mut out,
            ),
            Message::Commit {
                term,
                first_index,
                entries,
                commit_index,
            } => self.handle_commit(
                storage,
                from,
                term,
                first_index,
                entries,
                commit_index,
                &mut out,
            ),
        }
        out
    }

    fn on_timer<S: Storage>(&mut self, storage: &mut S, timer: Timer) -> Outputs {
        let mut out = Vec::new();
        match timer {
            Timer::Election if self.role != Role::Leader => self.start_election(storage, &mut out),
            Timer::Heartbeat if self.role == Role::Leader => self.broadcast(storage, &mut out),
            _ => {}
        }
        out
    }

    fn propose<S: Storage>(
        &mut self,
        storage: &mut S,
        command: Command,
    ) -> Result<Outputs, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader,
            });
        }
        // Spec §5, leaders, rule 1.
        let index = storage.last_index().next();
        storage.put(
            index,
            Entry {
                term: self.term,
                payload: Payload::Command(command),
            },
        );
        self.accepted.insert(index, BTreeSet::from([self.id]));
        let mut out = Vec::new();
        self.replicate(storage, &mut out);
        self.advance_commit(storage, &mut out);
        Ok(out)
    }

    fn confirm_leadership<S: Storage>(
        &mut self,
        storage: &mut S,
        request: RequestId,
    ) -> Result<Outputs, NotLeader> {
        self.request_confirmation(storage, request)
    }

    fn status(&self) -> Status {
        Status {
            role: self.role,
            term: self.term,
            leader: self.leader,
            commit_index: self.commit_index,
        }
    }

    fn message_kind(message: &Message) -> &'static str {
        match message {
            Message::Prepare { .. } => "Prepare",
            Message::Promise { .. } => "Promise",
            Message::Accept { .. } => "Accept",
            Message::Accepted { .. } => "Accepted",
            Message::Commit { .. } => "Commit",
        }
    }

    fn entry_count(message: &Message) -> usize {
        match message {
            Message::Promise { entries, .. } => entries.len(),
            Message::Accept { entries, .. } | Message::Commit { entries, .. } => entries.len(),
            Message::Prepare { .. } | Message::Accepted { .. } => 0,
        }
    }
}

impl Paxos {
    /// The number of servers that makes a majority, including this one.
    fn quorum(&self) -> usize {
        self.servers as usize / 2 + 1
    }

    fn persist_term<S: Storage>(&self, storage: &mut S) {
        storage.set_hard_state(HardState {
            term: self.term,
            voted_for: None,
        });
    }

    fn reset_election_timer(&mut self, out: &mut Outputs) {
        out.push(Output::SetTimer {
            timer: Timer::Election,
            after: self.config.election_timeout(&mut self.rng),
        });
    }

    /// Moves to a higher term as a follower (spec §5, all servers, rule 1).
    fn step_down<S: Storage>(&mut self, storage: &mut S, term: Term, out: &mut Outputs) {
        self.term = term;
        self.persist_term(storage);
        self.become_follower(None, out);
    }

    /// Becomes a follower of `leader` in the current term.
    fn become_follower(&mut self, leader: Option<NodeId>, out: &mut Outputs) {
        if self.role == Role::Leader {
            out.push(Output::CancelTimer {
                timer: Timer::Heartbeat,
            });
            self.reset_election_timer(out);
            self.fail_reads(out);
            self.followers.clear();
            self.accepted.clear();
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.promises.clear();
    }
}
