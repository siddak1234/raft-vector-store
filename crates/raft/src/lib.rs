//! Raft consensus, implemented from Figure 2 of Ongaro and Ousterhout, *In Search of an
//! Understandable Consensus Algorithm* (2014), with the no-op entry and leader-confirmed
//! reads from §8.
//!
//! The code is split by subproblem. Multi-Paxos uses the same split, so code complexity
//! can be compared per subproblem (`docs/RESEARCH.md` §5):
//!
//! - `election`: terms, votes, and becoming leader (§5.2, §5.4.1).
//! - `replication`: `AppendEntries` and follower progress (§5.3).
//! - `commit`: the commit rule, applying entries, and leadership confirmation (§5.4.2, §8).
//! - this file: the server's state, the messages, and the [`Consensus`] implementation.
//!
//! Two details go beyond Figure 2; both are recorded in the Amendments section of
//! `docs/RESEARCH.md`:
//!
//! - **Probing and pipelining.** The leader tracks each follower in one of two modes, as
//!   etcd's Raft library does. While *probing*, it sends one `AppendEntries` at a time and
//!   backs up after each rejection: one entry, as in Figure 2, or straight to the
//!   follower's last index + 1 if that is lower. Once the logs match, it *replicates*:
//!   it sends entries ahead of acknowledgments, up to the in-flight limit. Each reply
//!   names the `AppendEntries` it answers, so rejections of messages sent before the last
//!   change are ignored. The term-based backtracking optimization of §5.3 is not used.
//! - **Heartbeat rounds.** `AppendEntries` and its reply carry a round number, so the leader
//!   can tell which replies answer heartbeats sent after a leadership confirmation request.
//!
//! See `docs/PLAN.md` §4.2.

mod commit;
mod election;
mod replication;

use std::collections::{BTreeMap, BTreeSet};

use consensus::{
    Command, Config, Consensus, Entry, HardState, LogIndex, NodeId, NotLeader, Output, Payload,
    RequestId, Rng, Role, Status, Storage, Term, Timer,
};
use serde::{Deserialize, Serialize};

/// The four Raft messages: two RPCs and their replies (Figure 2).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Message {
    /// Sent by a candidate to ask for a vote (§5.2).
    RequestVote {
        /// The candidate's term.
        term: Term,
        /// Index of the candidate's last log entry (§5.4.1).
        last_log_index: LogIndex,
        /// Term of the candidate's last log entry (§5.4.1).
        last_log_term: Term,
    },
    /// The answer to `RequestVote`.
    RequestVoteReply {
        /// The voter's current term.
        term: Term,
        /// Whether the vote was granted.
        granted: bool,
    },
    /// Sent by the leader to replicate entries. With no entries, it is a heartbeat (§5.3).
    AppendEntries {
        /// The leader's term.
        term: Term,
        /// Index of the entry just before the new ones.
        prev_log_index: LogIndex,
        /// Term of the entry at `prev_log_index`.
        prev_log_term: Term,
        /// Entries to store, starting at `prev_log_index + 1`.
        entries: Vec<Entry>,
        /// The leader's commit index.
        leader_commit: LogIndex,
        /// The leader's heartbeat round when it sent this message.
        round: u64,
    },
    /// The answer to `AppendEntries`.
    AppendEntriesReply {
        /// The follower's current term.
        term: Term,
        /// Whether the follower's log matched at `prev_log_index`.
        success: bool,
        /// The `prev_log_index` of the `AppendEntries` being answered.
        prev_log_index: LogIndex,
        /// On success, the last index the follower now shares with the leader. On failure,
        /// the follower's last log index.
        index: LogIndex,
        /// The round of the `AppendEntries` being answered.
        round: u64,
    },
}

impl Message {
    /// The sender's term.
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteReply { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesReply { term, .. } => *term,
        }
    }
}

type Outputs = Vec<Output<Message>>;

/// What the leader knows about one follower (Figure 2, "volatile state on leaders").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Progress {
    /// The next entry to send.
    next_index: LogIndex,
    /// The highest entry known to be stored on the follower.
    match_index: LogIndex,
    /// The highest heartbeat round the follower has answered.
    acked_round: u64,
    /// How entries are sent to this follower.
    flow: Flow,
}

/// How the leader sends entries to one follower.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Flow {
    /// The leader does not yet know where the follower's log matches its own. It sends one
    /// `AppendEntries` and waits for the answer (or the next heartbeat) before sending
    /// another. `waiting` is set while a probe is unanswered.
    Probe { waiting: bool },
    /// The logs match up to `match_index`. The leader sends entries without waiting for
    /// acknowledgments, up to the in-flight limit.
    Replicate,
}

/// A leadership confirmation request waiting for a majority (§8).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PendingRead {
    request: RequestId,
    /// The heartbeat round started for this request.
    round: u64,
    /// The commit index to report, set once the leader's no-op is committed.
    read_index: Option<LogIndex>,
}

/// One Raft server.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Raft {
    id: NodeId,
    /// Every other server, in ascending order.
    peers: Vec<NodeId>,
    config: Config,
    rng: Rng,
    role: Role,
    /// The leader of the current term, if known.
    leader: Option<NodeId>,
    /// Copies of the persistent `currentTerm` and `votedFor`, which live in storage.
    term: Term,
    voted_for: Option<NodeId>,
    commit_index: LogIndex,
    last_applied: LogIndex,
    /// Candidate only: servers that granted their vote, including this one.
    votes: BTreeSet<NodeId>,
    /// Leader only: progress of each follower.
    followers: BTreeMap<NodeId, Progress>,
    /// Leader only: index of the no-op appended on election.
    noop_index: LogIndex,
    /// Leader only: the current heartbeat round.
    round: u64,
    /// Leader only: leadership confirmations not yet answered.
    reads: Vec<PendingRead>,
}

impl Consensus for Raft {
    type Message = Message;

    fn start<S: Storage>(
        id: NodeId,
        cluster: &[NodeId],
        config: &Config,
        seed: u64,
        storage: &mut S,
    ) -> (Self, Outputs) {
        let hard = storage.hard_state();
        let mut peers: Vec<NodeId> = cluster.iter().copied().filter(|&p| p != id).collect();
        peers.sort_unstable();
        let mut raft = Raft {
            id,
            peers,
            config: config.clone(),
            rng: Rng::new(seed),
            role: Role::Follower,
            leader: None,
            term: hard.term,
            voted_for: hard.voted_for,
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
            votes: BTreeSet::new(),
            followers: BTreeMap::new(),
            noop_index: LogIndex::ZERO,
            round: 0,
            reads: Vec::new(),
        };
        let mut out = Vec::new();
        raft.reset_election_timer(&mut out);
        (raft, out)
    }

    fn step<S: Storage>(&mut self, storage: &mut S, from: NodeId, message: Message) -> Outputs {
        let mut out = Vec::new();
        // Figure 2, all servers: a higher term means this server is out of date.
        if message.term() > self.term {
            self.step_down(storage, message.term(), &mut out);
        }
        match message {
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(
                storage,
                from,
                term,
                (last_log_index, last_log_term),
                &mut out,
            ),
            Message::RequestVoteReply { term, granted } => {
                self.handle_vote_reply(storage, from, term, granted, &mut out);
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                round,
            } => self.handle_append_entries(
                storage,
                from,
                term,
                (prev_log_index, prev_log_term),
                entries,
                leader_commit,
                round,
                &mut out,
            ),
            Message::AppendEntriesReply {
                term,
                success,
                prev_log_index,
                index,
                round,
            } => self.handle_append_reply(
                storage,
                from,
                term,
                success,
                (prev_log_index, index),
                round,
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
        let index = storage.last_index().next();
        storage.put(
            index,
            Entry {
                term: self.term,
                payload: Payload::Command(command),
            },
        );
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
            Message::RequestVote { .. } => "RequestVote",
            Message::RequestVoteReply { .. } => "RequestVoteReply",
            Message::AppendEntries { .. } => "AppendEntries",
            Message::AppendEntriesReply { .. } => "AppendEntriesReply",
        }
    }

    fn entry_count(message: &Message) -> usize {
        match message {
            Message::AppendEntries { entries, .. } => entries.len(),
            _ => 0,
        }
    }
}

impl Raft {
    /// The number of servers that makes a majority, including this one.
    fn quorum(&self) -> usize {
        let servers = self.peers.len() + 1;
        servers / 2 + 1
    }

    fn persist_hard_state<S: Storage>(&self, storage: &mut S) {
        storage.set_hard_state(HardState {
            term: self.term,
            voted_for: self.voted_for,
        });
    }

    fn reset_election_timer(&mut self, out: &mut Outputs) {
        out.push(Output::SetTimer {
            timer: Timer::Election,
            after: self.config.election_timeout(&mut self.rng),
        });
    }

    /// Moves to a higher term as a follower that has not voted in it (Figure 2).
    fn step_down<S: Storage>(&mut self, storage: &mut S, term: Term, out: &mut Outputs) {
        self.term = term;
        self.voted_for = None;
        self.persist_hard_state(storage);
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
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
    }
}

/// Returns the index and term of the last entry in the log.
fn last_log<S: Storage>(storage: &S) -> (LogIndex, Term) {
    let last = storage.last_index();
    (last, storage.entry(last).map_or(Term::ZERO, |e| e.term))
}

/// Returns the term of the entry at `index`, `Term::ZERO` for index 0, or `None` if the log
/// has no entry there.
fn term_at<S: Storage>(storage: &S, index: LogIndex) -> Option<Term> {
    if index == LogIndex::ZERO {
        Some(Term::ZERO)
    } else {
        storage.entry(index).map(|e| e.term)
    }
}
