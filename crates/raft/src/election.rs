//! Leader election (Raft §5.2) with the election restriction (§5.4.1).

use std::collections::BTreeSet;

use consensus::{Entry, LogIndex, NodeId, Output, Payload, Role, Storage, Term, Timer};

use crate::{Flow, Message, Outputs, Progress, Raft, last_log};

impl Raft {
    /// Starts an election: moves to the next term, votes for itself, and asks every other
    /// server for its vote (Figure 2, "Candidates").
    pub(crate) fn start_election<S: Storage>(&mut self, storage: &mut S, out: &mut Outputs) {
        self.term = Term(self.term.0 + 1);
        self.voted_for = Some(self.id);
        self.persist_hard_state(storage);
        self.role = Role::Candidate;
        self.leader = None;
        self.votes = BTreeSet::from([self.id]);
        self.reset_election_timer(out);

        if self.votes.len() >= self.quorum() {
            // A one-server cluster wins with its own vote.
            self.become_leader(storage, out);
            return;
        }
        let (last_log_index, last_log_term) = last_log(storage);
        for &peer in &self.peers {
            out.push(Output::Send {
                to: peer,
                message: Message::RequestVote {
                    term: self.term,
                    last_log_index,
                    last_log_term,
                },
            });
        }
    }

    /// Decides whether to grant a vote (Figure 2, `RequestVote` receiver).
    ///
    /// The vote is granted only if this server has not voted for someone else in this term
    /// and the candidate's log is at least as up to date as its own: a later last term, or
    /// the same last term and an equal or higher last index (§5.4.1). This restriction is
    /// what guarantees that every leader already holds every committed entry.
    pub(crate) fn handle_request_vote<S: Storage>(
        &mut self,
        storage: &mut S,
        candidate: NodeId,
        term: Term,
        (last_log_index, last_log_term): (LogIndex, Term),
        out: &mut Outputs,
    ) {
        let (my_index, my_term) = last_log(storage);
        let up_to_date = (last_log_term, last_log_index) >= (my_term, my_index);
        let granted =
            term == self.term && self.voted_for.is_none_or(|v| v == candidate) && up_to_date;
        if granted {
            self.voted_for = Some(candidate);
            self.persist_hard_state(storage);
            self.reset_election_timer(out);
        }
        out.push(Output::Send {
            to: candidate,
            message: Message::RequestVoteReply {
                term: self.term,
                granted,
            },
        });
    }

    /// Counts a vote; with a majority, becomes leader.
    pub(crate) fn handle_vote_reply<S: Storage>(
        &mut self,
        storage: &mut S,
        voter: NodeId,
        term: Term,
        granted: bool,
        out: &mut Outputs,
    ) {
        if self.role != Role::Candidate || term != self.term || !granted {
            return;
        }
        self.votes.insert(voter);
        if self.votes.len() >= self.quorum() {
            self.become_leader(storage, out);
        }
    }

    /// Takes over as leader of the current term.
    ///
    /// The new leader appends a no-op entry in its own term. Committing it also commits
    /// every earlier entry, which a leader cannot do by counting replicas of old entries
    /// (§5.4.2, §8).
    fn become_leader<S: Storage>(&mut self, storage: &mut S, out: &mut Outputs) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.votes.clear();

        let last = storage.last_index();
        self.followers = self
            .peers
            .iter()
            .map(|&peer| {
                let progress = Progress {
                    next_index: last.next(),
                    match_index: LogIndex::ZERO,
                    acked_round: 0,
                    flow: Flow::Probe { waiting: false },
                };
                (peer, progress)
            })
            .collect();
        self.noop_index = last.next();
        storage.put(
            self.noop_index,
            Entry {
                term: self.term,
                payload: Payload::Noop,
            },
        );

        out.push(Output::CancelTimer {
            timer: Timer::Election,
        });
        self.broadcast(storage, out);
        self.advance_commit(storage, out);
    }
}
