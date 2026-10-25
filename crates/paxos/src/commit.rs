//! Choosing and applying entries (spec §5, "Leaders" rule 3.2, "Followers" rules 3.4 and
//! 4.3, all servers rule 2) and leadership confirmation ("Leaders" rule 4).

use consensus::{LogIndex, NotLeader, Output, RequestId, Role, Storage, Term};

use crate::{Outputs, Paxos, PendingRead};

impl Paxos {
    /// Advances the leader's commit index while the next index has been accepted by a
    /// majority in the current term, which makes it chosen (spec §6, "Chosen").
    pub(crate) fn advance_commit<S: Storage>(&mut self, storage: &mut S, out: &mut Outputs) {
        if self.role != Role::Leader {
            return;
        }
        let quorum = self.quorum();
        let before = self.commit_index;
        while self
            .accepted
            .get(&self.commit_index.next())
            .is_some_and(|servers| servers.len() >= quorum)
        {
            self.commit_index = self.commit_index.next();
        }
        if self.commit_index > before {
            // Chosen indexes no longer need their acceptance sets.
            self.accepted = self.accepted.split_off(&self.commit_index.next());
            self.apply(storage, out);
            self.confirm_reads(out);
        }
    }

    /// Advances a follower's commit index over entries it holds in the leader's term, up to
    /// the leader's commit index (spec §5, "Followers" rule 3.4). Such an entry is above the
    /// leader's base index, so the leader's commit index passing it means it is chosen.
    pub(crate) fn follower_commit<S: Storage>(
        &mut self,
        storage: &mut S,
        term: Term,
        leader_commit: LogIndex,
        out: &mut Outputs,
    ) {
        while self.commit_index < leader_commit
            && storage
                .entry(self.commit_index.next())
                .is_some_and(|entry| entry.term == term)
        {
            self.commit_index = self.commit_index.next();
        }
        self.apply(storage, out);
    }

    /// Passes newly committed entries to the state machine, in index order.
    pub(crate) fn apply<S: Storage>(&mut self, storage: &S, out: &mut Outputs) {
        while self.last_applied < self.commit_index {
            self.last_applied = self.last_applied.next();
            let entry = storage
                .entry(self.last_applied)
                .expect("committed entries are stored");
            out.push(Output::Apply {
                index: self.last_applied,
                payload: entry.payload.clone(),
            });
        }
    }

    /// Starts a leadership confirmation: sends a round of heartbeats now and confirms once
    /// a majority answers it and this leader's no-op is committed (spec §5, "Leaders" rule
    /// 4).
    pub(crate) fn request_confirmation<S: Storage>(
        &mut self,
        storage: &mut S,
        request: RequestId,
    ) -> Result<Outputs, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader,
            });
        }
        let mut out = Vec::new();
        self.broadcast(storage, &mut out);
        let read_index = (self.commit_index >= self.noop_index).then_some(self.commit_index);
        self.reads.push(PendingRead {
            request,
            round: self.round,
            read_index,
        });
        self.confirm_reads(&mut out);
        Ok(out)
    }

    /// Answers every confirmation whose heartbeat round a majority has acknowledged.
    pub(crate) fn confirm_reads(&mut self, out: &mut Outputs) {
        if self.reads.is_empty() {
            return;
        }
        let quorum = self.quorum();
        let commit = self.commit_index;
        let noop_committed = commit >= self.noop_index;
        let followers = &self.followers;
        self.reads.retain_mut(|read| {
            if read.read_index.is_none() && noop_committed {
                read.read_index = Some(commit);
            }
            let acks = 1 + followers
                .values()
                .filter(|p| p.acked_round >= read.round)
                .count();
            match read.read_index {
                Some(read_index) if acks >= quorum => {
                    out.push(Output::LeadershipConfirmed {
                        request: read.request,
                        read_index,
                    });
                    false
                }
                _ => true,
            }
        });
    }

    /// Fails every pending confirmation, because this server stopped leading.
    pub(crate) fn fail_reads(&mut self, out: &mut Outputs) {
        for read in self.reads.drain(..) {
            out.push(Output::LeadershipLost {
                request: read.request,
            });
        }
    }
}
