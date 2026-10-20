//! Committing and applying entries (Raft §5.3, §5.4.2) and leadership confirmation (§8).

use consensus::{LogIndex, NotLeader, Output, RequestId, Role, Storage};

use crate::{Outputs, PendingRead, Raft, term_at};

impl Raft {
    /// Advances the leader's commit index to the highest entry stored on a majority, if
    /// that entry is from the current term (Figure 2, "Leaders").
    ///
    /// An entry from an earlier term is never committed by counting its replicas, because
    /// a later leader could still overwrite it (§5.4.2, Figure 8). It becomes committed
    /// indirectly once an entry from the current term after it is committed.
    pub(crate) fn advance_commit<S: Storage>(&mut self, storage: &mut S, out: &mut Outputs) {
        if self.role != Role::Leader {
            return;
        }
        let mut stored: Vec<LogIndex> = self.followers.values().map(|p| p.match_index).collect();
        stored.push(storage.last_index());
        stored.sort_unstable_by(|a, b| b.cmp(a));
        // The highest index that at least a majority of servers store.
        let majority = stored[self.quorum() - 1];
        if majority > self.commit_index && term_at(storage, majority) == Some(self.term) {
            self.commit_index = majority;
            self.apply(storage, out);
            self.confirm_reads(out);
        }
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
    /// a majority answers it and this leader's no-op is committed (§8).
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
