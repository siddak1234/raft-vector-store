//! Log replication (Raft §5.3).

use consensus::{Entry, LogIndex, NodeId, Output, Role, Storage, Term, Timer};

use crate::{Flow, Message, Outputs, Raft, term_at};

impl Raft {
    /// Starts a heartbeat round: sends every follower an `AppendEntries`, with entries if it
    /// is missing any, and sets the next heartbeat.
    pub(crate) fn broadcast<S: Storage>(&mut self, storage: &mut S, out: &mut Outputs) {
        self.round += 1;
        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            self.send_append(storage, peer, true, out);
        }
        out.push(Output::SetTimer {
            timer: Timer::Heartbeat,
            after: self.config.heartbeat_interval,
        });
    }

    /// Sends new entries to every follower that can take them now, without waiting for the
    /// next heartbeat.
    pub(crate) fn replicate<S: Storage>(&mut self, storage: &mut S, out: &mut Outputs) {
        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            self.send_append(storage, peer, false, out);
        }
    }

    /// Sends `peer` an `AppendEntries` that starts at its `next_index`, carrying up to
    /// `max_entries_per_message` entries.
    ///
    /// - While probing, at most one message is unanswered. A heartbeat resends the probe,
    ///   which repairs a lost message.
    /// - While replicating, `next_index` advances past the entries sent, and no more than
    ///   `max_entries_in_flight` entries are unacknowledged.
    ///
    /// When there are no entries to send, a message goes out only if `heartbeat` is set.
    pub(crate) fn send_append<S: Storage>(
        &mut self,
        storage: &mut S,
        peer: NodeId,
        heartbeat: bool,
        out: &mut Outputs,
    ) {
        let last = storage.last_index();
        let progress = self
            .followers
            .get_mut(&peer)
            .expect("the leader tracks every peer");
        let available = (last.0 + 1).saturating_sub(progress.next_index.0);
        let batch = self.config.max_entries_per_message as u64;
        let count = match progress.flow {
            Flow::Probe { waiting } => {
                if waiting && !heartbeat {
                    return;
                }
                progress.flow = Flow::Probe { waiting: true };
                available.min(batch)
            }
            Flow::Replicate => {
                let in_flight = progress
                    .next_index
                    .0
                    .saturating_sub(progress.match_index.0 + 1);
                let room = (self.config.max_entries_in_flight as u64).saturating_sub(in_flight);
                let count = available.min(batch).min(room);
                if count == 0 && !heartbeat {
                    return;
                }
                count
            }
        };

        let prev_log_index = LogIndex(progress.next_index.0 - 1);
        let prev_log_term = term_at(storage, prev_log_index).expect("a Raft log has no gaps");
        let entries: Vec<Entry> = (0..count)
            .map(|k| {
                let index = LogIndex(progress.next_index.0 + k);
                storage
                    .entry(index)
                    .expect("a Raft log has no gaps")
                    .clone()
            })
            .collect();
        if progress.flow == Flow::Replicate {
            progress.next_index = LogIndex(progress.next_index.0 + count);
        }

        out.push(Output::Send {
            to: peer,
            message: Message::AppendEntries {
                term: self.term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
                round: self.round,
            },
        });
    }

    /// Handles `AppendEntries` (Figure 2, receiver implementation).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn handle_append_entries<S: Storage>(
        &mut self,
        storage: &mut S,
        leader: NodeId,
        term: Term,
        (prev_log_index, prev_log_term): (LogIndex, Term),
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        round: u64,
        out: &mut Outputs,
    ) {
        let reply = |success: bool, index: LogIndex, term: Term| Output::Send {
            to: leader,
            message: Message::AppendEntriesReply {
                term,
                success,
                prev_log_index,
                index,
                round,
            },
        };

        // 1. Reply false if the leader's term is stale.
        if term < self.term {
            out.push(reply(false, storage.last_index(), self.term));
            return;
        }
        // The sender is the leader of this term.
        self.become_follower(Some(leader), out);
        self.reset_election_timer(out);

        // 2. Reply false if the log has no entry at `prev_log_index` with `prev_log_term`.
        if term_at(storage, prev_log_index) != Some(prev_log_term) {
            out.push(reply(false, storage.last_index(), self.term));
            return;
        }

        // 3 and 4. Delete conflicting entries and everything after them, then append the
        // entries not already in the log. Entries that already match are left alone, so a
        // delayed, shorter message never removes entries a newer message added.
        let mut index = prev_log_index;
        for entry in entries {
            index = index.next();
            match storage.entry(index) {
                Some(existing) if existing.term == entry.term => {}
                Some(_) => {
                    storage.truncate(index);
                    storage.put(index, entry);
                }
                None => storage.put(index, entry),
            }
        }

        // 5. Advance the commit index, but not past the last entry this message covers.
        if leader_commit > self.commit_index {
            self.commit_index = self.commit_index.max(leader_commit.min(index));
            self.apply(storage, out);
        }
        out.push(reply(true, index, self.term));
    }

    /// Handles a follower's answer to `AppendEntries` (Figure 2, "Leaders").
    ///
    /// `rejected` is the `prev_log_index` of the message answered. `index` is, on success,
    /// the last index the follower shares with the leader and, on failure, the follower's
    /// last log index.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn handle_append_reply<S: Storage>(
        &mut self,
        storage: &mut S,
        follower: NodeId,
        term: Term,
        success: bool,
        (rejected, index): (LogIndex, LogIndex),
        round: u64,
        out: &mut Outputs,
    ) {
        if self.role != Role::Leader || term != self.term {
            return;
        }
        let Some(progress) = self.followers.get_mut(&follower) else {
            return;
        };
        // Any reply in the current term shows the follower still accepts this leader.
        progress.acked_round = progress.acked_round.max(round);

        if success {
            progress.match_index = progress.match_index.max(index);
            if progress.flow != Flow::Replicate || progress.next_index <= progress.match_index {
                progress.next_index = progress.match_index.next();
            }
            progress.flow = Flow::Replicate;
            self.advance_commit(storage, out);
            self.send_append(storage, follower, false, out);
        } else {
            // A rejection of a message sent before the leader last changed `next_index` is
            // stale: it says nothing about the current position, so it is ignored.
            let stale = match progress.flow {
                Flow::Replicate => rejected <= progress.match_index,
                Flow::Probe { .. } => rejected.next() != progress.next_index,
            };
            if !stale {
                progress.next_index = match progress.flow {
                    // Entries after `match_index` were lost; resend from there.
                    Flow::Replicate => progress.match_index.next(),
                    // Back up one entry (Figure 2), or to the end of a shorter follower log.
                    Flow::Probe { .. } => {
                        rejected.min(index.next()).max(progress.match_index.next())
                    }
                };
                progress.flow = Flow::Probe { waiting: false };
                self.send_append(storage, follower, false, out);
            }
        }
        self.confirm_reads(out);
    }
}
