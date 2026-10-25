//! Log replication: `Accept`, `Accepted`, and `Commit` (spec §5, "Followers" rules 3 and
//! 4, and "Leaders" rules 2 and 3).

use consensus::{Entry, LogIndex, NodeId, Output, Role, Storage, Term, Timer};

use crate::{Message, Outputs, Paxos};

impl Paxos {
    /// Starts a heartbeat round: sends every follower an `Accept` with the oldest entries it
    /// has not accepted, or an empty one, and sets the next heartbeat (spec §5, "Leaders"
    /// rule 2).
    pub(crate) fn broadcast<S: Storage>(&mut self, storage: &mut S, out: &mut Outputs) {
        self.round += 1;
        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            if let Some(progress) = self.followers.get_mut(&peer) {
                // A `Commit` still unanswered after a heartbeat interval may be resent.
                progress.commit_waiting = None;
            }
            self.send_accept(storage, peer, true, out);
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
            self.send_accept(storage, peer, false, out);
        }
    }

    /// Sends `peer` `Accept` messages, each carrying up to `max_entries_per_message` entries.
    ///
    /// - A heartbeat sends one, starting at the oldest entry the follower has not accepted,
    ///   so a lost message is repaired. With nothing to send, the `Accept` is empty.
    /// - Otherwise they start at the first entry never sent and go out until
    ///   `max_entries_in_flight` entries are unacknowledged or none are left to send. With
    ///   nothing to send, no message goes out.
    fn send_accept<S: Storage>(
        &mut self,
        storage: &mut S,
        peer: NodeId,
        heartbeat: bool,
        out: &mut Outputs,
    ) {
        while self.send_one_accept(storage, peer, heartbeat, out) {}
    }

    /// Sends `peer` one `Accept` as [`Paxos::send_accept`] describes. Returns whether it sent
    /// new entries, in which case more may follow.
    fn send_one_accept<S: Storage>(
        &mut self,
        storage: &mut S,
        peer: NodeId,
        heartbeat: bool,
        out: &mut Outputs,
    ) -> bool {
        let last = storage.last_index();
        let batch = self.config.max_entries_per_message as u64;
        let limit = self.config.max_entries_in_flight as u64;
        let progress = self
            .followers
            .get_mut(&peer)
            .expect("the leader tracks every peer");
        let (first, count) = if heartbeat {
            let first = progress.ack_floor;
            (first, (last.0 + 1).saturating_sub(first.0).min(batch))
        } else {
            let first = progress.next_index;
            let in_flight = first.0.saturating_sub(progress.ack_floor.0);
            let room = limit.saturating_sub(in_flight);
            let count = (last.0 + 1).saturating_sub(first.0).min(batch).min(room);
            if count == 0 {
                return false;
            }
            (first, count)
        };
        progress.next_index = progress.next_index.max(LogIndex(first.0 + count));

        let entries: Vec<Entry> = (0..count)
            .map(|k| {
                storage
                    .entry(LogIndex(first.0 + k))
                    .expect("a leader stores every index above its base index")
                    .clone()
            })
            .collect();
        out.push(Output::Send {
            to: peer,
            message: Message::Accept {
                term: self.term,
                first_index: first,
                entries,
                commit_index: self.commit_index,
                round: self.round,
            },
        });
        !heartbeat
    }

    /// Handles `Accept` (spec §5, "Followers" rule 3). Each index is written on its own,
    /// replacing any entry already there; there is no consistency check, so the log may
    /// have gaps.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn handle_accept<S: Storage>(
        &mut self,
        storage: &mut S,
        leader: NodeId,
        term: Term,
        first: LogIndex,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        round: u64,
        out: &mut Outputs,
    ) {
        if term < self.term {
            self.reply(
                leader,
                false,
                (first, LogIndex(first.0.saturating_sub(1))),
                round,
                out,
            );
            return;
        }
        // The sender is the leader of this term.
        self.become_follower(Some(leader), out);
        self.reset_election_timer(out);

        let count = entries.len() as u64;
        for (k, entry) in entries.into_iter().enumerate() {
            storage.put(LogIndex(first.0 + k as u64), entry);
        }
        self.follower_commit(storage, term, leader_commit, out);
        let last = LogIndex((first.0 + count).saturating_sub(1));
        self.reply(leader, true, (first, last), round, out);
    }

    /// Handles a follower's answer to `Accept` or `Commit` (spec §5, "Leaders" rule 3).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn handle_accepted<S: Storage>(
        &mut self,
        storage: &mut S,
        follower: NodeId,
        term: Term,
        success: bool,
        (first, last): (LogIndex, LogIndex),
        follower_commit: LogIndex,
        round: u64,
        out: &mut Outputs,
    ) {
        if self.role != Role::Leader || term != self.term || !success {
            return;
        }
        let commit = self.commit_index;
        let Some(progress) = self.followers.get_mut(&follower) else {
            return;
        };
        progress.acked_round = progress.acked_round.max(round);
        progress.commit_index = follower_commit;
        if progress
            .commit_waiting
            .is_some_and(|waiting| follower_commit >= waiting)
        {
            progress.commit_waiting = None;
        }
        for i in first.0..=last.0 {
            let index = LogIndex(i);
            if index >= progress.ack_floor {
                progress.acked.insert(index);
            }
            if index > commit {
                self.accepted.entry(index).or_default().insert(follower);
            }
        }
        while progress.acked.remove(&progress.ack_floor) {
            progress.ack_floor = progress.ack_floor.next();
        }

        self.advance_commit(storage, out);
        self.send_commit(storage, follower, out);
        self.send_accept(storage, follower, false, out);
        self.confirm_reads(out);
    }

    /// Sends chosen entries to a follower whose commit index is behind and which has not
    /// accepted the next index in this term, so cannot learn it from an `Accept` (spec §5,
    /// "Leaders" rule 3.3). At most one `Commit` per follower is unanswered at a time; a
    /// heartbeat allows a resend.
    fn send_commit<S: Storage>(&mut self, storage: &mut S, follower: NodeId, out: &mut Outputs) {
        let batch = self.config.max_entries_per_message as u64;
        let Some(progress) = self.followers.get_mut(&follower) else {
            return;
        };
        if progress.commit_index >= self.commit_index || progress.commit_waiting.is_some() {
            return;
        }
        let first = progress.commit_index.next();
        if first > self.base_index && progress.has_accepted(first) {
            return;
        }
        let last = self.commit_index.0.min(first.0 + batch - 1);
        let entries: Vec<Entry> = (first.0..=last)
            .map(|i| {
                storage
                    .entry(LogIndex(i))
                    .expect("committed entries are stored")
                    .clone()
            })
            .collect();
        progress.commit_waiting = Some(first);
        out.push(Output::Send {
            to: follower,
            message: Message::Commit {
                term: self.term,
                first_index: first,
                entries,
                commit_index: self.commit_index,
            },
        });
    }

    /// Handles `Commit` (spec §5, "Followers" rule 4): installs chosen entries above the
    /// commit index.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn handle_commit<S: Storage>(
        &mut self,
        storage: &mut S,
        leader: NodeId,
        term: Term,
        first: LogIndex,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        out: &mut Outputs,
    ) {
        if term < self.term {
            let empty = (self.commit_index.next(), self.commit_index);
            self.reply(leader, false, empty, 0, out);
            return;
        }
        self.become_follower(Some(leader), out);
        self.reset_election_timer(out);

        let count = entries.len() as u64;
        for (k, entry) in entries.into_iter().enumerate() {
            let index = LogIndex(first.0 + k as u64);
            if index > self.commit_index {
                storage.put(index, entry);
            }
        }
        if count > 0 && first <= self.commit_index.next() {
            self.commit_index = self.commit_index.max(LogIndex(first.0 + count - 1));
        }
        self.follower_commit(storage, term, leader_commit, out);
        let empty = (self.commit_index.next(), self.commit_index);
        self.reply(leader, true, empty, 0, out);
    }

    /// Sends an `Accepted` to the leader.
    fn reply(
        &self,
        leader: NodeId,
        success: bool,
        (first_index, last_index): (LogIndex, LogIndex),
        round: u64,
        out: &mut Outputs,
    ) {
        out.push(Output::Send {
            to: leader,
            message: Message::Accepted {
                term: self.term,
                success,
                first_index,
                last_index,
                commit_index: self.commit_index,
                round,
            },
        });
    }
}
