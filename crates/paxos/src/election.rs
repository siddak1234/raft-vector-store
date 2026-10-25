//! Leader election: term ownership, `Prepare` and `Promise`, and becoming leader (spec §3,
//! "Term ownership", and §5, "Followers" rule 2, "Candidates", and "Becoming leader").

use std::collections::{BTreeMap, BTreeSet};

use consensus::{Entry, LogIndex, NodeId, Output, Payload, Role, Storage, Term, Timer};

use crate::{Message, Outputs, Paxos, Progress};

impl Paxos {
    /// The smallest term above `currentTerm` that this server owns (`nextOwnTerm`): terms
    /// `t` with `t mod N = rank`.
    fn next_own_term(&self) -> Term {
        let next = self.term.0 + 1;
        let offset = (self.rank + self.servers - next % self.servers) % self.servers;
        Term(next + offset)
    }

    /// Starts an election in the next term this server owns (spec §5, "Candidates" rule
    /// 1): records its own promise and asks every other server to promise the term.
    pub(crate) fn start_election<S: Storage>(&mut self, storage: &mut S, out: &mut Outputs) {
        self.term = self.next_own_term();
        self.persist_term(storage);
        self.role = Role::Candidate;
        self.leader = None;
        let own = entries_above(storage, self.commit_index);
        self.promises = BTreeMap::from([(self.id, own)]);
        self.reset_election_timer(out);

        if self.promises.len() >= self.quorum() {
            // A one-server cluster wins with its own promise.
            self.become_leader(storage, out);
            return;
        }
        for &peer in &self.peers {
            out.push(Output::Send {
                to: peer,
                message: Message::Prepare {
                    term: self.term,
                    commit_index: self.commit_index,
                },
            });
        }
    }

    /// Answers a `Prepare` (spec §5, "Followers" rule 2).
    ///
    /// A higher term has already been adopted by the time this runs, so the promise is
    /// granted exactly when the candidate's term equals `currentTerm`. A granted promise
    /// carries every entry above the candidate's commit index.
    pub(crate) fn handle_prepare<S: Storage>(
        &mut self,
        storage: &mut S,
        candidate: NodeId,
        term: Term,
        commit_index: LogIndex,
        out: &mut Outputs,
    ) {
        let granted = term == self.term;
        let entries = if granted {
            self.reset_election_timer(out);
            entries_above(storage, commit_index)
        } else {
            Vec::new()
        };
        out.push(Output::Send {
            to: candidate,
            message: Message::Promise {
                term: self.term,
                granted,
                entries,
            },
        });
    }

    /// Records a promise; with a majority, becomes leader (spec §5, "Candidates" rules 2
    /// and 3).
    pub(crate) fn handle_promise<S: Storage>(
        &mut self,
        storage: &mut S,
        voter: NodeId,
        term: Term,
        granted: bool,
        entries: Vec<(LogIndex, Entry)>,
        out: &mut Outputs,
    ) {
        if self.role != Role::Candidate || term != self.term || !granted {
            return;
        }
        self.promises.entry(voter).or_insert(entries);
        if self.promises.len() >= self.quorum() {
            self.become_leader(storage, out);
        }
    }

    /// Takes over as leader of the current term (spec §5, "Becoming leader").
    ///
    /// For every index above the commit index, the new leader keeps the entry with the
    /// highest term among the promises, or a no-op if no promise holds one, and re-proposes
    /// it in its own term. It then appends a no-op, as Raft does (§8).
    fn become_leader<S: Storage>(&mut self, storage: &mut S, out: &mut Outputs) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        let promises = std::mem::take(&mut self.promises);

        // 1. The highest index in any promise, or the commit index.
        let highest = promises
            .values()
            .flatten()
            .map(|(index, _)| *index)
            .max()
            .unwrap_or(LogIndex::ZERO)
            .max(self.commit_index);

        // 2. For each index, the entry with the highest term among the promises.
        let mut chosen: BTreeMap<LogIndex, Entry> = BTreeMap::new();
        for (index, entry) in promises.values().flatten() {
            if *index <= self.commit_index {
                continue;
            }
            match chosen.get(index) {
                Some(best) if best.term >= entry.term => {}
                _ => {
                    chosen.insert(*index, entry.clone());
                }
            }
        }

        // 3. Re-propose each one in the current term; indexes no promise holds get no-ops.
        for i in self.commit_index.0 + 1..=highest.0 {
            let index = LogIndex(i);
            let payload = chosen
                .remove(&index)
                .map_or(Payload::Noop, |entry| entry.payload);
            storage.put(
                index,
                Entry {
                    term: self.term,
                    payload,
                },
            );
        }

        // 4. Append a no-op.
        self.noop_index = highest.next();
        storage.put(
            self.noop_index,
            Entry {
                term: self.term,
                payload: Payload::Noop,
            },
        );

        // 5. The leader has accepted every index it proposes.
        self.base_index = self.commit_index;
        self.accepted = (self.base_index.0 + 1..=self.noop_index.0)
            .map(|i| (LogIndex(i), BTreeSet::from([self.id])))
            .collect();
        self.followers = self
            .peers
            .iter()
            .map(|&peer| (peer, Progress::new(self.base_index)))
            .collect();

        // 6. Stop campaigning and send the proposals.
        out.push(Output::CancelTimer {
            timer: Timer::Election,
        });
        self.broadcast(storage, out);
        self.advance_commit(storage, out);
    }
}

/// Every entry above `index`, with its index.
fn entries_above<S: Storage>(storage: &S, index: LogIndex) -> Vec<(LogIndex, Entry)> {
    (index.0 + 1..=storage.last_index().0)
        .filter_map(|i| {
            storage
                .entry(LogIndex(i))
                .map(|entry| (LogIndex(i), entry.clone()))
        })
        .collect()
}
