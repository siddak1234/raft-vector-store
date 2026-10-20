//! Persistent state: the hard state and the log.

use std::collections::BTreeMap;

use crate::{Entry, LogIndex, NodeId, Term};

/// The persistent state other than the log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct HardState {
    /// Raft's `currentTerm`, or the highest term a Multi-Paxos server has promised.
    pub term: Term,
    /// The candidate this server voted for in `term`. Raft only; Multi-Paxos leaves it
    /// `None` because each Multi-Paxos term belongs to a single server.
    pub voted_for: Option<NodeId>,
}

/// Persistent storage for one server.
///
/// A write is visible to reads immediately but survives a crash only after [`sync`]. The
/// driver calls `sync` once after each input, before carrying out any output (see
/// [`Consensus`](crate::Consensus)). The fsync policy is therefore in shared code, and both
/// algorithms persist under the same rule.
///
/// The log may have gaps, because Multi-Paxos accepts entries at any index. Raft never
/// creates gaps.
///
/// An implementation that cannot write must panic; the server then restarts from what was
/// synced, as after any crash.
///
/// [`sync`]: Storage::sync
pub trait Storage {
    /// Returns the hard state.
    fn hard_state(&self) -> HardState;

    /// Replaces the hard state.
    fn set_hard_state(&mut self, state: HardState);

    /// Returns the entry at `index`, or `None` if there is none.
    fn entry(&self, index: LogIndex) -> Option<&Entry>;

    /// Returns the highest index that holds an entry, or [`LogIndex::ZERO`] for an empty
    /// log.
    fn last_index(&self) -> LogIndex;

    /// Writes `entry` at `index`, replacing any entry already there.
    ///
    /// # Panics
    ///
    /// Panics if `index` is [`LogIndex::ZERO`].
    fn put(&mut self, index: LogIndex, entry: Entry);

    /// Removes every entry at `from` or later.
    fn truncate(&mut self, from: LogIndex);

    /// Makes all earlier writes durable. Returns `true` if there was anything to write, so
    /// the driver can count fsyncs.
    fn sync(&mut self) -> bool;
}

/// In-memory storage for the simulator, the model checker, and tests.
///
/// It models a crash faithfully: [`crash`](MemStorage::crash) discards every write made
/// since the last [`sync`](Storage::sync).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct MemStorage {
    /// State including unsynced writes.
    current: State,
    /// State as of the last sync.
    durable: State,
    /// Writes since the last sync, replayed onto `durable` by `sync`.
    pending: Vec<Write>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
struct State {
    hard_state: HardState,
    log: BTreeMap<LogIndex, Entry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Write {
    HardState(HardState),
    Put(LogIndex, Entry),
    Truncate(LogIndex),
}

impl State {
    fn apply(&mut self, write: Write) {
        match write {
            Write::HardState(state) => self.hard_state = state,
            Write::Put(index, entry) => {
                self.log.insert(index, entry);
            }
            Write::Truncate(from) => {
                self.log.split_off(&from);
            }
        }
    }
}

impl MemStorage {
    /// Creates empty storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulates a crash: discards every write since the last sync.
    pub fn crash(&mut self) {
        self.current = self.durable.clone();
        self.pending.clear();
    }

    /// Returns the number of entries in the log. With gaps, this is less than
    /// [`last_index`](Storage::last_index).
    pub fn len(&self) -> usize {
        self.current.log.len()
    }

    /// Returns `true` if the log holds no entries.
    pub fn is_empty(&self) -> bool {
        self.current.log.is_empty()
    }

    fn write(&mut self, write: Write) {
        self.current.apply(write.clone());
        self.pending.push(write);
    }
}

impl Storage for MemStorage {
    fn hard_state(&self) -> HardState {
        self.current.hard_state
    }

    fn set_hard_state(&mut self, state: HardState) {
        self.write(Write::HardState(state));
    }

    fn entry(&self, index: LogIndex) -> Option<&Entry> {
        self.current.log.get(&index)
    }

    fn last_index(&self) -> LogIndex {
        self.current
            .log
            .keys()
            .next_back()
            .copied()
            .unwrap_or(LogIndex::ZERO)
    }

    fn put(&mut self, index: LogIndex, entry: Entry) {
        assert!(index > LogIndex::ZERO, "log indexes start at 1");
        self.write(Write::Put(index, entry));
    }

    fn truncate(&mut self, from: LogIndex) {
        self.write(Write::Truncate(from));
    }

    fn sync(&mut self) -> bool {
        if self.pending.is_empty() {
            return false;
        }
        for write in self.pending.drain(..) {
            self.durable.apply(write);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{HardState, MemStorage, Storage};
    use crate::{Command, Entry, LogIndex, NodeId, Payload, Term};

    fn entry(term: u64, byte: u8) -> Entry {
        Entry {
            term: Term(term),
            payload: Payload::Command(Command(vec![byte])),
        }
    }

    #[test]
    fn empty_storage() {
        let storage = MemStorage::new();
        assert_eq!(storage.hard_state(), HardState::default());
        assert_eq!(storage.last_index(), LogIndex::ZERO);
        assert_eq!(storage.entry(LogIndex(1)), None);
    }

    #[test]
    fn put_overwrite_and_gaps() {
        let mut storage = MemStorage::new();
        storage.put(LogIndex(1), entry(1, 1));
        storage.put(LogIndex(3), entry(1, 3));
        storage.put(LogIndex(1), entry(2, 9));
        assert_eq!(storage.entry(LogIndex(1)), Some(&entry(2, 9)));
        assert_eq!(storage.entry(LogIndex(2)), None);
        assert_eq!(storage.last_index(), LogIndex(3));
    }

    #[test]
    fn truncate_removes_suffix() {
        let mut storage = MemStorage::new();
        for i in 1..=5 {
            storage.put(LogIndex(i), entry(1, 0));
        }
        storage.truncate(LogIndex(3));
        assert_eq!(storage.last_index(), LogIndex(2));
        assert_eq!(storage.entry(LogIndex(3)), None);
    }

    #[test]
    fn sync_reports_whether_anything_was_written() {
        let mut storage = MemStorage::new();
        assert!(!storage.sync());
        storage.put(LogIndex(1), entry(1, 1));
        assert!(storage.sync());
        assert!(!storage.sync());
    }

    #[test]
    fn crash_keeps_only_synced_writes() {
        let mut storage = MemStorage::new();
        let voted = HardState {
            term: Term(2),
            voted_for: Some(NodeId(3)),
        };
        storage.set_hard_state(voted);
        storage.put(LogIndex(1), entry(1, 1));
        storage.sync();

        storage.set_hard_state(HardState {
            term: Term(3),
            voted_for: None,
        });
        storage.put(LogIndex(2), entry(3, 2));
        storage.truncate(LogIndex(1));
        storage.crash();

        assert_eq!(storage.hard_state(), voted);
        assert_eq!(storage.entry(LogIndex(1)), Some(&entry(1, 1)));
        assert_eq!(storage.last_index(), LogIndex(1));
    }
}
