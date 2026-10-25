//! A storage wrapper that measures what an algorithm writes.

use consensus::{Entry, HardState, LogIndex, MemStorage, Storage};

/// Wraps [`MemStorage`] and records, in shared code, what the checks and metrics need:
/// the indexes written since the last check, the entries discarded (metric M17), the gaps
/// in the log (metric M15), and the bytes each sync writes (`docs/RESEARCH.md` amendment
/// A8). The algorithm sees an ordinary [`Storage`].
#[derive(Debug, Clone, Default)]
pub(crate) struct TrackedStorage {
    inner: MemStorage,
    written: Vec<LogIndex>,
    discarded: u64,
    /// Bytes of the writes not yet synced, and of the synced ones not yet taken.
    unsynced_bytes: u64,
    synced_bytes: u64,
}

/// The size of one write in the server's write-ahead log (`controlplane::storage`): a
/// 4-byte length, then the record in postcard. A record is an enum variant, which postcard
/// encodes as the variant's index followed by its fields, the same bytes as this tuple.
fn record_bytes(record: &impl serde::Serialize) -> u64 {
    4 + postcard::to_allocvec(record).map_or(0, |encoded| encoded.len() as u64)
}

impl TrackedStorage {
    pub(crate) fn mem(&self) -> &MemStorage {
        &self.inner
    }

    /// Returns the indexes written since the last call.
    pub(crate) fn take_written(&mut self) -> Vec<LogIndex> {
        std::mem::take(&mut self.written)
    }

    /// Entries overwritten with a different payload or removed by truncation.
    pub(crate) fn discarded(&self) -> u64 {
        self.discarded
    }

    /// Indexes below the last index that hold no entry.
    pub(crate) fn gaps(&self) -> u64 {
        self.inner.last_index().0 - self.inner.len() as u64
    }

    /// Bytes written by syncs since the last call.
    pub(crate) fn take_synced_bytes(&mut self) -> u64 {
        std::mem::take(&mut self.synced_bytes)
    }

    /// Simulates a crash: unsynced writes are lost.
    pub(crate) fn crash(&mut self) {
        self.inner.crash();
        self.written.clear();
        self.unsynced_bytes = 0;
    }
}

impl Storage for TrackedStorage {
    fn hard_state(&self) -> HardState {
        self.inner.hard_state()
    }

    fn set_hard_state(&mut self, state: HardState) {
        self.unsynced_bytes += record_bytes(&(0u32, state.term.0, state.voted_for.map(|n| n.0)));
        self.inner.set_hard_state(state);
    }

    fn entry(&self, index: LogIndex) -> Option<&Entry> {
        self.inner.entry(index)
    }

    fn last_index(&self) -> LogIndex {
        self.inner.last_index()
    }

    fn put(&mut self, index: LogIndex, entry: Entry) {
        if self
            .inner
            .entry(index)
            .is_some_and(|old| old.payload != entry.payload)
        {
            self.discarded += 1;
        }
        self.written.push(index);
        self.unsynced_bytes += record_bytes(&(1u32, index.0, &entry));
        self.inner.put(index, entry);
    }

    fn truncate(&mut self, from: LogIndex) {
        let last = self.inner.last_index().0;
        let removed = (from.0.max(1)..=last)
            .filter(|&i| self.inner.entry(LogIndex(i)).is_some())
            .count();
        self.discarded += removed as u64;
        self.unsynced_bytes += record_bytes(&(2u32, from.0));
        self.inner.truncate(from);
    }

    fn sync(&mut self) -> bool {
        let wrote = self.inner.sync();
        if wrote {
            self.synced_bytes += self.unsynced_bytes;
        }
        self.unsynced_bytes = 0;
        wrote
    }
}

#[cfg(test)]
mod tests {
    use consensus::{Command, Entry, LogIndex, Payload, Storage, Term};

    use super::TrackedStorage;

    fn entry(term: u64, byte: u8) -> Entry {
        Entry {
            term: Term(term),
            payload: Payload::Command(Command(vec![byte])),
        }
    }

    #[test]
    fn counts_discards_and_gaps() {
        let mut storage = TrackedStorage::default();
        storage.put(LogIndex(1), entry(1, 1));
        storage.put(LogIndex(4), entry(1, 4));
        assert_eq!(storage.gaps(), 2);

        // Same payload under a new term is not a discard; a different payload is.
        storage.put(LogIndex(1), entry(2, 1));
        storage.put(LogIndex(4), entry(2, 9));
        assert_eq!(storage.discarded(), 1);

        storage.truncate(LogIndex(1));
        assert_eq!(storage.discarded(), 3);
        assert_eq!(
            storage.take_written(),
            vec![LogIndex(1), LogIndex(4), LogIndex(1), LogIndex(4)]
        );
        assert!(storage.take_written().is_empty());
    }

    #[test]
    fn counts_the_bytes_each_sync_writes() {
        let mut storage = TrackedStorage::default();
        storage.put(LogIndex(1), entry(1, 1));
        assert!(storage.sync());
        let one = storage.take_synced_bytes();
        // A 4-byte length, then variant 1, index 1, and the entry.
        let encoded = postcard::to_allocvec(&(1u32, 1u64, entry(1, 1))).expect("encode");
        assert_eq!(one, 4 + encoded.len() as u64);
        assert_eq!(storage.take_synced_bytes(), 0);

        // Writes lost in a crash are never counted.
        storage.put(LogIndex(2), entry(1, 2));
        storage.crash();
        assert!(!storage.sync());
        assert_eq!(storage.take_synced_bytes(), 0);
    }
}
