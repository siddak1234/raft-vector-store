//! File-backed storage with fsync (`docs/PLAN.md` §8, Phase 3).
//!
//! Every write is appended to a write-ahead log, and [`Storage::sync`] writes the appended
//! records and calls `fsync(2)` (`docs/RESEARCH.md` amendment A6). On start, the log is
//! replayed. A record cut short by a crash during a write is ignored,
//! which is safe because the driver never acts on a write before `sync` returns.
//!
//! The wrapper also measures, in shared code, the log gaps (metric M15), the entries
//! discarded (metric M17), and the syncs and bytes written (`docs/RESEARCH.md` amendment
//! A8), as the simulator's storage wrapper does.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use consensus::{Entry, HardState, LogIndex, NodeId, Storage, Term};
use serde::{Deserialize, Serialize};

/// One write, as recorded in the log file.
#[derive(Debug, Serialize, Deserialize)]
enum Record {
    HardState { term: u64, voted_for: Option<u64> },
    Put { index: u64, entry: Entry },
    Truncate { from: u64 },
}

/// How [`Storage::sync`] makes writes durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// `File::sync_data`, which on macOS issues `F_FULLFSYNC` and flushes the drive's cache.
    Full,
    /// `fsync(2)`: the data reaches the drive, which may keep it in its cache. Enough for a
    /// process crash, the only fault the experiments inject.
    Plain,
}

impl FsyncMode {
    /// The mode named by the `RVS_FSYNC` environment variable (`full` or `plain`); `plain`
    /// if it is unset. Experiments use `plain` (`docs/RESEARCH.md` amendment A6); `full`
    /// would also survive a power failure.
    pub fn from_env() -> FsyncMode {
        match std::env::var("RVS_FSYNC").as_deref() {
            Ok("full") => FsyncMode::Full,
            _ => FsyncMode::Plain,
        }
    }
}

/// Durable storage in one append-only file.
#[derive(Debug)]
pub struct FileStorage {
    file: File,
    mode: FsyncMode,
    hard: HardState,
    log: BTreeMap<LogIndex, Entry>,
    /// Encoded records not yet written to the file.
    pending: Vec<u8>,
    discarded: u64,
    max_gaps: u64,
    syncs: u64,
    bytes_written: u64,
}

impl FileStorage {
    /// Opens the storage in `dir`, creating it if needed, and replays what was synced.
    ///
    /// # Errors
    ///
    /// Returns any I/O error from opening or reading the file.
    pub fn open(dir: &Path) -> io::Result<FileStorage> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("log");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut storage = FileStorage {
            file,
            mode: FsyncMode::from_env(),
            hard: HardState::default(),
            log: BTreeMap::new(),
            pending: Vec::new(),
            discarded: 0,
            max_gaps: 0,
            syncs: 0,
            bytes_written: 0,
        };
        let mut rest = bytes.as_slice();
        while rest.len() >= 4 {
            let length = u32::from_le_bytes(rest[..4].try_into().expect("four bytes")) as usize;
            let Some(body) = rest.get(4..4 + length) else {
                break;
            };
            let Ok(record) = postcard::from_bytes::<Record>(body) else {
                break;
            };
            storage.replay(record);
            rest = &rest[4 + length..];
        }
        Ok(storage)
    }

    fn replay(&mut self, record: Record) {
        match record {
            Record::HardState { term, voted_for } => {
                self.hard = HardState {
                    term: Term(term),
                    voted_for: voted_for.map(NodeId),
                };
            }
            Record::Put { index, entry } => {
                self.log.insert(LogIndex(index), entry);
            }
            Record::Truncate { from } => {
                self.log.split_off(&LogIndex(from));
            }
        }
    }

    fn record(&mut self, record: &Record) {
        let body = postcard::to_allocvec(record).expect("in-memory encoding cannot fail");
        self.pending
            .extend_from_slice(&(body.len() as u32).to_le_bytes());
        self.pending.extend_from_slice(&body);
    }

    /// Entries overwritten with a different payload or removed by truncation (metric M17).
    pub fn discarded(&self) -> u64 {
        self.discarded
    }

    /// The largest number of gaps the log has had (metric M15).
    pub fn max_gaps(&self) -> u64 {
        self.max_gaps
    }

    /// Syncs that wrote something, since the storage was opened.
    pub fn syncs(&self) -> u64 {
        self.syncs
    }

    /// Bytes appended to the log file, since the storage was opened.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    fn note_gaps(&mut self) {
        let last = self.last_index().0;
        let gaps = last - self.log.len() as u64;
        self.max_gaps = self.max_gaps.max(gaps);
    }
}

impl Storage for FileStorage {
    fn hard_state(&self) -> HardState {
        self.hard
    }

    fn set_hard_state(&mut self, state: HardState) {
        self.hard = state;
        self.record(&Record::HardState {
            term: state.term.0,
            voted_for: state.voted_for.map(|n| n.0),
        });
    }

    fn entry(&self, index: LogIndex) -> Option<&Entry> {
        self.log.get(&index)
    }

    fn last_index(&self) -> LogIndex {
        self.log
            .keys()
            .next_back()
            .copied()
            .unwrap_or(LogIndex::ZERO)
    }

    fn put(&mut self, index: LogIndex, entry: Entry) {
        assert!(index > LogIndex::ZERO, "log indexes start at 1");
        if self
            .log
            .get(&index)
            .is_some_and(|old| old.payload != entry.payload)
        {
            self.discarded += 1;
        }
        self.record(&Record::Put {
            index: index.0,
            entry: entry.clone(),
        });
        self.log.insert(index, entry);
        self.note_gaps();
    }

    fn truncate(&mut self, from: LogIndex) {
        let removed = self.log.split_off(&from);
        self.discarded += removed.len() as u64;
        self.record(&Record::Truncate { from: from.0 });
    }

    fn sync(&mut self) -> bool {
        if self.pending.is_empty() {
            return false;
        }
        let result = self
            .file
            .write_all(&self.pending)
            .and_then(|()| match self.mode {
                FsyncMode::Full => self.file.sync_data(),
                FsyncMode::Plain => rustix::fs::fsync(&self.file).map_err(io::Error::from),
            });
        // The driver contract: storage that cannot write must panic, and the server then
        // restarts from what was synced.
        result.expect("cannot write the storage file");
        self.syncs += 1;
        self.bytes_written += self.pending.len() as u64;
        self.pending.clear();
        true
    }
}

#[cfg(test)]
mod tests {
    use consensus::{Command, Entry, HardState, LogIndex, NodeId, Payload, Storage, Term};

    use super::FileStorage;

    fn entry(term: u64, byte: u8) -> Entry {
        Entry {
            term: Term(term),
            payload: Payload::Command(Command(vec![byte])),
        }
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rvs-storage-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn synced_writes_survive_a_restart_and_unsynced_ones_do_not() {
        let dir = temp_dir("restart");
        {
            let mut storage = FileStorage::open(&dir).expect("open");
            storage.set_hard_state(HardState {
                term: Term(3),
                voted_for: Some(NodeId(2)),
            });
            storage.put(LogIndex(1), entry(1, 1));
            storage.put(LogIndex(3), entry(3, 3));
            storage.put(LogIndex(2), entry(2, 2));
            storage.truncate(LogIndex(3));
            assert!(storage.sync());
            assert!(!storage.sync());
            storage.put(LogIndex(4), entry(4, 4));
            // Dropped without a sync, as in a crash.
        }
        let storage = FileStorage::open(&dir).expect("reopen");
        assert_eq!(storage.hard_state().term, Term(3));
        assert_eq!(storage.hard_state().voted_for, Some(NodeId(2)));
        assert_eq!(storage.entry(LogIndex(2)), Some(&entry(2, 2)));
        assert_eq!(storage.last_index(), LogIndex(2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn counts_syncs_and_the_bytes_they_write() {
        let dir = temp_dir("syncs");
        let mut storage = FileStorage::open(&dir).expect("open");
        storage.put(LogIndex(1), entry(1, 1));
        storage.set_hard_state(HardState {
            term: Term(2),
            voted_for: None,
        });
        assert!(storage.sync());
        assert!(!storage.sync());
        assert_eq!(storage.syncs(), 1);
        let on_disk = std::fs::metadata(dir.join("log")).expect("stat").len();
        assert_eq!(storage.bytes_written(), on_disk);
        // The simulator sizes the same records as tuples of the variant's index and its
        // fields (`sim::storage`), which postcard encodes the same way.
        let put = postcard::to_allocvec(&(1u32, 1u64, entry(1, 1))).expect("encode");
        let hard = postcard::to_allocvec(&(0u32, 2u64, None::<u64>)).expect("encode");
        assert_eq!(on_disk, (4 + put.len() + 4 + hard.len()) as u64);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_torn_record_at_the_end_is_ignored() {
        let dir = temp_dir("torn");
        {
            let mut storage = FileStorage::open(&dir).expect("open");
            storage.put(LogIndex(1), entry(1, 1));
            storage.sync();
        }
        let mut bytes = std::fs::read(dir.join("log")).expect("read");
        bytes.extend_from_slice(&[200, 0, 0, 0, 1, 2]);
        std::fs::write(dir.join("log"), bytes).expect("write");
        let storage = FileStorage::open(&dir).expect("reopen");
        assert_eq!(storage.last_index(), LogIndex(1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn counts_gaps_and_discards() {
        let dir = temp_dir("counts");
        let mut storage = FileStorage::open(&dir).expect("open");
        storage.put(LogIndex(1), entry(1, 1));
        storage.put(LogIndex(4), entry(1, 4));
        assert_eq!(storage.max_gaps(), 2);
        storage.put(LogIndex(4), entry(2, 9));
        storage.truncate(LogIndex(1));
        assert_eq!(storage.discarded(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
