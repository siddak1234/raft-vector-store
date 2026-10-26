//! One replica of one shard: its log, its vectors, and its epoch (`docs/DESIGN.md` §6.1).
//!
//! The vectors are the result of applying the log in offset order. A backup that adopts a
//! new epoch deletes the entries above the new primary's last offset (§6.4) and rebuilds
//! its vectors from what remains.

use std::collections::BTreeMap;

use rpc::{Epoch, LogEntry, Metric, Neighbor, WriteOp};

/// The outcome of storing an entry that arrived from the primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stored {
    /// The replica now holds every offset up to and including this one.
    Through(u64),
    /// The replica is missing offsets from here on, so the entry was not stored.
    Gap(u64),
    /// The replica has a newer epoch than the message.
    Stale(Epoch),
}

/// One replica's state for one shard.
#[derive(Debug, Clone)]
pub struct ShardReplica {
    epoch: Epoch,
    /// Entry at offset `o` is at position `o - 1`.
    log: Vec<LogEntry>,
    vectors: BTreeMap<u64, Vec<f32>>,
    metric: Metric,
}

impl ShardReplica {
    /// An empty replica at `epoch`.
    pub fn new(epoch: Epoch, metric: Metric) -> Self {
        ShardReplica {
            epoch,
            log: Vec::new(),
            vectors: BTreeMap::new(),
            metric,
        }
    }

    /// The replica's epoch.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The last offset held, or 0.
    pub fn last_offset(&self) -> u64 {
        self.log.len() as u64
    }

    /// The entries from `from` (at least 1) up to `count` of them.
    pub fn entries_from(&self, from: u64, count: usize) -> Vec<LogEntry> {
        let start = (from.max(1) - 1) as usize;
        self.log.iter().skip(start).take(count).cloned().collect()
    }

    /// Number of vectors held.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Whether the replica holds no vectors.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// The vector with `id`, if held.
    pub fn vector(&self, id: u64) -> Option<&[f32]> {
        self.vectors.get(&id).map(Vec::as_slice)
    }

    /// Every vector ID held, in order.
    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.vectors.keys().copied()
    }

    /// Appends a new write as the primary, at the next offset. Returns the offset.
    pub fn append(&mut self, op: WriteOp) -> u64 {
        let offset = self.last_offset() + 1;
        self.apply(&op);
        self.log.push(LogEntry {
            epoch: self.epoch,
            offset,
            op,
        });
        offset
    }

    /// Stores an entry sent by the primary of `epoch` (§6.2, step 4): appended if it is the
    /// next offset, acknowledged again if already held, and refused with the first missing
    /// offset otherwise. A message from an older epoch is refused.
    pub fn store(&mut self, epoch: Epoch, entry: LogEntry) -> Stored {
        if epoch < self.epoch {
            return Stored::Stale(self.epoch);
        }
        if epoch > self.epoch {
            // A higher epoch is adopted only through `adopt`, which first aligns the log
            // with the new primary's (§6.4). Until then, ask for the alignment.
            return Stored::Stale(self.epoch);
        }
        let next = self.last_offset() + 1;
        match entry.offset {
            o if o == next => {
                self.apply(&entry.op);
                self.log.push(entry);
                Stored::Through(next)
            }
            o if o < next => Stored::Through(self.last_offset()),
            _ => Stored::Gap(next),
        }
    }

    /// For each epoch in the log, in order, the last offset written in it.
    pub fn epoch_ends(&self) -> Vec<(Epoch, u64)> {
        let mut ends: Vec<(Epoch, u64)> = Vec::new();
        for entry in &self.log {
            match ends.last_mut() {
                Some((epoch, end)) if *epoch == entry.epoch => *end = entry.offset,
                _ => ends.push((entry.epoch, entry.offset)),
            }
        }
        ends
    }

    /// Adopts a new epoch announced by its new primary (§6.4, step 2). The replica keeps
    /// the longest prefix of its log that the new primary also holds: entries up to the new
    /// primary's last offset whose epoch matches the epoch the new primary has at that
    /// offset. Each epoch has one primary, which writes each offset once, so an epoch and an
    /// offset identify an entry. Returns the last offset kept, or the replica's newer epoch
    /// if it already has one.
    pub fn adopt(
        &mut self,
        epoch: Epoch,
        end_offset: u64,
        primary_epochs: &[(Epoch, u64)],
    ) -> Result<u64, Epoch> {
        if epoch < self.epoch {
            return Err(self.epoch);
        }
        let primary_epoch_at = |offset: u64| {
            primary_epochs
                .iter()
                .find(|(_, end)| *end >= offset)
                .map(|(epoch, _)| *epoch)
        };
        let keep = self
            .log
            .iter()
            .take_while(|entry| {
                entry.offset <= end_offset && primary_epoch_at(entry.offset) == Some(entry.epoch)
            })
            .count();
        if keep < self.log.len() {
            self.log.truncate(keep);
            self.rebuild();
        }
        self.epoch = epoch;
        Ok(self.last_offset())
    }

    /// Becomes the primary of `epoch`, keeping its whole log (§6.4).
    pub fn promote(&mut self, epoch: Epoch) {
        self.epoch = self.epoch.max(epoch);
    }

    fn apply(&mut self, op: &WriteOp) {
        match op {
            WriteOp::Upsert { id, vector } => {
                self.vectors.insert(*id, vector.clone());
            }
            WriteOp::Delete { id } => {
                self.vectors.remove(id);
            }
        }
    }

    fn rebuild(&mut self) {
        self.vectors.clear();
        let log = std::mem::take(&mut self.log);
        for entry in &log {
            self.apply(&entry.op);
        }
        self.log = log;
    }

    /// The `k` vectors nearest to `query`, closest first, by brute-force search. Ties are
    /// broken by the lower ID.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<Neighbor> {
        let query_norm = norm(query);
        let mut hits: Vec<Neighbor> = self
            .vectors
            .iter()
            .map(|(&id, vector)| Neighbor {
                id,
                distance: distance(self.metric, query, query_norm, vector),
            })
            .collect();
        let k = k.min(hits.len());
        if k == 0 {
            return Vec::new();
        }
        let order =
            |a: &Neighbor, b: &Neighbor| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id));
        hits.select_nth_unstable_by(k - 1, order);
        hits.truncate(k);
        hits.sort_by(order);
        hits
    }
}

fn norm(vector: &[f32]) -> f32 {
    vector.iter().map(|x| x * x).sum::<f32>().sqrt()
}

/// The distance between `query` and `vector` under `metric`. Angular distance is one minus
/// the cosine similarity, as ANN-Benchmarks defines it.
pub fn distance(metric: Metric, query: &[f32], query_norm: f32, vector: &[f32]) -> f32 {
    match metric {
        Metric::Euclidean => query
            .iter()
            .zip(vector)
            .map(|(a, b)| (a - b) * (a - b))
            .sum::<f32>()
            .sqrt(),
        Metric::Angular => {
            let dot: f32 = query.iter().zip(vector).map(|(a, b)| a * b).sum();
            let denominator = query_norm * norm(vector);
            if denominator == 0.0 {
                1.0
            } else {
                1.0 - dot / denominator
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rpc::{Epoch, LogEntry, Metric, WriteOp};

    use super::{ShardReplica, Stored};

    fn upsert(id: u64, x: f32) -> WriteOp {
        WriteOp::Upsert {
            id,
            vector: vec![x, 0.0],
        }
    }

    fn entry(epoch: u64, offset: u64, op: WriteOp) -> LogEntry {
        LogEntry {
            epoch: Epoch(epoch),
            offset,
            op,
        }
    }

    #[test]
    fn a_backup_stores_in_offset_order() {
        let mut backup = ShardReplica::new(Epoch(1), Metric::Euclidean);
        assert_eq!(
            backup.store(Epoch(1), entry(1, 1, upsert(1, 1.0))),
            Stored::Through(1)
        );
        // A duplicate is acknowledged again; a gap asks for the missing offset.
        assert_eq!(
            backup.store(Epoch(1), entry(1, 1, upsert(1, 1.0))),
            Stored::Through(1)
        );
        assert_eq!(
            backup.store(Epoch(1), entry(1, 3, upsert(3, 3.0))),
            Stored::Gap(2)
        );
        assert_eq!(
            backup.store(Epoch(0), entry(0, 2, upsert(2, 2.0))),
            Stored::Stale(Epoch(1))
        );
        assert_eq!(backup.len(), 1);
    }

    #[test]
    fn adopting_a_new_epoch_discards_writes_the_new_primary_lacks() {
        let mut backup = ShardReplica::new(Epoch(1), Metric::Euclidean);
        for offset in 1..=3 {
            backup.store(Epoch(1), entry(1, offset, upsert(offset, offset as f32)));
        }
        // The new primary has offsets up to 2, so offset 3 was never acknowledged.
        assert_eq!(backup.adopt(Epoch(2), 2, &[(Epoch(1), 2)]), Ok(2));
        assert_eq!(backup.epoch(), Epoch(2));
        assert_eq!(backup.len(), 2);
        assert!(backup.vector(3).is_none());
        assert_eq!(backup.adopt(Epoch(1), 0, &[]), Err(Epoch(2)));
    }

    #[test]
    fn adopting_discards_entries_from_an_epoch_the_new_primary_did_not_follow() {
        // The backup missed epoch 2: it holds offsets 1-5 from epoch 1. The new primary of
        // epoch 3 holds 1-3 from epoch 1 and 4-6 from epoch 2.
        let mut backup = ShardReplica::new(Epoch(1), Metric::Euclidean);
        for offset in 1..=5 {
            backup.store(Epoch(1), entry(1, offset, upsert(offset, 1.0)));
        }
        let epochs = [(Epoch(1), 3), (Epoch(2), 6)];
        // Truncating only above offset 6 would keep the epoch-1 entries at 4 and 5, which
        // the new primary does not have.
        assert_eq!(backup.adopt(Epoch(3), 6, &epochs), Ok(3));
        assert_eq!(backup.len(), 3);
    }

    #[test]
    fn epoch_ends_follow_the_log() {
        let mut replica = ShardReplica::new(Epoch(1), Metric::Euclidean);
        replica.append(upsert(1, 1.0));
        replica.append(upsert(2, 1.0));
        replica.promote(Epoch(4));
        replica.append(upsert(3, 1.0));
        assert_eq!(replica.epoch_ends(), vec![(Epoch(1), 2), (Epoch(4), 3)]);
    }

    #[test]
    fn search_finds_the_nearest_vectors() {
        let mut primary = ShardReplica::new(Epoch(1), Metric::Euclidean);
        for id in 0..100 {
            primary.append(upsert(id, id as f32));
        }
        primary.append(WriteOp::Delete { id: 50 });
        let hits = primary.search(&[50.2, 0.0], 3);
        let ids: Vec<u64> = hits.iter().map(|h| h.id).collect();
        assert_eq!(ids, [51, 49, 52]);
    }

    #[test]
    fn angular_distance_ignores_length() {
        let mut primary = ShardReplica::new(Epoch(1), Metric::Angular);
        primary.append(WriteOp::Upsert {
            id: 1,
            vector: vec![10.0, 0.0],
        });
        primary.append(WriteOp::Upsert {
            id: 2,
            vector: vec![1.0, 1.0],
        });
        let hits = primary.search(&[1.0, 0.1], 2);
        assert_eq!(hits[0].id, 1);
    }
}
