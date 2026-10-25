//! Measurements collected in shared code (`docs/RESEARCH.md` §5).

use std::collections::BTreeMap;
use std::ops::Sub;

use consensus::NodeId;

/// Message totals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Messages sent.
    pub messages: u64,
    /// Bytes sent, in the shared binary encoding. Zero when byte counting is off.
    pub bytes: u64,
    /// Log entries carried by the messages.
    pub entries: u64,
}

impl Counts {
    fn add(&mut self, bytes: u64, entries: u64) {
        self.messages += 1;
        self.bytes += bytes;
        self.entries += entries;
    }
}

impl Sub for Counts {
    type Output = Counts;

    fn sub(self, earlier: Counts) -> Counts {
        Counts {
            messages: self.messages.saturating_sub(earlier.messages),
            bytes: self.bytes.saturating_sub(earlier.bytes),
            entries: self.entries.saturating_sub(earlier.entries),
        }
    }
}

/// Counters for one simulation run.
///
/// To measure an interval, keep a copy of the metrics at its start and call
/// [`Metrics::since`] at its end. Only consensus messages between servers are counted;
/// client traffic is the same for both algorithms and is left out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Metrics {
    /// Every message sent between servers.
    pub sent: Counts,
    /// Messages sent, by type ([`consensus::Consensus::message_kind`]).
    pub by_kind: BTreeMap<&'static str, Counts>,
    /// Messages sent, by sending server.
    pub by_sender: BTreeMap<NodeId, Counts>,
    /// Messages lost: dropped by the network or sent to a stopped server.
    pub dropped: u64,
    /// Messages delivered to a running server.
    pub delivered: u64,
    /// Log entries carried by messages delivered to a candidate or leader (metric M16).
    pub entries_to_leaders: u64,
    /// Storage syncs that wrote something.
    pub fsyncs: u64,
    /// Bytes those syncs wrote, sized as in the server's write-ahead log.
    pub storage_bytes: u64,
    /// Client commands acknowledged to their clients.
    pub client_replies: u64,
}

impl Metrics {
    pub(crate) fn record_send(
        &mut self,
        from: NodeId,
        kind: &'static str,
        bytes: u64,
        entries: u64,
    ) {
        self.sent.add(bytes, entries);
        self.by_kind.entry(kind).or_default().add(bytes, entries);
        self.by_sender.entry(from).or_default().add(bytes, entries);
    }

    /// Returns what was counted after `earlier`, an earlier copy of these metrics.
    pub fn since(&self, earlier: &Metrics) -> Metrics {
        fn diff<K: Ord + Copy>(
            now: &BTreeMap<K, Counts>,
            then: &BTreeMap<K, Counts>,
        ) -> BTreeMap<K, Counts> {
            now.iter()
                .map(|(key, counts)| (*key, *counts - then.get(key).copied().unwrap_or_default()))
                .filter(|(_, counts)| *counts != Counts::default())
                .collect()
        }
        Metrics {
            sent: self.sent - earlier.sent,
            by_kind: diff(&self.by_kind, &earlier.by_kind),
            by_sender: diff(&self.by_sender, &earlier.by_sender),
            dropped: self.dropped.saturating_sub(earlier.dropped),
            delivered: self.delivered.saturating_sub(earlier.delivered),
            entries_to_leaders: self
                .entries_to_leaders
                .saturating_sub(earlier.entries_to_leaders),
            fsyncs: self.fsyncs.saturating_sub(earlier.fsyncs),
            storage_bytes: self.storage_bytes.saturating_sub(earlier.storage_bytes),
            client_replies: self.client_replies.saturating_sub(earlier.client_replies),
        }
    }

    /// Returns the totals for messages sent by `server`.
    pub fn sent_by(&self, server: NodeId) -> Counts {
        self.by_sender.get(&server).copied().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use consensus::NodeId;

    use super::Metrics;

    #[test]
    fn since_subtracts_an_earlier_copy() {
        let mut metrics = Metrics::default();
        metrics.record_send(NodeId(1), "A", 10, 1);
        let earlier = metrics.clone();
        metrics.record_send(NodeId(2), "B", 5, 0);
        metrics.fsyncs += 3;
        metrics.storage_bytes += 40;

        let interval = metrics.since(&earlier);
        assert_eq!(interval.sent.messages, 1);
        assert_eq!(interval.sent.bytes, 5);
        assert_eq!(interval.fsyncs, 3);
        assert_eq!(interval.storage_bytes, 40);
        assert!(!interval.by_kind.contains_key("A"));
        assert_eq!(interval.sent_by(NodeId(2)).messages, 1);
        assert_eq!(interval.sent_by(NodeId(1)).messages, 0);
    }
}
