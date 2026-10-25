//! gRPC service definitions, generated code, and the vector-store types shared by the
//! control plane, data nodes, and clients: collection, shard, and node IDs, epochs, and the
//! shard map.
//!
//! The gRPC services are defined in `proto/rvs.proto` and compiled at build time. Their
//! complex payloads are the serde types in this crate, encoded with [`encode`] in the
//! shared binary format, so every component reads them the same way.
//!
//! See `docs/PLAN.md` §3 and §4.5, and `docs/DESIGN.md` §8.

use std::collections::{BTreeMap, BTreeSet};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Code generated from `proto/rvs.proto`.
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod proto {
    tonic::include_proto!("rvs");
}

/// Encodes a value in the shared binary format (postcard).
pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("in-memory encoding cannot fail")
}

/// Decodes a value encoded with [`encode`].
///
/// # Errors
///
/// Returns a description of the problem if the bytes are not a valid encoding.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    postcard::from_bytes(bytes).map_err(|err| format!("cannot decode message: {err}"))
}

// ----- Identifiers -----------------------------------------------------------------------

/// Identifies a data node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DataNodeId(pub u64);

/// A shard's epoch. It increases every time the shard's primary changes, and data nodes
/// reject requests that carry an older one (`docs/DESIGN.md` §3).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Epoch(pub u64);

/// Identifies one shard of a collection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardKey {
    /// The collection's name.
    pub collection: String,
    /// The shard's number, from 0.
    pub shard: u32,
}

/// The shard that holds vector `id` in a collection with `shards` shards: a fixed hash of
/// the ID, modulo the shard count (`docs/PLAN.md` §4.5).
pub fn shard_of(id: u64, shards: u32) -> u32 {
    // SplitMix64 finalizer: a fixed, well-mixed hash that does not depend on the process.
    let mut z = id.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    (z % u64::from(shards.max(1))) as u32
}

// ----- The shard map ---------------------------------------------------------------------

/// How a collection measures distance between vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Metric {
    /// Euclidean distance.
    Euclidean,
    /// Angular distance: one minus the cosine similarity.
    Angular,
}

/// A collection's settings.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectionSpec {
    /// Number of dimensions of every vector.
    pub dimension: u32,
    /// The distance metric.
    pub metric: Metric,
    /// Number of shards.
    pub shards: u32,
    /// Copies of each shard.
    pub replication: u32,
}

/// Whether the control plane considers a data node alive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Holding a valid lease, or not yet timed out.
    Alive,
    /// Marked dead; its shards are reassigned.
    Dead,
}

/// What the control plane records about a data node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataNodeInfo {
    /// Where the data node serves requests.
    pub address: String,
    /// Increases every time the data node starts; a restarted node is a new replica.
    pub incarnation: u64,
    /// Alive or dead.
    pub status: NodeStatus,
}

/// One copy of a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Replica {
    /// The data node holding the copy.
    pub node: DataNodeId,
    /// The incarnation of that data node.
    pub incarnation: u64,
}

/// What the control plane records about a shard (`docs/DESIGN.md` §3).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardInfo {
    /// Every copy, including the primary and replicas still catching up.
    pub replicas: Vec<Replica>,
    /// The data node that serves writes and searches, if any.
    pub primary: Option<DataNodeId>,
    /// Replicas known to hold every acknowledged write. Always contains the primary.
    pub in_sync: BTreeSet<DataNodeId>,
    /// Increases with every change of primary.
    pub epoch: Epoch,
}

/// The cluster's metadata, as applied by one control-plane server.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardMap {
    /// The log index of the last command applied to produce this map.
    pub version: u64,
    /// Collections by name.
    pub collections: BTreeMap<String, CollectionSpec>,
    /// Data nodes by ID.
    pub nodes: BTreeMap<DataNodeId, DataNodeInfo>,
    /// Shards of every collection.
    pub shards: BTreeMap<ShardKey, ShardInfo>,
}

// ----- Control commands ------------------------------------------------------------------

/// A command to the control-plane state machine (`docs/PLAN.md` §4.4). Every ownership
/// change carries the epoch it expects, and fails if the shard's epoch has changed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ControlCommand {
    /// Creates a collection. `placement[s]` lists the data nodes of shard `s`, primary first.
    /// A client sends an empty placement; the leader fills it in before proposing, so
    /// applying the command is deterministic.
    CreateCollection {
        /// The collection's name.
        name: String,
        /// Its settings.
        spec: CollectionSpec,
        /// The data nodes of each shard, primary first.
        placement: Vec<Vec<DataNodeId>>,
    },
    /// Deletes a collection and its shards.
    DropCollection {
        /// The collection's name.
        name: String,
    },
    /// Records a data node, or a new incarnation of one, as alive.
    RegisterNode {
        /// The data node.
        node: DataNodeId,
        /// Where it serves requests.
        address: String,
        /// Its incarnation.
        incarnation: u64,
    },
    /// Records that a data node's lease expired (`docs/DESIGN.md` §5).
    MarkNodeDead {
        /// The data node.
        node: DataNodeId,
        /// The incarnation that expired.
        incarnation: u64,
    },
    /// Adds a replacement replica to a shard (§6.3).
    AddReplica {
        /// The shard.
        shard: ShardKey,
        /// The new replica's data node.
        node: DataNodeId,
        /// Its incarnation.
        incarnation: u64,
        /// The epoch the shard is expected to have.
        epoch: Epoch,
    },
    /// Removes a replica from a shard.
    RemoveReplica {
        /// The shard.
        shard: ShardKey,
        /// The replica's data node.
        node: DataNodeId,
        /// The epoch the shard is expected to have.
        epoch: Epoch,
    },
    /// Makes an in-sync replica the shard's primary and increases the epoch (§6.4).
    PromotePrimary {
        /// The shard.
        shard: ShardKey,
        /// The new primary.
        node: DataNodeId,
        /// The epoch the shard is expected to have; the shard moves to the next one.
        epoch: Epoch,
    },
    /// Removes a replica from the in-sync set (§6.2).
    ShrinkInSync {
        /// The shard.
        shard: ShardKey,
        /// The replica leaving the set.
        node: DataNodeId,
        /// The epoch the shard is expected to have.
        epoch: Epoch,
    },
    /// Adds a caught-up replica to the in-sync set (§6.3).
    ExpandInSync {
        /// The shard.
        shard: ShardKey,
        /// The replica joining the set.
        node: DataNodeId,
        /// The epoch the shard is expected to have.
        epoch: Epoch,
    },
    /// Changes nothing. Experiment E1's clients send it, padded to the protocol's command
    /// size (`docs/RESEARCH.md` §3).
    Bench {
        /// Filler bytes.
        padding: Vec<u8>,
    },
}

/// A command with its client session, so a retried command is applied once (Raft §8).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Request {
    /// The client's ID, unique per client process.
    pub client: u64,
    /// The client's sequence number for this command, increasing from 1.
    pub seq: u64,
    /// The command.
    pub command: ControlCommand,
}

/// The result of applying a command.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Outcome {
    /// The command took effect.
    Applied,
    /// The command was valid to log but did not take effect, for example because an epoch
    /// check failed.
    Rejected(String),
}

// ----- The data plane --------------------------------------------------------------------

/// A change to one vector. Both are idempotent per ID (`docs/DESIGN.md` §6.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WriteOp {
    /// Inserts or replaces a vector.
    Upsert {
        /// The vector's ID.
        id: u64,
        /// Its components.
        vector: Vec<f32>,
    },
    /// Removes a vector.
    Delete {
        /// The vector's ID.
        id: u64,
    },
}

impl WriteOp {
    /// The vector's ID.
    pub fn id(&self) -> u64 {
        match self {
            WriteOp::Upsert { id, .. } | WriteOp::Delete { id } => *id,
        }
    }
}

/// A client write sent to a shard's primary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Write {
    /// The shard.
    pub shard: ShardKey,
    /// The epoch in the client's shard map.
    pub epoch: Epoch,
    /// The change.
    pub op: WriteOp,
}

/// Why a data node refused a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Refusal {
    /// This node is not the shard's primary at the request's epoch. `epoch` is the node's
    /// own epoch for the shard, so the client knows its map is stale.
    NotPrimary {
        /// The node's epoch for the shard.
        epoch: Epoch,
    },
    /// The node's lease has expired.
    NoLease,
    /// Fewer than the minimum number of replicas are in sync.
    TooFewInSync,
    /// The node does not hold the shard.
    UnknownShard,
}

/// The answer to a [`Write`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteResult {
    /// Every in-sync replica holds the write, at this offset.
    Acknowledged {
        /// The write's offset in the shard log.
        offset: u64,
    },
    /// The write was refused and not applied.
    Refused(Refusal),
}

/// A nearest-neighbor query to one shard's primary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchQuery {
    /// The shard.
    pub shard: ShardKey,
    /// The epoch in the client's shard map.
    pub epoch: Epoch,
    /// The query vector.
    pub vector: Vec<f32>,
    /// Number of neighbors to return.
    pub k: u32,
}

/// One search hit.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Neighbor {
    /// The vector's ID.
    pub id: u64,
    /// Its distance to the query.
    pub distance: f32,
}

/// The answer to a [`SearchQuery`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SearchResult {
    /// The shard's nearest neighbors, closest first.
    Found(Vec<Neighbor>),
    /// The query was refused.
    Refused(Refusal),
}

/// A write sent from a primary to a backup (`docs/DESIGN.md` §6.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Replicate {
    /// The shard.
    pub shard: ShardKey,
    /// The primary's epoch.
    pub epoch: Epoch,
    /// The write's offset.
    pub offset: u64,
    /// The change.
    pub op: WriteOp,
}

/// A backup's answer to [`Replicate`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicateAck {
    /// The backup holds every offset up to and including this one.
    Stored {
        /// The highest offset held without gaps.
        through: u64,
    },
    /// The backup is missing earlier offsets and needs them resent from `next`.
    Gap {
        /// The first missing offset.
        next: u64,
    },
    /// The backup has a newer epoch.
    StaleEpoch {
        /// The backup's epoch.
        epoch: Epoch,
    },
}

/// A new primary aligning a backup: delete entries above `end_offset`, adopt `epoch`
/// (`docs/DESIGN.md` §6.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewEpoch {
    /// The shard.
    pub shard: ShardKey,
    /// The new epoch.
    pub epoch: Epoch,
    /// The new primary's last offset.
    pub end_offset: u64,
}

/// A backup's answer to [`NewEpoch`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NewEpochAck {
    /// The backup adopted the epoch and holds offsets up to `through`.
    Adopted {
        /// The highest offset the backup now holds.
        through: u64,
    },
    /// The backup already has a newer epoch.
    StaleEpoch {
        /// The backup's epoch.
        epoch: Epoch,
    },
}

/// One entry of a shard log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    /// The epoch the entry was written in.
    pub epoch: Epoch,
    /// Its offset.
    pub offset: u64,
    /// The change.
    pub op: WriteOp,
}

/// A chunk of a shard log sent to a replacement replica (`docs/DESIGN.md` §6.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyShard {
    /// The shard.
    pub shard: ShardKey,
    /// The primary's epoch.
    pub epoch: Epoch,
    /// Consecutive log entries.
    pub entries: Vec<LogEntry>,
}

/// A replacement replica's answer to [`CopyShard`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CopyShardAck {
    /// The replica holds every offset up to and including `through`.
    Stored {
        /// The highest offset held without gaps.
        through: u64,
    },
    /// The replica has a newer epoch.
    StaleEpoch {
        /// The replica's epoch.
        epoch: Epoch,
    },
}

// ----- Delay injection -------------------------------------------------------------------

/// Network delay injection, shared by the control-plane transport and the clients, so
/// every message between processes waits the same way (`docs/RESEARCH.md` §3).
pub mod delay {
    use std::time::{Duration, Instant};

    /// A small seeded generator (SplitMix64) for delay jitter.
    #[derive(Debug, Clone)]
    pub struct Jitter {
        state: u64,
    }

    impl Jitter {
        /// A generator seeded with `seed`.
        pub fn new(seed: u64) -> Self {
            Jitter { state: seed }
        }

        fn next(&mut self) -> u64 {
            self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        /// A number in `0..bound`, for a positive `bound`.
        pub fn below(&mut self, bound: u64) -> u64 {
            ((u128::from(self.next()) * u128::from(bound.max(1))) >> 64) as u64
        }

        /// A one-way delay drawn uniformly from `[0.5 d, 1.5 d]` for a mean of `mean_us`
        /// microseconds.
        pub fn delay(&mut self, mean_us: u64) -> Duration {
            let span = mean_us * 1000 + 1;
            let offset = ((u128::from(self.next()) * u128::from(span)) >> 64) as u64;
            Duration::from_nanos(mean_us * 1000 / 2 + offset)
        }
    }

    /// Sleeps until `deadline` with sub-millisecond precision: a timed wait for most of
    /// the time, then a short spin.
    pub fn sleep_until(deadline: Instant) {
        const SPIN: Duration = Duration::from_micros(300);
        let now = Instant::now();
        if deadline > now + SPIN {
            std::thread::sleep(deadline - now - SPIN);
        }
        while Instant::now() < deadline {
            std::hint::spin_loop();
        }
    }
}

// ----- Measurement -----------------------------------------------------------------------

/// Microseconds since the Unix epoch. Every process on one machine reads the same clock,
/// so times from different processes can be compared (`docs/RESEARCH.md` §3).
pub fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

/// A consensus message one control-plane server sent to another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendEvent {
    /// When it was sent, in microseconds since the Unix epoch.
    pub time: u64,
    /// The receiver.
    pub to: u64,
    /// The message type (`Consensus::message_kind`).
    pub kind: String,
    /// Encoded size in bytes.
    pub bytes: u64,
    /// Log entries carried (`Consensus::entry_count`).
    pub entries: u64,
}

/// A change of a control-plane server's role, term, or commit index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusEvent {
    /// When it happened, in microseconds since the Unix epoch.
    pub time: u64,
    /// "follower", "candidate", or "leader".
    pub role: String,
    /// The term.
    pub term: u64,
    /// The commit index.
    pub commit_index: u64,
    /// The highest index in the log.
    pub last_index: u64,
}

/// A control-plane server's measurement log, read by experiment runners.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Events {
    /// The server's ID.
    pub server: u64,
    /// Every consensus message sent.
    pub sends: Vec<SendEvent>,
    /// Every change of role, term, or commit index.
    pub status: Vec<StatusEvent>,
    /// Storage syncs that wrote something.
    pub fsyncs: u64,
    /// The largest number of gaps seen in the log (metric M15).
    pub max_log_gaps: u64,
    /// Entries overwritten with a different payload or truncated (metric M17).
    pub discarded_entries: u64,
    /// Log entries carried by messages delivered while this server was a candidate or
    /// leader (metric M16).
    pub entries_to_leaders: u64,
    /// Raft only: votes this server refused because the candidate's log was behind
    /// (metric M5).
    pub stale_log_rejections: u64,
}

#[cfg(test)]
mod tests {
    use super::{ControlCommand, Request, decode, encode, shard_of};

    #[test]
    fn round_trip() {
        let request = Request {
            client: 7,
            seq: 3,
            command: ControlCommand::Bench {
                padding: vec![0; 10],
            },
        };
        let bytes = encode(&request);
        assert_eq!(decode::<Request>(&bytes), Ok(request));
        assert!(decode::<Request>(&[0xff]).is_err());
    }

    #[test]
    fn delays_stay_in_range() {
        let mut jitter = super::delay::Jitter::new(1);
        for _ in 0..10_000 {
            let d = jitter.delay(500);
            assert!(d >= std::time::Duration::from_micros(250));
            assert!(d <= std::time::Duration::from_micros(750));
        }
    }

    #[test]
    fn shards_spread_evenly() {
        let mut counts = [0u32; 4];
        for id in 0..40_000 {
            counts[shard_of(id, 4) as usize] += 1;
        }
        for count in counts {
            assert!((9_000..11_000).contains(&count), "{counts:?}");
        }
    }
}
