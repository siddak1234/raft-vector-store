//! The data node: leases, primary-to-backup replication, promotion, re-replication, and
//! search (`docs/DESIGN.md` §5–§6).
//!
//! A node learns its role in each shard from the shard map, which comes back on heartbeat
//! replies. As a shard's primary it serves writes and searches only at the shard's
//! current epoch and while its lease is valid. It replicates every write to the replicas
//! in its `ackSet` and acknowledges the client once all of them hold it (§6.2).
//!
//! The `ackSet` never shrinks below the committed in-sync set: a lagging backup leaves it
//! only after the control plane has committed `ShrinkInSync`, and a caught-up replica
//! joins it before `ExpandInSync` is proposed (§6.1).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rpc::proto::data_node_client::DataNodeClient;
use rpc::proto::data_node_server::DataNode;
use rpc::proto::{
    CopyShardReply, CopyShardRequest, Empty, EventsReply, FaultsRequest, HeartbeatRequest,
    InspectReply, InspectRequest, NewEpochReply, NewEpochRequest, ReplicateReply, ReplicateRequest,
    SearchReply, SearchRequest, WriteReply, WriteRequest,
};
use rpc::{
    ControlCommand, CopyShard, CopyShardAck, DataNodeEvents, DataNodeId, Epoch, LogEntry, Metric,
    NewEpoch, NewEpochAck, NodeStatus, Outcome, Refusal, Replicate, ReplicateAck, SearchQuery,
    SearchResult, ServeEvent, ShardDump, ShardKey, ShardMap, Write, WriteResult,
};
use tokio::sync::Notify;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::control::ControlLink;
use crate::shard::{ShardReplica, Stored};

/// Settings of one data node (`docs/DESIGN.md` §4).
#[derive(Debug, Clone)]
pub struct NodeOptions {
    /// The node's ID.
    pub id: DataNodeId,
    /// Where it serves requests.
    pub address: String,
    /// The control-plane servers.
    pub control: Vec<(u64, String)>,
    /// Heartbeat interval `h`.
    pub heartbeat: Duration,
    /// Lease duration `L`.
    pub lease: Duration,
    /// How long a primary waits for a backup's acknowledgment before asking to remove it
    /// from the in-sync set (`a`).
    pub ack_timeout: Duration,
    /// Fewest in-sync replicas with which a shard accepts writes.
    pub min_in_sync: usize,
    /// Whether to keep the log of served requests.
    pub record: bool,
}

impl NodeOptions {
    /// The design's parameters: `h` = 1 s, `L` = 3 s, `a` = 1 s, `minInSync` = 2.
    pub fn new(id: DataNodeId, address: String, control: Vec<(u64, String)>) -> Self {
        NodeOptions {
            id,
            address,
            control,
            heartbeat: Duration::from_secs(1),
            lease: Duration::from_secs(3),
            ack_timeout: Duration::from_secs(1),
            min_in_sync: 2,
            record: false,
        }
    }
}

/// What a primary tracks for one shard in its epoch.
#[derive(Debug)]
struct Primary {
    epoch: Epoch,
    /// The last offset when this node became primary, sent in `NewEpoch` (§6.4).
    end_offset: u64,
    /// The epochs of the log when this node became primary, sent in `NewEpoch`.
    epochs: Vec<(Epoch, u64)>,
    /// Replicas whose acknowledgment every write needs, including this node.
    ack_set: BTreeSet<DataNodeId>,
    /// The highest offset each other replica holds without gaps, in this epoch.
    acked: BTreeMap<DataNodeId, u64>,
    /// Replicas that adopted this epoch.
    aligned: BTreeSet<DataNodeId>,
    /// Since when each replica in `ack_set` has been behind the last offset.
    behind_since: BTreeMap<DataNodeId, Instant>,
    /// Replicas with a `ShrinkInSync` or `ExpandInSync` proposal under way.
    proposing: BTreeSet<DataNodeId>,
    /// Replicas with a replication task.
    workers: BTreeSet<DataNodeId>,
}

#[derive(Debug)]
struct Local {
    replica: ShardReplica,
    primary: Option<Primary>,
}

struct State {
    incarnation: u64,
    map: ShardMap,
    lease_expiry: Option<Instant>,
    shards: BTreeMap<ShardKey, Local>,
    serves: Vec<ServeEvent>,
}

/// One data node.
pub struct Node {
    options: NodeOptions,
    blocked: Arc<Mutex<BTreeSet<u64>>>,
    state: Mutex<State>,
    changed: Notify,
    /// The link heartbeats use. Proposals use their own link, so a slow proposal never
    /// delays a lease renewal.
    heartbeat_link: tokio::sync::Mutex<ControlLink>,
    control: tokio::sync::Mutex<ControlLink>,
    peers: Mutex<BTreeMap<(DataNodeId, String), DataNodeClient<Channel>>>,
}

impl Node {
    /// A node with a fresh incarnation and no shards.
    ///
    /// # Errors
    ///
    /// Returns a description if a control-plane address is invalid.
    pub fn new(options: NodeOptions) -> Result<Arc<Node>, String> {
        let control = ControlLink::new(&options.control)?;
        let heartbeat_link = ControlLink::with_blocked(&options.control, control.blocked())?;
        Ok(Arc::new(Node {
            blocked: control.blocked(),
            heartbeat_link: tokio::sync::Mutex::new(heartbeat_link),
            options,
            state: Mutex::new(State {
                incarnation: rpc::now_micros(),
                map: ShardMap::default(),
                lease_expiry: None,
                shards: BTreeMap::new(),
                serves: Vec::new(),
            }),
            changed: Notify::new(),
            control: tokio::sync::Mutex::new(control),
            peers: Mutex::new(BTreeMap::new()),
        }))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("no thread panicked")
    }

    fn lease_valid(state: &State) -> bool {
        state
            .lease_expiry
            .is_some_and(|expiry| Instant::now() < expiry)
    }

    // ----- Heartbeats and the shard map ----------------------------------------------

    /// Sends heartbeats every `h` forever (`docs/DESIGN.md` §5, data node rules 1–2).
    pub async fn heartbeats(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(self.options.heartbeat);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let (incarnation, known) = {
                let state = self.lock();
                (state.incarnation, state.map.version)
            };
            let sent = Instant::now();
            let request = HeartbeatRequest {
                node: self.options.id.0,
                incarnation,
                address: self.options.address.clone(),
                known_version: known,
            };
            let reply = self.heartbeat_link.lock().await.heartbeat(request).await;
            let Some(reply) = reply else {
                continue;
            };
            if reply.granted {
                let mut state = self.lock();
                if state.incarnation == incarnation {
                    let expiry = sent + self.options.lease;
                    state.lease_expiry = Some(state.lease_expiry.map_or(expiry, |e| e.max(expiry)));
                }
            }
            if !reply.map.is_empty()
                && let Ok(map) = rpc::decode::<ShardMap>(&reply.map)
            {
                self.on_map(map);
            }
        }
    }

    /// Adopts a newer shard map and reacts to role changes.
    fn on_map(self: &Arc<Self>, map: ShardMap) {
        let me = self.options.id;
        let mut workers = Vec::new();
        {
            let mut state = self.lock();
            if map.version < state.map.version {
                return;
            }
            // An incarnation the control plane marked dead is never given a lease again;
            // the node starts over as a new replica (`docs/DESIGN.md` §5, data-node rule 4).
            if map.nodes.get(&me).is_some_and(|info| {
                info.incarnation == state.incarnation && info.status == NodeStatus::Dead
            }) {
                state.incarnation = rpc::now_micros();
                state.shards.clear();
                state.lease_expiry = None;
                state.map = ShardMap::default();
                return;
            }
            let incarnation = state.incarnation;
            for (key, info) in &map.shards {
                let holds = info
                    .replicas
                    .iter()
                    .any(|r| r.node == me && r.incarnation == incarnation);
                if !holds {
                    continue;
                }
                let Some(spec) = map.collections.get(&key.collection) else {
                    continue;
                };
                let local = state.shards.entry(key.clone()).or_insert_with(|| Local {
                    replica: ShardReplica::new(Epoch(0), spec.metric),
                    primary: None,
                });
                if info.primary != Some(me) {
                    local.primary = None;
                    continue;
                }
                let current = local.primary.as_ref().map(|p| p.epoch);
                if current != Some(info.epoch) && info.epoch >= local.replica.epoch() {
                    // Promotion, or the first epoch of a new shard (§6.4).
                    local.replica.promote(info.epoch);
                    local.primary = Some(Primary {
                        epoch: info.epoch,
                        end_offset: local.replica.last_offset(),
                        epochs: local.replica.epoch_ends(),
                        ack_set: info.in_sync.clone(),
                        acked: BTreeMap::new(),
                        aligned: BTreeSet::new(),
                        behind_since: BTreeMap::new(),
                        proposing: BTreeSet::new(),
                        workers: BTreeSet::new(),
                    });
                }
                let Some(primary) = local.primary.as_mut() else {
                    continue;
                };
                let replicas: BTreeSet<DataNodeId> = info.replicas.iter().map(|r| r.node).collect();
                // A replica removed from the shard leaves the ack set: its removal is
                // committed.
                primary.ack_set.retain(|n| *n == me || replicas.contains(n));
                for &node in &replicas {
                    if node != me && primary.workers.insert(node) {
                        workers.push((key.clone(), node, primary.epoch, spec.metric));
                    }
                }
            }
            let held: Vec<ShardKey> = state.shards.keys().cloned().collect();
            for key in held {
                let still = map.shards.get(&key).is_some_and(|info| {
                    info.replicas
                        .iter()
                        .any(|r| r.node == me && r.incarnation == incarnation)
                });
                if !still {
                    state.shards.remove(&key);
                }
            }
            state.map = map;
        }
        for (shard, peer, epoch, metric) in workers {
            let node = Arc::clone(self);
            tokio::spawn(async move { node.replicate_to(shard, peer, epoch, metric).await });
        }
        self.changed.notify_waiters();
    }

    fn peer(&self, node: DataNodeId) -> Option<DataNodeClient<Channel>> {
        let address = self.lock().map.nodes.get(&node)?.address.clone();
        let mut peers = self.peers.lock().expect("no thread panicked");
        if let Some(client) = peers.get(&(node, address.clone())) {
            return Some(client.clone());
        }
        let endpoint = Endpoint::from_shared(format!("http://{address}"))
            .ok()?
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_millis(200));
        let client = DataNodeClient::new(endpoint.connect_lazy());
        peers.insert((node, address), client.clone());
        Some(client)
    }

    // ----- Replication as primary ----------------------------------------------------

    /// Keeps replica `peer` of `shard` up to date while this node is the shard's primary at
    /// `epoch`: aligns it with `NewEpoch`, then sends it every entry it lacks.
    async fn replicate_to(
        self: Arc<Self>,
        shard: ShardKey,
        peer: DataNodeId,
        epoch: Epoch,
        metric: Metric,
    ) {
        const BATCH: usize = 256;
        loop {
            // What to send, if this node is still the primary at `epoch`.
            let step = {
                let state = self.lock();
                let Some(local) = state.shards.get(&shard) else {
                    return;
                };
                let Some(primary) = local.primary.as_ref().filter(|p| p.epoch == epoch) else {
                    return;
                };
                let in_shard = state
                    .map
                    .shards
                    .get(&shard)
                    .is_some_and(|info| info.replicas.iter().any(|r| r.node == peer));
                if !in_shard {
                    return;
                }
                if !primary.aligned.contains(&peer) {
                    Step::Align(primary.end_offset, primary.epochs.clone())
                } else {
                    let next = primary.acked.get(&peer).copied().unwrap_or(0) + 1;
                    let entries = local.replica.entries_from(next, BATCH);
                    if entries.is_empty() {
                        Step::Wait
                    } else {
                        Step::Send(entries)
                    }
                }
            };
            let waiting = self.changed.notified();
            let Some(mut client) = self.peer(peer) else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            match step {
                Step::Wait => {
                    let _ = tokio::time::timeout(Duration::from_millis(100), waiting).await;
                }
                Step::Align(end_offset, epochs) => {
                    let request = NewEpochRequest {
                        new_epoch: rpc::encode(&NewEpoch {
                            shard: shard.clone(),
                            epoch,
                            end_offset,
                            epochs,
                            metric,
                        }),
                    };
                    match timed(client.new_epoch(request)).await {
                        Some(reply) => match rpc::decode::<NewEpochAck>(&reply.ack) {
                            Ok(NewEpochAck::Adopted { through }) => {
                                self.record_ack(&shard, epoch, peer, through, true);
                            }
                            Ok(NewEpochAck::StaleEpoch { epoch: newer }) if newer > epoch => {
                                self.step_down(&shard, epoch);
                                return;
                            }
                            _ => {}
                        },
                        None => tokio::time::sleep(Duration::from_millis(50)).await,
                    }
                }
                Step::Send(entries) => {
                    let through = if entries.len() == 1 {
                        let entry = entries.into_iter().next().expect("one entry");
                        let request = ReplicateRequest {
                            replicate: rpc::encode(&Replicate {
                                shard: shard.clone(),
                                epoch,
                                offset: entry.offset,
                                op: entry.op,
                            }),
                        };
                        timed(client.replicate(request)).await.and_then(
                            |reply| match rpc::decode::<ReplicateAck>(&reply.ack).ok()? {
                                ReplicateAck::Stored { through } => Some(Ok(through)),
                                ReplicateAck::Gap { next } => Some(Ok(next - 1)),
                                ReplicateAck::StaleEpoch { epoch } => Some(Err(epoch)),
                            },
                        )
                    } else {
                        let request = CopyShardRequest {
                            copy: rpc::encode(&CopyShard {
                                shard: shard.clone(),
                                epoch,
                                entries,
                            }),
                        };
                        timed(client.copy_shard(request)).await.and_then(|reply| {
                            match rpc::decode::<CopyShardAck>(&reply.ack).ok()? {
                                CopyShardAck::Stored { through } => Some(Ok(through)),
                                CopyShardAck::StaleEpoch { epoch } => Some(Err(epoch)),
                            }
                        })
                    };
                    match through {
                        Some(Ok(through)) => self.record_ack(&shard, epoch, peer, through, false),
                        Some(Err(newer)) if newer > epoch => {
                            self.step_down(&shard, epoch);
                            return;
                        }
                        Some(Err(_)) => self.unalign(&shard, epoch, peer),
                        None => tokio::time::sleep(Duration::from_millis(50)).await,
                    }
                }
            }
        }
    }

    fn record_ack(
        self: &Arc<Self>,
        shard: &ShardKey,
        epoch: Epoch,
        peer: DataNodeId,
        through: u64,
        aligned: bool,
    ) {
        let mut expand = false;
        {
            let mut state = self.lock();
            let Some(local) = state.shards.get_mut(shard) else {
                return;
            };
            let last = local.replica.last_offset();
            let Some(primary) = local.primary.as_mut().filter(|p| p.epoch == epoch) else {
                return;
            };
            if aligned {
                primary.aligned.insert(peer);
                primary.acked.insert(peer, through.min(last));
            } else {
                let acked = primary.acked.entry(peer).or_insert(0);
                *acked = (*acked).max(through);
            }
            if primary.acked.get(&peer).copied().unwrap_or(0) >= last {
                primary.behind_since.remove(&peer);
                // A caught-up replica joins the ack set before ExpandInSync is proposed,
                // so the ack set never lacks a member of the in-sync set (§6.3).
                if !primary.ack_set.contains(&peer) && primary.proposing.insert(peer) {
                    primary.ack_set.insert(peer);
                    expand = true;
                }
            }
        }
        self.changed.notify_waiters();
        if expand {
            let node = Arc::clone(self);
            let shard = shard.clone();
            tokio::spawn(async move {
                let command = ControlCommand::ExpandInSync {
                    shard: shard.clone(),
                    node: peer,
                    epoch,
                };
                node.control
                    .lock()
                    .await
                    .propose(command, Duration::from_secs(5))
                    .await;
                node.done_proposing(&shard, epoch, peer);
            });
        }
    }

    fn done_proposing(&self, shard: &ShardKey, epoch: Epoch, peer: DataNodeId) {
        let mut state = self.lock();
        if let Some(primary) = state
            .shards
            .get_mut(shard)
            .and_then(|l| l.primary.as_mut())
            .filter(|p| p.epoch == epoch)
        {
            primary.proposing.remove(&peer);
        }
    }

    fn unalign(&self, shard: &ShardKey, epoch: Epoch, peer: DataNodeId) {
        let mut state = self.lock();
        if let Some(primary) = state
            .shards
            .get_mut(shard)
            .and_then(|l| l.primary.as_mut())
            .filter(|p| p.epoch == epoch)
        {
            primary.aligned.remove(&peer);
        }
    }

    fn step_down(&self, shard: &ShardKey, epoch: Epoch) {
        let mut state = self.lock();
        if let Some(local) = state.shards.get_mut(shard)
            && local.primary.as_ref().is_some_and(|p| p.epoch == epoch)
        {
            local.primary = None;
        }
        drop(state);
        self.changed.notify_waiters();
    }

    /// Whether every member of the ack set holds `offset`. Starts a `ShrinkInSync` proposal
    /// for members that have been behind for longer than the acknowledgment timeout.
    fn check_acks(
        self: &Arc<Self>,
        shard: &ShardKey,
        epoch: Epoch,
        offset: u64,
    ) -> Result<bool, Refusal> {
        let me = self.options.id;
        let mut shrink = Vec::new();
        let done = {
            let mut state = self.lock();
            let Some(local) = state.shards.get_mut(shard) else {
                return Err(Refusal::UnknownShard);
            };
            let last = local.replica.last_offset();
            let own_epoch = local.replica.epoch();
            let Some(primary) = local.primary.as_mut().filter(|p| p.epoch == epoch) else {
                return Err(Refusal::NotPrimary { epoch: own_epoch });
            };
            let now = Instant::now();
            let mut done = true;
            for &member in &primary.ack_set {
                if member == me {
                    continue;
                }
                let acked = primary.acked.get(&member).copied().unwrap_or(0);
                if acked < last {
                    let since = *primary.behind_since.entry(member).or_insert(now);
                    if now.duration_since(since) > self.options.ack_timeout
                        && primary.proposing.insert(member)
                    {
                        shrink.push(member);
                    }
                }
                if acked < offset {
                    done = false;
                }
            }
            done
        };
        for member in shrink {
            let node = Arc::clone(self);
            let shard = shard.clone();
            tokio::spawn(async move {
                let command = ControlCommand::ShrinkInSync {
                    shard: shard.clone(),
                    node: member,
                    epoch,
                };
                let outcome = node
                    .control
                    .lock()
                    .await
                    .propose(command, Duration::from_secs(5))
                    .await;
                {
                    let mut state = node.lock();
                    if let Some(primary) = state
                        .shards
                        .get_mut(&shard)
                        .and_then(|l| l.primary.as_mut())
                        .filter(|p| p.epoch == epoch)
                    {
                        // Only a committed removal takes the member out of the ack set.
                        if outcome == Some(Outcome::Applied) {
                            primary.ack_set.remove(&member);
                            primary.behind_since.remove(&member);
                        }
                        primary.proposing.remove(&member);
                    }
                }
                node.changed.notify_waiters();
            });
        }
        Ok(done)
    }

    // ----- Client requests -----------------------------------------------------------

    async fn write(self: &Arc<Self>, write: Write) -> WriteResult {
        let offset = {
            let mut state = self.lock();
            let lease = Self::lease_valid(&state);
            let min_in_sync = self.options.min_in_sync;
            let record = self.options.record;
            let Some(local) = state.shards.get_mut(&write.shard) else {
                return WriteResult::Refused(Refusal::UnknownShard);
            };
            let own_epoch = local.replica.epoch();
            let Some(primary) = local.primary.as_ref().filter(|p| p.epoch == write.epoch) else {
                return WriteResult::Refused(Refusal::NotPrimary { epoch: own_epoch });
            };
            if !lease {
                return WriteResult::Refused(Refusal::NoLease);
            }
            if primary.ack_set.len() < min_in_sync {
                return WriteResult::Refused(Refusal::TooFewInSync);
            }
            let offset = local.replica.append(write.op);
            if record {
                state.serves.push(ServeEvent {
                    time: rpc::now_micros(),
                    shard: write.shard.clone(),
                    epoch: write.epoch,
                    write: true,
                });
            }
            offset
        };
        self.changed.notify_waiters();
        loop {
            let waiting = self.changed.notified();
            match self.check_acks(&write.shard, write.epoch, offset) {
                Ok(true) => return WriteResult::Acknowledged { offset },
                Ok(false) => {}
                Err(refusal) => return WriteResult::Refused(refusal),
            }
            let _ = tokio::time::timeout(Duration::from_millis(50), waiting).await;
        }
    }

    fn search(&self, query: &SearchQuery) -> SearchResult {
        let mut state = self.lock();
        let lease = Self::lease_valid(&state);
        let record = self.options.record;
        let Some(local) = state.shards.get(&query.shard) else {
            return SearchResult::Refused(Refusal::UnknownShard);
        };
        let own_epoch = local.replica.epoch();
        if local
            .primary
            .as_ref()
            .is_none_or(|p| p.epoch != query.epoch)
        {
            return SearchResult::Refused(Refusal::NotPrimary { epoch: own_epoch });
        }
        if !lease {
            return SearchResult::Refused(Refusal::NoLease);
        }
        let found = local.replica.search(&query.vector, query.k as usize);
        if record {
            state.serves.push(ServeEvent {
                time: rpc::now_micros(),
                shard: query.shard.clone(),
                epoch: query.epoch,
                write: false,
            });
        }
        SearchResult::Found(found)
    }

    // ----- Messages from a primary ---------------------------------------------------

    fn on_replicate(&self, message: Replicate) -> ReplicateAck {
        let mut state = self.lock();
        let Some(local) = state.shards.get_mut(&message.shard) else {
            return ReplicateAck::StaleEpoch { epoch: Epoch(0) };
        };
        let entry = LogEntry {
            epoch: message.epoch,
            offset: message.offset,
            op: message.op,
        };
        match local.replica.store(message.epoch, entry) {
            Stored::Through(through) => ReplicateAck::Stored { through },
            Stored::Gap(next) => ReplicateAck::Gap { next },
            Stored::Stale(epoch) => ReplicateAck::StaleEpoch { epoch },
        }
    }

    fn on_new_epoch(&self, message: NewEpoch) -> NewEpochAck {
        let mut state = self.lock();
        let local = state
            .shards
            .entry(message.shard.clone())
            .or_insert_with(|| Local {
                replica: ShardReplica::new(Epoch(0), message.metric),
                primary: None,
            });
        if local
            .primary
            .as_ref()
            .is_some_and(|p| p.epoch >= message.epoch)
        {
            return NewEpochAck::StaleEpoch {
                epoch: local.replica.epoch(),
            };
        }
        match local
            .replica
            .adopt(message.epoch, message.end_offset, &message.epochs)
        {
            Ok(through) => {
                local.primary = None;
                NewEpochAck::Adopted { through }
            }
            Err(epoch) => NewEpochAck::StaleEpoch { epoch },
        }
    }

    fn on_copy(&self, message: CopyShard) -> CopyShardAck {
        let mut state = self.lock();
        let Some(local) = state.shards.get_mut(&message.shard) else {
            return CopyShardAck::StaleEpoch { epoch: Epoch(0) };
        };
        for entry in message.entries {
            match local.replica.store(message.epoch, entry) {
                Stored::Through(_) | Stored::Gap(_) => {}
                Stored::Stale(epoch) => return CopyShardAck::StaleEpoch { epoch },
            }
        }
        CopyShardAck::Stored {
            through: local.replica.last_offset(),
        }
    }

    fn dump(&self, shard: &ShardKey) -> ShardDump {
        let state = self.lock();
        match state.shards.get(shard) {
            Some(local) => ShardDump {
                shard: shard.clone(),
                epoch: local.replica.epoch(),
                last_offset: local.replica.last_offset(),
                primary: local.primary.is_some(),
                ids: local.replica.ids().collect(),
            },
            None => ShardDump {
                shard: shard.clone(),
                epoch: Epoch(0),
                last_offset: 0,
                primary: false,
                ids: Vec::new(),
            },
        }
    }
}

enum Step {
    Wait,
    Align(u64, Vec<(Epoch, u64)>),
    Send(Vec<LogEntry>),
}

async fn timed<T>(
    call: impl std::future::Future<Output = Result<Response<T>, Status>>,
) -> Option<T> {
    match tokio::time::timeout(Duration::from_millis(500), call).await {
        Ok(Ok(reply)) => Some(reply.into_inner()),
        _ => None,
    }
}

/// The `DataNode` gRPC service.
pub struct DataNodeService {
    node: Arc<Node>,
}

impl DataNodeService {
    /// The service of `node`.
    pub fn new(node: Arc<Node>) -> Self {
        DataNodeService { node }
    }
}

fn invalid(why: String) -> Status {
    Status::invalid_argument(why)
}

#[tonic::async_trait]
impl DataNode for DataNodeService {
    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<WriteReply>, Status> {
        let write = rpc::decode::<Write>(&request.into_inner().write).map_err(invalid)?;
        let result = self.node.write(write).await;
        Ok(Response::new(WriteReply {
            result: rpc::encode(&result),
        }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchReply>, Status> {
        let query = rpc::decode::<SearchQuery>(&request.into_inner().query).map_err(invalid)?;
        let result = self.node.search(&query);
        Ok(Response::new(SearchReply {
            result: rpc::encode(&result),
        }))
    }

    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateReply>, Status> {
        let message = rpc::decode::<Replicate>(&request.into_inner().replicate).map_err(invalid)?;
        let ack = self.node.on_replicate(message);
        Ok(Response::new(ReplicateReply {
            ack: rpc::encode(&ack),
        }))
    }

    async fn new_epoch(
        &self,
        request: Request<NewEpochRequest>,
    ) -> Result<Response<NewEpochReply>, Status> {
        let message = rpc::decode::<NewEpoch>(&request.into_inner().new_epoch).map_err(invalid)?;
        let ack = self.node.on_new_epoch(message);
        Ok(Response::new(NewEpochReply {
            ack: rpc::encode(&ack),
        }))
    }

    async fn copy_shard(
        &self,
        request: Request<CopyShardRequest>,
    ) -> Result<Response<CopyShardReply>, Status> {
        let message = rpc::decode::<CopyShard>(&request.into_inner().copy).map_err(invalid)?;
        let ack = self.node.on_copy(message);
        Ok(Response::new(CopyShardReply {
            ack: rpc::encode(&ack),
        }))
    }

    async fn events(&self, _: Request<Empty>) -> Result<Response<EventsReply>, Status> {
        let state = self.node.lock();
        let events = DataNodeEvents {
            node: self.node.options.id.0,
            incarnation: state.incarnation,
            serves: state.serves.clone(),
        };
        Ok(Response::new(EventsReply {
            events: rpc::encode(&events),
        }))
    }

    async fn set_faults(&self, request: Request<FaultsRequest>) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        *self.node.blocked.lock().expect("no thread panicked") =
            request.drop_to.into_iter().collect();
        Ok(Response::new(Empty {}))
    }

    async fn inspect(
        &self,
        request: Request<InspectRequest>,
    ) -> Result<Response<InspectReply>, Status> {
        let shard = rpc::decode::<ShardKey>(&request.into_inner().shard).map_err(invalid)?;
        Ok(Response::new(InspectReply {
            dump: rpc::encode(&self.node.dump(&shard)),
        }))
    }
}
