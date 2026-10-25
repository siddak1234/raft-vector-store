//! Data-node leases, failure detection, and shard recovery on the control-plane leader
//! (`docs/DESIGN.md` §5, §6.3, §6.4).
//!
//! - **Renewals.** Heartbeats are batched; each batch waits for one leader-confirmed read
//!   (`Consensus::confirm_leadership`) and for the state machine to reach its read index,
//!   then the lease is granted to every heartbeat whose node is registered with that
//!   incarnation and alive (§5, rules 1–3).
//! - **Failure detection.** When the local time passes
//!   `max(lastGrant[n], leaderSince) + L + δ`, the leader proposes `MarkNodeDead(n, k)` and
//!   never grants `(n, k)` a lease again (§5, rule 4).
//! - **Recovery.** For every shard on a dead node, the leader proposes the promotion of an
//!   alive in-sync replica if the dead node was primary, the removal of the dead replica,
//!   and a replacement replica (§6.3, §6.4).
//!
//! Every proposal goes through consensus like any client command, so a new leader sees the
//! same state and continues where the old one stopped.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use consensus::Role;
use rpc::proto::{HeartbeatReply, HeartbeatRequest};
use rpc::{ControlCommand, DataNodeId, NodeStatus, Outcome, Request, ShardMap};
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use crate::driver::{Confirmed, Input, Proposed, Shared};
use crate::state::replacement;

/// Lease timing (`docs/DESIGN.md` §4).
#[derive(Debug, Clone, Copy)]
pub struct LeaseTiming {
    /// Lease duration `L`.
    pub lease: Duration,
    /// Safety margin `δ`.
    pub margin: Duration,
    /// How often the leader checks for expired leases and incomplete recoveries.
    pub check_every: Duration,
}

impl Default for LeaseTiming {
    fn default() -> Self {
        LeaseTiming {
            lease: Duration::from_secs(3),
            margin: Duration::from_millis(500),
            check_every: Duration::from_millis(100),
        }
    }
}

struct Grants {
    /// When each `(node, incarnation)` was last granted a lease, by this server.
    last: BTreeMap<(DataNodeId, u64), Instant>,
    /// Incarnations this server proposed to mark dead; they are never granted again.
    condemned: BTreeSet<(DataNodeId, u64)>,
}

/// The lease manager of one control-plane server.
pub struct Leases {
    heartbeats: tokio_mpsc::UnboundedSender<(HeartbeatRequest, oneshot::Sender<HeartbeatReply>)>,
}

impl Leases {
    /// Starts the lease manager's tasks on `runtime`.
    pub fn start(
        runtime: &tokio::runtime::Handle,
        inputs: mpsc::Sender<Input>,
        shared: Arc<Mutex<Shared>>,
        timing: LeaseTiming,
    ) -> Arc<Leases> {
        let (heartbeats, queue) = tokio_mpsc::unbounded_channel();
        let grants = Arc::new(Mutex::new(Grants {
            last: BTreeMap::new(),
            condemned: BTreeSet::new(),
        }));
        let proposer = Arc::new(tokio::sync::Mutex::new(Proposer {
            client: rpc::now_micros(),
            seq: 0,
            inputs: inputs.clone(),
        }));
        runtime.spawn(renew(
            queue,
            inputs.clone(),
            Arc::clone(&shared),
            Arc::clone(&grants),
            Arc::clone(&proposer),
        ));
        runtime.spawn(watch(shared, grants, proposer, timing));
        Arc::new(Leases { heartbeats })
    }

    /// Handles one heartbeat and returns the reply.
    pub async fn heartbeat(&self, request: HeartbeatRequest) -> HeartbeatReply {
        let (reply, answer) = oneshot::channel();
        if self.heartbeats.send((request, reply)).is_err() {
            return refused(0);
        }
        answer.await.unwrap_or_else(|_| refused(0))
    }
}

fn refused(leader: u64) -> HeartbeatReply {
    HeartbeatReply {
        granted: false,
        leader,
        map: Vec::new(),
    }
}

/// Proposes the lease manager's commands one at a time, so its session's sequence numbers
/// are applied in order.
struct Proposer {
    client: u64,
    seq: u64,
    inputs: mpsc::Sender<Input>,
}

impl Proposer {
    async fn propose(&mut self, command: ControlCommand) -> Option<Outcome> {
        self.seq += 1;
        let request = Request {
            client: self.client,
            seq: self.seq,
            command,
        };
        let (reply, answer) = oneshot::channel();
        self.inputs.send(Input::Propose { request, reply }).ok()?;
        match tokio::time::timeout(Duration::from_secs(2), answer).await {
            Ok(Ok(Proposed::Applied { outcome, .. })) => Some(outcome),
            _ => None,
        }
    }
}

/// Answers heartbeats in batches, each behind one leader-confirmed read.
async fn renew(
    mut queue: tokio_mpsc::UnboundedReceiver<(HeartbeatRequest, oneshot::Sender<HeartbeatReply>)>,
    inputs: mpsc::Sender<Input>,
    shared: Arc<Mutex<Shared>>,
    grants: Arc<Mutex<Grants>>,
    proposer: Arc<tokio::sync::Mutex<Proposer>>,
) {
    while let Some(first) = queue.recv().await {
        let mut batch = vec![first];
        while let Ok(more) = queue.try_recv() {
            batch.push(more);
        }
        let (leader, is_leader) = {
            let shared = shared.lock().expect("no thread panicked");
            (
                shared.leader.map_or(0, |l| l.0),
                shared.role == Role::Leader,
            )
        };
        if !is_leader {
            for (_, reply) in batch {
                let _ = reply.send(refused(leader));
            }
            continue;
        }

        // Register new nodes and new incarnations first (`docs/DESIGN.md` §2).
        let map = Arc::clone(&shared.lock().expect("no thread panicked").map);
        for (request, _) in &batch {
            let node = DataNodeId(request.node);
            let known = map.nodes.get(&node).map(|info| info.incarnation);
            if known.is_none_or(|k| k < request.incarnation) {
                let command = ControlCommand::RegisterNode {
                    node,
                    address: request.address.clone(),
                    incarnation: request.incarnation,
                };
                proposer.lock().await.propose(command).await;
            }
        }

        // One confirmation for the whole batch. It is requested after every heartbeat in
        // the batch arrived, so after each was sent.
        let (reply, answer) = oneshot::channel();
        if inputs.send(Input::Confirm { reply }).is_err() {
            return;
        }
        let map = match answer.await {
            Ok(Confirmed::Map(map)) => map,
            Ok(Confirmed::NotLeader(leader)) => {
                for (_, reply) in batch {
                    let _ = reply.send(refused(leader.map_or(0, |l| l.0)));
                }
                continue;
            }
            Err(_) => return,
        };
        let now = Instant::now();
        let mut grants = grants.lock().expect("no thread panicked");
        for (request, reply) in batch {
            let key = (DataNodeId(request.node), request.incarnation);
            let alive = map
                .nodes
                .get(&key.0)
                .is_some_and(|info| info.incarnation == key.1 && info.status == NodeStatus::Alive);
            let granted = alive && !grants.condemned.contains(&key);
            if granted {
                grants.last.insert(key, now);
            }
            let map_bytes = if map.version > request.known_version {
                rpc::encode(map.as_ref())
            } else {
                Vec::new()
            };
            let _ = reply.send(HeartbeatReply {
                granted,
                leader: 0,
                map: map_bytes,
            });
        }
    }
}

/// Periodically marks nodes with expired leases dead and drives shard recovery, while this
/// server leads.
async fn watch(
    shared: Arc<Mutex<Shared>>,
    grants: Arc<Mutex<Grants>>,
    proposer: Arc<tokio::sync::Mutex<Proposer>>,
    timing: LeaseTiming,
) {
    let mut ticker = tokio::time::interval(timing.check_every);
    loop {
        ticker.tick().await;
        let (map, since) = {
            let shared = shared.lock().expect("no thread panicked");
            if shared.role != Role::Leader {
                continue;
            }
            let Some(since) = shared.leader_since else {
                continue;
            };
            (Arc::clone(&shared.map), since)
        };
        let now = Instant::now();
        let expired: Vec<(DataNodeId, u64)> = {
            let mut grants = grants.lock().expect("no thread panicked");
            let expired: Vec<(DataNodeId, u64)> = map
                .nodes
                .iter()
                .filter(|(_, info)| info.status == NodeStatus::Alive)
                .map(|(id, info)| (*id, info.incarnation))
                .filter(|key| {
                    let last = grants.last.get(key).copied().unwrap_or(since).max(since);
                    now > last + timing.lease + timing.margin
                })
                .collect();
            grants.condemned.extend(expired.iter().copied());
            expired
        };
        let mut proposer = proposer.lock().await;
        for (node, incarnation) in expired {
            proposer
                .propose(ControlCommand::MarkNodeDead { node, incarnation })
                .await;
        }
        for command in recovery(&map) {
            proposer.propose(command).await;
        }
    }
}

/// The next recovery step for every shard that has a dead replica (`docs/DESIGN.md` §6.3,
/// §6.4): promote an alive in-sync replica if the primary is dead, otherwise remove the
/// dead replica, and add a replacement while the shard has too few copies.
pub fn recovery(map: &ShardMap) -> Vec<ControlCommand> {
    let dead = |node: &DataNodeId| {
        map.nodes
            .get(node)
            .is_none_or(|info| info.status == NodeStatus::Dead)
    };
    let mut commands = Vec::new();
    for (key, info) in &map.shards {
        let Some(spec) = map.collections.get(&key.collection) else {
            continue;
        };
        if let Some(primary) = info.primary
            && dead(&primary)
        {
            // The alive member of the in-sync set with the lowest ID; none means the
            // shard stays unavailable.
            if let Some(&next) = info.in_sync.iter().find(|n| !dead(n)) {
                commands.push(ControlCommand::PromotePrimary {
                    shard: key.clone(),
                    node: next,
                    epoch: info.epoch,
                });
            }
            continue;
        }
        if let Some(replica) = info.replicas.iter().find(|r| dead(&r.node)) {
            commands.push(ControlCommand::RemoveReplica {
                shard: key.clone(),
                node: replica.node,
                epoch: info.epoch,
            });
            continue;
        }
        if info.replicas.len() < spec.replication as usize
            && let Some(node) = replacement(map, key)
        {
            let incarnation = map.nodes[&node].incarnation;
            commands.push(ControlCommand::AddReplica {
                shard: key.clone(),
                node,
                incarnation,
                epoch: info.epoch,
            });
        }
    }
    commands
}
