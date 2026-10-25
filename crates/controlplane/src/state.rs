//! The control-plane state machine (`docs/PLAN.md` §4.4).
//!
//! Every server applies the same committed commands in the same order and must reach the
//! same state. The state uses `BTreeMap`, never reads the clock or a random number
//! generator, and leaves every choice, such as where to place a shard, to the leader, which
//! writes the choice into the command it proposes.
//!
//! Each command carries its client's ID and sequence number. A client sends one command at
//! a time and numbers them in order, so the state keeps only the last sequence number and
//! outcome of each client; a retried command is answered from there and applied once (Raft
//! §8).

use std::collections::{BTreeMap, BTreeSet};

use rpc::{
    CollectionSpec, ControlCommand, DataNodeId, DataNodeInfo, Epoch, NodeStatus, Outcome, Replica,
    Request, ShardInfo, ShardKey, ShardMap,
};

/// The replicated metadata and the client sessions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateMachine {
    map: ShardMap,
    /// For each client, its last applied sequence number and that command's outcome.
    sessions: BTreeMap<u64, (u64, Outcome)>,
}

impl StateMachine {
    /// An empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// The current shard map.
    pub fn map(&self) -> &ShardMap {
        &self.map
    }

    /// Applies the request committed at `index` and returns its outcome. A request whose
    /// sequence number was already applied is not applied again; its recorded outcome is
    /// returned. The map's version becomes `index` if the command changed the map.
    pub fn apply(&mut self, index: u64, request: &Request) -> Outcome {
        if let Some((last, outcome)) = self.sessions.get(&request.client)
            && request.seq <= *last
        {
            return if request.seq == *last {
                outcome.clone()
            } else {
                Outcome::Rejected("an older command from this client".into())
            };
        }
        let outcome = self.execute(&request.command);
        if outcome == Outcome::Applied && !matches!(request.command, ControlCommand::Bench { .. }) {
            self.map.version = index;
        }
        self.sessions
            .insert(request.client, (request.seq, outcome.clone()));
        outcome
    }

    fn execute(&mut self, command: &ControlCommand) -> Outcome {
        match command {
            ControlCommand::CreateCollection {
                name,
                spec,
                placement,
            } => self.create_collection(name, spec, placement),
            ControlCommand::DropCollection { name } => {
                if self.map.collections.remove(name).is_none() {
                    return rejected(format!("no collection {name}"));
                }
                self.map.shards.retain(|key, _| key.collection != *name);
                Outcome::Applied
            }
            ControlCommand::RegisterNode {
                node,
                address,
                incarnation,
            } => {
                if let Some(known) = self.map.nodes.get(node)
                    && known.incarnation >= *incarnation
                {
                    return rejected(format!(
                        "node {} is already at incarnation {}",
                        node.0, known.incarnation
                    ));
                }
                self.map.nodes.insert(
                    *node,
                    DataNodeInfo {
                        address: address.clone(),
                        incarnation: *incarnation,
                        status: NodeStatus::Alive,
                    },
                );
                Outcome::Applied
            }
            ControlCommand::MarkNodeDead { node, incarnation } => {
                match self.map.nodes.get_mut(node) {
                    Some(info) if info.incarnation == *incarnation => {
                        info.status = NodeStatus::Dead;
                        Outcome::Applied
                    }
                    _ => rejected(format!(
                        "node {} is not at incarnation {incarnation}",
                        node.0
                    )),
                }
            }
            ControlCommand::AddReplica {
                shard,
                node,
                incarnation,
                epoch,
            } => self.with_shard(shard, *epoch, |info| {
                if info.replicas.iter().any(|r| r.node == *node) {
                    return rejected(format!("node {} already holds the shard", node.0));
                }
                info.replicas.push(Replica {
                    node: *node,
                    incarnation: *incarnation,
                });
                Outcome::Applied
            }),
            ControlCommand::RemoveReplica { shard, node, epoch } => {
                self.with_shard(shard, *epoch, |info| {
                    if info.primary == Some(*node) {
                        return rejected("the primary cannot be removed".into());
                    }
                    let before = info.replicas.len();
                    info.replicas.retain(|r| r.node != *node);
                    info.in_sync.remove(node);
                    if info.replicas.len() == before {
                        return rejected(format!("node {} does not hold the shard", node.0));
                    }
                    Outcome::Applied
                })
            }
            ControlCommand::PromotePrimary { shard, node, epoch } => {
                self.with_shard(shard, *epoch, |info| {
                    // A replica outside the in-sync set may be missing acknowledged writes,
                    // so it is never promoted (`docs/DESIGN.md` §6.4).
                    if !info.in_sync.contains(node) {
                        return rejected(format!("node {} is not in sync", node.0));
                    }
                    if let Some(old) = info.primary
                        && old != *node
                    {
                        info.in_sync.remove(&old);
                    }
                    info.primary = Some(*node);
                    info.epoch = Epoch(info.epoch.0 + 1);
                    Outcome::Applied
                })
            }
            ControlCommand::ShrinkInSync { shard, node, epoch } => {
                self.with_shard(shard, *epoch, |info| {
                    if info.primary == Some(*node) {
                        return rejected("the primary stays in sync".into());
                    }
                    if !info.in_sync.remove(node) {
                        return rejected(format!("node {} is not in sync", node.0));
                    }
                    Outcome::Applied
                })
            }
            ControlCommand::ExpandInSync { shard, node, epoch } => {
                self.with_shard(shard, *epoch, |info| {
                    if !info.replicas.iter().any(|r| r.node == *node) {
                        return rejected(format!("node {} does not hold the shard", node.0));
                    }
                    if !info.in_sync.insert(*node) {
                        return rejected(format!("node {} is already in sync", node.0));
                    }
                    Outcome::Applied
                })
            }
            ControlCommand::Bench { .. } => Outcome::Applied,
        }
    }

    fn create_collection(
        &mut self,
        name: &str,
        spec: &CollectionSpec,
        placement: &[Vec<DataNodeId>],
    ) -> Outcome {
        if self.map.collections.contains_key(name) {
            return rejected(format!("collection {name} exists"));
        }
        if spec.shards == 0 || spec.replication == 0 || spec.dimension == 0 {
            return rejected("shards, replication, and dimension must be positive".into());
        }
        if placement.len() != spec.shards as usize
            || placement
                .iter()
                .any(|nodes| nodes.is_empty() || nodes.len() > spec.replication as usize)
        {
            return rejected("the placement does not match the collection".into());
        }
        let mut shards = Vec::new();
        for (number, nodes) in placement.iter().enumerate() {
            let mut replicas = Vec::new();
            for node in nodes {
                match self.map.nodes.get(node) {
                    Some(info) if info.status == NodeStatus::Alive => replicas.push(Replica {
                        node: *node,
                        incarnation: info.incarnation,
                    }),
                    _ => return rejected(format!("node {} is not alive", node.0)),
                }
            }
            let key = ShardKey {
                collection: name.to_string(),
                shard: number as u32,
            };
            shards.push((
                key,
                ShardInfo {
                    replicas,
                    primary: Some(nodes[0]),
                    in_sync: nodes.iter().copied().collect::<BTreeSet<_>>(),
                    epoch: Epoch(1),
                },
            ));
        }
        self.map.collections.insert(name.to_string(), spec.clone());
        self.map.shards.extend(shards);
        Outcome::Applied
    }

    /// Runs `change` on a shard if its epoch is `epoch`.
    fn with_shard(
        &mut self,
        shard: &ShardKey,
        epoch: Epoch,
        change: impl FnOnce(&mut ShardInfo) -> Outcome,
    ) -> Outcome {
        match self.map.shards.get_mut(shard) {
            None => rejected(format!("no shard {}/{}", shard.collection, shard.shard)),
            Some(info) if info.epoch != epoch => rejected(format!(
                "shard {}/{} is at epoch {}, not {}",
                shard.collection, shard.shard, info.epoch.0, epoch.0
            )),
            Some(info) => change(info),
        }
    }
}

fn rejected(reason: String) -> Outcome {
    Outcome::Rejected(reason)
}

/// Chooses data nodes for each shard of a new collection: for every replica, the alive
/// node holding the fewest replicas that is not already a replica of the shard, ties broken
/// by the lowest ID (`docs/DESIGN.md` §6.3). Only the leader calls this; the result goes
/// into the proposed command. Returns `None` if there are not enough alive nodes.
pub fn place(map: &ShardMap, spec: &CollectionSpec) -> Option<Vec<Vec<DataNodeId>>> {
    let mut load: BTreeMap<DataNodeId, usize> = map
        .nodes
        .iter()
        .filter(|(_, info)| info.status == NodeStatus::Alive)
        .map(|(id, _)| (*id, 0))
        .collect();
    for info in map.shards.values() {
        for replica in &info.replicas {
            if let Some(count) = load.get_mut(&replica.node) {
                *count += 1;
            }
        }
    }
    if load.len() < spec.replication as usize {
        return None;
    }
    let mut placement = Vec::new();
    for _ in 0..spec.shards {
        let mut nodes = Vec::new();
        for _ in 0..spec.replication {
            let (&node, _) = load
                .iter()
                .filter(|(id, _)| !nodes.contains(*id))
                .min_by_key(|(id, count)| (**count, **id))?;
            *load.get_mut(&node).expect("chosen from the map") += 1;
            nodes.push(node);
        }
        placement.push(nodes);
    }
    Some(placement)
}

/// Chooses a replacement replica for `shard`: the alive node with the fewest replicas that
/// does not already hold it, ties broken by the lowest ID (`docs/DESIGN.md` §6.3).
pub fn replacement(map: &ShardMap, shard: &ShardKey) -> Option<DataNodeId> {
    let holders: BTreeSet<DataNodeId> = map
        .shards
        .get(shard)?
        .replicas
        .iter()
        .map(|r| r.node)
        .collect();
    let mut load: BTreeMap<DataNodeId, usize> = map
        .nodes
        .iter()
        .filter(|(id, info)| info.status == NodeStatus::Alive && !holders.contains(id))
        .map(|(id, _)| (*id, 0))
        .collect();
    for info in map.shards.values() {
        for replica in &info.replicas {
            if let Some(count) = load.get_mut(&replica.node) {
                *count += 1;
            }
        }
    }
    load.into_iter()
        .min_by_key(|(id, count)| (*count, *id))
        .map(|(id, _)| id)
}

#[cfg(test)]
mod tests {
    use rpc::{
        CollectionSpec, ControlCommand, DataNodeId, Epoch, Metric, NodeStatus, Outcome, Request,
        ShardKey,
    };

    use super::{StateMachine, place, replacement};

    fn request(client: u64, seq: u64, command: ControlCommand) -> Request {
        Request {
            client,
            seq,
            command,
        }
    }

    fn spec(shards: u32, replication: u32) -> CollectionSpec {
        CollectionSpec {
            dimension: 25,
            metric: Metric::Angular,
            shards,
            replication,
        }
    }

    fn register(state: &mut StateMachine, nodes: u64) {
        for n in 1..=nodes {
            let command = ControlCommand::RegisterNode {
                node: DataNodeId(n),
                address: format!("127.0.0.1:{}", 8000 + n),
                incarnation: 1,
            };
            assert_eq!(state.apply(n, &request(0, n, command)), Outcome::Applied);
        }
    }

    fn shard0() -> ShardKey {
        ShardKey {
            collection: "glove".into(),
            shard: 0,
        }
    }

    fn create(state: &mut StateMachine, index: u64) {
        let spec = spec(4, 3);
        let placement = place(state.map(), &spec).expect("enough nodes");
        let command = ControlCommand::CreateCollection {
            name: "glove".into(),
            spec,
            placement,
        };
        assert_eq!(
            state.apply(index, &request(9, 1, command)),
            Outcome::Applied
        );
    }

    #[test]
    fn creates_a_collection_with_balanced_placement() {
        let mut state = StateMachine::new();
        register(&mut state, 4);
        create(&mut state, 10);
        let map = state.map();
        assert_eq!(map.shards.len(), 4);
        // 12 replicas over 4 nodes: 3 each.
        let mut load = [0; 4];
        for info in map.shards.values() {
            assert_eq!(info.replicas.len(), 3);
            assert_eq!(info.in_sync.len(), 3);
            assert_eq!(info.epoch, Epoch(1));
            for replica in &info.replicas {
                load[(replica.node.0 - 1) as usize] += 1;
            }
        }
        assert_eq!(load, [3, 3, 3, 3]);
        assert_eq!(map.version, 10);
    }

    #[test]
    fn a_retried_command_is_applied_once() {
        let mut state = StateMachine::new();
        register(&mut state, 3);
        let command = ControlCommand::RegisterNode {
            node: DataNodeId(7),
            address: "a".into(),
            incarnation: 1,
        };
        let first = state.apply(20, &request(5, 1, command.clone()));
        assert_eq!(first, Outcome::Applied);
        // Applying the same request again would be rejected as a stale incarnation; the
        // session answers it with the first outcome instead.
        assert_eq!(state.apply(21, &request(5, 1, command)), Outcome::Applied);
    }

    #[test]
    fn ownership_changes_check_the_epoch() {
        let mut state = StateMachine::new();
        register(&mut state, 4);
        create(&mut state, 10);
        let primary = state.map().shards[&shard0()].primary.expect("a primary");
        let other = *state.map().shards[&shard0()]
            .in_sync
            .iter()
            .find(|&&n| n != primary)
            .expect("a backup");

        let stale = ControlCommand::PromotePrimary {
            shard: shard0(),
            node: other,
            epoch: Epoch(5),
        };
        assert!(matches!(
            state.apply(11, &request(1, 1, stale)),
            Outcome::Rejected(_)
        ));

        let promote = ControlCommand::PromotePrimary {
            shard: shard0(),
            node: other,
            epoch: Epoch(1),
        };
        assert_eq!(state.apply(12, &request(1, 2, promote)), Outcome::Applied);
        let info = &state.map().shards[&shard0()];
        assert_eq!(info.primary, Some(other));
        assert_eq!(info.epoch, Epoch(2));
        // The old primary is no longer known to hold every acknowledged write.
        assert!(!info.in_sync.contains(&primary));
    }

    #[test]
    fn only_in_sync_replicas_are_promoted() {
        let mut state = StateMachine::new();
        register(&mut state, 4);
        create(&mut state, 10);
        let info = state.map().shards[&shard0()].clone();
        let backup = *info
            .in_sync
            .iter()
            .find(|&&n| Some(n) != info.primary)
            .expect("a backup");
        let shrink = ControlCommand::ShrinkInSync {
            shard: shard0(),
            node: backup,
            epoch: Epoch(1),
        };
        assert_eq!(state.apply(11, &request(1, 1, shrink)), Outcome::Applied);
        let promote = ControlCommand::PromotePrimary {
            shard: shard0(),
            node: backup,
            epoch: Epoch(1),
        };
        assert!(matches!(
            state.apply(12, &request(1, 2, promote)),
            Outcome::Rejected(_)
        ));
    }

    #[test]
    fn replacement_avoids_current_holders_and_dead_nodes() {
        let mut state = StateMachine::new();
        register(&mut state, 5);
        create(&mut state, 10);
        let dead = ControlCommand::MarkNodeDead {
            node: DataNodeId(5),
            incarnation: 1,
        };
        assert_eq!(state.apply(11, &request(1, 1, dead)), Outcome::Applied);
        assert_eq!(state.map().nodes[&DataNodeId(5)].status, NodeStatus::Dead);
        let holders: Vec<DataNodeId> = state.map().shards[&shard0()]
            .replicas
            .iter()
            .map(|r| r.node)
            .collect();
        let chosen = replacement(state.map(), &shard0()).expect("a free node");
        assert!(!holders.contains(&chosen));
        assert_ne!(chosen, DataNodeId(5));
    }

    #[test]
    fn the_same_commands_give_the_same_state() {
        let run = || {
            let mut state = StateMachine::new();
            register(&mut state, 4);
            create(&mut state, 10);
            state
        };
        assert_eq!(run(), run());
    }
}
