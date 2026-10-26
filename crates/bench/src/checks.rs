//! Checks of the data plane's correctness claims (`docs/DESIGN.md` §7), run over the logs
//! that data nodes and clients write. Phase 4 and experiment E8 use them
//! (`docs/PLAN.md` §8, `docs/RESEARCH.md` amendment A3.3).

use std::collections::{BTreeMap, BTreeSet};

use rpc::{DataNodeEvents, Epoch, ShardKey};

/// A violation of C1 or C2, described.
pub type Violation = String;

/// C1, at most one serving primary: checks the requests every data node served as
/// primary. Every process runs on one machine and reads the same clock, so the times can
/// be compared directly.
///
/// Two things would mean two primaries served a shard at once:
///
/// - two data nodes served the shard at the same epoch, or
/// - a data node served the shard at an older epoch after some node had already served it
///   at a newer one.
pub fn single_primary(logs: &[DataNodeEvents]) -> Vec<Violation> {
    let mut by_shard: BTreeMap<&ShardKey, Vec<(u64, u64, Epoch)>> = BTreeMap::new();
    for log in logs {
        for serve in &log.serves {
            by_shard
                .entry(&serve.shard)
                .or_default()
                .push((serve.time, log.node, serve.epoch));
        }
    }
    let mut violations = Vec::new();
    for (shard, mut serves) in by_shard {
        serves.sort_unstable();
        let mut server_of: BTreeMap<Epoch, u64> = BTreeMap::new();
        let mut newest = Epoch(0);
        for (time, node, epoch) in serves {
            match server_of.get(&epoch) {
                Some(&other) if other != node => violations.push(format!(
                    "shard {}/{}: nodes {other} and {node} both served epoch {}",
                    shard.collection, shard.shard, epoch.0
                )),
                Some(_) => {}
                None => {
                    server_of.insert(epoch, node);
                }
            }
            if epoch < newest {
                violations.push(format!(
                    "shard {}/{}: node {node} served epoch {} at {time} after epoch {} had been served",
                    shard.collection, shard.shard, epoch.0, newest.0
                ));
            }
            newest = newest.max(epoch);
        }
    }
    violations
}

/// C2, no acknowledged write lost: every acknowledged vector ID must be held by the shard's
/// primary and by every in-sync replica. `held` maps each replica to the IDs it holds.
pub fn no_lost_writes(
    acknowledged: &BTreeMap<ShardKey, BTreeSet<u64>>,
    held: &BTreeMap<(ShardKey, u64), BTreeSet<u64>>,
    in_sync: &BTreeMap<ShardKey, Vec<u64>>,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    for (shard, ids) in acknowledged {
        let Some(replicas) = in_sync.get(shard) else {
            violations.push(format!(
                "shard {}/{} has no in-sync replicas",
                shard.collection, shard.shard
            ));
            continue;
        };
        for node in replicas {
            let empty = BTreeSet::new();
            let holds = held.get(&(shard.clone(), *node)).unwrap_or(&empty);
            let missing = ids.difference(holds).count();
            if missing > 0 {
                violations.push(format!(
                    "shard {}/{}: in-sync replica {node} is missing {missing} acknowledged writes",
                    shard.collection, shard.shard
                ));
            }
        }
    }
    violations
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use rpc::{DataNodeEvents, Epoch, ServeEvent, ShardKey};

    use super::{no_lost_writes, single_primary};

    fn shard() -> ShardKey {
        ShardKey {
            collection: "c".into(),
            shard: 0,
        }
    }

    fn serve(time: u64, epoch: u64) -> ServeEvent {
        ServeEvent {
            time,
            shard: shard(),
            epoch: Epoch(epoch),
            write: true,
        }
    }

    fn log(node: u64, serves: Vec<ServeEvent>) -> DataNodeEvents {
        DataNodeEvents {
            node,
            incarnation: 1,
            serves,
        }
    }

    #[test]
    fn a_clean_failover_passes() {
        let logs = [
            log(1, vec![serve(10, 1), serve(20, 1)]),
            log(2, vec![serve(30, 2)]),
        ];
        assert!(single_primary(&logs).is_empty());
    }

    #[test]
    fn an_old_primary_serving_after_the_new_one_fails() {
        let logs = [
            log(1, vec![serve(10, 1), serve(40, 1)]),
            log(2, vec![serve(30, 2)]),
        ];
        assert_eq!(single_primary(&logs).len(), 1);
    }

    #[test]
    fn two_nodes_at_one_epoch_fail() {
        let logs = [log(1, vec![serve(10, 1)]), log(2, vec![serve(20, 1)])];
        assert_eq!(single_primary(&logs).len(), 1);
    }

    #[test]
    fn a_missing_write_is_reported() {
        let acknowledged = BTreeMap::from([(shard(), BTreeSet::from([1, 2, 3]))]);
        let held = BTreeMap::from([
            ((shard(), 1), BTreeSet::from([1, 2, 3])),
            ((shard(), 2), BTreeSet::from([1, 2])),
        ]);
        let in_sync = BTreeMap::from([(shard(), vec![1, 2])]);
        assert_eq!(no_lost_writes(&acknowledged, &held, &in_sync).len(), 1);
    }
}
