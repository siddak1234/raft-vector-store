//! The vector-store client: routing to shard primaries, refreshing a stale shard map, and
//! merging search results (`docs/DESIGN.md` §6.5).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use client::data::merge;
use client::{Acknowledged, ControlClient, RetryPolicy, VectorClient, VectorError};
use rpc::{
    CollectionSpec, DataNodeId, DataNodeInfo, Epoch, Metric, Neighbor, NodeStatus, Refusal,
    Replica, SearchResult, ShardInfo, ShardKey, ShardMap, WriteOp, WriteResult, shard_of,
};

use crate::fake::{self, Answer, Control, Node};

const COLLECTION: &str = "vectors";

/// A shard map of one collection whose shard `i` has the primary `primaries[i]`, a data
/// node `(id, address)`, at `epoch`.
fn map(version: u64, primaries: &[(u64, &str)], epoch: u64) -> ShardMap {
    let shards = primaries.len() as u32;
    let collections = BTreeMap::from([(
        COLLECTION.to_string(),
        CollectionSpec {
            dimension: 2,
            metric: Metric::Euclidean,
            shards,
            replication: 1,
        },
    )]);
    let nodes = primaries
        .iter()
        .map(|&(id, address)| {
            (
                DataNodeId(id),
                DataNodeInfo {
                    address: address.to_string(),
                    incarnation: 1,
                    status: NodeStatus::Alive,
                },
            )
        })
        .collect();
    let shards = primaries
        .iter()
        .enumerate()
        .map(|(number, &(id, _))| {
            let node = DataNodeId(id);
            (
                ShardKey {
                    collection: COLLECTION.to_string(),
                    shard: number as u32,
                },
                ShardInfo {
                    replicas: vec![Replica {
                        node,
                        incarnation: 1,
                    }],
                    primary: Some(node),
                    in_sync: BTreeSet::from([node]),
                    epoch: Epoch(epoch),
                },
            )
        })
        .collect();
    ShardMap {
        version,
        collections,
        nodes,
        shards,
    }
}

async fn client(control: &Arc<Control>, give_up: Duration) -> VectorClient {
    let address = fake::serve_control(Arc::clone(control)).await;
    let control =
        ControlClient::new(&[(control.id, address)], RetryPolicy::default(), 1).expect("client");
    let mut client = VectorClient::new(control);
    client.give_up = give_up;
    client
}

fn acknowledged(offset: u64) -> WriteResult {
    WriteResult::Acknowledged { offset }
}

fn found(hits: &[(u64, f32)]) -> SearchResult {
    SearchResult::Found(
        hits.iter()
            .map(|&(id, distance)| Neighbor { id, distance })
            .collect(),
    )
}

fn upsert(id: u64) -> WriteOp {
    WriteOp::Upsert {
        id,
        vector: vec![0.0, 1.0],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_goes_to_the_primary_of_its_shard() {
    let first = Node::new([acknowledged(1)], [found(&[])]);
    let second = Node::new([acknowledged(9)], [found(&[])]);
    let addresses = [
        fake::serve_node(Arc::clone(&first)).await,
        fake::serve_node(Arc::clone(&second)).await,
    ];
    let control = Control::with_maps(
        1,
        [Answer::Lead],
        [map(1, &[(10, &addresses[0]), (20, &addresses[1])], 4)],
    );
    let mut client = client(&control, Duration::from_secs(5)).await;
    let id = (0..)
        .find(|&id| shard_of(id, 2) == 1)
        .expect("some ID is in shard 1");

    let ack = client
        .write(COLLECTION, upsert(id))
        .await
        .expect("acknowledged");
    assert_eq!(
        ack,
        Acknowledged {
            epoch: Epoch(4),
            offset: 9
        }
    );
    let writes = second.writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].shard.shard, 1);
    assert_eq!(writes[0].epoch, Epoch(4));
    assert_eq!(writes[0].op, upsert(id));
    assert!(first.writes().is_empty());
    assert_eq!(control.map_reads(), [false]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_write_reads_the_map_from_the_leader_and_retries() {
    let old = Node::new(
        [WriteResult::Refused(Refusal::NotPrimary {
            epoch: Epoch(2),
        })],
        [found(&[])],
    );
    let new = Node::new([acknowledged(3)], [found(&[])]);
    let addresses = [
        fake::serve_node(Arc::clone(&old)).await,
        fake::serve_node(Arc::clone(&new)).await,
    ];
    // The first read returns a map in which the old primary still serves the shard.
    let control = Control::with_maps(
        1,
        [Answer::Lead],
        [
            map(1, &[(10, &addresses[0])], 1),
            map(2, &[(20, &addresses[1])], 2),
        ],
    );
    let mut client = client(&control, Duration::from_secs(5)).await;

    let ack = client
        .write(COLLECTION, upsert(7))
        .await
        .expect("acknowledged");
    assert_eq!(
        ack,
        Acknowledged {
            epoch: Epoch(2),
            offset: 3
        }
    );
    assert_eq!(old.writes().len(), 1);
    assert_eq!(new.writes()[0].epoch, Epoch(2));
    assert_eq!(control.map_reads(), [false, true]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_collection_missing_from_a_stale_map_is_read_from_the_leader() {
    let node = Node::new([acknowledged(1)], [found(&[])]);
    let address = fake::serve_node(Arc::clone(&node)).await;
    let control = Control::with_maps(
        1,
        [Answer::Lead],
        [ShardMap::default(), map(2, &[(10, &address)], 1)],
    );
    let mut client = client(&control, Duration::from_secs(5)).await;

    client
        .write(COLLECTION, upsert(7))
        .await
        .expect("acknowledged");
    assert_eq!(control.map_reads(), [false, true]);
    assert_eq!(node.writes().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_that_no_primary_accepts_gives_up() {
    let node = Node::new([WriteResult::Refused(Refusal::NoLease)], [found(&[])]);
    let address = fake::serve_node(Arc::clone(&node)).await;
    let control = Control::with_maps(1, [Answer::Lead], [map(1, &[(10, &address)], 1)]);
    let give_up = Duration::from_millis(300);
    let mut client = client(&control, give_up).await;

    let started = Instant::now();
    assert_eq!(
        client.write(COLLECTION, upsert(7)).await,
        Err(VectorError::GaveUp)
    );
    assert!(started.elapsed() >= give_up);
    assert!(node.writes().len() > 1, "the write was retried");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_search_merges_the_nearest_from_every_shard() {
    let first = Node::new([acknowledged(1)], [found(&[(1, 0.5), (3, 0.1)])]);
    let second = Node::new([acknowledged(1)], [found(&[(2, 0.3), (4, 0.9)])]);
    let addresses = [
        fake::serve_node(Arc::clone(&first)).await,
        fake::serve_node(Arc::clone(&second)).await,
    ];
    let control = Control::with_maps(
        1,
        [Answer::Lead],
        [map(1, &[(10, &addresses[0]), (20, &addresses[1])], 1)],
    );
    let mut client = client(&control, Duration::from_secs(5)).await;

    let hits = client
        .search(COLLECTION, &[0.0, 1.0], 2)
        .await
        .expect("found");
    let ids: Vec<u64> = hits.iter().map(|n| n.id).collect();
    assert_eq!(ids, [3, 2]);
    assert_eq!(first.searches()[0].shard.shard, 0);
    assert_eq!(second.searches()[0].shard.shard, 1);
    assert_eq!(second.searches()[0].k, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_search_asks_again_only_the_shard_that_refused() {
    let first = Node::new([acknowledged(1)], [found(&[(1, 0.5)])]);
    let second = Node::new(
        [acknowledged(1)],
        [
            SearchResult::Refused(Refusal::NotPrimary { epoch: Epoch(1) }),
            found(&[(2, 0.3)]),
        ],
    );
    let addresses = [
        fake::serve_node(Arc::clone(&first)).await,
        fake::serve_node(Arc::clone(&second)).await,
    ];
    let control = Control::with_maps(
        1,
        [Answer::Lead],
        [map(1, &[(10, &addresses[0]), (20, &addresses[1])], 1)],
    );
    let mut client = client(&control, Duration::from_secs(5)).await;

    let hits = client
        .search(COLLECTION, &[0.0, 1.0], 5)
        .await
        .expect("found");
    assert_eq!(hits.len(), 2);
    assert_eq!(first.searches().len(), 1);
    assert_eq!(second.searches().len(), 2);
    assert!(control.map_reads().contains(&true));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_search_of_a_collection_the_leader_does_not_know_fails_at_once() {
    let control = Control::with_maps(1, [Answer::Lead], [ShardMap::default()]);
    let mut client = client(&control, Duration::from_secs(5)).await;
    let started = Instant::now();
    assert_eq!(
        client.search(COLLECTION, &[0.0, 1.0], 1).await,
        Err(VectorError::NoCollection(COLLECTION.to_string()))
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(control.map_reads(), [false, true]);
}

#[test]
fn merge_keeps_the_k_nearest_and_breaks_ties_by_id() {
    let hit = |id, distance| Neighbor { id, distance };
    let merged = merge(
        [
            vec![hit(5, 0.2), hit(1, 0.9)],
            vec![hit(4, 0.2), hit(2, 0.1)],
            vec![],
        ],
        3,
    );
    assert_eq!(merged, [hit(2, 0.1), hit(4, 0.2), hit(5, 0.2)]);
    assert!(merge([vec![hit(1, 0.5)]], 0).is_empty());
}
