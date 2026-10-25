//! `bench demo`: a live failover demonstration (`docs/PLAN.md` §8, Phase 6).
//!
//! Starts 3 control-plane servers and 4 data nodes, loads 20,000 GloVe-25 vectors, and runs
//! 4 writers while printing the cluster's state twice a second. It then:
//!
//! 1. kills the control-plane leader with `SIGKILL` and shows the election and that writes
//!    continue;
//! 2. kills the primary of shard 0 and shows the promotion, the recovery time, and
//!    re-replication;
//! 3. runs a search and checks that no acknowledged write was lost and that no two
//!    primaries served a shard at once.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use client::{Dataset, VectorClient};
use controlplane::Algorithm;
use rpc::proto::Empty;
use rpc::{Metric, NodeStatus, ShardKey};

use crate::dataplane::{COLLECTION, Env, create, dataset_dir, load, start_writers};

/// Runs `bench demo`.
///
/// # Errors
///
/// Returns a description if the cluster cannot start or the dataset is missing.
pub fn run(args: &[String]) -> Result<(), String> {
    let algorithm = match args {
        [flag, name] if flag == "--consensus" => Algorithm::parse(name)?,
        [] => Algorithm::Raft,
        _ => return Err("usage: bench demo [--consensus raft|paxos]".into()),
    };
    let glove = dataset_dir().join("glove-25-angular.hdf5");
    let dataset = Arc::new(
        Dataset::load(&glove)
            .map_err(|err| format!("{err}; run `cargo xtask dataset glove-25`"))?,
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    runtime.block_on(demo(algorithm, dataset))
}

async fn status(env: &Env, writes: usize, since: Instant) {
    let mut line = String::new();
    for (id, _) in &env.servers {
        let state = match env.control(50).ok().and_then(|c| c.server(*id)) {
            Some(mut raw) if env.cluster.is_running(*id) => {
                match tokio::time::timeout(Duration::from_millis(200), raw.status(Empty {})).await {
                    Ok(Ok(s)) => {
                        let s = s.into_inner();
                        format!("{}:{}@{}", id, s.role[..1].to_uppercase(), s.term)
                    }
                    _ => format!("{id}:?"),
                }
            }
            _ => format!("{id}:down"),
        };
        line.push_str(&format!("{state:<10}"));
    }
    println!(
        "  t={:>5.1}s  servers {line} acknowledged writes {writes}",
        since.elapsed().as_secs_f64()
    );
}

async fn demo(algorithm: Algorithm, dataset: Arc<Dataset>) -> Result<(), String> {
    println!("Starting 3 control-plane servers ({algorithm:?}) and 4 data nodes ...");
    let mut env = Env::start(algorithm, "demo").await?;
    create(&env, dataset.dimension as u32, Metric::Angular).await?;
    println!(
        "Created collection `{COLLECTION}`: 4 shards x 3 replicas. Loading 20,000 GloVe-25 vectors ..."
    );
    load(&env, &dataset, 20_000).await?;
    let writers = start_writers(&env, 4, dataset.dimension, 4)?;
    let started = Instant::now();
    let tick =
        |n: usize| async move { tokio::time::sleep(Duration::from_millis(500 * n as u64)).await };

    for _ in 0..4 {
        tick(1).await;
        status(&env, writers.count(), started).await;
    }
    let (leader, term) = env.leader_and_term().await.ok_or("no leader")?;
    println!("\n>>> kill -9 control-plane leader {leader} (term {term})");
    let killed = Instant::now();
    env.cluster.kill(leader);
    let mut elected = None;
    while killed.elapsed() < Duration::from_secs(10) {
        if let Some((new, new_term)) = env.leader_and_term().await
            && new_term > term
        {
            elected = Some((new, new_term, killed.elapsed()));
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    match elected {
        Some((new, new_term, after)) => println!(
            ">>> server {new} leads term {new_term}, {:.0} ms after the crash (as observed by polling)",
            after.as_secs_f64() * 1000.0
        ),
        None => println!(">>> no new leader within 10 s"),
    }
    for _ in 0..4 {
        tick(1).await;
        status(&env, writers.count(), started).await;
    }

    let shard0 = ShardKey {
        collection: COLLECTION.into(),
        shard: 0,
    };
    let map = env.map().await?;
    let primary = map.shards[&shard0]
        .primary
        .ok_or("shard 0 has no primary")?;
    let epoch = map.shards[&shard0].epoch;
    println!(
        "\n>>> kill -9 data node {}, primary of shard 0 at epoch {}",
        primary.0, epoch.0
    );
    let killed = Instant::now();
    env.cluster.kill_data(primary.0);
    let mut recovered = None;
    while killed.elapsed() < Duration::from_secs(30) {
        if let Some(after) = writers.first_ack_after(&shard0, killed, epoch) {
            recovered = Some(after);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let map = env.map().await?;
    let info = &map.shards[&shard0];
    match recovered {
        Some(after) => println!(
            ">>> shard 0 accepts writes again at epoch {} on data node {}, {:.2} s after the crash \
             (the lease must expire first: L + delta = 3.5 s from the last renewal)",
            info.epoch.0,
            info.primary.map_or(0, |p| p.0),
            after.as_secs_f64()
        ),
        None => println!(">>> shard 0 did not recover within 30 s"),
    }
    for _ in 0..6 {
        tick(1).await;
        status(&env, writers.count(), started).await;
    }
    let acknowledged = writers.stop().await;
    let map = env.map().await?;
    let copies: Vec<String> = map
        .shards
        .iter()
        .map(|(k, i)| {
            let alive = i
                .in_sync
                .iter()
                .filter(|n| {
                    map.nodes
                        .get(n)
                        .is_some_and(|x| x.status == NodeStatus::Alive)
                })
                .count();
            format!("shard {}: {alive} in-sync copies", k.shard)
        })
        .collect();
    println!("\nRe-replication: {}", copies.join(", "));

    let mut vectors = VectorClient::new(env.control(60)?);
    let hits = vectors
        .search(COLLECTION, dataset.query(0), 5)
        .await
        .map_err(|e| e.to_string())?;
    let ids: Vec<u64> = hits.iter().map(|h| h.id).collect();
    println!("A search for test query 0 returns {ids:?}");
    let acked: BTreeSet<u64> = acknowledged.iter().map(|(_, id, _, _)| *id).collect();
    println!("{} writes were acknowledged during the demo.", acked.len());
    let (lost, doubled, serves, expected) = env.check(&acknowledged, 20_000).await?;
    println!(
        "Checks: {} of {expected} acknowledged writes missing from an in-sync replica (C2); \
         {} single-primary violations over {serves} served requests (C1).",
        lost.len(),
        doubled.len()
    );
    env.cluster.stop();
    Ok(())
}
