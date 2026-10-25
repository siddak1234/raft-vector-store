//! E8: vector store recovery on the real cluster (`docs/RESEARCH.md` §6, with details in
//! amendments A3.3 and A5).
//!
//! Each trial starts the control plane (3 or 5 servers) and 4 data nodes, creates one
//! collection with 4 shards of 3 replicas, loads 100,000 GloVe-25 vectors, and runs 4
//! clients that upsert new vectors continuously. After 2 s:
//!
//! - **S1** kills the primary of shard 0 with `SIGKILL`;
//! - **S2** kills the primary of shard 0 and the control-plane leader in the same instant.
//!
//! Metric M21 runs from the kill until a client receives the first acknowledged write to
//! shard 0 from its new primary, at a newer epoch: an acknowledgment still in flight from
//! the killed primary does not count. Every trial also runs the single-primary (C1) and
//! no-lost-write (C2) checks.

use std::sync::Arc;
use std::time::{Duration, Instant};

use client::Dataset;
use rpc::{Metric, NodeStatus, ShardKey};

use crate::config::RunConfig;
use crate::dataplane::{COLLECTION, Env, create, load, start_writers};
use crate::results::Row;
use crate::scenarios::{optional, run_columns};

/// Vectors loaded before the fault.
pub const PRELOAD: usize = 100_000;
/// How long to wait for shard 0 to accept writes again.
const CAP: Duration = Duration::from_secs(60);

/// One E8 trial.
#[derive(Debug, Clone)]
pub struct VectorRow {
    pub(crate) run: String,
    pub(crate) seed: u64,
    pub(crate) ready: bool,
    /// Metric M21.
    pub(crate) recovery_ms: Option<f64>,
    pub(crate) writes: usize,
    pub(crate) lost_writes: usize,
    pub(crate) double_primaries: usize,
    pub(crate) violations: usize,
}

impl Row for VectorRow {
    fn header() -> &'static str {
        concat!(
            "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,",
            "election_timeout_max_ms,uncommitted,",
            "seed,ready,recovery_ms,writes,lost_writes,double_primaries,violations"
        )
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{}",
            self.run,
            self.seed,
            self.ready,
            optional(self.recovery_ms),
            self.writes,
            self.lost_writes,
            self.double_primaries,
            self.violations
        )
    }
}

/// Runs one E8 trial. `crash_leader` selects S2.
pub async fn trial(
    run: &RunConfig,
    seed: u64,
    crash_leader: bool,
    dataset: &Arc<Dataset>,
) -> VectorRow {
    let mut row = VectorRow {
        run: run_columns(run),
        seed,
        ready: false,
        recovery_ms: None,
        writes: 0,
        lost_writes: 0,
        double_primaries: 0,
        violations: 0,
    };
    let Ok(mut env) = Env::start_with(run, seed).await else {
        return row;
    };
    if create(&env, dataset.dimension as u32, Metric::Angular)
        .await
        .is_err()
        || load(&env, dataset, PRELOAD).await.is_err()
    {
        return row;
    }
    let Ok(writers) = start_writers(&env, 4, dataset.dimension, 4) else {
        return row;
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    let shard0 = ShardKey {
        collection: COLLECTION.into(),
        shard: 0,
    };
    let Ok(map) = env.map().await else {
        return row;
    };
    let Some(primary) = map.shards.get(&shard0).and_then(|info| info.primary) else {
        return row;
    };
    let old_epoch = map.shards[&shard0].epoch;
    let leader = if crash_leader {
        env.leader().await
    } else {
        None
    };
    if crash_leader && leader.is_none() {
        return row;
    }
    row.ready = true;
    let killed = Instant::now();
    env.cluster.kill_data(primary.0);
    if let Some(leader) = leader {
        env.cluster.kill(leader);
    }
    // Wait until shard 0 acknowledges a write from a new primary.
    let deadline = killed + CAP;
    let mut recovered = None;
    while Instant::now() < deadline {
        recovered = writers.first_ack_after(&shard0, killed, old_epoch);
        if recovered.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    row.recovery_ms = recovered.map(|d| d.as_secs_f64() * 1000.0);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let acknowledged = writers.stop().await;
    row.writes = acknowledged.len();

    // Wait for re-replication to settle, then run C2 and C1.
    let settle = Instant::now() + Duration::from_secs(20);
    loop {
        let Ok(map) = env.map().await else {
            return row;
        };
        let settled = map.shards.values().all(|info| {
            info.primary.is_some_and(|p| {
                map.nodes
                    .get(&p)
                    .is_some_and(|n| n.status == NodeStatus::Alive)
            }) && info.in_sync.len() >= 2
        });
        if settled || Instant::now() > settle {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    match env.check(&acknowledged, PRELOAD).await {
        Ok((lost, doubled, _, _)) => {
            row.lost_writes = lost.len();
            row.double_primaries = doubled.len();
        }
        Err(_) => row.ready = false,
    }
    row.violations = row.lost_writes + row.double_primaries;
    env.cluster.stop();
    row
}
