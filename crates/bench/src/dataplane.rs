//! `bench dataplane`: the Phase 4 exit checks (`docs/PLAN.md` §8), run on real processes
//! for both algorithms.
//!
//! 1. **Ground truth.** Load every GloVe-25 training vector (and SIFT, if downloaded) and
//!    compare search results with the dataset's true nearest neighbors.
//! 2. **Data-node crash.** Kill a shard's primary with `SIGKILL` while clients write; the
//!    shard must accept writes again, re-replication must restore three in-sync copies,
//!    and no acknowledged write may be lost.
//! 3. **Control-plane leader partition.** Cut the leader off with a minority while data
//!    nodes still send it heartbeats.
//! 4. **Primary partition.** Cut a shard's primary off from the control plane.
//!
//! Every test also runs the single-primary check (C1) and the no-lost-write check (C2).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use client::{ControlClient, Dataset, RetryPolicy, VectorClient};
use controlplane::Algorithm;
use controlplane::local::{LocalCluster, LocalOptions};
use rpc::proto::data_node_client::DataNodeClient;
use rpc::proto::{Empty, FaultsRequest, InspectRequest};
use rpc::{
    CollectionSpec, ControlCommand, DataNodeEvents, Metric, NodeStatus, ShardDump, ShardKey,
    ShardMap, shard_of,
};

use crate::checks;
use crate::cluster::{binary_dir, fresh_dir};
use crate::results::{self, Row};

const COLLECTION: &str = "vectors";

/// One check's result.
#[derive(Debug, Clone)]
pub struct CheckRow {
    algorithm: &'static str,
    test: &'static str,
    passed: bool,
    detail: String,
}

impl Row for CheckRow {
    fn header() -> &'static str {
        "algorithm,test,passed,detail"
    }

    fn line(&self) -> String {
        format!(
            "{},{},{},\"{}\"",
            self.algorithm,
            self.test,
            self.passed,
            self.detail.replace('"', "'")
        )
    }
}

/// The folder ANN-Benchmarks datasets are downloaded to (`cargo xtask dataset`).
pub fn dataset_dir() -> PathBuf {
    std::env::var_os("RVS_DATASETS").map_or_else(
        || {
            let home = std::env::var_os("HOME").unwrap_or_default();
            PathBuf::from(home).join(".cache/raft-vector-store/datasets")
        },
        PathBuf::from,
    )
}

/// Runs `bench dataplane` with its arguments.
///
/// # Errors
///
/// Returns a description of the problem if a check could not run or any check failed.
pub fn run(args: &[String]) -> Result<(), String> {
    let mut scratch = None;
    let mut algorithms = vec![Algorithm::Raft, Algorithm::Paxos];
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--scratch" => {
                scratch = Some(PathBuf::from(
                    iter.next().ok_or("--scratch needs a folder")?,
                ))
            }
            "--algorithm" => {
                algorithms = vec![Algorithm::parse(
                    iter.next().ok_or("--algorithm needs a name")?,
                )?];
            }
            other => return Err(format!("unknown option `{other}`")),
        }
    }
    let glove = dataset_dir().join("glove-25-angular.hdf5");
    if !glove.is_file() {
        return Err(format!(
            "{} is missing; run `cargo xtask dataset glove-25`",
            glove.display()
        ));
    }
    let sift = dataset_dir().join("sift-128-euclidean.hdf5");
    let output = results::prepare("dataplane", scratch.as_deref())?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    let mut rows = Vec::new();
    for algorithm in algorithms {
        let name = match algorithm {
            Algorithm::Raft => "raft",
            Algorithm::Paxos => "paxos",
        };
        // A check that cannot run is recorded as failed, and the others still run.
        let record = |rows: &mut Vec<CheckRow>,
                      test: &'static str,
                      result: Result<Vec<CheckRow>, String>| {
            match result {
                Ok(found) => rows.extend(found),
                Err(why) => rows.push(CheckRow {
                    algorithm: name,
                    test,
                    passed: false,
                    detail: format!("could not run: {why}"),
                }),
            }
        };
        eprintln!("{name}: ground truth (GloVe-25)");
        let result = runtime.block_on(ground_truth(algorithm, name, &glove, Metric::Angular, 1000));
        record(
            &mut rows,
            "ground truth (GloVe-25)",
            result.map(|r| vec![r]),
        );
        results::write_csv(&output, "checks.csv", &rows)?;
        if sift.is_file() {
            eprintln!("{name}: ground truth (SIFT)");
            let result =
                runtime.block_on(ground_truth(algorithm, name, &sift, Metric::Euclidean, 200));
            record(&mut rows, "ground truth (SIFT)", result.map(|r| vec![r]));
            results::write_csv(&output, "checks.csv", &rows)?;
        }
        for (fault, test) in [
            (Fault::KillPrimary, "data-node crash"),
            (Fault::PartitionLeader, "control-plane leader partition"),
            (Fault::PartitionPrimary, "primary partition"),
        ] {
            eprintln!("{name}: {test}");
            let result = runtime.block_on(fault_test(algorithm, name, fault, &glove));
            record(&mut rows, test, result);
            results::write_csv(&output, "checks.csv", &rows)?;
        }
    }
    results::write_manifest(&output, "dataplane", None, None, rows.len())?;
    let failed: Vec<&CheckRow> = rows.iter().filter(|r| !r.passed).collect();
    for row in &rows {
        println!(
            "{:<6} {:<34} {}  {}",
            row.algorithm,
            row.test,
            if row.passed { "PASS" } else { "FAIL" },
            row.detail
        );
    }
    println!("Results: {}", output.dir.display());
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!("{} checks failed", failed.len()))
    }
}

/// A running cluster of 3 control-plane servers and 4 data nodes.
struct Env {
    cluster: LocalCluster,
    servers: Vec<(u64, String)>,
}

impl Env {
    async fn start(algorithm: Algorithm, label: &str) -> Result<Env, String> {
        let bins = binary_dir()?;
        let options = LocalOptions {
            algorithm,
            election_timeout_ms: (150, 300),
            delay_us: 0,
            record: true,
        };
        let mut cluster =
            LocalCluster::start(&bins.join("controlplane"), &fresh_dir(label), 3, options)?;
        cluster.start_data_nodes(&bins.join("datanode"), 4)?;
        let servers = cluster.servers().to_vec();
        let env = Env { cluster, servers };
        // Wait until every data node is registered and alive.
        let mut control = env.control(1)?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(map) = control.shard_map(true).await
                && map
                    .nodes
                    .values()
                    .filter(|n| n.status == NodeStatus::Alive)
                    .count()
                    == 4
            {
                return Ok(env);
            }
            if Instant::now() > deadline {
                return Err("the data nodes did not register".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn control(&self, id: u64) -> Result<ControlClient, String> {
        let policy = RetryPolicy {
            give_up: Some(Duration::from_secs(30)),
            ..RetryPolicy::default()
        };
        ControlClient::new(&self.servers, policy, rpc::now_micros() ^ id)
    }

    async fn map(&self) -> Result<ShardMap, String> {
        self.control(7)?
            .shard_map(true)
            .await
            .map_err(|err| err.to_string())
    }

    fn data_client(
        &self,
        address: &str,
    ) -> Result<DataNodeClient<tonic::transport::Channel>, String> {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .map_err(|err| err.to_string())?;
        Ok(DataNodeClient::new(endpoint.connect_lazy()))
    }

    fn data_address(&self, node: u64) -> Option<String> {
        self.cluster
            .data_nodes()
            .iter()
            .find(|(id, _)| *id == node)
            .map(|(_, a)| a.clone())
    }
}

async fn create(env: &Env, dimension: u32, metric: Metric) -> Result<(), String> {
    let command = ControlCommand::CreateCollection {
        name: COLLECTION.into(),
        spec: CollectionSpec {
            dimension,
            metric,
            shards: 4,
            replication: 3,
        },
        placement: Vec::new(),
    };
    let committed = env
        .control(2)?
        .propose(command)
        .await
        .map_err(|err| err.to_string())?;
    if committed.outcome != rpc::Outcome::Applied {
        return Err(format!(
            "cannot create the collection: {:?}",
            committed.outcome
        ));
    }
    Ok(())
}

/// Upserts training vectors `0..count` with 64 concurrent writers.
async fn load(env: &Env, dataset: &Arc<Dataset>, count: usize) -> Result<(), String> {
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for worker in 0..64 {
        let mut vectors = VectorClient::new(env.control(100 + worker)?);
        let dataset = Arc::clone(dataset);
        let next = Arc::clone(&next);
        tasks.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= count {
                    return Ok::<(), String>(());
                }
                vectors
                    .upsert(COLLECTION, i as u64, dataset.vector(i).to_vec())
                    .await
                    .map_err(|err| err.to_string())?;
            }
        }));
    }
    for task in tasks {
        task.await.map_err(|err| err.to_string())??;
    }
    Ok(())
}

async fn ground_truth(
    algorithm: Algorithm,
    name: &'static str,
    path: &Path,
    metric: Metric,
    queries: usize,
) -> Result<CheckRow, String> {
    let dataset = Arc::new(Dataset::load(path)?);
    let env = Env::start(algorithm, "truth").await?;
    create(&env, dataset.dimension as u32, metric).await?;
    let started = Instant::now();
    load(&env, &dataset, dataset.len()).await?;
    let loaded = started.elapsed();
    let mut vectors = VectorClient::new(env.control(3)?);
    const K: usize = 10;
    let (mut exact, mut ties, mut wrong) = (0, 0, 0);
    for q in 0..queries.min(dataset.queries()) {
        let hits = vectors
            .search(COLLECTION, dataset.query(q), K as u32)
            .await
            .map_err(|err| err.to_string())?;
        let ids: Vec<u64> = hits.iter().map(|h| h.id).collect();
        if ids == dataset.neighbors[q][..K] {
            exact += 1;
        } else if hits
            .iter()
            .zip(&dataset.distances[q][..K])
            .all(|(hit, truth)| (hit.distance - truth).abs() <= 1e-4 * truth.abs().max(1.0))
        {
            // Same distances, different IDs: neighbors at equal distance in another order.
            ties += 1;
        } else {
            wrong += 1;
        }
    }
    let label = path
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_default();
    Ok(CheckRow {
        algorithm: name,
        test: if metric == Metric::Angular {
            "ground truth (GloVe-25)"
        } else {
            "ground truth (SIFT)"
        },
        passed: wrong == 0,
        detail: format!(
            "{label}: {} vectors loaded in {:.1} s; {} queries, top-{K}: {exact} exact, {ties} equal-distance ties, {wrong} wrong",
            dataset.len(),
            loaded.as_secs_f64(),
            exact + ties + wrong
        ),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    KillPrimary,
    PartitionLeader,
    PartitionPrimary,
}

/// An acknowledged write: its shard, vector ID, when the acknowledgment arrived, and the
/// epoch it was acknowledged at.
type Ack = (ShardKey, u64, Instant, rpc::Epoch);

/// Clients that upsert new vectors continuously and record every acknowledged write.
struct Writers {
    stop: Arc<AtomicBool>,
    acknowledged: Arc<Mutex<Vec<Ack>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

fn start_writers(
    env: &Env,
    clients: u64,
    dimension: usize,
    shards: u32,
) -> Result<Writers, String> {
    let stop = Arc::new(AtomicBool::new(false));
    let acknowledged = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for worker in 0..clients {
        let mut vectors = VectorClient::new(env.control(200 + worker)?);
        vectors.give_up = Duration::from_secs(60);
        let stop = Arc::clone(&stop);
        let acknowledged = Arc::clone(&acknowledged);
        tasks.push(tokio::spawn(async move {
            let mut id = 10_000_000 + worker * 1_000_000_000;
            let mut seed = id;
            while !stop.load(Ordering::Relaxed) {
                let vector: Vec<f32> = (0..dimension)
                    .map(|_| {
                        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                        (seed >> 40) as f32 / (1u64 << 24) as f32
                    })
                    .collect();
                if let Ok(ack) = vectors.upsert(COLLECTION, id, vector).await {
                    let shard = ShardKey {
                        collection: COLLECTION.into(),
                        shard: shard_of(id, shards),
                    };
                    acknowledged.lock().expect("no task panicked").push((
                        shard,
                        id,
                        Instant::now(),
                        ack.epoch,
                    ));
                }
                id += 1;
            }
        }));
    }
    Ok(Writers {
        stop,
        acknowledged,
        tasks,
    })
}

impl Writers {
    async fn stop(self) -> Vec<Ack> {
        self.stop.store(true, Ordering::Relaxed);
        for task in self.tasks {
            let _ = task.await;
        }
        Arc::try_unwrap(self.acknowledged)
            .map(|m| m.into_inner().expect("no task panicked"))
            .unwrap_or_default()
    }
}

async fn set_control_faults(env: &Env, id: u64, drop: Vec<u64>) -> Result<(), String> {
    let mut raw = env.control(9)?.server(id).ok_or("no such server")?;
    let request = FaultsRequest {
        drop_to: drop.clone(),
        drop_from: drop,
        delay_us: 0,
    };
    raw.set_faults(request)
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

async fn leader(env: &Env) -> Option<u64> {
    for (id, _) in &env.servers {
        let mut raw = env.control(10).ok()?.server(*id)?;
        if let Ok(Ok(status)) =
            tokio::time::timeout(Duration::from_millis(300), raw.status(Empty {})).await
            && status.into_inner().role == "leader"
        {
            return Some(*id);
        }
    }
    None
}

async fn fault_test(
    algorithm: Algorithm,
    name: &'static str,
    fault: Fault,
    glove: &Path,
) -> Result<Vec<CheckRow>, String> {
    let dataset = Arc::new(Dataset::load(glove)?);
    let label = match fault {
        Fault::KillPrimary => "crash",
        Fault::PartitionLeader => "cp-partition",
        Fault::PartitionPrimary => "primary-partition",
    };
    let mut env = Env::start(algorithm, label).await?;
    create(&env, dataset.dimension as u32, Metric::Angular).await?;
    let preload = 20_000;
    load(&env, &dataset, preload).await?;
    let writers = start_writers(&env, 4, dataset.dimension, 4)?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let shard0 = ShardKey {
        collection: COLLECTION.into(),
        shard: 0,
    };
    let map = env.map().await?;
    let primary = map.shards[&shard0]
        .primary
        .ok_or("shard 0 has no primary")?
        .0;
    let first_epoch = map.shards[&shard0].epoch;
    let faulted = Instant::now();
    let mut healed = faulted;
    let test: &'static str;
    match fault {
        Fault::KillPrimary => {
            test = "data-node crash";
            env.cluster.kill_data(primary);
            tokio::time::sleep(Duration::from_secs(12)).await;
        }
        Fault::PartitionLeader => {
            test = "control-plane leader partition";
            let old = leader(&env).await.ok_or("no leader")?;
            let others: Vec<u64> = env
                .servers
                .iter()
                .map(|(id, _)| *id)
                .filter(|id| *id != old)
                .collect();
            set_control_faults(&env, old, others.clone()).await?;
            for &other in &others {
                set_control_faults(&env, other, vec![old]).await?;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
            for (id, _) in env.servers.clone() {
                set_control_faults(&env, id, Vec::new()).await?;
            }
            healed = Instant::now();
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        Fault::PartitionPrimary => {
            test = "primary partition";
            let address = env.data_address(primary).ok_or("no such data node")?;
            let mut client = env.data_client(&address)?;
            let all: Vec<u64> = env.servers.iter().map(|(id, _)| *id).collect();
            client
                .set_faults(FaultsRequest {
                    drop_to: all,
                    drop_from: Vec::new(),
                    delay_us: 0,
                })
                .await
                .map_err(|err| err.to_string())?;
            tokio::time::sleep(Duration::from_secs(12)).await;
            client
                .set_faults(FaultsRequest::default())
                .await
                .map_err(|err| err.to_string())?;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    let acknowledged = writers.stop().await;

    // A crash or a partition of the primary must move shard 0 to a new primary, at a newer
    // epoch, which then acknowledges writes. A partition of the control-plane leader must
    // not stop writes at all.
    let new_epoch_after = acknowledged
        .iter()
        .filter(|(shard, _, at, epoch)| *shard == shard0 && *at > faulted && *epoch > first_epoch)
        .map(|(_, _, at, _)| at.duration_since(faulted))
        .min();
    let during = acknowledged
        .iter()
        .filter(|(shard, _, at, _)| *shard == shard0 && *at > faulted && *at < healed)
        .count();
    // Re-replication restores three in-sync copies of every shard.
    let deadline = Instant::now() + Duration::from_secs(40);
    let map = loop {
        let map = env.map().await?;
        let restored = map.shards.values().all(|info| {
            info.in_sync.len() == 3
                && info.replicas.len() == 3
                && info.in_sync.iter().all(|n| {
                    map.nodes
                        .get(n)
                        .is_some_and(|i| i.status == NodeStatus::Alive)
                })
        });
        if restored || Instant::now() > deadline {
            break map;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let restored = map
        .shards
        .values()
        .all(|info| info.in_sync.len() == 3 && info.replicas.len() == 3);

    // C2: every acknowledged write is on every in-sync replica.
    let mut expected: BTreeMap<ShardKey, BTreeSet<u64>> = BTreeMap::new();
    for id in 0..preload as u64 {
        expected
            .entry(ShardKey {
                collection: COLLECTION.into(),
                shard: shard_of(id, 4),
            })
            .or_default()
            .insert(id);
    }
    for (shard, id, _, _) in &acknowledged {
        expected.entry(shard.clone()).or_default().insert(*id);
    }
    let mut held = BTreeMap::new();
    let mut in_sync = BTreeMap::new();
    for (key, info) in &map.shards {
        in_sync.insert(
            key.clone(),
            info.in_sync.iter().map(|n| n.0).collect::<Vec<_>>(),
        );
        for node in &info.in_sync {
            let Some(address) = env.data_address(node.0) else {
                continue;
            };
            let mut client = env.data_client(&address)?;
            let reply = client
                .inspect(InspectRequest {
                    shard: rpc::encode(key),
                })
                .await
                .map_err(|err| err.to_string())?;
            let dump: ShardDump = rpc::decode(&reply.into_inner().dump)?;
            held.insert(
                (key.clone(), node.0),
                dump.ids.into_iter().collect::<BTreeSet<u64>>(),
            );
        }
    }
    let lost = checks::no_lost_writes(&expected, &held, &in_sync);

    // C1: at most one serving primary, from every data node still running.
    let mut logs: Vec<DataNodeEvents> = Vec::new();
    for (id, address) in env.cluster.data_nodes().to_vec() {
        let mut client = env.data_client(&address)?;
        if let Ok(Ok(reply)) =
            tokio::time::timeout(Duration::from_secs(2), client.events(Empty {})).await
            && let Ok(events) = rpc::decode::<DataNodeEvents>(&reply.into_inner().events)
        {
            logs.push(events);
        } else if id != primary || fault != Fault::KillPrimary {
            return Err(format!("data node {id} did not return its log"));
        }
    }
    let doubled = checks::single_primary(&logs);
    let serves: usize = logs.iter().map(|l| l.serves.len()).sum();
    env.cluster.stop();

    let writes = acknowledged.len();
    Ok(vec![
        CheckRow {
            algorithm: name,
            test,
            passed: restored
                && match fault {
                    Fault::KillPrimary | Fault::PartitionPrimary => new_epoch_after.is_some(),
                    Fault::PartitionLeader => during > 0,
                },
            detail: match fault {
                Fault::KillPrimary | Fault::PartitionPrimary => format!(
                    "{writes} writes acknowledged; shard 0 acknowledged its first write at a newer epoch {} after the fault; three in-sync copies restored: {restored}",
                    new_epoch_after
                        .map_or("never".to_string(), |d| format!("{:.2} s", d.as_secs_f64()))
                ),
                Fault::PartitionLeader => format!(
                    "{writes} writes acknowledged; shard 0 acknowledged {during} writes while the leader was cut off; three in-sync copies restored: {restored}"
                ),
            },
        },
        CheckRow {
            algorithm: name,
            test: match fault {
                Fault::KillPrimary => "data-node crash: no lost write (C2)",
                Fault::PartitionLeader => "leader partition: no lost write (C2)",
                Fault::PartitionPrimary => "primary partition: no lost write (C2)",
            },
            passed: lost.is_empty(),
            detail: if lost.is_empty() {
                format!(
                    "{} acknowledged writes found on every in-sync replica",
                    expected.values().map(BTreeSet::len).sum::<usize>()
                )
            } else {
                lost.join("; ")
            },
        },
        CheckRow {
            algorithm: name,
            test: match fault {
                Fault::KillPrimary => "data-node crash: single primary (C1)",
                Fault::PartitionLeader => "leader partition: single primary (C1)",
                Fault::PartitionPrimary => "primary partition: single primary (C1)",
            },
            passed: doubled.is_empty(),
            detail: if doubled.is_empty() {
                format!("{serves} served requests checked")
            } else {
                doubled.join("; ")
            },
        },
    ])
}
