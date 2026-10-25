//! `rvs`: the command-line client.
//!
//! ```text
//! rvs --cluster FILE status
//! rvs --cluster FILE propose-bench [COUNT]
//! rvs --cluster FILE map [--linearizable]
//! rvs --cluster FILE faults SERVER [--drop-to IDS] [--drop-from IDS] [--delay-us D]
//! rvs --cluster FILE create-collection NAME DIMENSION angular|euclidean SHARDS REPLICAS
//! rvs --cluster FILE load NAME DATASET [COUNT]
//! rvs --cluster FILE search NAME DATASET [QUERIES] [K]
//! ```

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use client::{ControlClient, Dataset, RetryPolicy, VectorClient};
use rpc::proto::{Empty, FaultsRequest};
use rpc::{CollectionSpec, ControlCommand, Metric};
use serde::Deserialize;

const HELP: &str = "\
Usage: rvs --cluster FILE COMMAND

Commands:
  status                       Show every control-plane server's role, term, and indexes
  propose-bench [COUNT]        Commit COUNT (default 1) empty commands and time them
  map [--linearizable]         Print the shard map
  faults SERVER [--drop-to IDS] [--drop-from IDS] [--delay-us D]
                               Set the faults SERVER injects (IDS is comma-separated);
                               with no options, clear them
  create-collection NAME DIMENSION angular|euclidean SHARDS REPLICAS
                               Create a collection; the leader places its shards
  load NAME DATASET [COUNT]    Upsert the first COUNT (default all) training vectors of an
                               ANN-Benchmarks HDF5 file, with 64 concurrent writers
  search NAME DATASET [QUERIES] [K]
                               Run QUERIES (default 100) test queries with K (default 10)
                               neighbors and compare them with the dataset's ground truth
";

#[derive(Deserialize)]
struct ClusterFile {
    server: Vec<ServerEntry>,
}

#[derive(Deserialize)]
struct ServerEntry {
    id: u64,
    address: String,
}

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(std::env::args().skip(1).collect())) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Vec<String>) -> Result<(), String> {
    let mut args = args.into_iter().peekable();
    let mut cluster = None;
    if args.peek().map(String::as_str) == Some("--cluster") {
        args.next();
        cluster = args.next().map(PathBuf::from);
    }
    let Some(path) = cluster else {
        return Err(HELP.to_string());
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    let file: ClusterFile =
        toml::from_str(&text).map_err(|err| format!("{}: {err}", path.display()))?;
    let servers: Vec<(u64, String)> = file.server.into_iter().map(|s| (s.id, s.address)).collect();
    let policy = RetryPolicy {
        give_up: Some(Duration::from_secs(10)),
        ..RetryPolicy::default()
    };
    let mut client = ControlClient::new(&servers, policy, rpc::now_micros())?;
    let rest: Vec<String> = args.collect();
    match rest.first().map(String::as_str) {
        Some("status") => status(&client, &servers).await,
        Some("propose-bench") => {
            let count: usize = rest
                .get(1)
                .map_or(Ok(1), |n| n.parse())
                .map_err(|_| "COUNT must be a number")?;
            let started = Instant::now();
            let mut last = 0;
            for _ in 0..count {
                let committed = client
                    .propose(ControlCommand::Bench {
                        padding: vec![0; 100],
                    })
                    .await
                    .map_err(|err| err.to_string())?;
                last = committed.index;
            }
            let elapsed = started.elapsed();
            println!(
                "{count} commands committed in {:.1} ms ({:.2} ms each); last index {last}",
                elapsed.as_secs_f64() * 1000.0,
                elapsed.as_secs_f64() * 1000.0 / count.max(1) as f64
            );
            Ok(())
        }
        Some("map") => {
            let linearizable = rest.iter().any(|a| a == "--linearizable");
            let map = client
                .shard_map(linearizable)
                .await
                .map_err(|err| err.to_string())?;
            println!("{map:#?}");
            Ok(())
        }
        Some("faults") => faults(&client, &rest[1..]).await,
        Some("create-collection") => create(&mut client, &rest[1..]).await,
        Some("load") => load(&servers, &rest[1..]).await,
        Some("search") => search(client, &rest[1..]).await,
        _ => Err(HELP.to_string()),
    }
}

async fn status(client: &ControlClient, servers: &[(u64, String)]) -> Result<(), String> {
    println!(
        "{:>6}  {:<10} {:>6} {:>7} {:>8} {:>8}  address",
        "server", "role", "term", "leader", "commit", "applied"
    );
    for (id, address) in servers {
        let Some(mut raw) = client.server(*id) else {
            continue;
        };
        match tokio::time::timeout(Duration::from_millis(500), raw.status(Empty {})).await {
            Ok(Ok(reply)) => {
                let s = reply.into_inner();
                println!(
                    "{:>6}  {:<10} {:>6} {:>7} {:>8} {:>8}  {address}",
                    s.id, s.role, s.term, s.leader, s.commit_index, s.applied_index
                );
            }
            _ => println!(
                "{id:>6}  {:<10} {:>6} {:>7} {:>8} {:>8}  {address}",
                "down", "-", "-", "-", "-"
            ),
        }
    }
    Ok(())
}

async fn faults(client: &ControlClient, args: &[String]) -> Result<(), String> {
    let server: u64 = args
        .first()
        .and_then(|s| s.parse().ok())
        .ok_or("faults needs a SERVER")?;
    let mut request = FaultsRequest::default();
    let mut iter = args[1..].iter();
    let ids = |s: &str| -> Result<Vec<u64>, String> {
        s.split(',')
            .filter(|p| !p.is_empty())
            .map(|p| p.parse().map_err(|_| format!("`{p}` is not a server ID")))
            .collect()
    };
    while let Some(arg) = iter.next() {
        let value = iter.next().ok_or(format!("{arg} needs a value"))?;
        match arg.as_str() {
            "--drop-to" => request.drop_to = ids(value)?,
            "--drop-from" => request.drop_from = ids(value)?,
            "--delay-us" => {
                request.delay_us = value.parse().map_err(|_| "--delay-us needs a number")?
            }
            other => return Err(format!("unknown option `{other}`")),
        }
    }
    let mut raw = client.server(server).ok_or(format!("no server {server}"))?;
    raw.set_faults(request)
        .await
        .map_err(|err| err.to_string())?;
    println!("server {server}: faults set");
    Ok(())
}

async fn create(client: &mut ControlClient, args: &[String]) -> Result<(), String> {
    let [name, dimension, metric, shards, replication] = args else {
        return Err("create-collection NAME DIMENSION angular|euclidean SHARDS REPLICAS".into());
    };
    let number = |s: &str| {
        s.parse::<u32>()
            .map_err(|_| format!("`{s}` is not a number"))
    };
    let metric = match metric.as_str() {
        "angular" => Metric::Angular,
        "euclidean" => Metric::Euclidean,
        other => return Err(format!("unknown metric `{other}`")),
    };
    let command = ControlCommand::CreateCollection {
        name: name.clone(),
        spec: CollectionSpec {
            dimension: number(dimension)?,
            metric,
            shards: number(shards)?,
            replication: number(replication)?,
        },
        placement: Vec::new(),
    };
    let committed = client
        .propose(command)
        .await
        .map_err(|err| err.to_string())?;
    println!("index {}: {:?}", committed.index, committed.outcome);
    Ok(())
}

async fn load(control_servers: &[(u64, String)], args: &[String]) -> Result<(), String> {
    let (Some(name), Some(path)) = (args.first(), args.get(1)) else {
        return Err("load NAME DATASET [COUNT]".into());
    };
    let dataset = std::sync::Arc::new(Dataset::load(std::path::Path::new(path))?);
    let count = match args.get(2) {
        Some(n) => n
            .parse::<usize>()
            .map_err(|_| "COUNT must be a number")?
            .min(dataset.len()),
        None => dataset.len(),
    };
    let started = Instant::now();
    let next = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for worker in 0..64 {
        let dataset = std::sync::Arc::clone(&dataset);
        let next = std::sync::Arc::clone(&next);
        let name = name.clone();
        let control = ControlClient::new(
            control_servers,
            RetryPolicy::default(),
            rpc::now_micros() + worker,
        )?;
        tasks.push(tokio::spawn(async move {
            let mut vectors = VectorClient::new(control);
            loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= count {
                    return Ok::<(), String>(());
                }
                vectors
                    .upsert(&name, i as u64, dataset.vector(i).to_vec())
                    .await
                    .map_err(|err| err.to_string())?;
            }
        }));
    }
    for task in tasks {
        task.await.map_err(|err| err.to_string())??;
    }
    let elapsed = started.elapsed();
    println!(
        "{count} vectors acknowledged in {:.1} s ({:.0} per second)",
        elapsed.as_secs_f64(),
        count as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}

async fn search(client: ControlClient, args: &[String]) -> Result<(), String> {
    let (Some(name), Some(path)) = (args.first(), args.get(1)) else {
        return Err("search NAME DATASET [QUERIES] [K]".into());
    };
    let queries: usize = args
        .get(2)
        .map_or(Ok(100), |n| n.parse())
        .map_err(|_| "QUERIES must be a number")?;
    let k: usize = args
        .get(3)
        .map_or(Ok(10), |n| n.parse())
        .map_err(|_| "K must be a number")?;
    let dataset = Dataset::load(std::path::Path::new(path))?;
    let mut vectors = VectorClient::new(client);
    let mut exact = 0;
    let mut recall = 0.0;
    let started = Instant::now();
    for q in 0..queries.min(dataset.queries()) {
        let hits = vectors
            .search(name, dataset.query(q), k as u32)
            .await
            .map_err(|err| err.to_string())?;
        let ids: Vec<u64> = hits.iter().map(|h| h.id).collect();
        let truth = &dataset.neighbors[q][..k];
        if ids == truth {
            exact += 1;
        }
        recall += ids.iter().filter(|id| truth.contains(id)).count() as f64 / k as f64;
    }
    let n = queries.min(dataset.queries());
    println!(
        "{n} queries in {:.1} s: {exact} match the ground truth exactly; mean recall@{k} {:.4}",
        started.elapsed().as_secs_f64(),
        recall / n as f64
    );
    Ok(())
}
