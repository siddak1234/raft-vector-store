//! `bench cluster`: starts a local cluster of control-plane servers and, optionally, data
//! nodes for demos and manual tests, and stops it when Enter is pressed
//! (`cargo xtask cluster`).

use std::path::PathBuf;

use controlplane::Algorithm;
use controlplane::local::{LocalCluster, LocalOptions};

/// The folder holding the server binaries: the folder of the running runner, since cargo
/// builds every binary of a profile into one folder.
pub fn binary_dir() -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|err| err.to_string())?;
    exe.parent()
        .map(PathBuf::from)
        .ok_or_else(|| "cannot find the binary folder".to_string())
}

/// A fresh folder for a cluster's data and logs, outside the repository.
pub fn fresh_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join("rvs").join(format!(
        "{label}-{}-{}",
        std::process::id(),
        rpc::now_micros()
    ))
}

/// Runs `bench cluster` with its arguments.
///
/// # Errors
///
/// Returns a description of the problem if the arguments are invalid or a server cannot
/// start.
pub fn run(args: &[String]) -> Result<(), String> {
    let mut servers = 3;
    let mut algorithm = Algorithm::Raft;
    let mut delay_us = 0;
    let mut data_nodes = 0;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let mut value = || iter.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--consensus" => algorithm = Algorithm::parse(value()?)?,
            "--delay-us" => {
                delay_us = value()?
                    .parse()
                    .map_err(|_| "--delay-us needs a number".to_string())?;
            }
            "--data-nodes" => {
                data_nodes = value()?
                    .parse()
                    .map_err(|_| "--data-nodes needs a number".to_string())?;
            }
            number => {
                servers = number
                    .parse()
                    .map_err(|_| format!("unknown argument `{number}`"))?;
            }
        }
    }
    if ![1, 3, 5].contains(&servers) {
        return Err("a cluster has 1, 3, or 5 servers".into());
    }
    let dir = fresh_dir("cluster");
    let options = LocalOptions {
        algorithm,
        election_timeout_ms: (150, 300),
        delay_us,
        record: false,
    };
    let binary = binary_dir()?.join("controlplane");
    let mut cluster = LocalCluster::start(&binary, &dir, servers, options)?;
    if data_nodes > 0 {
        cluster.start_data_nodes(&binary_dir()?.join("datanode"), data_nodes)?;
    }
    println!("Started {servers} control-plane servers ({algorithm:?}):");
    for (id, address) in cluster.servers() {
        println!("  server {id}: {address}");
    }
    for (id, address) in cluster.data_nodes() {
        println!("  data node {id}: {address}");
    }
    println!("Cluster file: {}", dir.join("cluster.toml").display());
    println!("Logs and data: {}", dir.display());
    println!("Press Enter to stop.");
    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    cluster.stop();
    Ok(())
}
