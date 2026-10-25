//! Control-plane server: the metadata state machine, file-backed storage, the network
//! transport, and the consensus module selected with `--consensus raft|paxos`.
//!
//! One binary runs either algorithm. Everything except the algorithm itself is shared:
//! the driver, storage, transport, state machine, leases, and API (`docs/PLAN.md` §5).
//!
//! See `docs/PLAN.md` §4.4.

pub mod config;
pub mod driver;
pub mod inspect;
pub mod leases;
pub mod local;
pub mod log;
pub mod service;
pub mod state;
pub mod storage;
pub mod transport;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use consensus::{Config, NodeId};
use paxos::Paxos;
use raft::Raft;
use rpc::proto::control_plane_server::ControlPlaneServer;
use rpc::proto::peer_server::PeerServer;

use crate::config::ClusterConfig;
use crate::driver::{DriverConfig, Shared};
use crate::inspect::Inspect;
use crate::leases::{LeaseTiming, Leases};
use crate::log::JsonLog;
use crate::service::ControlPlaneService;
use crate::transport::{Faults, PeerService};

/// The consensus algorithm a server runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    /// Raft.
    Raft,
    /// Multi-Paxos.
    Paxos,
}

impl Algorithm {
    /// Parses `raft` or `paxos`.
    ///
    /// # Errors
    ///
    /// Returns a description if the name is neither.
    pub fn parse(name: &str) -> Result<Algorithm, String> {
        match name {
            "raft" => Ok(Algorithm::Raft),
            "paxos" => Ok(Algorithm::Paxos),
            other => Err(format!(
                "unknown algorithm `{other}` (available: raft, paxos)"
            )),
        }
    }
}

/// Everything a server needs to start.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// This server's ID.
    pub id: NodeId,
    /// The cluster.
    pub cluster: ClusterConfig,
    /// The algorithm.
    pub algorithm: Algorithm,
    /// The storage folder.
    pub data_dir: PathBuf,
    /// The log file, or standard error.
    pub log: Option<PathBuf>,
    /// Shortest and longest election timeout. The heartbeat interval is half the shortest
    /// (`docs/RESEARCH.md` §3).
    pub election_timeout: (Duration, Duration),
    /// The one-way delay to inject from the start, in microseconds.
    pub delay_us: u64,
    /// Whether to keep the measurement log for experiment runners.
    pub record: bool,
    /// Seed of the algorithm's random number generator; each server adds its ID. `None`
    /// seeds from the clock.
    pub seed: Option<u64>,
    /// Lease timing.
    pub leases: LeaseTiming,
}

/// Runs a server until the process is stopped.
///
/// # Errors
///
/// Returns a description of the problem if the server cannot start.
pub fn run(options: ServerOptions) -> Result<(), String> {
    match options.algorithm {
        Algorithm::Raft => serve::<Raft>(options),
        Algorithm::Paxos => serve::<Paxos>(options),
    }
}

fn serve<C: Inspect>(options: ServerOptions) -> Result<(), String> {
    let address = options
        .cluster
        .address(options.id)
        .ok_or_else(|| format!("server {} is not in the cluster file", options.id.0))?
        .parse::<std::net::SocketAddr>()
        .map_err(|err| format!("bad address for server {}: {err}", options.id.0))?;
    let (min, max) = options.election_timeout;
    let consensus = Config {
        heartbeat_interval: min / 2,
        election_timeout_min: min,
        election_timeout_max: max,
        ..Config::default()
    };
    consensus.validate()?;
    let log = JsonLog::open(options.log.as_deref()).map_err(|err| err.to_string())?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    let faults = Arc::new(Mutex::new(Faults {
        delay_us: options.delay_us,
        ..Faults::default()
    }));
    let peers: Vec<(NodeId, String)> = options
        .cluster
        .server
        .iter()
        .map(|s| (NodeId(s.id), s.address.clone()))
        .collect();
    let outbound = transport::start(runtime.handle(), options.id, &peers, Arc::clone(&faults));
    let shared = Arc::new(Mutex::new(Shared::new()));
    let (inputs, receiver) = mpsc::channel();

    let settings = DriverConfig {
        id: options.id,
        cluster: options.cluster.ids(),
        consensus,
        seed: options.seed.unwrap_or_else(rpc::now_micros) ^ options.id.0,
        data_dir: options.data_dir.clone(),
        record: options.record,
    };
    let driver_shared = Arc::clone(&shared);
    let driver_log = log.clone();
    std::thread::Builder::new()
        .name("consensus".into())
        .spawn(move || driver::run::<C>(settings, receiver, outbound, driver_shared, driver_log))
        .map_err(|err| err.to_string())?;

    let leases = Leases::start(
        runtime.handle(),
        inputs.clone(),
        Arc::clone(&shared),
        options.leases,
    );
    let peer = PeerService::new(options.id, inputs.clone(), Arc::clone(&faults));
    let api = ControlPlaneService::new(
        options.id.0,
        inputs,
        shared,
        faults,
        leases,
        peer.delay_cpu_us(),
    );
    log.write(&serde_json::json!({
        "event": "listening",
        "server": options.id.0,
        "address": address.to_string(),
        "algorithm": format!("{:?}", options.algorithm).to_lowercase(),
    }));
    runtime.block_on(async move {
        tonic::transport::Server::builder()
            .tcp_nodelay(true)
            .add_service(PeerServer::new(peer))
            .add_service(ControlPlaneServer::new(api))
            .serve(address)
            .await
            .map_err(|err| err.to_string())
    })
}
