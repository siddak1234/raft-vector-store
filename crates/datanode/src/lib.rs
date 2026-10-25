//! Data node: in-memory shard storage, primary-to-backup replication, and brute-force
//! nearest-neighbor search.
//!
//! The data node never runs consensus. It learns the cluster's metadata from the control
//! plane through heartbeats and leases (`docs/DESIGN.md` §5) and replicates each shard from
//! its primary to the other replicas with the in-sync rules of §6.
//!
//! See `docs/PLAN.md` §4.5.

pub mod control;
pub mod node;
pub mod shard;

use std::net::SocketAddr;

use rpc::proto::data_node_server::DataNodeServer;

pub use node::{Node, NodeOptions};

/// Runs a data node until the process is stopped.
///
/// # Errors
///
/// Returns a description of the problem if the node cannot start.
pub fn run(options: NodeOptions) -> Result<(), String> {
    let address: SocketAddr = options
        .address
        .parse()
        .map_err(|err| format!("bad address {}: {err}", options.address))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    runtime.block_on(async move {
        let node = Node::new(options)?;
        tokio::spawn(std::sync::Arc::clone(&node).heartbeats());
        tonic::transport::Server::builder()
            .tcp_nodelay(true)
            .add_service(DataNodeServer::new(node::DataNodeService::new(node)))
            .serve(address)
            .await
            .map_err(|err| err.to_string())
    })
}
