//! The control-plane gRPC API: proposals, shard-map reads, status, fault injection, and the
//! measurement log.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use rpc::proto::control_plane_server::ControlPlane;
use rpc::proto::{
    CrashReply, CrashRequest, Empty, EventsReply, FaultsRequest, HeartbeatReply, HeartbeatRequest,
    ProposeReply, ProposeRequest, ShardMapReply, ShardMapRequest, StatusReply,
};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::driver::{Confirmed, Input, Proposed, Shared, role_name};
use crate::leases::Leases;
use crate::transport::Faults;

/// The CPU time this process has used, user plus system, in microseconds, and its peak
/// resident memory in bytes; zeros if the system does not report them.
fn process_usage() -> (u64, u64) {
    use nix::sys::resource::{UsageWho, getrusage};
    let Ok(usage) = getrusage(UsageWho::RUSAGE_SELF) else {
        return (0, 0);
    };
    let micros = |t: nix::sys::time::TimeVal| {
        u64::try_from(t.tv_sec()).unwrap_or(0) * 1_000_000 + u64::try_from(t.tv_usec()).unwrap_or(0)
    };
    let cpu = micros(usage.user_time()) + micros(usage.system_time());
    // macOS reports `ru_maxrss` in bytes, Linux in kilobytes.
    let rss = u64::try_from(usage.max_rss()).unwrap_or(0);
    let rss = if cfg!(target_os = "macos") {
        rss
    } else {
        rss * 1024
    };
    (cpu, rss)
}

/// The `ControlPlane` service of one server.
pub struct ControlPlaneService {
    id: u64,
    inputs: mpsc::Sender<Input>,
    shared: Arc<Mutex<Shared>>,
    faults: Arc<Mutex<Faults>>,
    leases: Arc<Leases>,
    delay_cpu_us: Arc<AtomicU64>,
}

impl ControlPlaneService {
    /// A service that sends work to the driver through `inputs`.
    pub fn new(
        id: u64,
        inputs: mpsc::Sender<Input>,
        shared: Arc<Mutex<Shared>>,
        faults: Arc<Mutex<Faults>>,
        leases: Arc<Leases>,
        delay_cpu_us: Arc<AtomicU64>,
    ) -> Self {
        ControlPlaneService {
            id,
            inputs,
            shared,
            faults,
            leases,
            delay_cpu_us,
        }
    }

    async fn ask<T>(&self, input: impl FnOnce(oneshot::Sender<T>) -> Input) -> Result<T, Status> {
        let (reply, answer) = oneshot::channel();
        self.inputs
            .send(input(reply))
            .map_err(|_| Status::unavailable("the server is stopping"))?;
        answer
            .await
            .map_err(|_| Status::unavailable("the server is stopping"))
    }
}

#[tonic::async_trait]
impl ControlPlane for ControlPlaneService {
    async fn propose(
        &self,
        request: Request<ProposeRequest>,
    ) -> Result<Response<ProposeReply>, Status> {
        let request = rpc::decode::<rpc::Request>(&request.into_inner().request)
            .map_err(Status::invalid_argument)?;
        let reply = match self.ask(|reply| Input::Propose { request, reply }).await? {
            Proposed::NotLeader(leader) => ProposeReply {
                accepted: false,
                leader: leader.map_or(0, |l| l.0),
                outcome: Vec::new(),
                index: 0,
            },
            Proposed::Applied { index, outcome } => ProposeReply {
                accepted: true,
                leader: self.id,
                outcome: rpc::encode(&outcome),
                index,
            },
        };
        Ok(Response::new(reply))
    }

    async fn read_shard_map(
        &self,
        request: Request<ShardMapRequest>,
    ) -> Result<Response<ShardMapReply>, Status> {
        if !request.into_inner().linearizable {
            let shared = self.shared.lock().expect("no thread panicked");
            return Ok(Response::new(ShardMapReply {
                ok: true,
                leader: shared.leader.map_or(0, |l| l.0),
                map: rpc::encode(shared.map.as_ref()),
            }));
        }
        let reply = match self.ask(|reply| Input::Confirm { reply }).await? {
            Confirmed::NotLeader(leader) => ShardMapReply {
                ok: false,
                leader: leader.map_or(0, |l| l.0),
                map: Vec::new(),
            },
            Confirmed::Map(map) => ShardMapReply {
                ok: true,
                leader: self.id,
                map: rpc::encode(map.as_ref()),
            },
        };
        Ok(Response::new(reply))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let reply = self.leases.heartbeat(request.into_inner()).await;
        Ok(Response::new(reply))
    }

    async fn status(&self, _: Request<Empty>) -> Result<Response<StatusReply>, Status> {
        let (cpu_us, max_rss_bytes) = process_usage();
        let shared = self.shared.lock().expect("no thread panicked");
        Ok(Response::new(StatusReply {
            id: self.id,
            role: role_name(shared.role).to_string(),
            term: shared.term,
            leader: shared.leader.map_or(0, |l| l.0),
            commit_index: shared.commit_index,
            applied_index: shared.applied_index,
            fsyncs: shared.fsyncs,
            storage_bytes: shared.storage_bytes,
            cpu_us,
            max_rss_bytes,
            delay_cpu_us: self.delay_cpu_us.load(Ordering::Relaxed),
        }))
    }

    async fn set_faults(&self, request: Request<FaultsRequest>) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        let mut faults = self.faults.lock().expect("no thread panicked");
        faults.drop_to = request.drop_to.into_iter().collect();
        faults.drop_from = request.drop_from.into_iter().collect();
        faults.delay_us = request.delay_us;
        faults.drop_entries_to = request.drop_entries_to.into_iter().collect();
        Ok(Response::new(Empty {}))
    }

    async fn crash(&self, request: Request<CrashRequest>) -> Result<Response<CrashReply>, Status> {
        let request = request.into_inner();
        let (time, term) = self
            .ask(|reply| Input::Crash {
                drop_entries_to: request.drop_entries_to,
                commands: request.commands,
                heartbeat: request.heartbeat,
                reply,
            })
            .await?;
        Ok(Response::new(CrashReply { time, term }))
    }

    async fn events(&self, _: Request<Empty>) -> Result<Response<EventsReply>, Status> {
        let events = self.ask(|reply| Input::Events { reply }).await?;
        Ok(Response::new(EventsReply {
            events: rpc::encode(&events),
        }))
    }
}
