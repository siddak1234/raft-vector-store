//! Scripted stand-ins for a control-plane server and a data node, served on localhost, for
//! testing the clients without a cluster. Each answers from a script and records what it
//! received; the methods the clients do not call answer `unimplemented`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rpc::proto::control_plane_server::{ControlPlane, ControlPlaneServer};
use rpc::proto::data_node_server::{DataNode, DataNodeServer};
use rpc::proto::{
    CopyShardReply, CopyShardRequest, CrashReply, CrashRequest, Empty, EventsReply, FaultsRequest,
    HeartbeatReply, HeartbeatRequest, InspectReply, InspectRequest, NewEpochReply, NewEpochRequest,
    ProposeReply, ProposeRequest, ReplicateReply, ReplicateRequest, SearchReply, SearchRequest,
    ShardMapReply, ShardMapRequest, StatusReply, WriteReply, WriteRequest,
};
use rpc::{Outcome, SearchQuery, SearchResult, ShardMap, Write, WriteResult};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

/// A queue of answers: each call takes the next one, and the last one repeats.
struct Script<T: Clone>(Mutex<VecDeque<T>>);

impl<T: Clone> Script<T> {
    fn new(answers: impl IntoIterator<Item = T>) -> Self {
        let answers: VecDeque<T> = answers.into_iter().collect();
        assert!(!answers.is_empty(), "a script needs at least one answer");
        Script(Mutex::new(answers))
    }

    fn next(&self) -> T {
        let mut answers = self.0.lock().expect("no test thread panicked");
        if answers.len() > 1 {
            answers.pop_front().expect("not empty")
        } else {
            answers.front().cloned().expect("not empty")
        }
    }
}

/// How a fake control-plane server answers a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// As the leader: proposals are applied, and maps are returned.
    Lead,
    /// As a follower that knows the leader: redirect to it.
    Redirect(u64),
    /// As a server that knows no leader.
    NoLeader,
    /// Never reply.
    Silent,
    /// As the leader, but with an outcome that cannot be decoded.
    Garbled,
}

/// A fake control-plane server.
pub struct Control {
    pub id: u64,
    answers: Script<Answer>,
    maps: Script<ShardMap>,
    /// Every proposal received, with when it arrived.
    proposals: Mutex<Vec<(Instant, rpc::Request)>>,
    /// Every shard-map read received: whether it asked for a linearizable read.
    map_reads: Mutex<Vec<bool>>,
}

impl Control {
    pub fn new(id: u64, answers: impl IntoIterator<Item = Answer>) -> Arc<Self> {
        Self::with_maps(id, answers, [ShardMap::default()])
    }

    pub fn with_maps(
        id: u64,
        answers: impl IntoIterator<Item = Answer>,
        maps: impl IntoIterator<Item = ShardMap>,
    ) -> Arc<Self> {
        Arc::new(Control {
            id,
            answers: Script::new(answers),
            maps: Script::new(maps),
            proposals: Mutex::new(Vec::new()),
            map_reads: Mutex::new(Vec::new()),
        })
    }

    pub fn proposals(&self) -> Vec<(Instant, rpc::Request)> {
        self.proposals
            .lock()
            .expect("no test thread panicked")
            .clone()
    }

    pub fn map_reads(&self) -> Vec<bool> {
        self.map_reads
            .lock()
            .expect("no test thread panicked")
            .clone()
    }
}

struct ControlService(Arc<Control>);

async fn never<T>() -> T {
    std::future::pending().await
}

#[tonic::async_trait]
impl ControlPlane for ControlService {
    async fn propose(
        &self,
        request: Request<ProposeRequest>,
    ) -> Result<Response<ProposeReply>, Status> {
        let decoded = rpc::decode::<rpc::Request>(&request.into_inner().request)
            .map_err(Status::invalid_argument)?;
        self.0
            .proposals
            .lock()
            .expect("no test thread panicked")
            .push((Instant::now(), decoded));
        let reply = |accepted: bool, leader: u64, outcome: Vec<u8>| ProposeReply {
            accepted,
            leader,
            outcome,
            index: 1,
        };
        Ok(Response::new(match self.0.answers.next() {
            Answer::Lead => reply(true, self.0.id, rpc::encode(&Outcome::Applied)),
            Answer::Garbled => reply(true, self.0.id, vec![0xFF, 0xFF, 0xFF]),
            Answer::Redirect(leader) => reply(false, leader, Vec::new()),
            Answer::NoLeader => reply(false, 0, Vec::new()),
            Answer::Silent => never().await,
        }))
    }

    async fn read_shard_map(
        &self,
        request: Request<ShardMapRequest>,
    ) -> Result<Response<ShardMapReply>, Status> {
        let linearizable = request.into_inner().linearizable;
        self.0
            .map_reads
            .lock()
            .expect("no test thread panicked")
            .push(linearizable);
        let answer = self.0.answers.next();
        if answer == Answer::Silent {
            return never().await;
        }
        let refuse = |leader| ShardMapReply {
            ok: false,
            leader,
            map: Vec::new(),
        };
        Ok(Response::new(match answer {
            Answer::Redirect(leader) if linearizable => refuse(leader),
            Answer::NoLeader if linearizable => refuse(0),
            _ => ShardMapReply {
                ok: true,
                leader: self.0.id,
                map: rpc::encode(&self.0.maps.next()),
            },
        }))
    }

    async fn heartbeat(
        &self,
        _: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn status(&self, _: Request<Empty>) -> Result<Response<StatusReply>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn set_faults(&self, _: Request<FaultsRequest>) -> Result<Response<Empty>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn events(&self, _: Request<Empty>) -> Result<Response<EventsReply>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn crash(&self, _: Request<CrashRequest>) -> Result<Response<CrashReply>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }
}

/// A fake data node.
pub struct Node {
    writes: Script<WriteResult>,
    searches: Script<SearchResult>,
    /// Every write received.
    received_writes: Mutex<Vec<Write>>,
    /// Every search received.
    received_searches: Mutex<Vec<SearchQuery>>,
}

impl Node {
    pub fn new(
        writes: impl IntoIterator<Item = WriteResult>,
        searches: impl IntoIterator<Item = SearchResult>,
    ) -> Arc<Self> {
        Arc::new(Node {
            writes: Script::new(writes),
            searches: Script::new(searches),
            received_writes: Mutex::new(Vec::new()),
            received_searches: Mutex::new(Vec::new()),
        })
    }

    pub fn writes(&self) -> Vec<Write> {
        self.received_writes
            .lock()
            .expect("no test thread panicked")
            .clone()
    }

    pub fn searches(&self) -> Vec<SearchQuery> {
        self.received_searches
            .lock()
            .expect("no test thread panicked")
            .clone()
    }
}

struct NodeService(Arc<Node>);

#[tonic::async_trait]
impl DataNode for NodeService {
    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<WriteReply>, Status> {
        let write =
            rpc::decode::<Write>(&request.into_inner().write).map_err(Status::invalid_argument)?;
        self.0
            .received_writes
            .lock()
            .expect("no test thread panicked")
            .push(write);
        Ok(Response::new(WriteReply {
            result: rpc::encode(&self.0.writes.next()),
        }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchReply>, Status> {
        let query = rpc::decode::<SearchQuery>(&request.into_inner().query)
            .map_err(Status::invalid_argument)?;
        self.0
            .received_searches
            .lock()
            .expect("no test thread panicked")
            .push(query);
        Ok(Response::new(SearchReply {
            result: rpc::encode(&self.0.searches.next()),
        }))
    }

    async fn replicate(
        &self,
        _: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateReply>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn new_epoch(
        &self,
        _: Request<NewEpochRequest>,
    ) -> Result<Response<NewEpochReply>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn copy_shard(
        &self,
        _: Request<CopyShardRequest>,
    ) -> Result<Response<CopyShardReply>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn events(&self, _: Request<Empty>) -> Result<Response<EventsReply>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn inspect(&self, _: Request<InspectRequest>) -> Result<Response<InspectReply>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn set_faults(&self, _: Request<FaultsRequest>) -> Result<Response<Empty>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }
}

async fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind localhost");
    let address = listener.local_addr().expect("bound").to_string();
    (listener, address)
}

/// Serves `control` on localhost and returns its address.
pub async fn serve_control(control: Arc<Control>) -> String {
    let (listener, address) = listener().await;
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ControlPlaneServer::new(ControlService(control)))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    address
}

/// Serves `node` on localhost and returns its address.
pub async fn serve_node(node: Arc<Node>) -> String {
    let (listener, address) = listener().await;
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(DataNodeServer::new(NodeService(node)))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    address
}

/// A client session ID whose first random picks among `servers` servers are `picks`, in
/// order. A client picks its first server, and every random retry, from a stream seeded by
/// its ID (`ControlClient::new`); with no injected delay it draws nothing else from it.
pub fn id_with_picks(picks: &[u64], servers: u64) -> u64 {
    (1..)
        .find(|&id| {
            let mut stream = rpc::delay::Jitter::new(id);
            picks.iter().all(|&slot| stream.below(servers) == slot)
        })
        .expect("some ID makes these picks")
}

/// A short reply timeout, so the tests run fast.
pub const TIMEOUT: Duration = Duration::from_millis(150);
