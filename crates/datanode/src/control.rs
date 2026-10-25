//! The data node's link to the control plane: heartbeats and the few commands a primary
//! proposes itself (`ShrinkInSync`, `ExpandInSync`). The data node may depend only on the
//! shared `rpc` crate, so this is a small client of its own.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rpc::proto::control_plane_client::ControlPlaneClient;
use rpc::proto::{HeartbeatReply, HeartbeatRequest, ProposeRequest};
use rpc::{ControlCommand, Outcome, Request};
use tonic::transport::{Channel, Endpoint};

/// The control-plane servers, and which one leads as far as this data node knows.
pub struct ControlLink {
    servers: Vec<(u64, Channel)>,
    leader: usize,
    client: u64,
    seq: u64,
    /// Control-plane servers this node cannot reach, for partition tests.
    blocked: Arc<Mutex<BTreeSet<u64>>>,
}

impl ControlLink {
    /// A link to the servers `(id, address)`.
    ///
    /// # Errors
    ///
    /// Returns a description if an address is invalid.
    pub fn new(servers: &[(u64, String)]) -> Result<Self, String> {
        Self::with_blocked(servers, Arc::new(Mutex::new(BTreeSet::new())))
    }

    /// A link that shares `blocked`, the servers this node cannot reach, with other links.
    ///
    /// # Errors
    ///
    /// Returns a description if an address is invalid.
    pub fn with_blocked(
        servers: &[(u64, String)],
        blocked: Arc<Mutex<BTreeSet<u64>>>,
    ) -> Result<Self, String> {
        let mut channels = Vec::new();
        for (id, address) in servers {
            let endpoint = Endpoint::from_shared(format!("http://{address}"))
                .map_err(|err| err.to_string())?
                .tcp_nodelay(true)
                .connect_timeout(Duration::from_millis(200));
            channels.push((*id, endpoint.connect_lazy()));
        }
        if channels.is_empty() {
            return Err("the data node needs at least one control-plane server".into());
        }
        Ok(ControlLink {
            servers: channels,
            leader: 0,
            client: rpc::now_micros(),
            seq: 0,
            blocked,
        })
    }

    /// The servers this node cannot reach, shared so a fault can be injected at any time.
    pub fn blocked(&self) -> Arc<Mutex<BTreeSet<u64>>> {
        Arc::clone(&self.blocked)
    }

    /// Whether the server at `slot` is cut off. A cut-off server behaves like one that never
    /// answers.
    async fn cut_off(&self, slot: usize) -> bool {
        let id = self.servers[slot].0;
        let blocked = self
            .blocked
            .lock()
            .expect("no thread panicked")
            .contains(&id);
        if blocked {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        blocked
    }

    fn follow(&mut self, leader: u64) {
        match self.servers.iter().position(|(id, _)| *id == leader) {
            Some(slot) => self.leader = slot,
            None => self.leader = (self.leader + 1) % self.servers.len(),
        }
    }

    /// Sends a heartbeat to the leader, following one redirect. Returns the reply, or
    /// `None` if no server answered in time.
    pub async fn heartbeat(&mut self, request: HeartbeatRequest) -> Option<HeartbeatReply> {
        for _ in 0..2 {
            if self.cut_off(self.leader).await {
                self.leader = (self.leader + 1) % self.servers.len();
                continue;
            }
            let mut client = ControlPlaneClient::new(self.servers[self.leader].1.clone());
            let reply = tokio::time::timeout(
                Duration::from_millis(500),
                client.heartbeat(request.clone()),
            )
            .await;
            match reply {
                Ok(Ok(reply)) => {
                    let reply = reply.into_inner();
                    if reply.granted {
                        return Some(reply);
                    }
                    self.follow(reply.leader);
                    if reply.leader == 0 {
                        return Some(reply);
                    }
                }
                _ => self.leader = (self.leader + 1) % self.servers.len(),
            }
        }
        None
    }

    /// Proposes `command`, following the leader, until it is applied or `limit` passes.
    pub async fn propose(&mut self, command: ControlCommand, limit: Duration) -> Option<Outcome> {
        self.seq += 1;
        let request = rpc::encode(&Request {
            client: self.client,
            seq: self.seq,
            command,
        });
        let deadline = tokio::time::Instant::now() + limit;
        while tokio::time::Instant::now() < deadline {
            if self.cut_off(self.leader).await {
                self.leader = (self.leader + 1) % self.servers.len();
                continue;
            }
            let mut client = ControlPlaneClient::new(self.servers[self.leader].1.clone());
            let call = client.propose(ProposeRequest {
                request: request.clone(),
            });
            match tokio::time::timeout(Duration::from_millis(500), call).await {
                Ok(Ok(reply)) => {
                    let reply = reply.into_inner();
                    if reply.accepted {
                        return rpc::decode::<Outcome>(&reply.outcome).ok();
                    }
                    if reply.leader == 0 {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    self.follow(reply.leader);
                }
                _ => self.leader = (self.leader + 1) % self.servers.len(),
            }
        }
        None
    }
}
