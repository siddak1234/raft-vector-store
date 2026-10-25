//! The control-plane client: proposals that follow the leader, and shard-map reads.
//!
//! Retries follow `docs/RESEARCH.md` amendment A1.1, the same rules as the simulator's
//! clients:
//!
//! - a client waits 500 ms for a reply, then retries at a random server;
//! - a server that knows the leader redirects the client to it, and the client resends
//!   immediately;
//! - if the server does not know the leader, the client waits 50 ms and retries at a random
//!   server.
//!
//! A server that cannot be reached is treated like one that never replies: the client
//! waits out the 500 ms before retrying. A retried command keeps its sequence number, so
//! the control plane applies it once.
//!
//! The client can inject the protocol's one-way delay on its own messages, so client
//! traffic crosses the same simulated network as traffic between servers.

use std::time::{Duration, Instant};

use rpc::delay::{Jitter, sleep_until};
use rpc::proto::control_plane_client::ControlPlaneClient;
use rpc::proto::{ProposeRequest, ShardMapRequest};
use rpc::{ControlCommand, Outcome, Request, ShardMap};
use tonic::transport::{Channel, Endpoint};

/// How a client retries (`docs/RESEARCH.md` amendment A1.1).
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// How long to wait for a reply before retrying elsewhere.
    pub reply_timeout: Duration,
    /// How long to wait when no server knows the leader.
    pub backoff: Duration,
    /// Mean one-way delay injected on each request and reply, in microseconds.
    pub delay_us: u64,
    /// Give up after this long, if set.
    pub give_up: Option<Duration>,
    /// Whether each injected delay ends with a short spin, for precision
    /// (`docs/RESEARCH.md` amendment A4.3). An open-loop load turns it off, so that
    /// thousands of commands in flight do not take CPU from the servers (amendment A8).
    pub precise_delay: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            reply_timeout: Duration::from_millis(500),
            backoff: Duration::from_millis(50),
            delay_us: 0,
            give_up: None,
            precise_delay: true,
        }
    }
}

/// A committed command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// The log index it was applied at.
    pub index: u64,
    /// What applying it did.
    pub outcome: Outcome,
}

/// Why a request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    /// No server answered before the retry policy gave up.
    GaveUp,
    /// A server sent something that could not be decoded.
    Invalid(String),
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlError::GaveUp => write!(f, "no control-plane server answered in time"),
            ControlError::Invalid(why) => write!(f, "invalid reply: {why}"),
        }
    }
}

impl std::error::Error for ControlError {}

/// A client of the control plane.
pub struct ControlClient {
    servers: Vec<(u64, Channel)>,
    id: u64,
    seq: u64,
    target: usize,
    jitter: Jitter,
    policy: RetryPolicy,
}

enum Answer<T> {
    Done(T),
    Redirect(u64),
    NoLeader,
}

impl ControlClient {
    /// A client of the servers `(id, address)`, with a session ID unique to this client.
    ///
    /// # Errors
    ///
    /// Returns a description if an address is invalid.
    pub fn new(servers: &[(u64, String)], policy: RetryPolicy, id: u64) -> Result<Self, String> {
        let mut channels = Vec::new();
        for (server, address) in servers {
            let endpoint = Endpoint::from_shared(format!("http://{address}"))
                .map_err(|err| err.to_string())?
                .tcp_nodelay(true)
                .connect_timeout(Duration::from_millis(200));
            channels.push((*server, endpoint.connect_lazy()));
        }
        if channels.is_empty() {
            return Err("a client needs at least one server".into());
        }
        let mut jitter = Jitter::new(id);
        let target = jitter.below(channels.len() as u64) as usize;
        Ok(ControlClient {
            servers: channels,
            id,
            seq: 0,
            target,
            jitter,
            policy,
        })
    }

    /// The client's session ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// A client with its own session `id` that shares this client's connections and
    /// starts at the server this one would try next. The control plane expects one command
    /// at a time per session, so concurrent commands each need a session of their own.
    pub fn session(&self, id: u64) -> ControlClient {
        ControlClient {
            servers: self.servers.clone(),
            id,
            seq: 0,
            target: self.target,
            jitter: Jitter::new(id),
            policy: self.policy,
        }
    }

    /// A raw gRPC client for server `id`, for status, faults, and measurement calls. It
    /// accepts replies of any size: a server's measurement log, which lists every message it
    /// sent, outgrows gRPC's default 4 MB limit at a few thousand commits per second.
    pub fn server(&self, id: u64) -> Option<ControlPlaneClient<Channel>> {
        self.servers
            .iter()
            .find(|(server, _)| *server == id)
            .map(|(_, channel)| {
                ControlPlaneClient::new(channel.clone()).max_decoding_message_size(usize::MAX)
            })
    }

    /// Proposes `command` and waits until it is committed and applied.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::GaveUp`] if the retry policy gives up first.
    pub async fn propose(&mut self, command: ControlCommand) -> Result<Committed, ControlError> {
        self.seq += 1;
        let request = rpc::encode(&Request {
            client: self.id,
            seq: self.seq,
            command,
        });
        self.retry(|mut client| {
            let request = request.clone();
            async move {
                let reply = client
                    .propose(ProposeRequest { request })
                    .await
                    .ok()?
                    .into_inner();
                Some(if reply.accepted {
                    match rpc::decode::<Outcome>(&reply.outcome) {
                        Ok(outcome) => Answer::Done(Ok(Committed {
                            index: reply.index,
                            outcome,
                        })),
                        Err(why) => Answer::Done(Err(ControlError::Invalid(why))),
                    }
                } else if reply.leader != 0 {
                    Answer::Redirect(reply.leader)
                } else {
                    Answer::NoLeader
                })
            }
        })
        .await?
    }

    /// Reads the shard map. A linearizable read goes to the leader, which confirms its
    /// leadership first; otherwise any server answers from its own copy, which may be stale.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::GaveUp`] if the retry policy gives up first.
    pub async fn shard_map(&mut self, linearizable: bool) -> Result<ShardMap, ControlError> {
        self.retry(|mut client| async move {
            let reply = client
                .read_shard_map(ShardMapRequest { linearizable })
                .await
                .ok()?
                .into_inner();
            Some(if reply.ok {
                Answer::Done(rpc::decode::<ShardMap>(&reply.map).map_err(ControlError::Invalid))
            } else if reply.leader != 0 {
                Answer::Redirect(reply.leader)
            } else {
                Answer::NoLeader
            })
        })
        .await?
    }

    /// Sends one request at a time, following redirects, until it gets an answer.
    async fn retry<T, F, Fut>(&mut self, mut call: F) -> Result<T, ControlError>
    where
        F: FnMut(ControlPlaneClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Option<Answer<T>>>,
    {
        let started = Instant::now();
        loop {
            if self
                .policy
                .give_up
                .is_some_and(|limit| started.elapsed() > limit)
            {
                return Err(ControlError::GaveUp);
            }
            let sent = Instant::now();
            self.delay().await;
            let client = ControlPlaneClient::new(self.servers[self.target].1.clone());
            let answer = tokio::time::timeout(self.policy.reply_timeout, call(client)).await;
            match answer {
                Ok(Some(Answer::Done(value))) => {
                    self.delay().await;
                    return Ok(value);
                }
                Ok(Some(Answer::Redirect(leader))) => {
                    self.delay().await;
                    match self.servers.iter().position(|(id, _)| *id == leader) {
                        Some(slot) => self.target = slot,
                        None => self.pick_random(),
                    }
                }
                Ok(Some(Answer::NoLeader)) => {
                    self.delay().await;
                    tokio::time::sleep(self.policy.backoff).await;
                    self.pick_random();
                }
                // An unreachable server is treated like a silent one: wait out the reply
                // timeout, then try a random server.
                Ok(None) | Err(_) => {
                    let deadline = sent + self.policy.reply_timeout;
                    tokio::time::sleep_until(deadline.into()).await;
                    self.pick_random();
                }
            }
        }
    }

    fn pick_random(&mut self) {
        self.target = self.jitter.below(self.servers.len() as u64) as usize;
    }

    /// Waits one injected one-way delay, if any.
    async fn delay(&mut self) {
        if self.policy.delay_us == 0 {
            return;
        }
        let deadline = Instant::now() + self.jitter.delay(self.policy.delay_us);
        if self.policy.precise_delay {
            let _ = tokio::task::spawn_blocking(move || sleep_until(deadline)).await;
        } else {
            tokio::time::sleep_until(deadline.into()).await;
        }
    }
}
