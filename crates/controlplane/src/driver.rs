//! The consensus driver: runs one algorithm on its own OS thread (`docs/PLAN.md` §8,
//! Phase 3).
//!
//! The driver follows the `Consensus` driver contract, as the simulator does: it delivers
//! one input at a time, calls [`Storage::sync`] after each input before carrying out any
//! output, and carries out outputs in order. Messages, bytes, fsyncs, and status changes
//! are recorded here, in shared code, for the experiment runners (`docs/PLAN.md` §5).
//!
//! Running consensus on its own thread keeps fsync from blocking the network runtime, so
//! slow disk writes do not delay heartbeats.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use consensus::{
    Command, Config, LogIndex, NodeId, Output, Payload, RequestId, Role, Storage, Timer,
};
use rpc::{ControlCommand, Events, Outcome, Request, SendEvent, ShardMap, StatusEvent};
use tokio::sync::oneshot;

use crate::inspect::Inspect;
use crate::log::JsonLog;
use crate::state::{StateMachine, place};
use crate::storage::FileStorage;
use crate::transport::Outbound;

/// The answer to a proposal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Proposed {
    /// This server is not the leader; the known leader, if any.
    NotLeader(Option<NodeId>),
    /// The command was committed and applied at `index` with this outcome.
    Applied {
        /// The log index.
        index: u64,
        /// What applying the command did.
        outcome: Outcome,
    },
}

/// The answer to a leader-confirmed read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirmed {
    /// This server is not the leader, or stopped leading; the known leader, if any.
    NotLeader(Option<NodeId>),
    /// Leadership was confirmed, and the state machine has applied every entry up to the
    /// read index. The shard map at that point.
    Map(Arc<ShardMap>),
}

/// An input for the driver thread.
pub enum Input {
    /// An encoded consensus message from another server.
    Message {
        /// The sender.
        from: NodeId,
        /// The encoded message.
        bytes: Vec<u8>,
    },
    /// A client request to propose.
    Propose {
        /// The request.
        request: Request,
        /// Where to send the answer.
        reply: oneshot::Sender<Proposed>,
    },
    /// A leader-confirmed read (Raft §8), also used for lease renewals.
    Confirm {
        /// Where to send the answer.
        reply: oneshot::Sender<Confirmed>,
    },
    /// A request for the measurement log.
    Events {
        /// Where to send it.
        reply: oneshot::Sender<Events>,
    },
    /// An experiment procedure: append `commands` commands, with messages carrying entries
    /// dropped to `drop_entries_to`, optionally send a heartbeat round, then stop.
    Crash {
        /// Servers that do not receive the new entries.
        drop_entries_to: Vec<u64>,
        /// Commands to append.
        commands: u32,
        /// Whether to send a heartbeat round first.
        heartbeat: bool,
        /// Where to send the time and term of the crash.
        reply: oneshot::Sender<(u64, u64)>,
    },
}

/// Size of every experiment command in bytes, as in the simulator (`docs/RESEARCH.md` §3).
pub const COMMAND_BYTES: usize = 128;

/// A `Bench` request whose encoding is [`COMMAND_BYTES`] long.
pub fn bench_request(client: u64, seq: u64) -> Request {
    let mut request = Request {
        client,
        seq,
        command: ControlCommand::Bench {
            padding: Vec::new(),
        },
    };
    let base = rpc::encode(&request).len();
    let mut padding = COMMAND_BYTES.saturating_sub(base + 1);
    loop {
        request.command = ControlCommand::Bench {
            padding: vec![0; padding],
        };
        let size = rpc::encode(&request).len();
        if size <= COMMAND_BYTES || padding == 0 {
            return request;
        }
        padding -= size - COMMAND_BYTES;
    }
}

/// What the network side of the server reads without asking the driver thread.
#[derive(Debug, Clone)]
pub struct Shared {
    /// The algorithm's status after the last input.
    pub role: Role,
    /// The current term.
    pub term: u64,
    /// The known leader.
    pub leader: Option<NodeId>,
    /// The commit index.
    pub commit_index: u64,
    /// The highest index applied to the state machine.
    pub applied_index: u64,
    /// The shard map as of the last change.
    pub map: Arc<ShardMap>,
    /// When this server became leader of the current term, if it leads.
    pub leader_since: Option<Instant>,
    /// Storage syncs that wrote something, since the server started.
    pub fsyncs: u64,
    /// Bytes those syncs wrote.
    pub storage_bytes: u64,
}

impl Shared {
    /// The state before the driver starts.
    pub fn new() -> Self {
        Shared {
            role: Role::Follower,
            term: 0,
            leader: None,
            commit_index: 0,
            applied_index: 0,
            map: Arc::new(ShardMap::default()),
            leader_since: None,
            fsyncs: 0,
            storage_bytes: 0,
        }
    }
}

impl Default for Shared {
    fn default() -> Self {
        Self::new()
    }
}

/// Settings for one driver.
pub struct DriverConfig {
    /// This server.
    pub id: NodeId,
    /// Every server, including this one.
    pub cluster: Vec<NodeId>,
    /// The consensus settings.
    pub consensus: Config,
    /// Seed of the algorithm's random number generator.
    pub seed: u64,
    /// The storage folder.
    pub data_dir: PathBuf,
    /// Whether to keep the measurement log of every message sent and status change. Only
    /// experiment runners need it, and it grows with every message.
    pub record: bool,
}

struct Waiting {
    /// Proposals waiting to be applied, by client session.
    proposals: BTreeMap<(u64, u64), Vec<oneshot::Sender<Proposed>>>,
    /// Reads waiting for leadership confirmation, by request.
    confirming: BTreeMap<RequestId, oneshot::Sender<Confirmed>>,
    /// Confirmed reads waiting for the state machine to reach their read index.
    applying: Vec<(LogIndex, oneshot::Sender<Confirmed>)>,
    next_request: u64,
}

/// Runs the driver until every input sender is dropped. Call it on a dedicated thread.
///
/// # Panics
///
/// Panics if the storage cannot be opened, as a server that cannot read its state cannot
/// run.
pub fn run<C: Inspect>(
    settings: DriverConfig,
    inputs: Receiver<Input>,
    outbound: Outbound,
    shared: Arc<Mutex<Shared>>,
    log: JsonLog,
) {
    let mut storage = FileStorage::open(&settings.data_dir).expect("cannot open storage");
    let (node, outputs) = C::start(
        settings.id,
        &settings.cluster,
        &settings.consensus,
        settings.seed,
        &mut storage,
    );
    let mut driver = Driver {
        id: settings.id,
        node,
        storage,
        state: StateMachine::new(),
        applied: LogIndex::ZERO,
        timers: BTreeMap::new(),
        outbound,
        shared,
        waiting: Waiting {
            proposals: BTreeMap::new(),
            confirming: BTreeMap::new(),
            applying: Vec::new(),
            next_request: 1,
        },
        events: Events {
            server: settings.id.0,
            ..Events::default()
        },
        last_status: None,
        record: settings.record,
        log,
    };
    driver.log.write(&serde_json::json!({
        "event": "start",
        "server": settings.id.0,
        "term": driver.storage.hard_state().term.0,
        "last_index": driver.storage.last_index().0,
    }));
    driver.finish(outputs);

    loop {
        let timeout = driver
            .timers
            .values()
            .min()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(3600));
        match inputs.recv_timeout(timeout) {
            Ok(Input::Crash {
                drop_entries_to,
                commands,
                heartbeat,
                reply,
            }) => {
                driver.crash(drop_entries_to, commands, heartbeat, reply);
                return;
            }
            Ok(input) => driver.handle(input),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        driver.fire_due_timers();
    }
}

struct Driver<C: Inspect> {
    id: NodeId,
    node: C,
    storage: FileStorage,
    state: StateMachine,
    applied: LogIndex,
    timers: BTreeMap<Timer, Instant>,
    outbound: Outbound,
    shared: Arc<Mutex<Shared>>,
    waiting: Waiting,
    events: Events,
    last_status: Option<(Role, u64, u64)>,
    record: bool,
    log: JsonLog,
}

impl<C: Inspect> Driver<C> {
    fn handle(&mut self, input: Input) {
        match input {
            Input::Message { from, bytes } => {
                let Ok(message) = rpc::decode::<C::Message>(&bytes) else {
                    return;
                };
                let status = self.node.status();
                if matches!(status.role, Role::Candidate | Role::Leader) {
                    self.events.entries_to_leaders += C::entry_count(&message) as u64;
                }
                if C::is_stale_vote(&message, status, &self.storage) {
                    self.events.stale_log_rejections += 1;
                }
                let outputs = self.node.step(&mut self.storage, from, message);
                self.finish(outputs);
            }
            Input::Propose { request, reply } => self.propose(request, reply),
            Input::Confirm { reply } => {
                let request = RequestId(self.waiting.next_request);
                self.waiting.next_request += 1;
                match self.node.confirm_leadership(&mut self.storage, request) {
                    Ok(outputs) => {
                        self.waiting.confirming.insert(request, reply);
                        self.finish(outputs);
                    }
                    Err(not_leader) => {
                        let _ = reply.send(Confirmed::NotLeader(not_leader.leader));
                    }
                }
            }
            Input::Crash { .. } => unreachable!("the run loop handles crashes"),
            Input::Events { reply } => {
                self.events.max_log_gaps = self.storage.max_gaps();
                self.events.discarded_entries = self.storage.discarded();
                let _ = reply.send(self.events.clone());
            }
        }
    }

    /// Runs the crash procedure, then stops the process without handling another input.
    fn crash(
        &mut self,
        drop_entries_to: Vec<u64>,
        commands: u32,
        heartbeat: bool,
        reply: oneshot::Sender<(u64, u64)>,
    ) {
        self.outbound
            .faults()
            .lock()
            .expect("no thread panicked")
            .drop_entries_to = drop_entries_to.into_iter().collect();
        // The commands are appended in one step with one sync, as in the simulator, where
        // they take no time. Every output is still carried out only after the sync that
        // covers it. One sync per command would stall the leader for `commands` flushes,
        // long enough for its followers to start an election before the crash.
        let client = rpc::now_micros();
        let mut outputs = Vec::new();
        for seq in 1..=u64::from(commands) {
            let command = Command(rpc::encode(&bench_request(client, seq)));
            if let Ok(more) = self.node.propose(&mut self.storage, command) {
                outputs.extend(more);
            }
        }
        if heartbeat {
            outputs.extend(self.node.on_timer(&mut self.storage, Timer::Heartbeat));
        }
        self.finish(outputs);
        let time = rpc::now_micros();
        let term = self.node.status().term.0;
        self.log.write(&serde_json::json!({
            "event": "crash",
            "server": self.id.0,
            "time": time,
            "term": term,
        }));
        let _ = reply.send((time, term));
        // Give the network tasks a moment to put the last messages on the wire, then stop
        // the process. No input is handled after the crash instant.
        std::thread::sleep(Duration::from_millis(50));
        std::process::exit(0);
    }

    fn propose(&mut self, mut request: Request, reply: oneshot::Sender<Proposed>) {
        if self.node.status().role != Role::Leader {
            let _ = reply.send(Proposed::NotLeader(self.node.status().leader));
            return;
        }
        // The leader makes placement decisions and logs them in the command, so applying
        // it stays deterministic (`docs/PLAN.md` §4.4).
        if let ControlCommand::CreateCollection {
            spec, placement, ..
        } = &mut request.command
            && placement.is_empty()
        {
            match place(self.state.map(), spec) {
                Some(chosen) => *placement = chosen,
                None => {
                    let _ = reply.send(Proposed::Applied {
                        index: 0,
                        outcome: Outcome::Rejected("not enough alive data nodes".into()),
                    });
                    return;
                }
            }
        }
        let key = (request.client, request.seq);
        let command = Command(rpc::encode(&request));
        match self.node.propose(&mut self.storage, command) {
            Ok(outputs) => {
                self.waiting.proposals.entry(key).or_default().push(reply);
                self.finish(outputs);
            }
            Err(not_leader) => {
                let _ = reply.send(Proposed::NotLeader(not_leader.leader));
            }
        }
    }

    fn fire_due_timers(&mut self) {
        loop {
            let now = Instant::now();
            let Some((&timer, _)) = self.timers.iter().find(|(_, deadline)| **deadline <= now)
            else {
                return;
            };
            self.timers.remove(&timer);
            let outputs = self.node.on_timer(&mut self.storage, timer);
            self.finish(outputs);
        }
    }

    /// Follows the driver contract after an input: sync storage, then carry out outputs.
    fn finish(&mut self, outputs: Vec<Output<C::Message>>) {
        if self.storage.sync() {
            self.events.fsyncs += 1;
        }
        let mut map_changed = false;
        for output in outputs {
            match output {
                Output::Send { to, message } => {
                    let bytes = rpc::encode(&message);
                    if self.record {
                        self.events.sends.push(SendEvent {
                            time: rpc::now_micros(),
                            to: to.0,
                            kind: C::message_kind(&message).to_string(),
                            bytes: bytes.len() as u64,
                            entries: C::entry_count(&message) as u64,
                        });
                    }
                    let entries = C::entry_count(&message);
                    self.outbound.send(to, bytes, entries);
                }
                Output::SetTimer { timer, after } => {
                    self.timers.insert(timer, Instant::now() + after);
                }
                Output::CancelTimer { timer } => {
                    self.timers.remove(&timer);
                }
                Output::Apply { index, payload } => {
                    map_changed |= self.apply(index, payload);
                }
                Output::LeadershipConfirmed {
                    request,
                    read_index,
                } => {
                    if let Some(reply) = self.waiting.confirming.remove(&request) {
                        self.waiting.applying.push((read_index, reply));
                    }
                }
                Output::LeadershipLost { request } => {
                    if let Some(reply) = self.waiting.confirming.remove(&request) {
                        let _ = reply.send(Confirmed::NotLeader(self.node.status().leader));
                    }
                }
            }
        }
        self.publish(map_changed);
        self.answer_reads();
    }

    /// Applies one committed entry. Returns whether the shard map changed.
    fn apply(&mut self, index: LogIndex, payload: Payload) -> bool {
        self.applied = index;
        let Payload::Command(Command(bytes)) = payload else {
            return false;
        };
        let Ok(request) = rpc::decode::<Request>(&bytes) else {
            return false;
        };
        let before = self.state.map().version;
        let outcome = self.state.apply(index.0, &request);
        if let Some(waiters) = self
            .waiting
            .proposals
            .remove(&(request.client, request.seq))
        {
            for reply in waiters {
                let _ = reply.send(Proposed::Applied {
                    index: index.0,
                    outcome: outcome.clone(),
                });
            }
        }
        self.state.map().version != before
    }

    /// Answers confirmed reads whose read index the state machine has reached.
    fn answer_reads(&mut self) {
        if self.waiting.applying.is_empty() {
            return;
        }
        let applied = self.applied;
        let map = Arc::clone(&self.shared.lock().expect("no thread panicked").map);
        let (ready, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.waiting.applying)
            .into_iter()
            .partition(|(read_index, _)| *read_index <= applied);
        self.waiting.applying = waiting;
        for (_, reply) in ready {
            let _ = reply.send(Confirmed::Map(Arc::clone(&map)));
        }
    }

    /// Publishes the status, and the shard map if it changed, and records status changes.
    fn publish(&mut self, map_changed: bool) {
        let status = self.node.status();
        {
            let mut shared = self.shared.lock().expect("no thread panicked");
            if status.role != Role::Leader {
                shared.leader_since = None;
            } else if shared.role != Role::Leader || shared.term != status.term.0 {
                shared.leader_since = Some(Instant::now());
            }
            shared.role = status.role;
            shared.term = status.term.0;
            shared.leader = status.leader;
            shared.commit_index = status.commit_index.0;
            shared.applied_index = self.applied.0;
            shared.fsyncs = self.storage.syncs();
            shared.storage_bytes = self.storage.bytes_written();
            if map_changed {
                shared.map = Arc::new(self.state.map().clone());
            }
        }
        let key = (status.role, status.term.0, status.commit_index.0);
        if self.last_status != Some(key) {
            let role_or_term_changed = self
                .last_status
                .is_none_or(|(role, term, _)| role != status.role || term != status.term.0);
            self.last_status = Some(key);
            let event = StatusEvent {
                time: rpc::now_micros(),
                role: role_name(status.role).to_string(),
                term: status.term.0,
                commit_index: status.commit_index.0,
                last_index: self.storage.last_index().0,
            };
            if role_or_term_changed {
                self.log.write(&serde_json::json!({
                    "event": "status",
                    "server": self.id.0,
                    "time": event.time,
                    "role": event.role,
                    "term": event.term,
                    "commit_index": event.commit_index,
                }));
            }
            if self.record {
                self.events.status.push(event);
            }
        }
        // Proposals whose clients gave up are dropped.
        self.waiting
            .proposals
            .retain(|_, waiters| waiters.iter().any(|w| !w.is_closed()));
    }
}

/// The name of a role, as reported over the API.
pub fn role_name(role: Role) -> &'static str {
    match role {
        Role::Follower => "follower",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
    }
}
