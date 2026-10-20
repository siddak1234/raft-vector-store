//! The simulation driver.

use std::collections::BTreeMap;
use std::time::Duration;

use consensus::{
    Command, Config, Consensus, LogIndex, MemStorage, NodeId, NotLeader, Output, Payload,
    RequestId, Role, Status, Storage, Term, Timer,
};

use crate::SimTime;
use crate::checks::{Check, Checker, Violation};
use crate::client::{self, Client, Outstanding};
use crate::faults::Fault;
use crate::metrics::Metrics;
use crate::network::{DropRule, Network, Route};
use crate::queue::EventQueue;
use crate::seeds::{Stream, stream_rng, stream_seed};
use crate::storage::TrackedStorage;

/// Settings for one simulation.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Number of servers. Their IDs are `1..=servers`.
    pub servers: usize,
    /// Settings given to every server.
    pub consensus: Config,
    /// Mean one-way message delay `d`. Each delay is uniform in `[0.5 d, 1.5 d]`.
    pub one_way_delay: Duration,
    /// Number of closed-loop clients.
    pub clients: usize,
    /// Size of each client command in bytes (at least 12).
    pub command_bytes: usize,
    /// How long a client waits for a reply before retrying at a random server.
    pub client_timeout: Duration,
    /// How long a client waits before retrying when a server does not know the leader.
    pub client_backoff: Duration,
    /// Whether to encode every message to count its bytes. Encoding costs time, so long
    /// safety runs turn it off.
    pub count_bytes: bool,
    /// Whether to run the Raft-only checks: log matching and leader completeness.
    pub raft_checks: bool,
}

impl Default for SimConfig {
    /// Three servers, the paper-like network (`d` = 7.5 ms), no clients, 128-byte commands.
    fn default() -> Self {
        SimConfig {
            servers: 3,
            consensus: Config::default(),
            one_way_delay: Duration::from_micros(7500),
            clients: 0,
            command_bytes: 128,
            client_timeout: Duration::from_millis(500),
            client_backoff: Duration::from_millis(50),
            count_bytes: true,
            raft_checks: false,
        }
    }
}

/// Hooks for measurements that need an algorithm's own message type, such as Raft's
/// count of votes refused because of a stale log (metric M5). The shared simulator calls
/// them but never looks inside messages itself.
pub trait Observer<C: Consensus> {
    /// Called just before `message` is handed to server `to`.
    fn on_deliver(
        &mut self,
        now: SimTime,
        from: NodeId,
        to: NodeId,
        message: &C::Message,
        receiver: ServerView<'_>,
    ) {
        let _ = (now, from, to, message, receiver);
    }

    /// Called after a server finishes processing an input.
    fn after_input(&mut self, now: SimTime, server: ServerView<'_>) {
        let _ = (now, server);
    }
}

impl<C: Consensus> Observer<C> for () {}

/// A read-only view of one server.
#[derive(Debug, Clone, Copy)]
pub struct ServerView<'a> {
    /// The server's ID.
    pub id: NodeId,
    /// The server's status, or `None` while it is stopped.
    pub status: Option<Status>,
    /// The server's storage.
    pub storage: &'a MemStorage,
}

/// A client command whose commit reply reached the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reply {
    /// When the client first sent the command.
    pub sent: SimTime,
    /// When the client received the reply.
    pub received: SimTime,
    /// The index at which the command was committed.
    pub index: LogIndex,
}

impl Reply {
    /// Commit latency as the client saw it (metric M7).
    pub fn latency(&self) -> Duration {
        self.received - self.sent
    }
}

/// The answer to a [`Simulation::confirm_leadership`] request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Confirmation {
    /// When the answer was produced.
    pub time: SimTime,
    /// The server that answered.
    pub server: NodeId,
    /// The request answered.
    pub request: RequestId,
    /// The read index if leadership was confirmed, or `None` if the server stopped leading.
    pub read_index: Option<LogIndex>,
}

enum Event<M> {
    Deliver {
        from: NodeId,
        to: NodeId,
        message: M,
    },
    Timer {
        slot: usize,
        timer: Timer,
        generation: u64,
    },
    Fault(Fault),
    ClientSend {
        client: usize,
    },
    ClientArrive {
        slot: usize,
        client: usize,
        seq: u64,
        attempt: u64,
    },
    ClientRedirect {
        client: usize,
        attempt: u64,
        leader: Option<NodeId>,
    },
    ClientRetry {
        client: usize,
        attempt: u64,
    },
    ClientReply {
        client: usize,
        seq: u64,
        index: LogIndex,
    },
    ClientTimeout {
        client: usize,
        attempt: u64,
    },
}

enum Input<M> {
    Message(NodeId, M),
    Timer(Timer),
    Propose(Command),
    Confirm(RequestId),
}

/// One server: the algorithm instance while it runs, and its storage, which survives
/// crashes.
struct Slot<C> {
    id: NodeId,
    node: Option<C>,
    storage: TrackedStorage,
    incarnation: u32,
    /// Bumped on every set or cancel, so a stale timer event is recognized and ignored.
    timer_generation: [u64; 2],
    crash_on_next_input: bool,
    /// The index the server's state machine expects to apply next.
    next_apply: u64,
    /// Client commands this server accepted and has not yet applied, with the attempt.
    pending: BTreeMap<(u32, u64), u64>,
}

fn timer_slot(timer: Timer) -> usize {
    match timer {
        Timer::Election => 0,
        Timer::Heartbeat => 1,
    }
}

/// A deterministic simulation of a cluster running consensus algorithm `C`.
///
/// Drive it with [`run_until`](Self::run_until) and
/// [`run_until_condition`](Self::run_until_condition), and change the environment with
/// [`inject`](Self::inject) and [`schedule`](Self::schedule). Safety checks run
/// automatically; read the results with [`violations`](Self::violations).
pub struct Simulation<C: Consensus, O: Observer<C> = ()> {
    config: SimConfig,
    seed: u64,
    now: SimTime,
    queue: EventQueue<Event<C::Message>>,
    ids: Vec<NodeId>,
    servers: Vec<Slot<C>>,
    network: Network,
    clients: Vec<Client>,
    workload: consensus::Rng,
    metrics: Metrics,
    checker: Checker,
    replies: Vec<Reply>,
    acknowledged: Vec<(LogIndex, Payload)>,
    confirmations: Vec<Confirmation>,
    max_log_gaps: u64,
    direct_seq: u64,
    events: u64,
    observer: O,
}

impl<C: Consensus> Simulation<C> {
    /// Starts a simulation without an observer.
    ///
    /// # Panics
    ///
    /// Panics if the configuration is invalid.
    pub fn new(config: SimConfig, seed: u64) -> Self {
        Simulation::with_observer(config, seed, ())
    }
}

impl<C: Consensus, O: Observer<C>> Simulation<C, O> {
    /// Starts a simulation: creates every server at time zero and starts the clients.
    ///
    /// # Panics
    ///
    /// Panics if there are no servers or the consensus settings are invalid.
    pub fn with_observer(config: SimConfig, seed: u64, observer: O) -> Self {
        assert!(config.servers > 0, "a simulation needs at least one server");
        if let Err(problem) = config.consensus.validate() {
            panic!("invalid consensus settings: {problem}");
        }
        let ids: Vec<NodeId> = (1..=config.servers as u64).map(NodeId).collect();
        let mut workload = stream_rng(seed, Stream::Workload);
        let servers = ids
            .iter()
            .map(|&id| Slot {
                id,
                node: None,
                storage: TrackedStorage::default(),
                incarnation: 0,
                timer_generation: [0; 2],
                crash_on_next_input: false,
                next_apply: 1,
                pending: BTreeMap::new(),
            })
            .collect();
        let clients = (0..config.clients)
            .map(|_| Client {
                target: workload.below(config.servers as u64) as usize,
                next_seq: 0,
                attempt: 0,
                outstanding: None,
            })
            .collect();

        let mut sim = Simulation {
            network: Network::new(config.one_way_delay, stream_rng(seed, Stream::Network)),
            checker: Checker::new(config.servers, config.raft_checks),
            config,
            seed,
            now: SimTime::ZERO,
            queue: EventQueue::default(),
            ids,
            servers,
            clients,
            workload,
            metrics: Metrics::default(),
            replies: Vec::new(),
            acknowledged: Vec::new(),
            confirmations: Vec::new(),
            max_log_gaps: 0,
            direct_seq: 0,
            events: 0,
            observer,
        };
        for slot in 0..sim.servers.len() {
            sim.start_server(slot);
        }
        for client in 0..sim.clients.len() {
            sim.queue.push(SimTime::ZERO, Event::ClientSend { client });
        }
        sim
    }

    // ----- Running -------------------------------------------------------------------

    /// Processes every event up to and including `deadline`, then sets the clock to it.
    pub fn run_until(&mut self, deadline: SimTime) {
        self.run_until_condition(deadline, |_| false);
    }

    /// Processes events until `condition` holds or `deadline` passes. The condition is
    /// checked before the first event and after every event. Returns `true` if the
    /// condition was met; [`now`](Self::now) is then the time it first held.
    pub fn run_until_condition(
        &mut self,
        deadline: SimTime,
        mut condition: impl FnMut(&Self) -> bool,
    ) -> bool {
        if condition(self) {
            return true;
        }
        while let Some(time) = self.queue.peek_time() {
            if time > deadline {
                break;
            }
            let (time, event) = self.queue.pop().expect("peeked");
            self.now = time;
            self.events += 1;
            self.process(event);
            if condition(self) {
                return true;
            }
        }
        self.now = self.now.max(deadline);
        false
    }

    // ----- Controlling the environment -----------------------------------------------

    /// Applies a fault now.
    pub fn inject(&mut self, fault: Fault) {
        self.apply_fault(fault);
    }

    /// Schedules a fault for time `at`.
    pub fn schedule(&mut self, at: SimTime, fault: Fault) {
        self.queue.push(at.max(self.now), Event::Fault(fault));
    }

    /// Adds a rule that drops matching messages until [`clear_drop_rules`](Self::clear_drop_rules).
    pub fn add_drop_rule(&mut self, rule: DropRule) {
        self.network.add_rule(rule);
    }

    /// Removes every drop rule.
    pub fn clear_drop_rules(&mut self) {
        self.network.clear_rules();
    }

    /// Proposes a command directly at server `id`, as an experiment procedure does,
    /// without a client or network delay. The command is unique but no client waits for
    /// it.
    ///
    /// # Errors
    ///
    /// Returns [`NotLeader`] if the server is not the leader or is stopped.
    pub fn propose(&mut self, id: NodeId) -> Result<(), NotLeader> {
        let command = client::encode(client::DIRECT, self.direct_seq, self.config.command_bytes);
        self.direct_seq += 1;
        let slot = self.slot(id);
        self.input(slot, Input::Propose(command))
            .unwrap_or(Err(NotLeader { leader: None }))
    }

    /// Fires `timer` at server `id` now, whether or not it is pending. Used to reproduce
    /// the paper's synchronized heartbeat before a leader crash (experiment E2).
    pub fn fire_timer(&mut self, id: NodeId, timer: Timer) {
        let slot = self.slot(id);
        self.input(slot, Input::Timer(timer));
    }

    /// Asks server `id` to confirm its leadership. The answer appears later in
    /// [`confirmations`](Self::confirmations).
    ///
    /// # Errors
    ///
    /// Returns [`NotLeader`] if the server is not the leader or is stopped.
    pub fn confirm_leadership(&mut self, id: NodeId, request: RequestId) -> Result<(), NotLeader> {
        let slot = self.slot(id);
        self.input(slot, Input::Confirm(request))
            .unwrap_or(Err(NotLeader { leader: None }))
    }

    // ----- Observing -----------------------------------------------------------------

    /// The current simulated time.
    pub fn now(&self) -> SimTime {
        self.now
    }

    /// The servers' IDs.
    pub fn ids(&self) -> &[NodeId] {
        &self.ids
    }

    /// The simulation's seed.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The status of server `id`, or `None` while it is stopped.
    pub fn status(&self, id: NodeId) -> Option<Status> {
        self.servers[self.slot(id)]
            .node
            .as_ref()
            .map(Consensus::status)
    }

    /// The storage of server `id`.
    pub fn storage(&self, id: NodeId) -> &MemStorage {
        self.servers[self.slot(id)].storage.mem()
    }

    /// Whether server `id` is running.
    pub fn is_running(&self, id: NodeId) -> bool {
        self.servers[self.slot(id)].node.is_some()
    }

    /// The running server that leads the highest term, with that term, if any.
    pub fn leader(&self) -> Option<(NodeId, Term)> {
        self.servers
            .iter()
            .filter_map(|slot| slot.node.as_ref().map(|node| (slot.id, node.status())))
            .filter(|(_, status)| status.role == Role::Leader)
            .max_by_key(|(_, status)| status.term)
            .map(|(id, status)| (id, status.term))
    }

    /// The highest index server `id` has applied since it last started.
    pub fn applied(&self, id: NodeId) -> LogIndex {
        LogIndex(self.servers[self.slot(id)].next_apply - 1)
    }

    /// Counters collected so far.
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Every client reply received so far, in order.
    pub fn replies(&self) -> &[Reply] {
        &self.replies
    }

    /// Every leadership confirmation answered so far, in order.
    pub fn confirmations(&self) -> &[Confirmation] {
        &self.confirmations
    }

    /// Violations recorded so far (the first 32 in full).
    pub fn violations(&self) -> &[Violation] {
        self.checker.violations()
    }

    /// The total number of violations, including those not recorded in full.
    pub fn violation_count(&self) -> u64 {
        self.checker.total()
    }

    /// Entries discarded by all servers so far (metric M17).
    pub fn discarded_entries(&self) -> u64 {
        self.servers.iter().map(|s| s.storage.discarded()).sum()
    }

    /// The largest number of gaps seen in any server's log (metric M15).
    pub fn max_log_gaps(&self) -> u64 {
        self.max_log_gaps
    }

    /// The number of events processed so far.
    pub fn events(&self) -> u64 {
        self.events
    }

    /// The observer.
    pub fn observer(&self) -> &O {
        &self.observer
    }

    /// The observer, mutably, for example to reset its counters.
    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }

    // ----- Checks run on request -----------------------------------------------------

    /// Records a violation found by an experiment procedure, such as a liveness failure.
    pub fn report(&mut self, check: Check, detail: String) {
        self.checker.report(self.now, check, detail);
    }

    /// Raft only: checks every pair of logs for the Log Matching property.
    pub fn check_log_matching(&mut self) {
        let logs: Vec<(NodeId, &MemStorage)> = self
            .servers
            .iter()
            .map(|slot| (slot.id, slot.storage.mem()))
            .collect();
        self.checker.check_log_matching(self.now, &logs);
    }

    /// Checks that every server has applied every acknowledged command. Call it at the
    /// end of a run, after faults have stopped and servers have had time to catch up.
    pub fn check_durability(&mut self) {
        let applied: Vec<(NodeId, u64)> = self
            .servers
            .iter()
            .map(|slot| (slot.id, slot.next_apply - 1))
            .collect();
        self.checker
            .check_durability(self.now, &self.acknowledged, &applied);
    }

    // ----- Servers -------------------------------------------------------------------

    fn slot(&self, id: NodeId) -> usize {
        self.ids
            .iter()
            .position(|&known| known == id)
            .unwrap_or_else(|| panic!("server {} is not in the cluster", id.0))
    }

    fn start_server(&mut self, slot: usize) {
        let server = &mut self.servers[slot];
        let seed = stream_seed(
            self.seed,
            Stream::Server {
                id: server.id,
                incarnation: server.incarnation,
            },
        );
        let (node, outputs) = C::start(
            server.id,
            &self.ids,
            &self.config.consensus,
            seed,
            &mut server.storage,
        );
        server.node = Some(node);
        self.finish_input(slot, outputs);
    }

    fn stop_server(&mut self, slot: usize) {
        let server = &mut self.servers[slot];
        server.node = None;
        server.storage.crash();
        server.pending.clear();
        server.crash_on_next_input = false;
        self.checker.on_stop(slot);
    }

    fn restart_server(&mut self, slot: usize) {
        let server = &mut self.servers[slot];
        server.incarnation += 1;
        for generation in &mut server.timer_generation {
            *generation += 1;
        }
        // The state machine starts empty and is rebuilt as entries are applied again.
        server.next_apply = 1;
        self.start_server(slot);
    }

    /// Delivers one input to a running server and carries out its outputs. Returns `None`
    /// if the server is stopped, otherwise whether the server accepted the input.
    fn input(&mut self, slot: usize, input: Input<C::Message>) -> Option<Result<(), NotLeader>> {
        let server = &mut self.servers[slot];
        let node = server.node.as_mut()?;
        let storage = &mut server.storage;
        let (outputs, result) = match input {
            Input::Message(from, message) => (node.step(storage, from, message), Ok(())),
            Input::Timer(timer) => (node.on_timer(storage, timer), Ok(())),
            Input::Propose(command) => match node.propose(storage, command) {
                Ok(outputs) => (outputs, Ok(())),
                Err(not_leader) => (Vec::new(), Err(not_leader)),
            },
            Input::Confirm(request) => match node.confirm_leadership(storage, request) {
                Ok(outputs) => (outputs, Ok(())),
                Err(not_leader) => (Vec::new(), Err(not_leader)),
            },
        };
        self.finish_input(slot, outputs);
        Some(result)
    }

    /// Follows the driver contract after an input: sync storage, then carry out outputs.
    /// Also runs the per-input checks.
    fn finish_input(&mut self, slot: usize, outputs: Vec<Output<C::Message>>) {
        let server = &mut self.servers[slot];
        if server.crash_on_next_input {
            // The crash happens before the sync: the writes are lost and nothing is sent.
            self.stop_server(slot);
            return;
        }
        if server.storage.sync() {
            self.metrics.fsyncs += 1;
        }
        for index in server.storage.take_written() {
            self.checker
                .on_write(self.now, server.id, index, server.storage.mem());
        }
        for output in outputs {
            self.carry_out(slot, output);
        }

        let server = &self.servers[slot];
        let Some(node) = &server.node else {
            return;
        };
        let status = node.status();
        self.checker
            .on_status(self.now, slot, server.id, status, server.storage.mem());
        self.max_log_gaps = self.max_log_gaps.max(server.storage.gaps());
        self.observer.after_input(
            self.now,
            ServerView {
                id: server.id,
                status: Some(status),
                storage: server.storage.mem(),
            },
        );
    }

    fn carry_out(&mut self, slot: usize, output: Output<C::Message>) {
        match output {
            Output::Send { to, message } => self.send(slot, to, message),
            Output::SetTimer { timer, after } => {
                let generation = &mut self.servers[slot].timer_generation[timer_slot(timer)];
                *generation += 1;
                let event = Event::Timer {
                    slot,
                    timer,
                    generation: *generation,
                };
                self.queue.push(self.now + after, event);
            }
            Output::CancelTimer { timer } => {
                self.servers[slot].timer_generation[timer_slot(timer)] += 1;
            }
            Output::Apply { index, payload } => self.apply(slot, index, payload),
            Output::LeadershipConfirmed {
                request,
                read_index,
            } => self.confirmations.push(Confirmation {
                time: self.now,
                server: self.ids[slot],
                request,
                read_index: Some(read_index),
            }),
            Output::LeadershipLost { request } => self.confirmations.push(Confirmation {
                time: self.now,
                server: self.ids[slot],
                request,
                read_index: None,
            }),
        }
    }

    fn send(&mut self, slot: usize, to: NodeId, message: C::Message) {
        let from = self.ids[slot];
        let entries = C::entry_count(&message) as u64;
        let bytes = if self.config.count_bytes {
            postcard::to_allocvec(&message).map_or(0, |encoded| encoded.len() as u64)
        } else {
            0
        };
        self.metrics
            .record_send(from, C::message_kind(&message), bytes, entries);
        match self.network.route(from, to, entries > 0) {
            Route::Dropped => self.metrics.dropped += 1,
            Route::Delivered { delay, duplicate } => {
                if let Some(extra) = duplicate {
                    let copy = Event::Deliver {
                        from,
                        to,
                        message: message.clone(),
                    };
                    self.queue.push(self.now + extra, copy);
                }
                self.queue
                    .push(self.now + delay, Event::Deliver { from, to, message });
            }
        }
    }

    fn apply(&mut self, slot: usize, index: LogIndex, payload: Payload) {
        let server = &mut self.servers[slot];
        self.checker
            .on_apply(self.now, server.id, index, &payload, &mut server.next_apply);
        let Payload::Command(command) = &payload else {
            return;
        };
        let Some(key) = client::decode(command) else {
            return;
        };
        if server.pending.remove(&key).is_some() {
            let delay = self.network.delay();
            let reply = Event::ClientReply {
                client: key.0 as usize,
                seq: key.1,
                index,
            };
            self.queue.push(self.now + delay, reply);
        }
    }

    // ----- Events --------------------------------------------------------------------

    fn process(&mut self, event: Event<C::Message>) {
        match event {
            Event::Deliver { from, to, message } => self.deliver(from, to, message),
            Event::Timer {
                slot,
                timer,
                generation,
            } => {
                let server = &self.servers[slot];
                if server.node.is_some() && server.timer_generation[timer_slot(timer)] == generation
                {
                    self.input(slot, Input::Timer(timer));
                }
            }
            Event::Fault(fault) => self.apply_fault(fault),
            Event::ClientSend { client } => self.client_send(client),
            Event::ClientArrive {
                slot,
                client,
                seq,
                attempt,
            } => self.client_arrive(slot, client, seq, attempt),
            Event::ClientRedirect {
                client,
                attempt,
                leader,
            } => self.client_redirect(client, attempt, leader),
            Event::ClientRetry { client, attempt } => {
                if self.clients[client].attempt == attempt {
                    self.client_attempt(client);
                }
            }
            Event::ClientReply { client, seq, index } => self.client_reply(client, seq, index),
            Event::ClientTimeout { client, attempt } => {
                let current = &self.clients[client];
                if current.attempt == attempt && current.outstanding.is_some() {
                    self.clients[client].target = self.random_server();
                    self.client_attempt(client);
                }
            }
        }
    }

    fn deliver(&mut self, from: NodeId, to: NodeId, message: C::Message) {
        let slot = self.slot(to);
        let server = &self.servers[slot];
        let Some(node) = &server.node else {
            self.metrics.dropped += 1;
            return;
        };
        let status = node.status();
        self.metrics.delivered += 1;
        if matches!(status.role, Role::Candidate | Role::Leader) {
            self.metrics.entries_to_leaders += C::entry_count(&message) as u64;
        }
        self.observer.on_deliver(
            self.now,
            from,
            to,
            &message,
            ServerView {
                id: to,
                status: Some(status),
                storage: server.storage.mem(),
            },
        );
        self.input(slot, Input::Message(from, message));
    }

    fn apply_fault(&mut self, fault: Fault) {
        match fault {
            Fault::Crash(id) => {
                let slot = self.slot(id);
                if self.servers[slot].node.is_some() {
                    self.stop_server(slot);
                }
            }
            Fault::CrashDuringNextInput(id) => {
                let slot = self.slot(id);
                if self.servers[slot].node.is_some() {
                    self.servers[slot].crash_on_next_input = true;
                }
            }
            Fault::Restart(id) => {
                let slot = self.slot(id);
                if self.servers[slot].node.is_none() {
                    self.restart_server(slot);
                }
            }
            Fault::Partition(groups) => self.network.partition(&groups),
            Fault::Block { from, to } => self.network.block(from, to),
            Fault::Unblock { from, to } => self.network.unblock(from, to),
            Fault::HealLinks => self.network.heal_links(),
            Fault::SetLoss(ppm) => self.network.loss_ppm = ppm,
            Fault::SetDuplication(ppm) => self.network.duplicate_ppm = ppm,
            Fault::SetDelayFactor(factor) => self.network.delay_factor = factor.max(1),
            Fault::HealAll => {
                self.network.heal_links();
                self.network.loss_ppm = 0;
                self.network.duplicate_ppm = 0;
                self.network.delay_factor = 1;
                for slot in 0..self.servers.len() {
                    self.servers[slot].crash_on_next_input = false;
                    if self.servers[slot].node.is_none() {
                        self.restart_server(slot);
                    }
                }
            }
        }
    }

    // ----- Clients -------------------------------------------------------------------

    fn random_server(&mut self) -> usize {
        self.workload.below(self.servers.len() as u64) as usize
    }

    fn client_send(&mut self, client: usize) {
        let state = &mut self.clients[client];
        if state.outstanding.is_some() {
            return;
        }
        state.outstanding = Some(Outstanding {
            seq: state.next_seq,
            first_sent: self.now,
        });
        state.next_seq += 1;
        self.client_attempt(client);
    }

    /// Sends the outstanding command to the client's current target.
    fn client_attempt(&mut self, client: usize) {
        let delay = self.network.delay();
        let state = &mut self.clients[client];
        let Some(outstanding) = &state.outstanding else {
            return;
        };
        state.attempt += 1;
        let arrive = Event::ClientArrive {
            slot: state.target,
            client,
            seq: outstanding.seq,
            attempt: state.attempt,
        };
        let timeout = Event::ClientTimeout {
            client,
            attempt: state.attempt,
        };
        self.queue.push(self.now + delay, arrive);
        self.queue
            .push(self.now + self.config.client_timeout, timeout);
    }

    fn client_arrive(&mut self, slot: usize, client: usize, seq: u64, attempt: u64) {
        if self.servers[slot].node.is_none() {
            return;
        }
        let key = (client as u32, seq);
        let command = client::encode(key.0, seq, self.config.command_bytes);
        // Registered first: a one-server cluster applies the command inside `propose`.
        self.servers[slot].pending.insert(key, attempt);
        if let Some(Err(NotLeader { leader })) = self.input(slot, Input::Propose(command)) {
            self.servers[slot].pending.remove(&key);
            let delay = self.network.delay();
            let redirect = Event::ClientRedirect {
                client,
                attempt,
                leader,
            };
            self.queue.push(self.now + delay, redirect);
        }
    }

    fn client_redirect(&mut self, client: usize, attempt: u64, leader: Option<NodeId>) {
        if self.clients[client].attempt != attempt || self.clients[client].outstanding.is_none() {
            return;
        }
        match leader.and_then(|id| self.ids.iter().position(|&known| known == id)) {
            Some(slot) => {
                self.clients[client].target = slot;
                self.client_attempt(client);
            }
            None => {
                self.clients[client].target = self.random_server();
                let retry = Event::ClientRetry { client, attempt };
                self.queue
                    .push(self.now + self.config.client_backoff, retry);
            }
        }
    }

    fn client_reply(&mut self, client: usize, seq: u64, index: LogIndex) {
        let state = &mut self.clients[client];
        let Some(outstanding) = &state.outstanding else {
            return;
        };
        if outstanding.seq != seq {
            return;
        }
        self.replies.push(Reply {
            sent: outstanding.first_sent,
            received: self.now,
            index,
        });
        let command = client::encode(client as u32, seq, self.config.command_bytes);
        self.acknowledged.push((index, Payload::Command(command)));
        self.metrics.client_replies += 1;
        state.outstanding = None;
        // Invalidates the pending timeout of the finished command.
        state.attempt += 1;
        self.queue.push(self.now, Event::ClientSend { client });
    }
}
