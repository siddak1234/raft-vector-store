//! Model-checking adapter that runs any `Consensus` implementation under Stateright and
//! counts the states it can reach (experiment E6).
//!
//! The adapter is a driver in the sense of the `Consensus` driver contract: it delivers one
//! input at a time, syncs storage, and carries out the outputs. Instead of choosing one
//! order of events, it lets Stateright explore every order breadth-first, within the bounds
//! of `docs/RESEARCH.md` §6 (E6):
//!
//! - a fixed number of servers, which never crash;
//! - an unordered, non-duplicating network: any message in flight may be delivered next, a
//!   delivered message is gone, and a message never delivered models a lost one;
//! - timers that fire in any order, whatever their durations;
//! - at most a few client commands, elections per server, and heartbeats per server.
//!
//! The search state holds each server's full algorithm state, so successors are computed
//! exactly. Metric M14 counts the distinct states of the compared fields only: each
//! server's role, term, vote, log, and commit index, plus the messages in flight. Random
//! number generator state, timer durations, and the bound counters are not compared.
//!
//! See `docs/PLAN.md` §4.6.

use std::collections::{BTreeSet, HashSet};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};

use consensus::{
    Command, Config, Consensus, LogIndex, MemStorage, NodeId, Output, Payload, Role, Storage, Timer,
};
use stateright::{Model, Property};

/// The limits of the search (`docs/RESEARCH.md` §6, E6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bounds {
    /// Number of servers.
    pub servers: usize,
    /// Most client commands proposed in total.
    pub commands: u8,
    /// Most elections each server starts.
    pub elections: u8,
    /// Most times each server's heartbeat timer fires.
    pub heartbeats: u8,
}

/// A message in flight.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Envelope<M> {
    /// The sender.
    pub from: NodeId,
    /// The receiver.
    pub to: NodeId,
    /// The message.
    pub message: M,
}

/// One server in the search state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Server<C> {
    /// The algorithm's state. Its random number generator is ignored by equality and
    /// hashing (`consensus::Rng`).
    pub node: C,
    /// The server's storage, synced after every input.
    pub storage: MemStorage,
    /// Timers that are set and may fire.
    pub timers: BTreeSet<Timer>,
    /// Elections started so far.
    pub elections: u8,
    /// Heartbeat timers fired so far.
    pub heartbeats: u8,
}

/// A state of the whole system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct State<C: Consensus> {
    /// The servers, in ID order.
    pub servers: Vec<Server<C>>,
    /// The messages in flight: a set, kept sorted by a fixed hash so equal sets compare and
    /// hash equally.
    pub network: Vec<Envelope<C::Message>>,
    /// Client commands proposed so far.
    pub commands: u8,
}

/// A step of the search.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action<M> {
    /// Deliver a message in flight.
    Deliver(Envelope<M>),
    /// Fire a timer at the server in this slot.
    Fire(usize, Timer),
    /// Propose the next client command at the server in this slot, which is a leader.
    Propose(usize),
}

/// An algorithm the model checker can explore: its state and messages can be copied,
/// compared, hashed, and shared between search threads.
pub trait Checkable:
    Consensus<Message: Send + Sync> + Clone + Debug + Hash + Eq + Send + Sync + 'static
{
}

impl<C> Checkable for C where
    C: Consensus<Message: Send + Sync> + Clone + Debug + Hash + Eq + Send + Sync + 'static
{
}

/// The search over one algorithm.
pub struct ConsensusModel<C> {
    bounds: Bounds,
    config: Config,
    ids: Vec<NodeId>,
    compared: Arc<Projections>,
    _algorithm: std::marker::PhantomData<fn() -> C>,
}

/// The fingerprints of the compared fields of every state generated, in shards so that
/// search threads rarely wait for each other.
#[derive(Debug, Default)]
pub struct Projections {
    shards: Vec<Mutex<HashSet<u64>>>,
}

const SHARDS: usize = 64;

impl Projections {
    fn new() -> Self {
        Projections {
            shards: (0..SHARDS).map(|_| Mutex::new(HashSet::new())).collect(),
        }
    }

    fn insert(&self, fingerprint: u64) {
        let shard = (fingerprint % SHARDS as u64) as usize;
        self.shards[shard]
            .lock()
            .expect("no search thread panicked")
            .insert(fingerprint);
    }

    /// The number of distinct compared states (metric M14).
    pub fn count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().expect("no search thread panicked").len())
            .sum()
    }
}

impl<C: Checkable> ConsensusModel<C> {
    /// A search with `bounds`, where every server uses `config`.
    pub fn new(bounds: Bounds, config: Config) -> Self {
        ConsensusModel {
            bounds,
            ids: (1..=bounds.servers as u64).map(NodeId).collect(),
            config,
            compared: Arc::new(Projections::new()),
            _algorithm: std::marker::PhantomData,
        }
    }

    /// The compared states found so far, shared with the checker's threads.
    pub fn projections(&self) -> Arc<Projections> {
        Arc::clone(&self.compared)
    }

    /// Records the compared fields of `state` (metric M14).
    fn record(&self, state: &State<C>) {
        self.compared.insert(compared_fingerprint(state));
    }

    /// Carries out the outputs of one input at server `slot`, after syncing its storage.
    fn finish(&self, state: &mut State<C>, slot: usize, outputs: Vec<Output<C::Message>>) {
        let server = &mut state.servers[slot];
        server.storage.sync();
        let from = self.ids[slot];
        for output in outputs {
            match output {
                Output::Send { to, message } => {
                    let envelope = Envelope { from, to, message };
                    if !state.network.contains(&envelope) {
                        state.network.push(envelope);
                    }
                }
                Output::SetTimer { timer, .. } => {
                    state.servers[slot].timers.insert(timer);
                }
                Output::CancelTimer { timer } => {
                    state.servers[slot].timers.remove(&timer);
                }
                Output::Apply { .. }
                | Output::LeadershipConfirmed { .. }
                | Output::LeadershipLost { .. } => {}
            }
        }
        sort_network(&mut state.network);
    }
}

impl<C: Checkable> Model for ConsensusModel<C> {
    type State = State<C>;
    type Action = Action<C::Message>;

    fn init_states(&self) -> Vec<State<C>> {
        let mut state = State {
            servers: Vec::new(),
            network: Vec::new(),
            commands: 0,
        };
        let mut started = Vec::new();
        for &id in &self.ids {
            let mut storage = MemStorage::new();
            let (node, outputs) = C::start(id, &self.ids, &self.config, id.0, &mut storage);
            state.servers.push(Server {
                node,
                storage,
                timers: BTreeSet::new(),
                elections: 0,
                heartbeats: 0,
            });
            started.push(outputs);
        }
        for (slot, outputs) in started.into_iter().enumerate() {
            self.finish(&mut state, slot, outputs);
        }
        self.record(&state);
        vec![state]
    }

    fn actions(&self, state: &State<C>, actions: &mut Vec<Action<C::Message>>) {
        for envelope in &state.network {
            actions.push(Action::Deliver(envelope.clone()));
        }
        for (slot, server) in state.servers.iter().enumerate() {
            for &timer in &server.timers {
                let allowed = match timer {
                    Timer::Election => server.elections < self.bounds.elections,
                    Timer::Heartbeat => server.heartbeats < self.bounds.heartbeats,
                };
                if allowed {
                    actions.push(Action::Fire(slot, timer));
                }
            }
            if state.commands < self.bounds.commands && server.node.status().role == Role::Leader {
                actions.push(Action::Propose(slot));
            }
        }
    }

    fn next_state(&self, last: &State<C>, action: Action<C::Message>) -> Option<State<C>> {
        let mut state = last.clone();
        match action {
            Action::Deliver(envelope) => {
                state.network.retain(|e| *e != envelope);
                let slot = self.slot(envelope.to);
                let server = &mut state.servers[slot];
                let outputs =
                    server
                        .node
                        .step(&mut server.storage, envelope.from, envelope.message);
                self.finish(&mut state, slot, outputs);
            }
            Action::Fire(slot, timer) => {
                let server = &mut state.servers[slot];
                server.timers.remove(&timer);
                let role = server.node.status().role;
                match timer {
                    // Both algorithms start an election when this timer fires at a server
                    // that is not leading.
                    Timer::Election if role != Role::Leader => server.elections += 1,
                    Timer::Heartbeat => server.heartbeats += 1,
                    Timer::Election => {}
                }
                let outputs = server.node.on_timer(&mut server.storage, timer);
                self.finish(&mut state, slot, outputs);
            }
            Action::Propose(slot) => {
                let command = Command(vec![state.commands + 1]);
                state.commands += 1;
                let server = &mut state.servers[slot];
                let outputs = server.node.propose(&mut server.storage, command).ok()?;
                self.finish(&mut state, slot, outputs);
            }
        }
        self.record(&state);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("election safety", |_, state: &State<C>| {
                election_safety(state)
            }),
            Property::always("state machine safety", |_, state: &State<C>| {
                state_machine_safety(state)
            }),
            Property::sometimes("a command is committed", |_, state: &State<C>| {
                command_committed(state)
            }),
        ]
    }
}

impl<C> ConsensusModel<C> {
    fn slot(&self, id: NodeId) -> usize {
        self.ids
            .iter()
            .position(|&known| known == id)
            .expect("messages go to servers in the cluster")
    }
}

/// At most one server leads each term at any moment.
pub fn election_safety<C: Consensus>(state: &State<C>) -> bool {
    let mut leading = BTreeSet::new();
    state
        .servers
        .iter()
        .map(|server| server.node.status())
        .filter(|status| status.role == Role::Leader)
        .all(|status| leading.insert(status.term))
}

/// Every two servers hold the same payload at every index both have committed. Both
/// algorithms apply exactly their committed prefix, so this is state machine safety.
pub fn state_machine_safety<C: Consensus>(state: &State<C>) -> bool {
    let committed: Vec<(LogIndex, &MemStorage)> = state
        .servers
        .iter()
        .map(|server| (server.node.status().commit_index, &server.storage))
        .collect();
    for (i, (commit_a, log_a)) in committed.iter().enumerate() {
        for (commit_b, log_b) in &committed[i + 1..] {
            for index in 1..=commit_a.min(commit_b).0 {
                let index = LogIndex(index);
                let a = log_a.entry(index).map(|e| &e.payload);
                let b = log_b.entry(index).map(|e| &e.payload);
                if a.is_none() || a != b {
                    return false;
                }
            }
        }
    }
    true
}

/// Some server has committed a client command.
pub fn command_committed<C: Consensus>(state: &State<C>) -> bool {
    state.servers.iter().any(|server| {
        let commit = server.node.status().commit_index;
        (1..=commit.0).any(|i| {
            server
                .storage
                .entry(LogIndex(i))
                .is_some_and(|e| matches!(e.payload, Payload::Command(_)))
        })
    })
}

/// Hashes the compared fields of a state: each server's role, term, vote, log, and commit
/// index, and the messages in flight.
pub fn compared_fingerprint<C: Consensus>(state: &State<C>) -> u64 {
    let mut hasher = fixed_hasher();
    for server in &state.servers {
        let status = server.node.status();
        let hard = server.storage.hard_state();
        status.role.hash(&mut hasher);
        status.term.hash(&mut hasher);
        hard.voted_for.hash(&mut hasher);
        status.commit_index.hash(&mut hasher);
        let last = server.storage.last_index();
        for i in 1..=last.0 {
            server.storage.entry(LogIndex(i)).hash(&mut hasher);
        }
        last.hash(&mut hasher);
    }
    state.network.hash(&mut hasher);
    hasher.finish()
}

/// A hasher with fixed keys, so fingerprints do not depend on the process.
fn fixed_hasher() -> std::hash::DefaultHasher {
    std::hash::BuildHasherDefault::<std::hash::DefaultHasher>::default().build_hasher()
}

/// Sorts the messages in flight by a fixed hash, so the same set always has the same order.
fn sort_network<M: Hash + Eq>(network: &mut [Envelope<M>]) {
    network.sort_by_cached_key(|envelope| {
        let mut hasher = fixed_hasher();
        envelope.hash(&mut hasher);
        hasher.finish()
    });
}

#[cfg(test)]
mod tests {
    use consensus::{Command, Entry, LogIndex, MemStorage, Payload, Storage, Term};

    use super::Envelope;

    #[test]
    fn sorting_makes_equal_sets_equal() {
        let a = Envelope {
            from: consensus::NodeId(1),
            to: consensus::NodeId(2),
            message: 1u8,
        };
        let b = Envelope {
            from: consensus::NodeId(2),
            to: consensus::NodeId(1),
            message: 2u8,
        };
        let mut x = vec![a.clone(), b.clone()];
        let mut y = vec![b, a];
        super::sort_network(&mut x);
        super::sort_network(&mut y);
        assert_eq!(x, y);
    }

    #[test]
    fn memstorage_is_compared_by_content() {
        let mut a = MemStorage::new();
        a.put(
            LogIndex(1),
            Entry {
                term: Term(1),
                payload: Payload::Command(Command(vec![1])),
            },
        );
        a.sync();
        let b = a.clone();
        assert_eq!(a, b);
    }
}
