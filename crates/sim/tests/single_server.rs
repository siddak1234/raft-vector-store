//! Runs the simulator with the smallest possible algorithm: one server that commits every
//! command as soon as it is proposed. This exercises the driver loop, clients, crashes,
//! and determinism without depending on Raft or Multi-Paxos.

use std::time::Duration;

use consensus::{
    Command, Config, Consensus, Entry, HardState, LogIndex, NodeId, NotLeader, Output, Payload,
    RequestId, Role, Status, Storage, Term, Timer,
};
use serde::{Deserialize, Serialize};
use sim::{Fault, SimConfig, SimTime, Simulation};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum NoMessage {}

struct Solo {
    id: NodeId,
    term: Term,
    commit_index: LogIndex,
}

impl Consensus for Solo {
    type Message = NoMessage;

    fn start<S: Storage>(
        id: NodeId,
        _cluster: &[NodeId],
        _config: &Config,
        _seed: u64,
        storage: &mut S,
    ) -> (Self, Vec<Output<NoMessage>>) {
        let term = Term(storage.hard_state().term.0 + 1);
        storage.set_hard_state(HardState {
            term,
            voted_for: Some(id),
        });
        let last = storage.last_index();
        let outputs = (1..=last.0)
            .map(LogIndex)
            .map(|index| Output::Apply {
                index,
                payload: storage.entry(index).expect("no gaps").payload.clone(),
            })
            .collect();
        let solo = Solo {
            id,
            term,
            commit_index: last,
        };
        (solo, outputs)
    }

    fn step<S: Storage>(
        &mut self,
        _: &mut S,
        _: NodeId,
        message: NoMessage,
    ) -> Vec<Output<NoMessage>> {
        match message {}
    }

    fn on_timer<S: Storage>(&mut self, _: &mut S, _: Timer) -> Vec<Output<NoMessage>> {
        Vec::new()
    }

    fn propose<S: Storage>(
        &mut self,
        storage: &mut S,
        command: Command,
    ) -> Result<Vec<Output<NoMessage>>, NotLeader> {
        let index = storage.last_index().next();
        let payload = Payload::Command(command);
        storage.put(
            index,
            Entry {
                term: self.term,
                payload: payload.clone(),
            },
        );
        self.commit_index = index;
        Ok(vec![Output::Apply { index, payload }])
    }

    fn confirm_leadership<S: Storage>(
        &mut self,
        _: &mut S,
        request: RequestId,
    ) -> Result<Vec<Output<NoMessage>>, NotLeader> {
        Ok(vec![Output::LeadershipConfirmed {
            request,
            read_index: self.commit_index,
        }])
    }

    fn status(&self) -> Status {
        Status {
            role: Role::Leader,
            term: self.term,
            leader: Some(self.id),
            commit_index: self.commit_index,
        }
    }

    fn message_kind(message: &NoMessage) -> &'static str {
        match *message {}
    }

    fn entry_count(message: &NoMessage) -> usize {
        match *message {}
    }
}

fn config() -> SimConfig {
    SimConfig {
        servers: 1,
        clients: 2,
        ..SimConfig::default()
    }
}

fn at(ms: u64) -> SimTime {
    SimTime::from_duration(Duration::from_millis(ms))
}

#[test]
fn clients_receive_replies_and_all_checks_pass() {
    let mut sim: Simulation<Solo> = Simulation::new(config(), 1);
    sim.run_until(at(1_000));

    // Each command takes a round trip of about 15 ms, so two clients finish well over 100.
    assert!(
        sim.replies().len() > 100,
        "only {} replies",
        sim.replies().len()
    );
    assert!(
        sim.replies()
            .iter()
            .all(|r| r.latency() >= Duration::from_micros(7500))
    );
    sim.check_durability();
    assert!(sim.violations().is_empty(), "{:?}", sim.violations());
    assert_eq!(sim.metrics().client_replies, sim.replies().len() as u64);
}

#[test]
fn restart_reapplies_the_stored_log() {
    let mut sim: Simulation<Solo> = Simulation::new(config(), 2);
    sim.run_until(at(500));
    let id = NodeId(1);
    let applied = sim.applied(id);
    assert!(applied.0 > 0);

    sim.inject(Fault::Crash(id));
    assert!(!sim.is_running(id));
    sim.schedule(at(700), Fault::Restart(id));
    sim.run_until(at(1_500));

    // After restarting, the server re-applied its log and kept serving clients.
    assert!(sim.is_running(id));
    assert!(sim.applied(id) > applied);
    sim.check_durability();
    assert!(sim.violations().is_empty(), "{:?}", sim.violations());
}

#[test]
fn same_seed_same_run() {
    let run = |seed| {
        let mut sim: Simulation<Solo> = Simulation::new(config(), seed);
        sim.run_until(at(800));
        (
            sim.replies().to_vec(),
            sim.events(),
            sim.storage(NodeId(1)).clone(),
        )
    };
    assert_eq!(run(5), run(5));
    assert_ne!(run(5).0, run(6).0);
}

#[test]
fn condition_stops_the_run_when_it_first_holds() {
    let mut sim: Simulation<Solo> = Simulation::new(config(), 3);
    let met = sim.run_until_condition(at(10_000), |s| s.replies().len() >= 10);
    assert!(met);
    assert_eq!(sim.replies().len(), 10);
    assert_eq!(sim.now(), sim.replies()[9].received);
}
