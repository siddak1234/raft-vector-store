//! Exercises the `Consensus` driver contract with the smallest possible implementation: a
//! single server that commits every command as soon as it is proposed.

use consensus::{
    Command, Config, Consensus, Entry, HardState, LogIndex, MemStorage, NodeId, NotLeader, Output,
    Payload, RequestId, Role, Status, Storage, Term, Timer,
};
use serde::{Deserialize, Serialize};

/// A single server never sends messages, so its message type has no values.
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
        // Each start begins a new term, and every stored entry is already committed.
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
        _storage: &mut S,
        _from: NodeId,
        message: NoMessage,
    ) -> Vec<Output<NoMessage>> {
        match message {}
    }

    fn on_timer<S: Storage>(&mut self, _storage: &mut S, _timer: Timer) -> Vec<Output<NoMessage>> {
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
        _storage: &mut S,
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

fn command(byte: u8) -> Command {
    Command(vec![byte])
}

fn applied(byte: u8, index: u64) -> Output<NoMessage> {
    Output::Apply {
        index: LogIndex(index),
        payload: Payload::Command(command(byte)),
    }
}

#[test]
fn restart_recovers_only_synced_entries() {
    let id = NodeId(1);
    let config = Config::default();
    let mut storage = MemStorage::new();

    let (mut solo, outputs) = Solo::start(id, &[id], &config, 0, &mut storage);
    assert!(outputs.is_empty());
    storage.sync(); // Rule 2: sync after every input.

    let outputs = solo.propose(&mut storage, command(10)).expect("leader");
    assert!(storage.sync());
    assert_eq!(outputs, vec![applied(10, 1)]);

    // The server crashes after proposing a second command but before the driver syncs.
    solo.propose(&mut storage, command(20)).expect("leader");
    storage.crash();

    // Rule 4: restart from the same storage. Only the synced command survives.
    let (restarted, outputs) = Solo::start(id, &[id], &config, 0, &mut storage);
    assert_eq!(outputs, vec![applied(10, 1)]);
    assert_eq!(restarted.status().commit_index, LogIndex(1));
    assert_eq!(restarted.status().term, Term(2));
}

#[test]
fn leadership_confirmation_reports_the_commit_index() {
    let id = NodeId(1);
    let mut storage = MemStorage::new();
    let (mut solo, _) = Solo::start(id, &[id], &Config::default(), 0, &mut storage);
    solo.propose(&mut storage, command(1)).expect("leader");
    storage.sync();

    let outputs = solo
        .confirm_leadership(&mut storage, RequestId(7))
        .expect("leader");
    assert_eq!(
        outputs,
        vec![Output::LeadershipConfirmed {
            request: RequestId(7),
            read_index: LogIndex(1),
        }]
    );
}
