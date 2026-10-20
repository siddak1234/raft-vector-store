//! Tests of individual Raft rules (Figure 2), driving one server by hand.

use consensus::{
    Command, Config, Consensus, Entry, HardState, LogIndex, MemStorage, NodeId, Output, Payload,
    Role, Storage, Term, Timer,
};
use raft::{Message, Raft};

const CLUSTER: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

fn entry(term: u64, byte: u8) -> Entry {
    Entry {
        term: Term(term),
        payload: Payload::Command(Command(vec![byte])),
    }
}

/// Storage holding `entries` from index 1, with `currentTerm` set to `term`.
fn storage(term: u64, entries: &[Entry]) -> MemStorage {
    let mut storage = MemStorage::new();
    storage.set_hard_state(HardState {
        term: Term(term),
        voted_for: None,
    });
    for (i, entry) in entries.iter().enumerate() {
        storage.put(LogIndex(i as u64 + 1), entry.clone());
    }
    storage.sync();
    storage
}

fn start(id: u64, storage: &mut MemStorage) -> Raft {
    Raft::start(NodeId(id), &CLUSTER, &Config::default(), 1, storage).0
}

/// The messages in `outputs`, with their receivers.
fn sent(outputs: &[Output<Message>]) -> Vec<(NodeId, Message)> {
    outputs
        .iter()
        .filter_map(|o| match o {
            Output::Send { to, message } => Some((*to, message.clone())),
            _ => None,
        })
        .collect()
}

fn vote_granted(outputs: &[Output<Message>]) -> bool {
    match sent(outputs).as_slice() {
        [(_, Message::RequestVoteReply { granted, .. })] => *granted,
        other => panic!("expected one vote reply, got {other:?}"),
    }
}

#[test]
fn refuses_a_candidate_whose_log_is_behind() {
    // The voter's last entry is from term 2; the candidate's is from term 1.
    let mut log = storage(2, &[entry(1, 1), entry(2, 2)]);
    let mut voter = start(1, &mut log);
    let stale = Message::RequestVote {
        term: Term(3),
        last_log_index: LogIndex(5),
        last_log_term: Term(1),
    };
    assert!(!vote_granted(&voter.step(&mut log, NodeId(2), stale)));

    // A candidate whose last entry is from the same term and at least as far along wins it.
    let current = Message::RequestVote {
        term: Term(3),
        last_log_index: LogIndex(2),
        last_log_term: Term(2),
    };
    assert!(vote_granted(&voter.step(&mut log, NodeId(3), current)));
    assert_eq!(log.hard_state().voted_for, Some(NodeId(3)));
}

#[test]
fn grants_at_most_one_vote_per_term() {
    let mut log = storage(1, &[]);
    let mut voter = start(1, &mut log);
    let ask = |term| Message::RequestVote {
        term: Term(term),
        last_log_index: LogIndex::ZERO,
        last_log_term: Term::ZERO,
    };
    assert!(vote_granted(&voter.step(&mut log, NodeId(2), ask(2))));
    assert!(!vote_granted(&voter.step(&mut log, NodeId(3), ask(2))));
    // The same candidate asking again (a retransmission) gets the same answer.
    assert!(vote_granted(&voter.step(&mut log, NodeId(2), ask(2))));
}

#[test]
fn replaces_conflicting_entries() {
    let mut log = storage(1, &[entry(1, 1), entry(1, 2), entry(1, 3)]);
    let mut follower = start(1, &mut log);
    let append = Message::AppendEntries {
        term: Term(2),
        prev_log_index: LogIndex(1),
        prev_log_term: Term(1),
        entries: vec![entry(2, 9)],
        leader_commit: LogIndex::ZERO,
        round: 1,
    };
    follower.step(&mut log, NodeId(2), append);
    assert_eq!(log.entry(LogIndex(2)), Some(&entry(2, 9)));
    assert_eq!(log.last_index(), LogIndex(2));
}

#[test]
fn a_delayed_shorter_append_keeps_newer_entries() {
    let mut log = storage(1, &[entry(1, 1), entry(1, 2), entry(1, 3)]);
    let mut follower = start(1, &mut log);
    // An old message that carries only the first entry arrives late.
    let late = Message::AppendEntries {
        term: Term(1),
        prev_log_index: LogIndex::ZERO,
        prev_log_term: Term::ZERO,
        entries: vec![entry(1, 1)],
        leader_commit: LogIndex::ZERO,
        round: 1,
    };
    follower.step(&mut log, NodeId(2), late);
    assert_eq!(log.last_index(), LogIndex(3));
}

#[test]
fn rejects_append_without_the_previous_entry() {
    let mut log = storage(1, &[entry(1, 1)]);
    let mut follower = start(1, &mut log);
    let gap = Message::AppendEntries {
        term: Term(1),
        prev_log_index: LogIndex(4),
        prev_log_term: Term(1),
        entries: vec![entry(1, 5)],
        leader_commit: LogIndex::ZERO,
        round: 1,
    };
    let replies = sent(&follower.step(&mut log, NodeId(2), gap));
    // The rejection reports the follower's last index so the leader can back up at once.
    assert!(matches!(
        replies.as_slice(),
        [(
            _,
            Message::AppendEntriesReply {
                success: false,
                prev_log_index: LogIndex(4),
                index: LogIndex(1),
                ..
            }
        )]
    ));
}

#[test]
fn a_leader_does_not_commit_an_old_entry_by_counting_replicas() {
    // Server 1 holds an entry from term 1 that was never committed (Figure 8).
    let mut log = storage(1, &[entry(1, 7)]);
    let mut leader = start(1, &mut log);
    leader.on_timer(&mut log, Timer::Election);
    let granted = Message::RequestVoteReply {
        term: Term(2),
        granted: true,
    };
    leader.step(&mut log, NodeId(2), granted);
    assert_eq!(leader.status().role, Role::Leader);
    // The new leader appended a no-op from term 2 at index 2.
    assert_eq!(
        log.entry(LogIndex(2)).map(|e| e.payload.clone()),
        Some(Payload::Noop)
    );

    // A majority (servers 1 and 2) now stores the term-1 entry, but it is not committed.
    let ack = |index| Message::AppendEntriesReply {
        term: Term(2),
        success: true,
        prev_log_index: LogIndex(1),
        index: LogIndex(index),
        round: 1,
    };
    leader.step(&mut log, NodeId(2), ack(1));
    assert_eq!(leader.status().commit_index, LogIndex::ZERO);

    // Once the no-op from term 2 is on a majority, both entries are committed.
    let outputs = leader.step(&mut log, NodeId(2), ack(2));
    assert_eq!(leader.status().commit_index, LogIndex(2));
    let applied: Vec<LogIndex> = outputs
        .iter()
        .filter_map(|o| match o {
            Output::Apply { index, .. } => Some(*index),
            _ => None,
        })
        .collect();
    assert_eq!(applied, [LogIndex(1), LogIndex(2)]);
}

#[test]
fn a_single_server_elects_itself_and_commits_at_once() {
    let mut log = MemStorage::new();
    let (mut solo, _) = Raft::start(NodeId(1), &[NodeId(1)], &Config::default(), 1, &mut log);
    solo.on_timer(&mut log, Timer::Election);
    assert_eq!(solo.status().role, Role::Leader);
    let outputs = solo
        .propose(&mut log, Command(vec![1]))
        .expect("the only server leads");
    assert!(outputs.iter().any(|o| matches!(
        o,
        Output::Apply {
            index: LogIndex(2),
            ..
        }
    )));
}
