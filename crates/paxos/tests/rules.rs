//! Tests of individual Multi-Paxos rules (`docs/PAXOS_SPEC.md` §5), driving one server by
//! hand.

use consensus::{
    Command, Config, Consensus, Entry, HardState, LogIndex, MemStorage, NodeId, Output, Payload,
    Role, Storage, Term, Timer,
};
use paxos::{Message, Paxos};

const CLUSTER: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

fn command(byte: u8) -> Payload {
    Payload::Command(Command(vec![byte]))
}

fn entry(term: u64, byte: u8) -> Entry {
    Entry {
        term: Term(term),
        payload: command(byte),
    }
}

/// Storage holding `entries` at the given indexes, with `currentTerm` set to `term`.
fn storage(term: u64, entries: &[(u64, Entry)]) -> MemStorage {
    let mut storage = MemStorage::new();
    storage.set_hard_state(HardState {
        term: Term(term),
        voted_for: None,
    });
    for (index, entry) in entries {
        storage.put(LogIndex(*index), entry.clone());
    }
    storage.sync();
    storage
}

fn start(id: u64, storage: &mut MemStorage) -> Paxos {
    Paxos::start(NodeId(id), &CLUSTER, &Config::default(), 1, storage).0
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

fn applied(outputs: &[Output<Message>]) -> Vec<LogIndex> {
    outputs
        .iter()
        .filter_map(|o| match o {
            Output::Apply { index, .. } => Some(*index),
            _ => None,
        })
        .collect()
}

/// Makes server 1 leader of term 3 with a promise from server 2 carrying `entries`.
fn elect_server_1(log: &mut MemStorage, entries: Vec<(LogIndex, Entry)>) -> Paxos {
    let mut leader = start(1, log);
    leader.on_timer(log, Timer::Election);
    assert_eq!(leader.status().term, Term(3));
    let promise = Message::Promise {
        term: Term(3),
        granted: true,
        entries,
    };
    leader.step(log, NodeId(2), promise);
    assert_eq!(leader.status().role, Role::Leader);
    leader
}

fn accepted(first: u64, last: u64, commit: u64, round: u64) -> Message {
    Message::Accepted {
        term: Term(3),
        success: true,
        first_index: LogIndex(first),
        last_index: LogIndex(last),
        commit_index: LogIndex(commit),
        round,
    }
}

#[test]
fn campaigns_only_in_terms_it_owns() {
    // With 3 servers, the server of rank r owns the terms t with t mod 3 = r.
    for (id, first_term) in [(1, 3), (2, 1), (3, 2)] {
        let mut log = storage(0, &[]);
        let mut server = start(id, &mut log);
        server.on_timer(&mut log, Timer::Election);
        assert_eq!(server.status().term, Term(first_term), "server {id}");
        assert_eq!(server.status().role, Role::Candidate);
        assert_eq!(log.hard_state().term, Term(first_term));
    }
    // From term 4, server 2 moves to term 7, the next term it owns.
    let mut log = storage(4, &[]);
    let mut server = start(2, &mut log);
    let outputs = server.on_timer(&mut log, Timer::Election);
    assert_eq!(server.status().term, Term(7));
    assert!(sent(&outputs).iter().all(|(_, m)| matches!(
        m,
        Message::Prepare {
            term: Term(7),
            commit_index: LogIndex::ZERO
        }
    )));
}

#[test]
fn a_promise_carries_every_entry_above_the_candidates_commit_index() {
    let mut log = storage(1, &[(1, entry(1, 1)), (3, entry(2, 3))]);
    let mut voter = start(2, &mut log);
    let prepare = Message::Prepare {
        term: Term(3),
        commit_index: LogIndex(1),
    };
    let outputs = voter.step(&mut log, NodeId(1), prepare);
    assert_eq!(
        sent(&outputs),
        [(
            NodeId(1),
            Message::Promise {
                term: Term(3),
                granted: true,
                entries: vec![(LogIndex(3), entry(2, 3))],
            }
        )]
    );
    // Promising a term persists it.
    assert_eq!(log.hard_state().term, Term(3));
}

#[test]
fn refuses_a_prepare_for_a_lower_term() {
    let mut log = storage(5, &[(1, entry(1, 1))]);
    let mut voter = start(2, &mut log);
    let prepare = Message::Prepare {
        term: Term(3),
        commit_index: LogIndex::ZERO,
    };
    let outputs = voter.step(&mut log, NodeId(1), prepare);
    assert_eq!(
        sent(&outputs),
        [(
            NodeId(1),
            Message::Promise {
                term: Term(5),
                granted: false,
                entries: Vec::new(),
            }
        )]
    );
}

#[test]
fn a_new_leader_keeps_the_highest_term_entry_and_fills_gaps_with_noops() {
    // Server 1 holds (1, term 1). Server 2 promises (1, term 2) and (3, term 1).
    let mut log = storage(0, &[(1, entry(1, 10))]);
    let promise = vec![(LogIndex(1), entry(2, 20)), (LogIndex(3), entry(1, 30))];
    let _leader = elect_server_1(&mut log, promise);

    let at = |i| log.entry(LogIndex(i)).cloned();
    // Every index is re-proposed in the leader's term.
    assert_eq!(
        at(1),
        Some(Entry {
            term: Term(3),
            payload: command(20)
        })
    );
    assert_eq!(
        at(2),
        Some(Entry {
            term: Term(3),
            payload: Payload::Noop
        })
    );
    assert_eq!(
        at(3),
        Some(Entry {
            term: Term(3),
            payload: command(30)
        })
    );
    // Then a no-op, as in Raft.
    assert_eq!(
        at(4),
        Some(Entry {
            term: Term(3),
            payload: Payload::Noop
        })
    );
    assert_eq!(log.last_index(), LogIndex(4));
}

#[test]
fn a_follower_accepts_each_index_on_its_own() {
    let mut log = storage(3, &[]);
    let mut follower = start(2, &mut log);
    let accept = Message::Accept {
        term: Term(3),
        first_index: LogIndex(3),
        entries: vec![entry(3, 7)],
        commit_index: LogIndex::ZERO,
        round: 1,
    };
    let outputs = follower.step(&mut log, NodeId(1), accept);
    // There is no consistency check: index 3 is stored although 1 and 2 are missing.
    assert_eq!(log.entry(LogIndex(3)), Some(&entry(3, 7)));
    assert_eq!(log.entry(LogIndex(1)), None);
    assert_eq!(
        sent(&outputs),
        [(
            NodeId(1),
            Message::Accepted {
                term: Term(3),
                success: true,
                first_index: LogIndex(3),
                last_index: LogIndex(3),
                commit_index: LogIndex::ZERO,
                round: 1,
            }
        )]
    );
    assert_eq!(follower.status().leader, Some(NodeId(1)));
}

#[test]
fn rejects_an_accept_from_an_earlier_term() {
    let mut log = storage(6, &[]);
    let mut follower = start(2, &mut log);
    let accept = Message::Accept {
        term: Term(3),
        first_index: LogIndex(1),
        entries: vec![entry(3, 7)],
        commit_index: LogIndex::ZERO,
        round: 1,
    };
    let outputs = follower.step(&mut log, NodeId(1), accept);
    assert_eq!(log.entry(LogIndex(1)), None);
    assert!(matches!(
        sent(&outputs).as_slice(),
        [(
            _,
            Message::Accepted {
                term: Term(6),
                success: false,
                ..
            }
        )]
    ));
}

#[test]
fn a_follower_commits_only_entries_it_holds_in_the_leaders_term() {
    // Server 3 leads term 2. The follower holds index 1 from term 1 and index 2 from term 2.
    let mut log = storage(2, &[(1, entry(1, 1)), (2, entry(2, 2))]);
    let mut follower = start(2, &mut log);
    let heartbeat = Message::Accept {
        term: Term(2),
        first_index: LogIndex(3),
        entries: Vec::new(),
        commit_index: LogIndex(2),
        round: 1,
    };
    follower.step(&mut log, NodeId(3), heartbeat);
    // Index 1 is from an earlier term, so the follower cannot know it holds the chosen
    // entry there.
    assert_eq!(follower.status().commit_index, LogIndex::ZERO);

    // A `Commit` message installs the chosen entry; then index 2 commits too.
    let commit = Message::Commit {
        term: Term(2),
        first_index: LogIndex(1),
        entries: vec![entry(1, 1)],
        commit_index: LogIndex(2),
    };
    let outputs = follower.step(&mut log, NodeId(3), commit);
    assert_eq!(follower.status().commit_index, LogIndex(2));
    assert_eq!(applied(&outputs), [LogIndex(1), LogIndex(2)]);
    assert!(matches!(
        sent(&outputs).as_slice(),
        [(
            NodeId(3),
            Message::Accepted {
                success: true,
                commit_index: LogIndex(2),
                round: 0,
                ..
            }
        )]
    ));
}

#[test]
fn the_leader_commits_once_a_majority_accepts_in_its_term() {
    let mut log = storage(0, &[]);
    let mut leader = elect_server_1(&mut log, Vec::new());
    // The no-op is at index 1, accepted only by the leader so far.
    assert_eq!(leader.status().commit_index, LogIndex::ZERO);
    let outputs = leader.step(&mut log, NodeId(2), accepted(1, 1, 0, 1));
    assert_eq!(leader.status().commit_index, LogIndex(1));
    assert_eq!(applied(&outputs), [LogIndex(1)]);
}

#[test]
fn the_leader_sends_chosen_entries_to_a_follower_that_cannot_learn_them() {
    // The leader re-proposes index 1, which server 2 promised, and appends a no-op at 2.
    let mut log = storage(0, &[]);
    let mut leader = elect_server_1(&mut log, vec![(LogIndex(1), entry(2, 5))]);
    leader.step(&mut log, NodeId(2), accepted(1, 2, 0, 1));
    assert_eq!(leader.status().commit_index, LogIndex(2));

    // Server 3 answers a heartbeat without having accepted index 1 in term 3, so it can
    // only learn the chosen entries from a `Commit`.
    let outputs = leader.step(&mut log, NodeId(3), accepted(1, 0, 0, 1));
    let commits: Vec<Message> = sent(&outputs)
        .into_iter()
        .filter(|(to, m)| *to == NodeId(3) && matches!(m, Message::Commit { .. }))
        .map(|(_, m)| m)
        .collect();
    assert_eq!(
        commits,
        [Message::Commit {
            term: Term(3),
            first_index: LogIndex(1),
            entries: vec![
                Entry {
                    term: Term(3),
                    payload: command(5)
                },
                Entry {
                    term: Term(3),
                    payload: Payload::Noop
                },
            ],
            commit_index: LogIndex(2),
        }]
    );

    // While that `Commit` is unanswered, another reply does not trigger a second one.
    let outputs = leader.step(&mut log, NodeId(3), accepted(1, 0, 0, 1));
    assert!(
        sent(&outputs)
            .iter()
            .all(|(_, m)| !matches!(m, Message::Commit { .. }))
    );
}

#[test]
fn a_single_server_elects_itself_and_commits_at_once() {
    let mut log = MemStorage::new();
    let (mut solo, _) = Paxos::start(NodeId(1), &[NodeId(1)], &Config::default(), 1, &mut log);
    solo.on_timer(&mut log, Timer::Election);
    assert_eq!(solo.status().role, Role::Leader);
    let outputs = solo
        .propose(&mut log, Command(vec![1]))
        .expect("the only server leads");
    assert_eq!(applied(&outputs), [LogIndex(2)]);
}

#[test]
fn a_reply_lets_the_leader_send_up_to_the_in_flight_limit() {
    // Server 1 holds 200 entries it never learned were chosen.
    let held: Vec<(u64, Entry)> = (1..=200).map(|i| (i, entry(1, i as u8))).collect();
    let mut log = storage(0, &held);
    let mut leader = elect_server_1(&mut log, Vec::new());
    assert_eq!(log.last_index(), LogIndex(201));

    // Once server 2 accepts the first message, the leader sends the rest of the 200
    // re-proposed entries and its no-op at once, in messages of at most 64 entries.
    let outputs = leader.step(&mut log, NodeId(2), accepted(1, 64, 0, 1));
    let batches: Vec<(u64, usize)> = sent(&outputs)
        .into_iter()
        .filter_map(|(to, message)| match message {
            Message::Accept {
                first_index,
                entries,
                ..
            } if to == NodeId(2) => Some((first_index.0, entries.len())),
            _ => None,
        })
        .collect();
    assert_eq!(batches, [(65, 64), (129, 64), (193, 9)]);
}
