//! The control-plane client's retry rules (`docs/RESEARCH.md` amendment A1.1), sessions,
//! and shard-map reads.

use std::sync::Arc;
use std::time::{Duration, Instant};

use client::{ControlClient, ControlError, RetryPolicy};
use rpc::{ControlCommand, ShardMap};

use crate::fake::{self, Answer, Control, TIMEOUT};

fn policy(reply_timeout: Duration) -> RetryPolicy {
    RetryPolicy {
        reply_timeout,
        give_up: Some(Duration::from_secs(10)),
        ..RetryPolicy::default()
    }
}

fn command() -> ControlCommand {
    ControlCommand::Bench {
        padding: Vec::new(),
    }
}

async fn serve(servers: &[Arc<Control>]) -> Vec<(u64, String)> {
    let mut addresses = Vec::new();
    for server in servers {
        addresses.push((server.id, fake::serve_control(Arc::clone(server)).await));
    }
    addresses
}

#[tokio::test(flavor = "multi_thread")]
async fn a_redirect_is_followed_at_once() {
    let servers = [
        Control::new(1, [Answer::Redirect(3)]),
        Control::new(2, [Answer::Redirect(3)]),
        Control::new(3, [Answer::Lead]),
    ];
    let addresses = serve(&servers).await;
    // The client starts at server 1, and a random retry would go to server 2, so only
    // following the redirect reaches the leader directly. A long reply timeout: following
    // it must not wait for the timeout either.
    let id = fake::id_with_picks(&[0, 1], 3);
    let mut client =
        ControlClient::new(&addresses, policy(Duration::from_secs(5)), id).expect("client");
    let started = Instant::now();
    client.propose(command()).await.expect("committed");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "took {:?}",
        started.elapsed()
    );
    // Straight from the first server to the leader it named, without trying another.
    let calls: Vec<usize> = servers.iter().map(|s| s.proposals().len()).collect();
    assert_eq!(calls, [1, 0, 1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_silent_server_is_waited_out_and_the_command_keeps_its_number() {
    let servers = [
        Control::new(1, [Answer::Silent]),
        Control::new(2, [Answer::Lead]),
    ];
    let addresses = serve(&servers).await;
    let id = fake::id_with_picks(&[0], 2);
    let mut client = ControlClient::new(&addresses, policy(TIMEOUT), id).expect("client");
    let started = Instant::now();
    client.propose(command()).await.expect("committed");
    assert!(
        started.elapsed() >= TIMEOUT,
        "retried after {:?}",
        started.elapsed()
    );

    let silent = servers[0].proposals();
    let leader = servers[1].proposals();
    assert!(!silent.is_empty());
    assert_eq!(leader.len(), 1);
    // The retry is the same command: same session, same sequence number.
    assert_eq!((leader[0].1.client, leader[0].1.seq), (id, 1));
    assert!(silent.iter().all(|(_, r)| (r.client, r.seq) == (id, 1)));
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_known_leader_the_client_waits_before_retrying() {
    let server = Control::new(1, [Answer::NoLeader, Answer::NoLeader, Answer::Lead]);
    let addresses = serve(std::slice::from_ref(&server)).await;
    let backoff = RetryPolicy::default().backoff;
    let mut client =
        ControlClient::new(&addresses, policy(Duration::from_secs(5)), 1).expect("client");
    client.propose(command()).await.expect("committed");

    let arrivals: Vec<Instant> = server.proposals().into_iter().map(|(at, _)| at).collect();
    assert_eq!(arrivals.len(), 3);
    for pair in arrivals.windows(2) {
        assert!(
            pair[1] - pair[0] >= backoff,
            "retried after {:?}",
            pair[1] - pair[0]
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_client_gives_up_after_its_limit() {
    let server = Control::new(1, [Answer::Silent]);
    let addresses = serve(std::slice::from_ref(&server)).await;
    let give_up = Duration::from_millis(400);
    let policy = RetryPolicy {
        reply_timeout: TIMEOUT,
        give_up: Some(give_up),
        ..RetryPolicy::default()
    };
    let mut client = ControlClient::new(&addresses, policy, 1).expect("client");
    let started = Instant::now();
    assert_eq!(client.propose(command()).await, Err(ControlError::GaveUp));
    assert!(started.elapsed() >= give_up);
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_outcome_that_cannot_be_decoded_is_an_error() {
    let server = Control::new(1, [Answer::Garbled]);
    let addresses = serve(std::slice::from_ref(&server)).await;
    let mut client = ControlClient::new(&addresses, policy(TIMEOUT), 1).expect("client");
    assert!(matches!(
        client.propose(command()).await,
        Err(ControlError::Invalid(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn each_session_numbers_its_own_commands() {
    let server = Control::new(1, [Answer::Lead]);
    let addresses = serve(std::slice::from_ref(&server)).await;
    let mut base = ControlClient::new(&addresses, policy(TIMEOUT), 7).expect("client");
    let mut session = base.session(8);
    session.propose(command()).await.expect("committed");
    session.propose(command()).await.expect("committed");
    base.propose(command()).await.expect("committed");

    let numbered: Vec<(u64, u64)> = server
        .proposals()
        .into_iter()
        .map(|(_, r)| (r.client, r.seq))
        .collect();
    assert_eq!(numbered, [(8, 1), (8, 2), (7, 1)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_linearizable_map_read_goes_to_the_leader() {
    let stale = ShardMap {
        version: 3,
        ..ShardMap::default()
    };
    let current = ShardMap {
        version: 5,
        ..ShardMap::default()
    };
    let servers = [
        Control::with_maps(1, [Answer::Redirect(2)], [stale]),
        Control::with_maps(2, [Answer::Lead], [current]),
    ];
    let addresses = serve(&servers).await;
    let id = fake::id_with_picks(&[0], 2);
    let mut client = ControlClient::new(&addresses, policy(TIMEOUT), id).expect("client");

    // Any server answers an ordinary read from its own copy, which may be stale.
    assert_eq!(client.shard_map(false).await.expect("read").version, 3);
    // A linearizable read is refused by a follower and redirected to the leader.
    assert_eq!(client.shard_map(true).await.expect("read").version, 5);
    assert_eq!(servers[0].map_reads(), [false, true]);
    assert_eq!(servers[1].map_reads(), [true]);
}
