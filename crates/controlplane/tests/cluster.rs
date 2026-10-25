//! The Phase 3 exit check (`docs/PLAN.md` §8): for both algorithms, writes resume after a
//! `kill -9` of the leader, and a restarted server catches up.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use client::{ControlClient, RetryPolicy};
use controlplane::Algorithm;
use controlplane::local::{LocalCluster, LocalOptions};
use rpc::proto::Empty;
use rpc::{ControlCommand, Outcome};

fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_controlplane"))
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rvs-cluster-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn bench() -> ControlCommand {
    ControlCommand::Bench {
        padding: vec![0; 100],
    }
}

/// The leader, if exactly one server reports leading the highest term.
async fn leader(client: &ControlClient, servers: &[(u64, String)]) -> Option<(u64, u64)> {
    let mut best: Option<(u64, u64)> = None;
    for (id, _) in servers {
        let Some(mut raw) = client.server(*id) else {
            continue;
        };
        let Ok(Ok(status)) =
            tokio::time::timeout(Duration::from_millis(200), raw.status(Empty {})).await
        else {
            continue;
        };
        let status = status.into_inner();
        if status.role == "leader" && best.is_none_or(|(_, term)| status.term > term) {
            best = Some((status.id, status.term));
        }
    }
    best
}

async fn applied(client: &ControlClient, id: u64) -> Option<u64> {
    let mut raw = client.server(id)?;
    let status = tokio::time::timeout(Duration::from_millis(200), raw.status(Empty {}))
        .await
        .ok()?
        .ok()?;
    Some(status.into_inner().applied_index)
}

async fn exercise(algorithm: Algorithm, name: &str) {
    let dir = temp_dir(name);
    let options = LocalOptions {
        algorithm,
        election_timeout_ms: (150, 300),
        delay_us: 0,
        record: false,
        seed: None,
    };
    let mut cluster = LocalCluster::start(&binary(), &dir, 3, options).expect("cluster starts");
    let servers = cluster.servers().to_vec();
    let policy = RetryPolicy {
        give_up: Some(Duration::from_secs(20)),
        ..RetryPolicy::default()
    };
    let mut client = ControlClient::new(&servers, policy, 42).expect("client");

    for _ in 0..20 {
        let committed = client.propose(bench()).await.expect("commits");
        assert_eq!(committed.outcome, Outcome::Applied);
    }
    let (old, term) = leader(&client, &servers).await.expect("a leader");

    cluster.kill(old);
    let killed = Instant::now();
    let committed = client.propose(bench()).await.expect("writes resume");
    let resumed = killed.elapsed();
    assert!(
        resumed < Duration::from_secs(10),
        "writes resumed after {resumed:?}"
    );
    let (new, new_term) = leader(&client, &servers).await.expect("a new leader");
    assert_ne!(new, old);
    assert!(new_term > term);
    for _ in 0..20 {
        client.propose(bench()).await.expect("commits");
    }
    let last = client.propose(bench()).await.expect("commits").index;
    assert!(last > committed.index);

    cluster.restart(old).expect("restarts");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        // Keep committing, so the leader keeps sending and the restarted server learns
        // the commit index.
        client.propose(bench()).await.expect("commits");
        if applied(&client, old).await.is_some_and(|a| a >= last) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the restarted server did not catch up"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cluster.stop();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_recovers_from_a_killed_leader() {
    exercise(Algorithm::Raft, "raft").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paxos_recovers_from_a_killed_leader() {
    exercise(Algorithm::Paxos, "paxos").await;
}
