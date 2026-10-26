//! Raft clusters run in the deterministic simulator.

use std::time::Duration;

use consensus::{NodeId, RequestId, Role, Storage};
use raft::Raft;
use sim::{
    ChaosConfig, Check, Fault, SimConfig, SimTime, Simulation, Stream, chaos_schedule, stream_rng,
};

fn at(ms: u64) -> SimTime {
    SimTime::from_duration(Duration::from_millis(ms))
}

fn config(servers: usize, clients: usize) -> SimConfig {
    SimConfig {
        servers,
        clients,
        raft_checks: true,
        ..SimConfig::default()
    }
}

fn assert_no_violations(sim: &Simulation<Raft>) {
    assert!(
        sim.violations().is_empty(),
        "seed {}: {:#?}",
        sim.seed(),
        sim.violations()
    );
}

#[test]
fn elects_exactly_one_leader_that_everyone_follows() {
    for servers in [3, 5] {
        let mut sim: Simulation<Raft> = Simulation::new(config(servers, 0), 1);
        sim.run_until(at(3_000));
        let (leader, term) = sim.leader().expect("a leader");
        for &id in sim.ids() {
            let status = sim.status(id).expect("running");
            assert_eq!(status.term, term);
            assert_eq!(status.leader, Some(leader));
            if id != leader {
                assert_eq!(status.role, Role::Follower);
            }
        }
        assert_no_violations(&sim);
    }
}

#[test]
fn clients_commit_commands_on_every_server() {
    let mut sim: Simulation<Raft> = Simulation::new(config(5, 3), 2);
    sim.run_until(at(5_000));
    assert!(sim.replies().len() > 200, "{} replies", sim.replies().len());
    sim.stop_clients();
    sim.run_until(at(6_000));
    sim.check_durability();
    sim.check_log_matching();
    assert_no_violations(&sim);
}

#[test]
fn a_new_leader_takes_over_after_a_crash() {
    let mut sim: Simulation<Raft> = Simulation::new(config(5, 3), 3);
    sim.run_until(at(2_000));
    let (old, old_term) = sim.leader().expect("a leader");
    sim.inject(Fault::Crash(old));
    let crashed_at = sim.now();

    let elected = sim.run_until_condition(at(5_000), |s| {
        s.leader().is_some_and(|(_, term)| term > old_term)
    });
    assert!(elected);
    // Election timeouts are 150–300 ms, so a new leader should appear well within a second.
    assert!(sim.now() - crashed_at < Duration::from_secs(1));

    let replies = sim.replies().len();
    sim.run_until(at(6_000));
    assert!(sim.replies().len() > replies + 50);

    sim.inject(Fault::Restart(old));
    sim.run_until(at(7_000));
    sim.stop_clients();
    sim.run_until(at(8_000));
    sim.check_durability();
    sim.check_log_matching();
    assert_no_violations(&sim);
}

#[test]
fn a_restarted_follower_catches_up() {
    let mut sim: Simulation<Raft> = Simulation::new(config(3, 2), 4);
    sim.run_until(at(1_000));
    let (leader, _) = sim.leader().expect("a leader");
    let follower = *sim
        .ids()
        .iter()
        .find(|&&id| id != leader)
        .expect("a follower");

    sim.inject(Fault::Crash(follower));
    sim.run_until(at(3_000));
    sim.inject(Fault::Restart(follower));
    sim.run_until(at(3_500));
    sim.stop_clients();
    sim.run_until(at(4_500));

    // The restarted follower has caught up: its log ends within 10 entries of the leader's.
    let leader_last = sim.storage(leader).last_index();
    assert!(sim.storage(follower).last_index() >= LogIndexBound::below(leader_last));
    sim.check_durability();
    sim.check_log_matching();
    assert_no_violations(&sim);
}

/// Entries proposed in the last moments may not have reached the follower yet.
struct LogIndexBound;

impl LogIndexBound {
    fn below(index: consensus::LogIndex) -> consensus::LogIndex {
        consensus::LogIndex(index.0.saturating_sub(10))
    }
}

#[test]
fn the_leader_confirms_its_leadership_and_followers_refuse() {
    let mut sim: Simulation<Raft> = Simulation::new(config(3, 0), 5);
    sim.run_until(at(2_000));
    let (leader, _) = sim.leader().expect("a leader");
    let follower = *sim
        .ids()
        .iter()
        .find(|&&id| id != leader)
        .expect("a follower");

    assert!(sim.confirm_leadership(follower, RequestId(1)).is_err());
    sim.confirm_leadership(leader, RequestId(2))
        .expect("leader");
    let answered = sim.run_until_condition(at(3_000), |s| !s.confirmations().is_empty());
    assert!(answered);
    let confirmation = sim.confirmations()[0];
    assert_eq!(confirmation.request, RequestId(2));
    // The read index covers at least the leader's no-op.
    assert!(confirmation.read_index.is_some_and(|i| i.0 >= 1));
}

#[test]
fn a_cut_off_leader_cannot_confirm_its_leadership() {
    let mut sim: Simulation<Raft> = Simulation::new(config(3, 0), 6);
    sim.run_until(at(2_000));
    let (leader, _) = sim.leader().expect("a leader");
    let others: Vec<NodeId> = sim
        .ids()
        .iter()
        .copied()
        .filter(|&id| id != leader)
        .collect();
    sim.inject(Fault::Partition(vec![vec![leader], others]));

    sim.confirm_leadership(leader, RequestId(1))
        .expect("still believes it leads");
    sim.run_until(at(4_000));
    // The request was never confirmed: either it is still waiting or it failed once the old
    // leader learned of a newer term.
    assert!(sim.confirmations().iter().all(|c| c.read_index.is_none()));
}

#[test]
fn the_same_seed_gives_the_same_run() {
    let run = |seed| {
        let mut sim: Simulation<Raft> = Simulation::new(config(5, 3), seed);
        sim.run_until(at(3_000));
        let logs: Vec<_> = sim
            .ids()
            .iter()
            .map(|&id| sim.storage(id).clone())
            .collect();
        (
            sim.events(),
            sim.metrics().clone(),
            sim.replies().to_vec(),
            logs,
        )
    };
    assert_eq!(run(11), run(11));
    assert_ne!(run(11).2, run(12).2);
}

/// A short version of the validation run in `docs/RESEARCH.md` §9: random faults for 10
/// seconds, then 10 seconds without faults.
#[test]
fn random_faults_never_break_safety() {
    for seed in 1..=24 {
        let servers = if seed % 2 == 1 { 3 } else { 5 };
        let mut sim: Simulation<Raft> = Simulation::new(config(servers, 3), seed);
        let chaos = ChaosConfig {
            start: Duration::from_secs(1),
            end: Duration::from_secs(10),
            mean_gap: Duration::from_millis(500),
            max_duration: Duration::from_secs(2),
        };
        let ids = sim.ids().to_vec();
        let mut rng = stream_rng(seed, Stream::Faults);
        for (time, fault) in chaos_schedule(&ids, &chaos, &mut rng) {
            sim.schedule(time, fault);
        }

        for checkpoint in (2_000..=10_000).step_by(2_000) {
            sim.run_until(at(checkpoint));
            sim.check_log_matching();
        }
        let quiet = sim.now();
        let live = sim.run_until_condition(at(20_000), |s| {
            s.replies().last().is_some_and(|r| r.sent >= quiet)
        });
        if !live {
            sim.report(
                Check::Liveness,
                "no command committed after faults stopped".into(),
            );
        }
        sim.stop_clients();
        sim.run_until(at(20_000));
        sim.check_durability();
        sim.check_log_matching();
        assert_no_violations(&sim);
    }
}
