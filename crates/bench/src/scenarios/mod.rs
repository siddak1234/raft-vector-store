//! The experiment procedures (`docs/RESEARCH.md` §6 and §9).
//!
//! Each procedure runs one trial: one simulation with one seed, or, on the real cluster, one
//! set of processes. The procedures are generic over the algorithm, so Raft and Multi-Paxos
//! follow exactly the same steps.

pub mod failover;
pub mod figure16;
pub mod modelcheck;
pub mod partition;
pub mod recovery;
pub mod steady;
pub mod validation;

use std::collections::BTreeSet;
use std::time::Duration;

use consensus::{Config, Consensus, NodeId, Rng, Role, Storage, Term};
use paxos::Paxos;
use raft::Raft;
use sim::{Observer, SimConfig, Simulation};

use crate::algorithm::{Algorithm, Kind};
use crate::config::{Environment, ExperimentConfig, RunConfig, Scenario};
use crate::results::{self, Output};
use crate::runner::run_parallel;

/// Size of every client command, in bytes (`docs/RESEARCH.md` §3).
pub const COMMAND_BYTES: usize = 128;

/// How long a trial may take to elect its first leader before it is recorded as not ready.
const READY_DEADLINE: Duration = Duration::from_secs(30);

/// Runs every trial of an experiment and writes one CSV file per scenario. `seed_limit`
/// caps the number of seeds per run, for quick scratch runs.
///
/// # Errors
///
/// Returns a description of the problem if a run names an unknown algorithm or a file
/// cannot be written.
pub fn run_experiment(
    config: &ExperimentConfig,
    output: &Output,
    seed_limit: Option<u64>,
) -> Result<usize, String> {
    for run in &config.run {
        Kind::parse(&run.algorithm).map_err(|err| format!("run {}: {err}", run.label))?;
    }
    let jobs = |scenario: Scenario| -> Vec<(&RunConfig, u64)> {
        config
            .run
            .iter()
            .filter(|run| run.scenario == scenario && run.environment == Environment::Simulator)
            .flat_map(|run| {
                let seeds = run.seeds();
                let limit = seed_limit.unwrap_or(u64::MAX) as usize;
                seeds.into_iter().take(limit).map(move |seed| (run, seed))
            })
            .collect()
    };

    let mut trials = 0;
    let steady = jobs(Scenario::Steady);
    if !steady.is_empty() {
        eprintln!("steady: {} trials", steady.len());
        let rows = run_parallel(&steady, 500, |(run, seed)| match kind(run) {
            Kind::Raft => steady::trial::<Raft>(run, *seed),
            Kind::Paxos => steady::trial::<Paxos>(run, *seed),
        });
        results::write_csv(output, "steady.csv", &rows)?;
        trials += rows.len();
    }
    let figure16 = jobs(Scenario::Figure16);
    if !figure16.is_empty() {
        eprintln!("figure16: {} trials", figure16.len());
        let rows = run_parallel(&figure16, 1000, |(run, seed)| match kind(run) {
            Kind::Raft => figure16::trial::<Raft>(run, *seed),
            Kind::Paxos => figure16::trial::<Paxos>(run, *seed),
        });
        results::write_csv(output, "figure16.csv", &rows)?;
        trials += rows.len();
    }
    // Model checking uses every core for one search at a time.
    let searches: Vec<&RunConfig> = config
        .run
        .iter()
        .filter(|run| run.scenario == Scenario::Modelcheck)
        .collect();
    if !searches.is_empty() {
        let mut rows = Vec::new();
        // A rung is skipped if a smaller rung did not complete for both algorithms
        // (docs/RESEARCH.md amendment A5.4).
        let mut incomplete: Vec<(u8, u8, u8)> = Vec::new();
        let bounds = |run: &RunConfig| {
            (
                run.max_commands.unwrap_or(0),
                run.max_elections.unwrap_or(0),
                run.max_heartbeats.unwrap_or(0),
            )
        };
        for run in searches {
            let rung = bounds(run);
            if incomplete.iter().any(|small| {
                *small != rung && small.0 <= rung.0 && small.1 <= rung.1 && small.2 <= rung.2
            }) {
                eprintln!(
                    "modelcheck {}: {} skipped, a smaller rung did not complete",
                    run.label, run.algorithm
                );
                rows.push(modelcheck::skipped(run));
                results::write_csv(output, "modelcheck.csv", &rows)?;
                continue;
            }
            eprintln!(
                "modelcheck {}: {} (commands {:?}, elections {:?}, heartbeats {:?})",
                run.label, run.algorithm, run.max_commands, run.max_elections, run.max_heartbeats
            );
            let row = match kind(run) {
                Kind::Raft => modelcheck::search::<Raft>(run),
                Kind::Paxos => modelcheck::search::<Paxos>(run),
            };
            if !row.complete() && !incomplete.contains(&rung) {
                incomplete.push(rung);
            }
            rows.push(row);
            // Rows are written after each search, so a long run keeps what it finished.
            results::write_csv(output, "modelcheck.csv", &rows)?;
        }
        trials += rows.len();
    }
    let failover = jobs(Scenario::Failover);
    if !failover.is_empty() {
        eprintln!("failover: {} trials", failover.len());
        let rows = run_parallel(&failover, 500, |(run, seed)| match kind(run) {
            Kind::Raft => failover::trial::<Raft>(run, *seed),
            Kind::Paxos => failover::trial::<Paxos>(run, *seed),
        });
        results::write_csv(output, "failover.csv", &rows)?;
        trials += rows.len();
    }
    let recovery_jobs: Vec<(&RunConfig, u64)> = jobs(Scenario::Recovery)
        .into_iter()
        .chain(jobs(Scenario::CatchUp))
        .collect();
    if !recovery_jobs.is_empty() {
        eprintln!("recovery: {} trials", recovery_jobs.len());
        let rows = run_parallel(&recovery_jobs, 500, |(run, seed)| {
            match (run.scenario, kind(run)) {
                (Scenario::Recovery, Kind::Raft) => recovery::consecutive::<Raft>(run, *seed),
                (Scenario::Recovery, Kind::Paxos) => recovery::consecutive::<Paxos>(run, *seed),
                (_, Kind::Raft) => recovery::catch_up::<Raft>(
                    run,
                    *seed,
                    run.catch_up.expect("checked"),
                    run.committed,
                ),
                (_, Kind::Paxos) => recovery::catch_up::<Paxos>(
                    run,
                    *seed,
                    run.catch_up.expect("checked"),
                    run.committed,
                ),
            }
        });
        results::write_csv(output, "recovery.csv", &rows)?;
        trials += rows.len();
    }
    let partition = jobs(Scenario::Partition);
    if !partition.is_empty() {
        eprintln!("partition: {} trials", partition.len());
        let rows = run_parallel(&partition, 500, |(run, seed)| {
            let scenario = run.partition.expect("load checks partition runs");
            match kind(run) {
                Kind::Raft => partition::trial::<Raft>(run, *seed, scenario),
                Kind::Paxos => partition::trial::<Paxos>(run, *seed, scenario),
            }
        });
        results::write_csv(output, "partition.csv", &rows)?;
        trials += rows.len();
    }
    trials += run_real(config, output, seed_limit)?;
    Ok(trials)
}

/// Runs every real-cluster trial of an experiment, one at a time in a random order fixed
/// by the experiment's ID, and writes the rows of each scenario in config order.
fn run_real(
    config: &ExperimentConfig,
    output: &Output,
    seed_limit: Option<u64>,
) -> Result<usize, String> {
    let mut jobs: Vec<(usize, &RunConfig, u64)> = config
        .run
        .iter()
        .filter(|run| run.environment == Environment::Real)
        .flat_map(|run| {
            let limit = seed_limit.unwrap_or(u64::MAX) as usize;
            run.seeds()
                .into_iter()
                .take(limit)
                .map(move |seed| (run, seed))
        })
        .enumerate()
        .map(|(order, (run, seed))| (order, run, seed))
        .collect();
    if jobs.is_empty() {
        return Ok(0);
    }
    // Interleave the algorithms and arms in a random order, the same on every run of the
    // experiment (`docs/PLAN.md` §5, rule 6).
    let mut rng = Rng::new(config.id.bytes().fold(0xC0FFEE, |h, b| {
        h.wrapping_mul(31).wrapping_add(u64::from(b))
    }));
    for i in (1..jobs.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        jobs.swap(i, j);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    let total = jobs.len();
    let mut steady_rows = Vec::new();
    let mut figure16_rows = Vec::new();
    let mut failover_rows = Vec::new();
    let mut partition_rows = Vec::new();
    let mut vector_rows = Vec::new();
    let mut load_rows = Vec::new();
    let dataset = if jobs
        .iter()
        .any(|(_, run, _)| run.scenario == Scenario::Vectors)
    {
        let path = crate::dataplane::dataset_dir().join("glove-25-angular.hdf5");
        Some(std::sync::Arc::new(client::Dataset::load(&path).map_err(
            |err| format!("{err}; run `cargo xtask dataset glove-25`"),
        )?))
    } else {
        None
    };
    for (done, (order, run, seed)) in jobs.into_iter().enumerate() {
        if done % 10 == 0 {
            eprintln!("  real cluster: {done} / {total} trials");
        }
        match run.scenario {
            Scenario::Steady => {
                steady_rows.push((order, runtime.block_on(crate::real::steady(run, seed))))
            }
            Scenario::Figure16 => {
                figure16_rows.push((order, runtime.block_on(crate::real::figure16(run, seed))))
            }
            Scenario::Failover => {
                failover_rows.push((order, runtime.block_on(crate::real::failover(run, seed))))
            }
            Scenario::Partition => {
                let kind = match run.partition {
                    Some(partition::PartitionKind::P1) => crate::real::Partition::LeaderAlone,
                    Some(partition::PartitionKind::P3) => crate::real::Partition::FollowerAlone,
                    other => {
                        return Err(format!(
                            "run {}: {other:?} is not run on a real cluster",
                            run.label
                        ));
                    }
                };
                partition_rows.push((
                    order,
                    runtime.block_on(crate::real::partition(run, seed, kind)),
                ));
            }
            Scenario::Load => {
                load_rows.push((order, runtime.block_on(crate::load::trial(run, seed))))
            }
            Scenario::Vectors => {
                let dataset = dataset.as_ref().expect("loaded above");
                vector_rows.push((
                    order,
                    runtime.block_on(crate::vectors::trial(run, seed, run.crash_leader, dataset)),
                ));
            }
            Scenario::Modelcheck | Scenario::Recovery | Scenario::CatchUp => {
                return Err(format!(
                    "run {}: this scenario has no real-cluster version",
                    run.label
                ));
            }
        }
    }
    let mut trials = 0;
    fn sorted<R>(mut rows: Vec<(usize, R)>) -> Vec<R> {
        rows.sort_by_key(|(order, _)| *order);
        rows.into_iter().map(|(_, row)| row).collect()
    }
    if !steady_rows.is_empty() {
        let rows = sorted(steady_rows);
        trials += rows.len();
        results::write_csv(output, "real-steady.csv", &rows)?;
    }
    if !figure16_rows.is_empty() {
        let rows = sorted(figure16_rows);
        trials += rows.len();
        results::write_csv(output, "real-figure16.csv", &rows)?;
    }
    if !failover_rows.is_empty() {
        let rows = sorted(failover_rows);
        trials += rows.len();
        results::write_csv(output, "real-failover.csv", &rows)?;
    }
    if !partition_rows.is_empty() {
        let rows = sorted(partition_rows);
        trials += rows.len();
        results::write_csv(output, "real-partition.csv", &rows)?;
    }
    if !vector_rows.is_empty() {
        let rows = sorted(vector_rows);
        trials += rows.len();
        results::write_csv(output, "real-vectors.csv", &rows)?;
    }
    if !load_rows.is_empty() {
        let rows = sorted(load_rows);
        trials += rows.len();
        results::write_csv(output, "real-load.csv", &rows)?;
    }
    Ok(trials)
}

/// The algorithm a run names, which `run_experiment` has already checked.
fn kind(run: &RunConfig) -> Kind {
    Kind::parse(&run.algorithm).expect("algorithms are checked before any trial runs")
}

/// Simulator settings for a run. The heartbeat interval is half the minimum election
/// timeout (`docs/RESEARCH.md` §3).
fn sim_config<A: Algorithm>(run: &RunConfig, clients: usize, count_bytes: bool) -> SimConfig {
    let mut consensus = Config::default();
    if let Some([min, max]) = run.election_timeout_ms {
        consensus.election_timeout_min = Duration::from_millis(min);
        consensus.election_timeout_max = Duration::from_millis(max);
        consensus.heartbeat_interval = Duration::from_millis(min) / 2;
    }
    SimConfig {
        servers: run.servers,
        consensus,
        one_way_delay: run.network().one_way_delay(),
        clients,
        command_bytes: COMMAND_BYTES,
        count_bytes,
        raft_checks: A::RAFT_CHECKS,
        ..SimConfig::default()
    }
}

/// Starts a simulation and runs it until a leader has committed an entry from its own
/// term (its no-op). Returns `None` if that does not happen within the deadline.
fn start_ready<A: Algorithm>(config: SimConfig, seed: u64) -> Option<Simulation<A, A::Observer>> {
    let mut sim = Simulation::with_observer(config, seed, A::Observer::default());
    let deadline = sim.now() + READY_DEADLINE;
    sim.run_until_condition(deadline, leader_ready)
        .then_some(sim)
}

/// Whether some leader has committed an entry from its own term. Both algorithms append a
/// no-op on election, so this means the leader's no-op is committed.
fn leader_ready<C: Consensus, O: Observer<C>>(sim: &Simulation<C, O>) -> bool {
    let Some((leader, term)) = sim.leader() else {
        return false;
    };
    let Some(status) = sim.status(leader) else {
        return false;
    };
    status.commit_index.0 > 0
        && sim
            .storage(leader)
            .entry(status.commit_index)
            .map(|e| e.term)
            == Some(term)
}

/// A leader elected after a fault, with the entries it had sent before the input in which
/// it became leader.
pub(crate) struct Elected {
    pub(crate) leader: NodeId,
    pub(crate) term: Term,
    /// Metric M10 counts from the election, so the entries a new leader sends in the input
    /// that elects it, such as Multi-Paxos's re-proposals, count.
    pub(crate) entries_before: u64,
}

/// Runs until a server leads a term above `term`, calling `on_step` after every event.
/// Returns `None` if no server does by `deadline`.
fn elect<C: Consensus, O: Observer<C>>(
    sim: &mut Simulation<C, O>,
    deadline: sim::SimTime,
    term: Term,
    mut on_step: impl FnMut(&Simulation<C, O>),
) -> Option<Elected> {
    let mut sent: std::collections::BTreeMap<NodeId, u64> = std::collections::BTreeMap::new();
    let elected = sim.run_until_condition(deadline, |s| {
        on_step(s);
        if s.leader().is_some_and(|(_, t)| t > term) {
            return true;
        }
        for &id in s.ids() {
            sent.insert(id, s.metrics().sent_by(id).entries);
        }
        false
    });
    if !elected {
        return None;
    }
    let (leader, term) = sim.leader()?;
    let entries_before = sent
        .get(&leader)
        .copied()
        .unwrap_or_else(|| sim.metrics().sent_by(leader).entries);
    Some(Elected {
        leader,
        term,
        entries_before,
    })
}

/// Adds the term of every current candidate to `terms` (metric M3).
fn record_candidates<C: Consensus, O: Observer<C>>(
    sim: &Simulation<C, O>,
    terms: &mut BTreeSet<Term>,
) {
    for &id in sim.ids() {
        if let Some(status) = sim.status(id)
            && status.role == Role::Candidate
        {
            terms.insert(status.term);
        }
    }
}

/// The followers of `leader`, in random order.
fn shuffled_followers(ids: &[NodeId], leader: NodeId, rng: &mut Rng) -> Vec<NodeId> {
    let mut followers: Vec<NodeId> = ids.iter().copied().filter(|&id| id != leader).collect();
    for i in (1..followers.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        followers.swap(i, j);
    }
    followers
}

pub(crate) fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

/// The median of sorted values; the mean of the two middle values for an even count.
pub(crate) fn median(sorted: &[f64]) -> f64 {
    match sorted.len() {
        0 => f64::NAN,
        n if n % 2 == 1 => sorted[n / 2],
        n => (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0,
    }
}

/// The nearest-rank percentile of sorted values, for `p` in (0, 100].
pub(crate) fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// The columns that describe a run, written at the start of every result row.
pub(crate) fn run_columns(run: &RunConfig) -> String {
    let [min, max] = run.election_timeout_ms.unwrap_or([150, 300]);
    format!(
        "{},{},{},{},{},{},{min},{max},{}",
        run.label,
        run.arm,
        run.algorithm,
        run.servers,
        run.network.map_or("", |n| n.name()),
        run.clients,
        run.uncommitted
    )
}

pub(crate) const RUN_HEADER: &str = "label,arm,algorithm,servers,network,clients,election_timeout_min_ms,election_timeout_max_ms,uncommitted";

pub(crate) fn optional(value: Option<f64>) -> String {
    value.map(results::number).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use consensus::{NodeId, Rng};

    use super::{median, percentile, shuffled_followers};

    #[test]
    fn median_and_percentile() {
        assert_eq!(median(&[1.0, 2.0, 3.0]), 2.0);
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), 2.5);
        let values: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&values, 99.0), 99.0);
        assert_eq!(percentile(&values, 100.0), 100.0);
        assert_eq!(percentile(&[5.0], 99.0), 5.0);
    }

    #[test]
    fn shuffle_excludes_the_leader() {
        let ids: Vec<NodeId> = (1..=5).map(NodeId).collect();
        let followers = shuffled_followers(&ids, NodeId(3), &mut Rng::new(1));
        assert_eq!(followers.len(), 4);
        assert!(!followers.contains(&NodeId(3)));
    }
}
