//! Experiment configuration files (`experiments/configs/*.toml`).
//!
//! A config names the experiment and lists its runs. Each run is one configuration of one
//! scenario, repeated for every seed in its range. Settings fixed by the research protocol,
//! such as warm-up and measurement durations, are constants in the scenarios, not config
//! fields.

use std::time::Duration;

use serde::Deserialize;

/// An experiment: an ID and the runs that make it up.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentConfig {
    /// The experiment ID, such as `E2`. Results go to `experiments/results/<id>/`.
    pub id: String,
    /// What the experiment measures.
    pub description: String,
    /// The runs.
    pub run: Vec<RunConfig>,
}

/// One configuration of one scenario.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    /// The procedure to run.
    pub scenario: Scenario,
    /// Where it runs: the simulator (the default) or a real cluster of processes.
    #[serde(default)]
    pub environment: Environment,
    /// A name for this run, written to every result row.
    pub label: String,
    /// The comparison arm (for example `A` or `B` in the A/A test). Empty if unused.
    #[serde(default)]
    pub arm: String,
    /// The algorithm under test.
    pub algorithm: String,
    /// Number of servers.
    pub servers: usize,
    /// The network environment. Required by every scenario except model checking.
    #[serde(default)]
    pub network: Option<NetworkKind>,
    /// First and last seed, inclusive. Required by every scenario except model checking.
    #[serde(default)]
    pub seeds: Option<[u64; 2]>,
    /// Number of closed-loop clients (steady scenario only).
    #[serde(default)]
    pub clients: usize,
    /// Offered load in commands per second (load scenario only).
    #[serde(default)]
    pub rate: u32,
    /// Election timeout range in milliseconds. The heartbeat interval is half the minimum.
    /// Defaults to 150–300 ms.
    #[serde(default)]
    pub election_timeout_ms: Option<[u64; 2]>,
    /// Commands the leader appends just before it crashes (failover scenario only).
    #[serde(default)]
    pub uncommitted: usize,
    /// Commands the leader commits while a follower is away (catch-up scenario only).
    #[serde(default)]
    pub committed: usize,
    /// How the follower is separated (catch-up scenario only).
    #[serde(default)]
    pub catch_up: Option<crate::scenarios::recovery::CatchUp>,
    /// Whether the control-plane leader crashes with the primary (vectors scenario only).
    #[serde(default)]
    pub crash_leader: bool,
    /// The partition scenario (partition scenario only).
    #[serde(default)]
    pub partition: Option<crate::scenarios::partition::PartitionKind>,
    /// Model checking: most client commands proposed in total.
    #[serde(default)]
    pub max_commands: Option<u8>,
    /// Model checking: most elections each server starts.
    #[serde(default)]
    pub max_elections: Option<u8>,
    /// Model checking: most times each server's heartbeat timer fires.
    #[serde(default)]
    pub max_heartbeats: Option<u8>,
}

impl RunConfig {
    /// The seeds in this run's range; none for model checking.
    pub fn seeds(&self) -> Vec<u64> {
        match self.seeds {
            Some([first, last]) => (first..=last).collect(),
            None => Vec::new(),
        }
    }

    /// The network environment, which `load` has checked is set.
    pub fn network(&self) -> NetworkKind {
        self.network
            .expect("load checks that every simulated run names its network")
    }
}

/// The experiment procedures (`docs/RESEARCH.md` §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    /// E1: steady-state latency, throughput, and cost per commit.
    Steady,
    /// E2: the Raft paper's Figure 16 election experiment.
    Figure16,
    /// E3: failover after a leader crash with uncommitted entries.
    Failover,
    /// E4a: log recovery after consecutive crashes.
    Recovery,
    /// E4b and E4c: catch-up of a follower that missed committed entries.
    CatchUp,
    /// E5: partitions.
    Partition,
    /// E8: vector store recovery (real cluster only).
    Vectors,
    /// E1-load: commit latency against offered load (real cluster only; amendment A8).
    Load,
    /// E6: exhaustive model checking of reachable states.
    Modelcheck,
}

/// Where a run's trials execute (`docs/RESEARCH.md` §3).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    /// The deterministic simulator.
    #[default]
    Simulator,
    /// One process per server on this machine, with delay injected in the transport and
    /// real fsync.
    Real,
}

/// The network environments (`docs/RESEARCH.md` §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkKind {
    /// Mean one-way delay 7.5 ms: a mean round trip of 15 ms, as in the paper's Figure 16.
    Paper,
    /// Mean one-way delay 0.5 ms: a mean round trip of 1 ms.
    Lan,
}

impl NetworkKind {
    /// The mean one-way delay `d`.
    pub fn one_way_delay(self) -> Duration {
        match self {
            NetworkKind::Paper => Duration::from_micros(7500),
            NetworkKind::Lan => Duration::from_micros(500),
        }
    }

    /// The name used in result files.
    pub fn name(self) -> &'static str {
        match self {
            NetworkKind::Paper => "paper",
            NetworkKind::Lan => "lan",
        }
    }
}

/// Reads and parses a config file.
///
/// # Errors
///
/// Returns a description of the problem if the file cannot be read or parsed.
pub fn load(path: &std::path::Path) -> Result<ExperimentConfig, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    let config: ExperimentConfig =
        toml::from_str(&text).map_err(|err| format!("cannot parse {}: {err}", path.display()))?;
    for run in &config.run {
        if run.scenario == Scenario::Modelcheck {
            if run.max_commands.is_none()
                || run.max_elections.is_none()
                || run.max_heartbeats.is_none()
            {
                return Err(format!(
                    "run {}: model checking needs max_commands, max_elections, and \
                     max_heartbeats",
                    run.label
                ));
            }
        } else {
            let Some([first, last]) = run.seeds else {
                return Err(format!("run {}: the run needs a seed range", run.label));
            };
            if first > last {
                return Err(format!("run {}: the seed range is empty", run.label));
            }
            if run.network.is_none() {
                return Err(format!("run {}: the run needs a network", run.label));
            }
        }
        if run.scenario == Scenario::CatchUp && (run.catch_up.is_none() || run.committed == 0) {
            return Err(format!(
                "run {}: a catch-up run needs `catch_up` and `committed`",
                run.label
            ));
        }
        if run.scenario == Scenario::Vectors && run.environment != Environment::Real {
            return Err(format!(
                "run {}: the vectors scenario runs on the real cluster",
                run.label
            ));
        }
        if run.scenario == Scenario::Load && (run.environment != Environment::Real || run.rate == 0)
        {
            return Err(format!(
                "run {}: a load run needs `rate` and runs on the real cluster",
                run.label
            ));
        }
        if run.scenario == Scenario::Partition && run.partition.is_none() {
            return Err(format!(
                "run {}: a partition run needs `partition`",
                run.label
            ));
        }
        if run.servers == 0 {
            return Err(format!(
                "run {}: a run needs at least one server",
                run.label
            ));
        }
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::{ExperimentConfig, NetworkKind, Scenario};

    #[test]
    fn parses_a_run() {
        let text = r#"
            id = "E2"
            description = "Figure 16"

            [[run]]
            scenario = "figure16"
            label = "150-155"
            algorithm = "raft"
            servers = 5
            network = "paper"
            seeds = [1, 1000]
            election_timeout_ms = [150, 155]
        "#;
        let config: ExperimentConfig = toml::from_str(text).expect("valid");
        let run = &config.run[0];
        assert_eq!(run.scenario, Scenario::Figure16);
        assert_eq!(run.network, Some(NetworkKind::Paper));
        assert_eq!(run.seeds().len(), 1000);
        assert_eq!(run.election_timeout_ms, Some([150, 155]));
    }

    #[test]
    fn a_load_run_needs_a_rate_and_the_real_cluster() {
        let run = |extra: &str| {
            format!(
                "id = \"X\"\ndescription = \"\"\n[[run]]\nscenario = \"load\"\nlabel = \"a\"\n\
                 algorithm = \"raft\"\nservers = 3\nnetwork = \"paper\"\nseeds = [1, 2]\n{extra}"
            )
        };
        let check = |text: String| {
            let dir = std::env::temp_dir().join(format!("rvs-config-test-{}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create");
            let path = dir.join("load.toml");
            std::fs::write(&path, text).expect("write");
            let result = super::load(&path).map(|_| ());
            let _ = std::fs::remove_dir_all(&dir);
            result
        };
        assert!(check(run("environment = \"real\"\nrate = 500\n")).is_ok());
        assert!(check(run("environment = \"real\"\n")).is_err());
        assert!(check(run("rate = 500\n")).is_err());
    }

    #[test]
    fn rejects_unknown_fields() {
        let text = r#"
            id = "X"
            description = ""
            [[run]]
            scenario = "steady"
            label = "a"
            algorithm = "raft"
            servers = 3
            network = "lan"
            seeds = [1, 2]
            warmup = 5
        "#;
        assert!(toml::from_str::<ExperimentConfig>(text).is_err());
    }
}
