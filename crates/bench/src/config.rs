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
    /// A name for this run, written to every result row.
    pub label: String,
    /// The comparison arm (for example `A` or `B` in the A/A test). Empty if unused.
    #[serde(default)]
    pub arm: String,
    /// The algorithm under test.
    pub algorithm: String,
    /// Number of servers.
    pub servers: usize,
    /// The network environment.
    pub network: NetworkKind,
    /// First and last seed, inclusive.
    pub seeds: [u64; 2],
    /// Number of closed-loop clients (steady scenario only).
    #[serde(default)]
    pub clients: usize,
    /// Election timeout range in milliseconds. The heartbeat interval is half the minimum.
    /// Defaults to 150–300 ms.
    #[serde(default)]
    pub election_timeout_ms: Option<[u64; 2]>,
    /// Commands the leader appends just before it crashes (failover scenario only).
    #[serde(default)]
    pub uncommitted: usize,
}

impl RunConfig {
    /// The seeds in this run's range.
    pub fn seeds(&self) -> std::ops::RangeInclusive<u64> {
        self.seeds[0]..=self.seeds[1]
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
        if run.seeds[0] > run.seeds[1] {
            return Err(format!("run {}: the seed range is empty", run.label));
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
        assert_eq!(run.network, NetworkKind::Paper);
        assert_eq!(run.seeds().count(), 1000);
        assert_eq!(run.election_timeout_ms, Some([150, 155]));
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
