//! `bench complexity`: experiment E7 (`docs/RESEARCH.md` §6, H5).
//!
//! - **M18, protocol size.** The message types are read from each algorithm's
//!   `message_kind`, one message of every type. The persistent state fields are those the
//!   algorithm writes through `Storage` in a simulated run with crashes: the term, the
//!   vote, and the log.
//! - **M19, code size and complexity.** `rust-code-analysis-cli` measures every module of
//!   `crates/raft/src` and `crates/paxos/src`; its JSON output is stored as is.
//!
//! M20 comes from `docs/BUGLOG.md` and is read by the analysis.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use consensus::{Consensus, Storage};
use paxos::Paxos;
use raft::Raft;
use sim::{ChaosConfig, SimConfig, SimTime, Simulation, Stream, chaos_schedule, stream_rng};

use crate::algorithm::Algorithm;
use crate::results;

const MODULES: [&str; 4] = ["election.rs", "replication.rs", "commit.rs", "lib.rs"];

/// The persistent state fields an algorithm writes in a simulated run with crashes.
fn persistent_fields<A: Algorithm>() -> Vec<&'static str> {
    let mut term = false;
    let mut vote = false;
    let mut log = false;
    for seed in 1..=20 {
        let config = SimConfig {
            servers: 5,
            clients: 3,
            count_bytes: false,
            raft_checks: A::RAFT_CHECKS,
            ..SimConfig::default()
        };
        let mut sim: Simulation<A, A::Observer> =
            Simulation::with_observer(config, seed, A::Observer::default());
        let chaos = ChaosConfig {
            start: Duration::from_secs(1),
            end: Duration::from_secs(8),
            mean_gap: Duration::from_millis(500),
            max_duration: Duration::from_secs(2),
        };
        let ids = sim.ids().to_vec();
        for (time, fault) in chaos_schedule(&ids, &chaos, &mut stream_rng(seed, Stream::Faults)) {
            sim.schedule(time, fault);
        }
        for step in 1..=10 {
            sim.run_until(SimTime::from_duration(Duration::from_secs(step)));
            for &id in &ids {
                let storage = sim.storage(id);
                let hard = storage.hard_state();
                term |= hard.term.0 > 0;
                vote |= hard.voted_for.is_some();
                log |= storage.last_index().0 > 0;
            }
        }
    }
    [("currentTerm", term), ("votedFor", vote), ("log", log)]
        .into_iter()
        .filter(|(_, used)| *used)
        .map(|(name, _)| name)
        .collect()
}

/// Raft's message types, one message of each.
fn raft_kinds() -> Vec<&'static str> {
    use consensus::{LogIndex, Term};
    use raft::Message;
    [
        Message::RequestVote {
            term: Term(1),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        },
        Message::RequestVoteReply {
            term: Term(1),
            granted: true,
        },
        Message::AppendEntries {
            term: Term(1),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            entries: Vec::new(),
            leader_commit: LogIndex(0),
            round: 0,
        },
        Message::AppendEntriesReply {
            term: Term(1),
            success: true,
            prev_log_index: LogIndex(0),
            index: LogIndex(0),
            round: 0,
        },
    ]
    .iter()
    .map(Raft::message_kind)
    .collect()
}

/// Multi-Paxos's message types, one message of each.
fn paxos_kinds() -> Vec<&'static str> {
    use consensus::{LogIndex, Term};
    use paxos::Message;
    [
        Message::Prepare {
            term: Term(1),
            commit_index: LogIndex(0),
        },
        Message::Promise {
            term: Term(1),
            granted: true,
            entries: Vec::new(),
        },
        Message::Accept {
            term: Term(1),
            first_index: LogIndex(1),
            entries: Vec::new(),
            commit_index: LogIndex(0),
            round: 0,
        },
        Message::Accepted {
            term: Term(1),
            success: true,
            first_index: LogIndex(1),
            last_index: LogIndex(0),
            commit_index: LogIndex(0),
            round: 0,
        },
        Message::Commit {
            term: Term(1),
            first_index: LogIndex(1),
            entries: Vec::new(),
            commit_index: LogIndex(0),
        },
    ]
    .iter()
    .map(Paxos::message_kind)
    .collect()
}

/// Runs `bench complexity`.
///
/// # Errors
///
/// Returns a description if `rust-code-analysis-cli` is missing or a file cannot be
/// written.
pub fn run(args: &[String]) -> Result<(), String> {
    let scratch = match args {
        [flag, dir] if flag == "--scratch" => Some(PathBuf::from(dir)),
        [] => None,
        _ => return Err("usage: bench complexity [--scratch DIR]".into()),
    };
    let output = results::prepare("E7", scratch.as_deref())?;
    let root = results::repo_root();

    let mut protocol = BTreeMap::new();
    protocol.insert(
        "raft",
        serde_json::json!({ "message_types": raft_kinds(), "persistent_fields": persistent_fields::<Raft>() }),
    );
    protocol.insert(
        "paxos",
        serde_json::json!({ "message_types": paxos_kinds(), "persistent_fields": persistent_fields::<Paxos>() }),
    );
    results::write(
        &output,
        "protocol.json",
        &serde_json::to_string_pretty(&protocol).map_err(|e| e.to_string())?,
    )?;

    let mut files = 1;
    for algorithm in ["raft", "paxos"] {
        for module in MODULES {
            let path = root.join("crates").join(algorithm).join("src").join(module);
            let out = Command::new("rust-code-analysis-cli")
                .args(["--metrics", "--output-format", "json", "--paths"])
                .arg(&path)
                .output()
                .map_err(|err| {
                    format!("cannot run rust-code-analysis-cli (install it with `cargo install rust-code-analysis-cli`): {err}")
                })?;
            if !out.status.success() {
                return Err(format!(
                    "rust-code-analysis-cli failed on {}: {}",
                    path.display(),
                    String::from_utf8_lossy(&out.stderr)
                ));
            }
            let text = String::from_utf8_lossy(&out.stdout).into_owned();
            results::write(&output, &format!("{algorithm}-{module}.json"), &text)?;
            files += 1;
        }
    }
    results::write_manifest(&output, "E7", None, None, files)?;
    println!("Results: {}", output.dir.display());
    Ok(())
}
