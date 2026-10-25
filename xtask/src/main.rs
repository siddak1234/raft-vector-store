//! Project commands for the raft-vector-store workspace.
//!
//! Every project task is a subcommand here, so the repository needs no loose scripts. Run
//! `cargo xtask help` for the list of commands. See `CONTRIBUTING.md`.

mod check_repo;
mod ci;
mod experiments;

use std::path::PathBuf;
use std::process::ExitCode;

const HELP: &str = "\
Usage: cargo xtask <command> [options]

Commands:
  check-repo         Check the repository layout, file rules, and crate dependency rules
  ci                 Run every check that CI runs: format, lint, test, check-repo, lockfiles
  validate [OPTS]    Run the validation campaign: seeds 1-10000 with random faults,
                     for one algorithm (--algorithm raft or paxos; default raft)
  experiment ID      Run experiments/configs/ID.toml and write results
  figures            Regenerate every analysis table and figure from the latest results
  complexity         Run experiment E7 (needs rust-code-analysis-cli)
  demo [OPTS]        A live failover demonstration (--consensus raft|paxos)
  dataset NAME       Download an ANN-Benchmarks dataset (glove-25 or sift) and check it
  dataplane [OPTS]   Run the Phase 4 data-plane checks on real processes
                     (--algorithm raft|paxos, --scratch DIR)
  cluster [N] [OPTS] Start a local cluster of N = 1, 3, or 5 control-plane servers and
                     M data nodes (--consensus raft|paxos, --delay-us D,
                     --data-nodes M) until Enter is pressed
  help               Show this message

Options for validate and experiment (passed to the experiment runner):
  --algorithm NAME   Validation: the algorithm to check, raft or paxos (default raft).
  --seeds N          Validation: number of seeds. Experiment: at most N seeds per run.
  --scratch DIR      Write results to DIR instead of experiments/results/; allowed with
                     uncommitted changes, for trying things out.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let root = workspace_root();

    let result = match args.as_slice() {
        [command] if command == "check-repo" => check_repo::run(&root),
        [command] if command == "ci" => ci::run(&root),
        [command, rest @ ..] if command == "validate" => experiments::validate(&root, rest),
        [command, id, rest @ ..] if command == "experiment" => {
            experiments::experiment(&root, id, rest)
        }
        [command] if command == "figures" => experiments::figures(&root),
        [command, rest @ ..] if command == "cluster" => experiments::cluster(&root, rest),
        [command, name] if command == "dataset" => experiments::dataset(name),
        [command, rest @ ..] if command == "complexity" => {
            experiments::bench_command(&root, "complexity", rest)
        }
        [command, rest @ ..] if command == "demo" => {
            experiments::bench_command(&root, "demo", rest)
        }
        [command, rest @ ..] if command == "dataplane" => experiments::dataplane(&root, rest),
        [] => {
            print!("{HELP}");
            Ok(())
        }
        [command] if command == "help" => {
            print!("{HELP}");
            Ok(())
        }
        _ => Err(format!("unknown command `{}`\n\n{HELP}", args.join(" "))),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Returns the workspace root, which is the parent of the `xtask` crate directory.
fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .expect("the xtask crate is inside the workspace root")
        .to_path_buf()
}
