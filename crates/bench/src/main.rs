//! Command-line entry point for the experiment runner. Usually run through
//! `cargo xtask validate` and `cargo xtask experiment <id>`.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use bench::algorithm::{Algorithm, Kind};
use bench::config;
use bench::results::{self, Output};
use bench::runner::run_parallel;
use bench::scenarios::run_experiment;
use bench::scenarios::validation::{self, SECONDS_PER_SEED, ValidationRow};
use paxos::Paxos;
use raft::Raft;

const HELP: &str = "\
Usage:
  bench validate [--algorithm NAME] [--seeds N] [--scratch DIR]
      Runs seeds 1..=N (default 10000) of the validation campaign (docs/RESEARCH.md §9)
      for one algorithm: raft (the default) or paxos.
  bench experiment CONFIG [--seeds N] [--scratch DIR]
      Runs every trial in an experiment config (experiments/configs/*.toml).
  bench dataplane [--algorithm NAME] [--scratch DIR]
      Runs the Phase 4 exit checks on real processes (docs/PLAN.md §8).
  bench cluster [1|3|5] [--consensus raft|paxos] [--delay-us D] [--data-nodes M]
      Starts a local cluster and stops it when Enter is pressed.

Options:
  --algorithm NAME  Validation: the algorithm to check, raft or paxos (default raft).
  --seeds N      Validation: number of seeds. Experiment: at most N seeds per run.
  --scratch DIR  Write results to DIR instead of experiments/results/. Allowed with
                 uncommitted changes; scratch results are never committed.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

struct Options {
    algorithm: Option<String>,
    seeds: Option<u64>,
    scratch: Option<PathBuf>,
    positional: Vec<String>,
}

fn parse(args: &[String]) -> Result<Options, String> {
    let mut options = Options {
        algorithm: None,
        seeds: None,
        scratch: None,
        positional: Vec::new(),
    };
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--seeds" => {
                let value = iter.next().ok_or("--seeds needs a number")?;
                let seeds = value
                    .parse()
                    .map_err(|_| format!("--seeds: `{value}` is not a number"))?;
                options.seeds = Some(seeds);
            }
            "--algorithm" => {
                let value = iter.next().ok_or("--algorithm needs a name")?;
                options.algorithm = Some(value.clone());
            }
            "--scratch" => {
                let value = iter.next().ok_or("--scratch needs a folder")?;
                options.scratch = Some(PathBuf::from(value));
            }
            other if other.starts_with("--") => return Err(format!("unknown option `{other}`")),
            other => options.positional.push(other.to_string()),
        }
    }
    Ok(options)
}

fn run(args: &[String]) -> Result<(), String> {
    let Some((command, rest)) = args.split_first() else {
        print!("{HELP}");
        return Ok(());
    };
    if command == "cluster" {
        return bench::cluster::run(rest);
    }
    if command == "dataplane" {
        return bench::dataplane::run(rest);
    }
    let options = parse(rest)?;
    match command.as_str() {
        "validate" => validate(&options),
        "experiment" => experiment(&options),
        "help" | "--help" => {
            print!("{HELP}");
            Ok(())
        }
        other => Err(format!("unknown command `{other}`\n\n{HELP}")),
    }
}

fn validate(options: &Options) -> Result<(), String> {
    let kind = Kind::parse(options.algorithm.as_deref().unwrap_or(Raft::NAME))?;
    let seeds: Vec<u64> = (1..=options.seeds.unwrap_or(10_000)).collect();
    let output = results::prepare("validation", options.scratch.as_deref())?;
    eprintln!("validation: {} seeds of {}", seeds.len(), kind.name());
    let rows: Vec<ValidationRow> = match kind {
        Kind::Raft => run_parallel(&seeds, 1000, |&seed| validation::trial::<Raft>(seed)),
        Kind::Paxos => run_parallel(&seeds, 1000, |&seed| validation::trial::<Paxos>(seed)),
    };
    results::write_csv(&output, "runs.csv", &rows)?;
    results::write_manifest(&output, "validation", None, Some(kind.name()), rows.len())?;

    let failed: Vec<&ValidationRow> = rows.iter().filter(|row| row.violations > 0).collect();
    let hours = (rows.len() as u64 * SECONDS_PER_SEED) as f64 / 3600.0;
    report_location(&output);
    println!(
        "{} of {} seeds passed ({hours:.1} simulated hours, algorithm {}).",
        rows.len() - failed.len(),
        rows.len(),
        kind.name()
    );
    for row in failed.iter().take(20) {
        println!("  {}", row.first_violation);
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!("{} seeds had violations", failed.len()))
    }
}

fn experiment(options: &Options) -> Result<(), String> {
    let [path] = options.positional.as_slice() else {
        return Err("experiment needs one config file".into());
    };
    let path = Path::new(path);
    let config = config::load(path)?;
    let output = results::prepare(&config.id, options.scratch.as_deref())?;
    let text = std::fs::read_to_string(path).map_err(|err| err.to_string())?;
    results::write(&output, "config.toml", &text)?;
    eprintln!("{}: {}", config.id, config.description);
    let trials = run_experiment(&config, &output, options.seeds)?;
    let shown = path
        .strip_prefix(results::repo_root())
        .unwrap_or(path)
        .display()
        .to_string();
    results::write_manifest(&output, &config.id, Some(&shown), None, trials)?;
    report_location(&output);
    println!("{trials} trials written.");
    Ok(())
}

fn report_location(output: &Output) {
    let shown = output
        .dir
        .strip_prefix(results::repo_root())
        .unwrap_or(&output.dir)
        .display()
        .to_string();
    println!("Results: {shown}");
}
