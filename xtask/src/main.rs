//! Project commands for the raft-vector-store workspace.
//!
//! Every project task is a subcommand here, so the repository needs no loose scripts. Run
//! `cargo xtask help` for the list of commands. See `CONTRIBUTING.md`.

mod check_repo;
mod ci;

use std::path::PathBuf;
use std::process::ExitCode;

const HELP: &str = "\
Usage: cargo xtask <command>

Commands:
  check-repo   Check the repository layout, file rules, and crate dependency rules
  ci           Run every check that CI runs: format, lint, test, check-repo, lockfiles
  help         Show this message
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let root = workspace_root();

    let result = match args.as_slice() {
        [command] if command == "check-repo" => check_repo::run(&root),
        [command] if command == "ci" => ci::run(&root),
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
