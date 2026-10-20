//! The `ci` command: runs the same checks as `.github/workflows/ci.yml`, locally.

use std::path::Path;
use std::process::Command;

/// Cargo commands run in order. Each must succeed before the next one starts.
const CARGO_STEPS: &[&[&str]] = &[
    &["fmt", "--all", "--check"],
    &[
        "clippy",
        "--workspace",
        "--all-targets",
        "--locked",
        "--",
        "-D",
        "warnings",
    ],
    &["test", "--workspace", "--locked"],
];

/// Runs every CI check and stops at the first failure.
pub fn run(root: &Path) -> Result<(), String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    for args in CARGO_STEPS {
        run_step(root, &cargo, args)?;
    }
    println!("==> cargo xtask check-repo");
    crate::check_repo::run(root)?;
    check_analysis_lock(root)?;
    println!("All checks passed.");
    Ok(())
}

/// Checks that `analysis/uv.lock` matches `analysis/pyproject.toml`. Skipped when uv is not
/// installed, because none of the Rust checks need it.
fn check_analysis_lock(root: &Path) -> Result<(), String> {
    let has_uv = Command::new("uv")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success());
    if !has_uv {
        println!("==> skipped the analysis lockfile check: uv is not installed");
        return Ok(());
    }
    run_step(&root.join("analysis"), "uv", &["lock", "--check"])
}

fn run_step(dir: &Path, program: &str, args: &[&str]) -> Result<(), String> {
    let display = format!("{} {}", program_name(program), args.join(" "));
    println!("==> {display}");
    let status = Command::new(program)
        .args(args)
        .current_dir(dir)
        .status()
        .map_err(|err| format!("could not start `{display}`: {err}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{display}` failed"))
    }
}

/// Shortens a full program path, such as the one in `$CARGO`, to its file name.
fn program_name(program: &str) -> &str {
    Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(program)
}
