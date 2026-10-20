//! Commands that run the experiment runner (`crates/bench`) and the analysis code.

use std::path::Path;
use std::process::Command;

/// Analysis scripts in `analysis/`, run by `figures` in this order. A script whose results
/// do not exist yet reports that and is skipped.
const ANALYSES: &[&str] = &["validation.py", "aa.py", "e2.py"];

/// Runs the validation campaign in release mode.
pub fn validate(root: &Path, options: &[String]) -> Result<(), String> {
    let mut args = vec!["validate".to_string()];
    args.extend_from_slice(options);
    run_bench(root, &args)
}

/// Runs the experiment defined in `experiments/configs/<id>.toml` in release mode.
pub fn experiment(root: &Path, id: &str, options: &[String]) -> Result<(), String> {
    let relative = format!("experiments/configs/{id}.toml");
    if !root.join(&relative).is_file() {
        return Err(format!(
            "no config `{relative}`; available: {}",
            available_configs(root).join(", ")
        ));
    }
    let mut args = vec!["experiment".to_string(), relative];
    args.extend_from_slice(options);
    run_bench(root, &args)
}

/// Runs every analysis script with the pinned Python environment.
pub fn figures(root: &Path) -> Result<(), String> {
    let dir = root.join("analysis");
    for script in ANALYSES {
        println!("==> uv run python {script}");
        let status = Command::new("uv")
            .args(["run", "--locked", "python", script])
            .current_dir(&dir)
            .status()
            .map_err(|err| format!("could not start uv (is it installed?): {err}"))?;
        if !status.success() {
            return Err(format!("analysis/{script} failed"));
        }
    }
    Ok(())
}

fn run_bench(root: &Path, args: &[String]) -> Result<(), String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = Command::new(cargo)
        .args([
            "run",
            "--release",
            "--locked",
            "--quiet",
            "-p",
            "bench",
            "--",
        ])
        .args(args)
        .current_dir(root)
        .status()
        .map_err(|err| format!("could not start cargo: {err}"))?;
    if status.success() {
        Ok(())
    } else {
        Err("the experiment runner reported a failure".into())
    }
}

fn available_configs(root: &Path) -> Vec<String> {
    let mut ids: Vec<String> = std::fs::read_dir(root.join("experiments").join("configs"))
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter_map(|entry| {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    name.strip_suffix(".toml").map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}
