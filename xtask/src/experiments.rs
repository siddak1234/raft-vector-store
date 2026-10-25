//! Commands that run the experiment runner (`crates/bench`) and the analysis code.

use std::path::Path;
use std::process::Command;

/// Analysis scripts in `analysis/`, run by `figures` in this order. A script whose results
/// do not exist yet reports that and is skipped.
const ANALYSES: &[&str] = &[
    "validation.py",
    "aa.py",
    "e1.py",
    "e2.py",
    "e3.py",
    "e4.py",
    "e5.py",
    "e6.py",
    "e7.py",
    "e8.py",
];

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

/// Starts a local cluster for demos and manual tests.
pub fn cluster(root: &Path, options: &[String]) -> Result<(), String> {
    let mut args = vec!["cluster".to_string()];
    args.extend_from_slice(options);
    run_bench(root, &args)
}

/// Runs a runner command, such as `complexity` or `demo`.
pub fn bench_command(root: &Path, command: &str, options: &[String]) -> Result<(), String> {
    let mut args = vec![command.to_string()];
    args.extend_from_slice(options);
    run_bench(root, &args)
}

/// Runs the Phase 4 data-plane checks.
pub fn dataplane(root: &Path, options: &[String]) -> Result<(), String> {
    let mut args = vec!["dataplane".to_string()];
    args.extend_from_slice(options);
    run_bench(root, &args)
}

/// ANN-Benchmarks datasets: name, file, and the SHA-256 of the file as first downloaded
/// for this project on 2026-10-24.
const DATASETS: &[(&str, &str, &str)] = &[
    (
        "glove-25",
        "glove-25-angular.hdf5",
        "51004cb0ae962159f0db507a51fec2b395de14b166f55976c89f16bd2f8b6391",
    ),
    (
        "sift",
        "sift-128-euclidean.hdf5",
        "dd6f0a6ed6b7ebb8934680f861a33ed01ff33991eaee4fd60914d854a0ca5984",
    ),
];

/// The folder datasets are kept in, outside the repository: `$RVS_DATASETS`, or
/// `~/.cache/raft-vector-store/datasets`.
fn dataset_dir() -> std::path::PathBuf {
    std::env::var_os("RVS_DATASETS").map_or_else(
        || {
            let home = std::env::var_os("HOME").unwrap_or_default();
            std::path::PathBuf::from(home).join(".cache/raft-vector-store/datasets")
        },
        std::path::PathBuf::from,
    )
}

/// Downloads a dataset with `curl` unless it is already present, then checks its SHA-256
/// with `shasum`.
pub fn dataset(name: &str) -> Result<(), String> {
    let Some((_, file, sha256)) = DATASETS.iter().find(|(n, _, _)| *n == name) else {
        let names: Vec<&str> = DATASETS.iter().map(|(n, _, _)| *n).collect();
        return Err(format!(
            "unknown dataset `{name}`; available: {}",
            names.join(", ")
        ));
    };
    let dir = dataset_dir();
    std::fs::create_dir_all(&dir).map_err(|err| format!("{}: {err}", dir.display()))?;
    let path = dir.join(file);
    if !path.is_file() {
        let partial = dir.join(format!("{file}.part"));
        println!("==> downloading {file} to {}", dir.display());
        let status = Command::new("curl")
            .args(["-fL", "-o"])
            .arg(&partial)
            .arg(format!("http://ann-benchmarks.com/{file}"))
            .status()
            .map_err(|err| format!("could not start curl: {err}"))?;
        if !status.success() {
            return Err("the download failed".into());
        }
        std::fs::rename(&partial, &path).map_err(|err| err.to_string())?;
    }
    let output = Command::new("shasum")
        .args(["-a", "256"])
        .arg(&path)
        .output()
        .map_err(|err| format!("could not start shasum: {err}"))?;
    let digest = String::from_utf8_lossy(&output.stdout);
    let digest = digest.split_whitespace().next().unwrap_or_default();
    if digest != *sha256 {
        return Err(format!(
            "{} has SHA-256 {digest}, expected {sha256}; delete it and download again",
            path.display()
        ));
    }
    println!("{} is present and its checksum matches", path.display());
    Ok(())
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

/// Builds the server binaries, which the runner starts for real-cluster experiments and
/// local clusters, then runs the runner with `args`. Everything is built in release mode.
fn run_bench(root: &Path, args: &[String]) -> Result<(), String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let built = Command::new(&cargo)
        .args(["build", "--release", "--locked", "--quiet", "--bins"])
        .args(["-p", "controlplane", "-p", "datanode", "-p", "client"])
        .current_dir(root)
        .status()
        .map_err(|err| format!("could not start cargo: {err}"))?;
    if !built.success() {
        return Err("building the server binaries failed".into());
    }
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
