//! The `check-repo` command: enforces the repository rules in `CONTRIBUTING.md`.
//!
//! Each rule is a function over plain data, so it can be unit tested without a real
//! repository. `run` collects that data from git and `cargo metadata` and reports every
//! problem it finds, not just the first.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::process::Command;

/// Entries allowed at the top level of the repository.
const TOP_LEVEL: &[&str] = &[
    ".cargo",
    ".github",
    ".gitignore",
    "CONTRIBUTING.md",
    "Cargo.lock",
    "Cargo.toml",
    "LICENSE",
    "README.md",
    "analysis",
    "crates",
    "docs",
    "experiments",
    "rust-toolchain.toml",
    "xtask",
];

/// Raw experiment results. Files here may be added but never modified or deleted.
const RESULTS_DIR: &str = "experiments/results/";

/// Largest file allowed outside `RESULTS_DIR`.
const MAX_FILE_BYTES: u64 = 1024 * 1024;

/// Dataset and archive formats. Datasets are stored outside the repository.
const DATA_EXTENSIONS: &[&str] = &[
    "hdf5", "h5", "npy", "npz", "fvecs", "ivecs", "zip", "tar", "gz", "tgz",
];

/// Script formats. Project commands belong in `xtask`.
const SCRIPT_EXTENSIONS: &[&str] = &["sh", "bash", "zsh", "fish", "ps1", "bat"];

/// File types allowed in `docs/`.
const DOC_EXTENSIONS: &[&str] = &["md", "png", "svg"];

/// The workspace crates each crate may depend on through normal or build dependencies.
/// Every workspace crate must appear here. See "Dependency rules" in `CONTRIBUTING.md`.
const ALLOWED_DEPS: &[(&str, &[&str])] = &[
    ("consensus", &[]),
    ("raft", &["consensus"]),
    ("paxos", &["consensus"]),
    ("sim", &["consensus"]),
    ("modelcheck", &["consensus"]),
    ("rpc", &[]),
    ("controlplane", &["consensus", "raft", "paxos", "rpc"]),
    ("datanode", &["rpc"]),
    ("client", &["rpc"]),
    (
        "bench",
        &[
            "consensus",
            "raft",
            "paxos",
            "sim",
            "modelcheck",
            "rpc",
            "controlplane",
            "datanode",
            "client",
        ],
    ),
    ("xtask", &[]),
];

/// Crates that must perform no I/O (`docs/PLAN.md` §4.1).
const NO_IO_CRATES: &[&str] = &["consensus", "raft", "paxos", "sim", "modelcheck"];

/// Async runtime and networking libraries, which the `NO_IO_CRATES` may not use.
const IO_LIBRARIES: &[&str] = &[
    "async-std",
    "hyper",
    "mio",
    "prost",
    "reqwest",
    "tokio",
    "tonic",
];

/// Crates that nothing may depend on, not even as a dev-dependency.
const LEAF_CRATES: &[&str] = &["bench", "xtask"];

/// A file that git tracks, or an untracked file that is not ignored.
#[derive(Debug)]
struct RepoFile {
    /// Path relative to the repository root, with `/` separators.
    path: String,
    /// Size in bytes, or `None` if the file was deleted from the working tree.
    size: Option<u64>,
}

/// A workspace crate and the dependencies it declares.
#[derive(Debug)]
struct Package {
    name: String,
    /// Directory relative to the workspace root, such as `crates/raft`.
    dir: String,
    dependencies: Vec<Dependency>,
}

#[derive(Debug)]
struct Dependency {
    name: String,
    kind: DepKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DepKind {
    Normal,
    Dev,
    Build,
}

/// A change under `RESULTS_DIR`, as reported by `git diff --name-status`.
#[derive(Debug)]
struct Change {
    /// Git status letter, such as `A`, `M`, `D`, or `R100`.
    status: String,
    path: String,
}

/// Runs every rule and prints all problems found.
pub fn run(root: &Path) -> Result<(), String> {
    let files = repo_files(root)?;
    let packages = workspace_packages(root)?;
    let changes = result_changes(root)?;

    let mut problems = Vec::new();
    problems.extend(check_top_level(&files));
    problems.extend(check_crates_dir(&files, &packages));
    problems.extend(check_file_sizes(&files));
    problems.extend(check_file_types(&files));
    problems.extend(check_results_append_only(&changes));
    problems.extend(check_dependencies(&packages));

    if problems.is_empty() {
        println!(
            "check-repo: {} files and {} crates checked, no problems found",
            files.len(),
            packages.len()
        );
        return Ok(());
    }
    for problem in &problems {
        eprintln!("  - {problem}");
    }
    Err(format!(
        "check-repo found {} problem(s); see CONTRIBUTING.md",
        problems.len()
    ))
}

/// Rule: only the agreed entries may exist at the top level.
fn check_top_level(files: &[RepoFile]) -> Vec<String> {
    let entries: BTreeSet<&str> = files
        .iter()
        .filter_map(|file| file.path.split('/').next())
        .filter(|entry| !TOP_LEVEL.contains(entry))
        .collect();
    entries
        .into_iter()
        .map(|entry| format!("`{entry}` is not an allowed top-level entry"))
        .collect()
}

/// Rule: every folder in `crates/` is a workspace crate, and no files sit directly in it.
fn check_crates_dir(files: &[RepoFile], packages: &[Package]) -> Vec<String> {
    let crate_dirs: BTreeSet<&str> = packages.iter().map(|p| p.dir.as_str()).collect();
    let mut problems = BTreeSet::new();
    for file in files {
        let mut parts = file.path.splitn(3, '/');
        if parts.next() != Some("crates") {
            continue;
        }
        match (parts.next(), parts.next()) {
            (Some(name), Some(_)) => {
                let dir = format!("crates/{name}");
                if !crate_dirs.contains(dir.as_str()) {
                    problems.insert(format!("`{dir}` is not a workspace crate"));
                }
            }
            _ => {
                problems.insert(format!("`{}` must be inside a crate folder", file.path));
            }
        }
    }
    problems.into_iter().collect()
}

/// Rule: no file over `MAX_FILE_BYTES` outside `RESULTS_DIR`.
fn check_file_sizes(files: &[RepoFile]) -> Vec<String> {
    files
        .iter()
        .filter(|file| !file.path.starts_with(RESULTS_DIR))
        .filter_map(|file| {
            let size = file.size?;
            (size > MAX_FILE_BYTES).then(|| {
                format!(
                    "`{}` is {} KiB; files outside {RESULTS_DIR} must be at most {} KiB",
                    file.path,
                    size / 1024,
                    MAX_FILE_BYTES / 1024
                )
            })
        })
        .collect()
}

/// Rule: no datasets, archives, scripts, or notebooks; Python only in `analysis/`; only
/// Markdown and images in `docs/`.
fn check_file_types(files: &[RepoFile]) -> Vec<String> {
    let mut problems = Vec::new();
    for file in files {
        let path = file.path.as_str();
        let extension = Path::new(path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let extension = extension.as_str();

        if DATA_EXTENSIONS.contains(&extension) {
            problems.push(format!(
                "`{path}`: datasets and archives are stored outside the repository"
            ));
        } else if SCRIPT_EXTENSIONS.contains(&extension) {
            problems.push(format!(
                "`{path}`: add project commands to xtask instead of scripts"
            ));
        } else if extension == "ipynb" {
            problems.push(format!(
                "`{path}`: notebooks are not allowed; put analysis code in .py files"
            ));
        } else if extension == "py" && !path.starts_with("analysis/") {
            problems.push(format!("`{path}`: Python files belong in analysis/"));
        } else if path.starts_with("docs/") && !DOC_EXTENSIONS.contains(&extension) {
            problems.push(format!(
                "`{path}`: docs/ may contain only Markdown, PNG, and SVG files"
            ));
        }
    }
    problems
}

/// Rule: files under `RESULTS_DIR` may be added but never modified, renamed, or deleted.
fn check_results_append_only(changes: &[Change]) -> Vec<String> {
    changes
        .iter()
        .filter(|change| change.status != "A")
        .map(|change| {
            format!(
                "`{}`: existing results may not be modified, renamed, or deleted (git status {})",
                change.path, change.status
            )
        })
        .collect()
}

/// Rule: crate dependencies follow `ALLOWED_DEPS`, the `NO_IO_CRATES` use no I/O libraries,
/// and nothing depends on the `LEAF_CRATES`.
fn check_dependencies(packages: &[Package]) -> Vec<String> {
    let workspace: BTreeSet<&str> = packages.iter().map(|p| p.name.as_str()).collect();
    let mut problems = Vec::new();

    for package in packages {
        let name = package.name.as_str();
        let Some(allowed) = ALLOWED_DEPS
            .iter()
            .find(|(crate_name, _)| *crate_name == name)
            .map(|(_, deps)| *deps)
        else {
            problems.push(format!(
                "crate `{name}` is missing from ALLOWED_DEPS in xtask/src/check_repo.rs"
            ));
            continue;
        };

        for dep in &package.dependencies {
            let dep_name = dep.name.as_str();
            if LEAF_CRATES.contains(&dep_name) {
                problems.push(format!(
                    "`{name}` depends on `{dep_name}`, which no crate may depend on"
                ));
                continue;
            }
            if dep.kind == DepKind::Dev {
                continue;
            }
            if workspace.contains(dep_name) && !allowed.contains(&dep_name) {
                problems.push(format!(
                    "`{name}` may not depend on `{dep_name}` (see ALLOWED_DEPS)"
                ));
            }
            if NO_IO_CRATES.contains(&name) && IO_LIBRARIES.contains(&dep_name) {
                problems.push(format!(
                    "`{name}` must not perform I/O, so it may not depend on `{dep_name}`"
                ));
            }
        }
    }
    problems
}

/// Lists tracked files plus untracked files that are not ignored, so a stray file is
/// caught before it is committed.
fn repo_files(root: &Path) -> Result<Vec<RepoFile>, String> {
    let output = git(
        root,
        &[
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
    )?;
    let paths: BTreeSet<&str> = output.split('\0').filter(|p| !p.is_empty()).collect();
    Ok(paths
        .into_iter()
        .map(|path| RepoFile {
            path: path.to_string(),
            size: fs::metadata(root.join(path)).ok().map(|meta| meta.len()),
        })
        .collect())
}

fn workspace_packages(root: &Path) -> Result<Vec<Package>, String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let json = output_of(
        Command::new(cargo)
            .args(["metadata", "--format-version", "1", "--no-deps"])
            .current_dir(root),
        "cargo metadata",
    )?;
    parse_metadata(&json)
}

fn parse_metadata(json: &str) -> Result<Vec<Package>, String> {
    let metadata: serde_json::Value =
        serde_json::from_str(json).map_err(|err| format!("cannot parse cargo metadata: {err}"))?;
    let root = metadata["workspace_root"]
        .as_str()
        .ok_or("cargo metadata has no workspace_root")?;
    let packages = metadata["packages"]
        .as_array()
        .ok_or("cargo metadata has no packages")?;

    packages
        .iter()
        .map(|package| -> Result<Package, String> {
            let name = package["name"]
                .as_str()
                .ok_or("cargo metadata lists a package without a name")?
                .to_string();
            let manifest = package["manifest_path"]
                .as_str()
                .ok_or_else(|| format!("crate `{name}` has no manifest_path"))?;
            let dir = Path::new(manifest)
                .parent()
                .and_then(|dir| dir.strip_prefix(root).ok())
                .map(|dir| dir.to_string_lossy().replace('\\', "/"))
                .ok_or_else(|| format!("crate `{name}` is outside the workspace root"))?;
            let dependencies = package["dependencies"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|dep| Dependency {
                    name: dep["name"].as_str().unwrap_or_default().to_string(),
                    kind: match dep["kind"].as_str() {
                        Some("dev") => DepKind::Dev,
                        Some("build") => DepKind::Build,
                        _ => DepKind::Normal,
                    },
                })
                .collect();
            Ok(Package {
                name,
                dir,
                dependencies,
            })
        })
        .collect()
}

/// Lists changes under `RESULTS_DIR` relative to `origin/main`, including uncommitted ones.
/// Returns no changes when `origin/main` is not available, such as in a fresh local repo.
fn result_changes(root: &Path) -> Result<Vec<Change>, String> {
    let has_main = Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", "origin/main"])
        .current_dir(root)
        .output()
        .is_ok_and(|output| output.status.success());
    if !has_main {
        println!("check-repo: origin/main not found, skipping the results check");
        return Ok(Vec::new());
    }

    let mut changes = Vec::new();
    // Committed changes on this branch, then uncommitted changes in the working tree.
    for range in ["origin/main...HEAD", "HEAD"] {
        let output = git(root, &["diff", "--name-status", range, "--", RESULTS_DIR])?;
        changes.extend(parse_name_status(&output));
    }
    Ok(changes)
}

fn parse_name_status(output: &str) -> Vec<Change> {
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.split('\t');
            let status = fields.next()?.to_string();
            let path = fields.collect::<Vec<_>>().join(" -> ");
            (!path.is_empty()).then_some(Change { status, path })
        })
        .collect()
}

fn git(root: &Path, args: &[&str]) -> Result<String, String> {
    output_of(Command::new("git").args(args).current_dir(root), "git")
}

fn output_of(command: &mut Command, name: &str) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|err| format!("could not run {name}: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "{name} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout).map_err(|err| format!("{name} printed invalid UTF-8: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(paths: &[&str]) -> Vec<RepoFile> {
        paths
            .iter()
            .map(|path| RepoFile {
                path: (*path).to_string(),
                size: Some(100),
            })
            .collect()
    }

    fn package(name: &str, deps: &[(&str, DepKind)]) -> Package {
        Package {
            name: name.to_string(),
            dir: format!("crates/{name}"),
            dependencies: deps
                .iter()
                .map(|(dep, kind)| Dependency {
                    name: (*dep).to_string(),
                    kind: *kind,
                })
                .collect(),
        }
    }

    fn change(status: &str, path: &str) -> Change {
        Change {
            status: status.to_string(),
            path: path.to_string(),
        }
    }

    #[test]
    fn top_level_accepts_allowed_entries() {
        let ok = files(&[
            "README.md",
            ".github/workflows/ci.yml",
            "crates/raft/src/lib.rs",
        ]);
        assert!(check_top_level(&ok).is_empty());
    }

    #[test]
    fn top_level_rejects_stray_entries() {
        let problems = check_top_level(&files(&["run.sh", "notes/todo.md", "notes/more.md"]));
        assert_eq!(problems.len(), 2);
    }

    #[test]
    fn crates_dir_accepts_workspace_crates() {
        let packages = [package("raft", &[])];
        assert!(check_crates_dir(&files(&["crates/raft/src/lib.rs"]), &packages).is_empty());
    }

    #[test]
    fn crates_dir_rejects_unknown_folders_and_loose_files() {
        let packages = [package("raft", &[])];
        let stray = files(&["crates/old/src/lib.rs", "crates/notes.md"]);
        assert_eq!(check_crates_dir(&stray, &packages).len(), 2);
    }

    #[test]
    fn file_sizes_allow_large_files_only_in_results() {
        let big = Some(2 * MAX_FILE_BYTES);
        let checked = [
            RepoFile {
                path: "experiments/results/E1/run/data.csv".to_string(),
                size: big,
            },
            RepoFile {
                path: "docs/PLAN.md".to_string(),
                size: big,
            },
            RepoFile {
                path: "docs/deleted.md".to_string(),
                size: None,
            },
        ];
        let problems = check_file_sizes(&checked);
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("docs/PLAN.md"));
    }

    #[test]
    fn file_types_accept_expected_files() {
        let ok = files(&[
            "crates/raft/src/lib.rs",
            "analysis/plots.py",
            "docs/PLAN.md",
            "docs/figure.svg",
        ]);
        assert!(check_file_types(&ok).is_empty());
    }

    #[test]
    fn file_types_reject_data_scripts_notebooks_and_misplaced_files() {
        let bad = files(&[
            "experiments/sift.hdf5",
            "tools/run.sh",
            "analysis/explore.ipynb",
            "crates/sim/generate.py",
            "docs/notes.txt",
        ]);
        assert_eq!(check_file_types(&bad).len(), 5);
    }

    #[test]
    fn results_accept_only_added_files() {
        let changes = [
            change("A", "experiments/results/E1/a.csv"),
            change("M", "experiments/results/E1/b.csv"),
            change("D", "experiments/results/E1/c.csv"),
            change(
                "R100",
                "experiments/results/E1/d.csv -> experiments/results/E1/e.csv",
            ),
        ];
        assert_eq!(check_results_append_only(&changes).len(), 3);
    }

    #[test]
    fn name_status_parsing_handles_renames() {
        let changes = parse_name_status("A\tresults/x.csv\nR090\told.csv\tnew.csv\n");
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[1].status, "R090");
        assert_eq!(changes[1].path, "old.csv -> new.csv");
    }

    #[test]
    fn dependencies_accept_the_planned_graph() {
        let packages = [
            package("consensus", &[]),
            package(
                "raft",
                &[("consensus", DepKind::Normal), ("sim", DepKind::Dev)],
            ),
            package("sim", &[("consensus", DepKind::Normal)]),
            package(
                "controlplane",
                &[("raft", DepKind::Normal), ("tokio", DepKind::Normal)],
            ),
        ];
        assert!(check_dependencies(&packages).is_empty());
    }

    #[test]
    fn dependencies_reject_rule_violations() {
        let packages = [
            package("consensus", &[]),
            package("raft", &[]),
            package("bench", &[]),
            // The simulator may not depend on a specific algorithm.
            package("sim", &[("raft", DepKind::Normal)]),
            // Consensus code may not perform I/O.
            package("paxos", &[("tokio", DepKind::Normal)]),
            // The data plane may not depend on consensus.
            package("client", &[("consensus", DepKind::Normal)]),
            // Nothing may depend on bench, even for tests.
            package("datanode", &[("bench", DepKind::Dev)]),
            // Every crate must be listed in ALLOWED_DEPS.
            package("extra", &[]),
        ];
        assert_eq!(check_dependencies(&packages).len(), 5);
    }

    #[test]
    fn metadata_parsing_reads_crate_dirs_and_dependency_kinds() {
        let json = r#"{
            "workspace_root": "/repo",
            "packages": [{
                "name": "raft",
                "manifest_path": "/repo/crates/raft/Cargo.toml",
                "dependencies": [
                    {"name": "consensus", "kind": null},
                    {"name": "sim", "kind": "dev"}
                ]
            }]
        }"#;
        let packages = parse_metadata(json).expect("valid metadata");
        assert_eq!(packages[0].dir, "crates/raft");
        assert_eq!(packages[0].dependencies[0].kind, DepKind::Normal);
        assert_eq!(packages[0].dependencies[1].kind, DepKind::Dev);
    }
}
