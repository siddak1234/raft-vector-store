//! Writing results: output folders, manifests, and CSV files (`experiments/README.md`).

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// A table of results: a header line and one line per trial.
pub trait Row {
    /// The CSV header.
    fn header() -> &'static str;
    /// The CSV line for this row.
    fn line(&self) -> String;
}

/// Where and when a set of results is being written.
#[derive(Debug, Clone)]
pub struct Output {
    /// The folder results are written to.
    pub dir: PathBuf,
    /// The full commit hash the results were produced from.
    pub commit: String,
    /// When the run started, in UTC (ISO 8601).
    pub started: String,
    /// Whether this is an official result under `experiments/results/`.
    pub official: bool,
}

/// The repository root.
pub fn repo_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| manifest.to_path_buf(), Path::to_path_buf)
}

/// Prepares the folder for a new set of results.
///
/// Without `scratch`, results are official: the working tree must have no uncommitted
/// changes, and the folder is `experiments/results/<id>/<UTC time>-<short commit>/`. With
/// `scratch`, results go to that folder instead and the clean-tree rule does not apply;
/// such results are for trying things out and are never committed.
///
/// # Errors
///
/// Returns a description of the problem, such as uncommitted changes.
pub fn prepare(id: &str, scratch: Option<&Path>) -> Result<Output, String> {
    let root = repo_root();
    let commit = git(&root, &["rev-parse", "HEAD"])?;
    let (iso, compact) = utc_now();
    let official = scratch.is_none();
    let dir = match scratch {
        Some(dir) => {
            let results = root.join("experiments").join("results");
            let absolute = std::env::current_dir()
                .map(|cwd| cwd.join(dir))
                .unwrap_or_else(|_| dir.to_path_buf());
            if absolute.starts_with(&results) {
                return Err("scratch results may not be written under experiments/results".into());
            }
            absolute
        }
        None => {
            let changes = git(&root, &["status", "--porcelain"])?;
            if !changes.is_empty() {
                return Err(
                    "the working tree has uncommitted changes; commit them before running an \
                     experiment, so the results map to an exact commit"
                        .into(),
                );
            }
            let short = &commit[..commit.len().min(7)];
            root.join("experiments")
                .join("results")
                .join(id)
                .join(format!("{compact}-{short}"))
        }
    };
    fs::create_dir_all(&dir).map_err(|err| format!("cannot create {}: {err}", dir.display()))?;
    Ok(Output {
        dir,
        commit,
        started: iso,
        official,
    })
}

/// Writes `rows` to `name` in the output folder.
///
/// # Errors
///
/// Returns a description of the problem if the file cannot be written.
pub fn write_csv<R: Row>(output: &Output, name: &str, rows: &[R]) -> Result<(), String> {
    let mut text = String::with_capacity(rows.len() * 80);
    text.push_str(R::header());
    text.push('\n');
    for row in rows {
        text.push_str(&row.line());
        text.push('\n');
    }
    write(output, name, &text)
}

/// Writes `text` to `name` in the output folder.
///
/// # Errors
///
/// Returns a description of the problem if the file cannot be written.
pub fn write(output: &Output, name: &str, text: &str) -> Result<(), String> {
    let path = output.dir.join(name);
    fs::write(&path, text).map_err(|err| format!("cannot write {}: {err}", path.display()))
}

/// Writes `manifest.toml`: what was run, from which commit, when, and on what machine.
///
/// # Errors
///
/// Returns a description of the problem if the file cannot be written.
pub fn write_manifest(
    output: &Output,
    experiment: &str,
    config: Option<&str>,
    trials: usize,
) -> Result<(), String> {
    let (finished, _) = utc_now();
    let mut text = String::new();
    let _ = writeln!(text, "experiment = {}", quote(experiment));
    if let Some(config) = config {
        let _ = writeln!(text, "config = {}", quote(config));
    }
    let _ = writeln!(text, "commit = {}", quote(&output.commit));
    let _ = writeln!(text, "started = {}", quote(&output.started));
    let _ = writeln!(text, "finished = {}", quote(&finished));
    let _ = writeln!(text, "trials = {trials}");
    let _ = writeln!(text, "official = {}", output.official);
    let _ = writeln!(text, "\n[machine]");
    let _ = writeln!(text, "os = {}", quote(std::env::consts::OS));
    let _ = writeln!(text, "arch = {}", quote(std::env::consts::ARCH));
    let _ = writeln!(text, "cpu = {}", quote(&cpu_name()));
    let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
    let _ = writeln!(text, "logical_cpus = {cpus}");
    let _ = writeln!(text, "rustc = {}", quote(&rustc_version()));
    write(output, "manifest.toml", &text)
}

fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn git(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|err| format!("cannot run git: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn command_line(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn cpu_name() -> String {
    command_line("sysctl", &["-n", "machdep.cpu.brand_string"])
        .or_else(|| {
            fs::read_to_string("/proc/cpuinfo").ok().and_then(|info| {
                info.lines()
                    .find(|line| line.starts_with("model name"))
                    .and_then(|line| line.split(':').nth(1))
                    .map(|name| name.trim().to_string())
            })
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn rustc_version() -> String {
    command_line("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string())
}

/// The current time in UTC, as ISO 8601 and as a compact folder name.
fn utc_now() -> (String, String) {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let (year, month, day) = civil_date(seconds.div_euclid(86_400) as i64);
    let rest = seconds % 86_400;
    let (hour, minute, second) = (rest / 3600, rest % 3600 / 60, rest % 60);
    (
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z"),
        format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
    )
}

/// Converts days since 1970-01-01 to a calendar date (Howard Hinnant's `civil_from_days`).
fn civil_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month as u32, day as u32)
}

/// Formats a float for CSV with enough precision for millisecond timings.
pub fn number(value: f64) -> String {
    format!("{value:.4}")
}

#[cfg(test)]
mod tests {
    use super::civil_date;

    #[test]
    fn converts_days_to_dates() {
        assert_eq!(civil_date(0), (1970, 1, 1));
        assert_eq!(civil_date(11_016), (2000, 2, 29));
        assert_eq!(civil_date(19_723), (2024, 1, 1));
        assert_eq!(civil_date(20_746), (2026, 10, 20));
    }
}
