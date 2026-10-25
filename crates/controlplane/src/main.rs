//! The control-plane server binary.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use consensus::NodeId;
use controlplane::config::ClusterConfig;
use controlplane::leases::LeaseTiming;
use controlplane::{Algorithm, ServerOptions};

const HELP: &str = "\
Usage: controlplane --id N --cluster FILE --consensus raft|paxos --data-dir DIR [options]

Options:
  --log FILE                 Write JSON log lines to FILE instead of standard error.
  --election-timeout MIN-MAX Election timeout range in milliseconds (default 150-300).
                             The heartbeat interval is half the minimum.
  --delay-us D               Inject a one-way delay of D microseconds on average.
  --record                   Keep the measurement log for experiment runners.
";

fn main() -> ExitCode {
    match parse(std::env::args().skip(1).collect()).and_then(controlplane::run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

fn parse(args: Vec<String>) -> Result<ServerOptions, String> {
    let mut id = None;
    let mut cluster = None;
    let mut algorithm = None;
    let mut data_dir = None;
    let mut log = None;
    let mut timeout = (Duration::from_millis(150), Duration::from_millis(300));
    let mut delay_us = 0;
    let mut record = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let mut value = || iter.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--id" => id = Some(value()?.parse::<u64>().map_err(|e| e.to_string())?),
            "--cluster" => cluster = Some(ClusterConfig::load(&PathBuf::from(value()?))?),
            "--consensus" => algorithm = Some(Algorithm::parse(&value()?)?),
            "--data-dir" => data_dir = Some(PathBuf::from(value()?)),
            "--log" => log = Some(PathBuf::from(value()?)),
            "--election-timeout" => {
                let range = value()?;
                let (min, max) = range
                    .split_once('-')
                    .ok_or("--election-timeout needs MIN-MAX")?;
                let ms = |s: &str| s.parse::<u64>().map(Duration::from_millis);
                timeout = (
                    ms(min).map_err(|e| e.to_string())?,
                    ms(max).map_err(|e| e.to_string())?,
                );
            }
            "--delay-us" => {
                delay_us = value()?
                    .parse()
                    .map_err(|e: std::num::ParseIntError| e.to_string())?
            }
            "--record" => record = true,
            "--help" | "-h" => return Err(HELP.to_string()),
            other => return Err(format!("unknown option `{other}`\n\n{HELP}")),
        }
    }
    Ok(ServerOptions {
        id: NodeId(id.ok_or("--id is required")?),
        cluster: cluster.ok_or("--cluster is required")?,
        algorithm: algorithm.ok_or("--consensus is required")?,
        data_dir: data_dir.ok_or("--data-dir is required")?,
        log,
        election_timeout: timeout,
        delay_us,
        record,
        leases: LeaseTiming::default(),
    })
}
