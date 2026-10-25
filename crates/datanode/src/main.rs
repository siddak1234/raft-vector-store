//! The data node binary.

use std::path::PathBuf;
use std::process::ExitCode;

use datanode::NodeOptions;
use rpc::DataNodeId;
use serde::Deserialize;

const HELP: &str = "\
Usage: datanode --id N --address HOST:PORT --cluster FILE [--record]

  --id N             The data node's ID.
  --address ADDR     Where it serves requests.
  --cluster FILE     The control-plane cluster file.
  --record           Keep the log of requests served as primary.
";

#[derive(Deserialize)]
struct ClusterFile {
    server: Vec<ServerEntry>,
}

#[derive(Deserialize)]
struct ServerEntry {
    id: u64,
    address: String,
}

fn main() -> ExitCode {
    match parse(std::env::args().skip(1).collect()).and_then(datanode::run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

fn parse(args: Vec<String>) -> Result<NodeOptions, String> {
    let mut id = None;
    let mut address = None;
    let mut cluster = None;
    let mut record = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let mut value = || iter.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--id" => id = Some(value()?.parse::<u64>().map_err(|e| e.to_string())?),
            "--address" => address = Some(value()?),
            "--cluster" => cluster = Some(PathBuf::from(value()?)),
            "--record" => record = true,
            _ => return Err(HELP.to_string()),
        }
    }
    let path = cluster.ok_or("--cluster is required")?;
    let text = std::fs::read_to_string(&path)
        .map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    let file: ClusterFile =
        toml::from_str(&text).map_err(|err| format!("{}: {err}", path.display()))?;
    let control = file.server.into_iter().map(|s| (s.id, s.address)).collect();
    let mut options = NodeOptions::new(
        DataNodeId(id.ok_or("--id is required")?),
        address.ok_or("--address is required")?,
        control,
    );
    options.record = record;
    Ok(options)
}
