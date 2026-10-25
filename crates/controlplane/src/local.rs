//! A cluster of control-plane processes on this machine, for tests, experiment runners,
//! and demos. Each server gets a fresh data folder and its own log file.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crate::Algorithm;
use crate::config::ClusterConfig;

/// Settings shared by every server in a local cluster.
#[derive(Debug, Clone)]
pub struct LocalOptions {
    /// The algorithm.
    pub algorithm: Algorithm,
    /// Election timeout range in milliseconds.
    pub election_timeout_ms: (u64, u64),
    /// Injected one-way delay in microseconds.
    pub delay_us: u64,
    /// Whether servers keep the measurement log.
    pub record: bool,
}

/// Running control-plane processes. Dropping the cluster kills them.
pub struct LocalCluster {
    binary: PathBuf,
    dir: PathBuf,
    options: LocalOptions,
    servers: Vec<(u64, String)>,
    children: BTreeMap<u64, Child>,
    restarts: BTreeMap<u64, u32>,
}

impl LocalCluster {
    /// Starts servers `1..=n` from `binary`, keeping their data and logs under `dir`,
    /// which must not exist yet or be empty.
    ///
    /// # Errors
    ///
    /// Returns a description if a folder, port, or process cannot be set up.
    pub fn start(
        binary: &Path,
        dir: &Path,
        n: u64,
        options: LocalOptions,
    ) -> Result<LocalCluster, String> {
        std::fs::create_dir_all(dir).map_err(|err| format!("{}: {err}", dir.display()))?;
        let mut servers = Vec::new();
        let mut text = String::new();
        for id in 1..=n {
            let port = free_port()?;
            let address = format!("127.0.0.1:{port}");
            text.push_str(&format!(
                "[[server]]\nid = {id}\naddress = \"{address}\"\n\n"
            ));
            servers.push((id, address));
        }
        ClusterConfig::parse(&text)?;
        std::fs::write(dir.join("cluster.toml"), text).map_err(|err| err.to_string())?;
        let mut cluster = LocalCluster {
            binary: binary.to_path_buf(),
            dir: dir.to_path_buf(),
            options,
            servers,
            children: BTreeMap::new(),
            restarts: BTreeMap::new(),
        };
        for id in 1..=n {
            cluster.spawn(id)?;
        }
        Ok(cluster)
    }

    fn spawn(&mut self, id: u64) -> Result<(), String> {
        let restarts = self.restarts.entry(id).or_insert(0);
        let log = self.dir.join(format!("server-{id}.{restarts}.log"));
        *restarts += 1;
        let (min, max) = self.options.election_timeout_ms;
        let mut command = Command::new(&self.binary);
        command
            .arg("--id")
            .arg(id.to_string())
            .arg("--cluster")
            .arg(self.dir.join("cluster.toml"))
            .arg("--consensus")
            .arg(match self.options.algorithm {
                Algorithm::Raft => "raft",
                Algorithm::Paxos => "paxos",
            })
            .arg("--data-dir")
            .arg(self.dir.join(format!("data-{id}")))
            .arg("--log")
            .arg(&log)
            .arg("--election-timeout")
            .arg(format!("{min}-{max}"))
            .arg("--delay-us")
            .arg(self.options.delay_us.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if self.options.record {
            command.arg("--record");
        }
        let child = command
            .spawn()
            .map_err(|err| format!("cannot start {}: {err}", self.binary.display()))?;
        self.children.insert(id, child);
        Ok(())
    }

    /// Every server's ID and address.
    pub fn servers(&self) -> &[(u64, String)] {
        &self.servers
    }

    /// The folder holding data and logs.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Kills server `id` with `SIGKILL`, as `kill -9` does.
    pub fn kill(&mut self, id: u64) {
        if let Some(mut child) = self.children.remove(&id) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Restarts a killed server with its data.
    ///
    /// # Errors
    ///
    /// Returns a description if the process cannot start.
    pub fn restart(&mut self, id: u64) -> Result<(), String> {
        if self.children.contains_key(&id) {
            return Ok(());
        }
        self.spawn(id)
    }

    /// Whether server `id` is running.
    pub fn is_running(&self, id: u64) -> bool {
        self.children.contains_key(&id)
    }

    /// Stops every server.
    pub fn stop(&mut self) {
        let ids: Vec<u64> = self.children.keys().copied().collect();
        for id in ids {
            self.kill(id);
        }
        // Give the OS a moment to release the ports.
        std::thread::sleep(Duration::from_millis(20));
    }
}

impl Drop for LocalCluster {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A TCP port that was free a moment ago.
///
/// # Errors
///
/// Returns a description if no port can be bound.
pub fn free_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
    listener
        .local_addr()
        .map(|addr| addr.port())
        .map_err(|err| err.to_string())
}
