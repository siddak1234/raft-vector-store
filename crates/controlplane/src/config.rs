//! The static cluster configuration: every control-plane server's ID and address
//! (`docs/PLAN.md` §8, Phase 3).

use std::path::Path;

use consensus::NodeId;
use serde::Deserialize;

/// The servers of one cluster, read from a TOML file with one `[[server]]` table per
/// server.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Every server.
    pub server: Vec<ServerEntry>,
}

/// One server.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerEntry {
    /// The server's ID.
    pub id: u64,
    /// Where it listens, such as `127.0.0.1:7101`.
    pub address: String,
}

impl ClusterConfig {
    /// Reads a cluster file.
    ///
    /// # Errors
    ///
    /// Returns a description of the problem if the file cannot be read or is invalid.
    pub fn load(path: &Path) -> Result<ClusterConfig, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|err| format!("cannot read {}: {err}", path.display()))?;
        Self::parse(&text).map_err(|err| format!("{}: {err}", path.display()))
    }

    /// Parses a cluster file's contents.
    ///
    /// # Errors
    ///
    /// Returns a description of the problem if the text is invalid.
    pub fn parse(text: &str) -> Result<ClusterConfig, String> {
        let config: ClusterConfig = toml::from_str(text).map_err(|err| err.to_string())?;
        let mut ids: Vec<u64> = config.server.iter().map(|s| s.id).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != config.server.len() || ids.contains(&0) || ids.is_empty() {
            return Err("server IDs must be unique and positive".into());
        }
        Ok(config)
    }

    /// Every server's ID.
    pub fn ids(&self) -> Vec<NodeId> {
        self.server.iter().map(|s| NodeId(s.id)).collect()
    }

    /// The address of server `id`.
    pub fn address(&self, id: NodeId) -> Option<&str> {
        self.server
            .iter()
            .find(|s| s.id == id.0)
            .map(|s| s.address.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::ClusterConfig;

    #[test]
    fn parses_a_local_cluster() {
        let text = (1..=3)
            .map(|id| {
                format!(
                    "[[server]]\nid = {id}\naddress = \"127.0.0.1:{}\"\n\n",
                    7100 + id
                )
            })
            .collect::<String>();
        let config = ClusterConfig::parse(&text).expect("valid");
        assert_eq!(config.ids().len(), 3);
        assert_eq!(config.address(consensus::NodeId(2)), Some("127.0.0.1:7102"));
    }

    #[test]
    fn rejects_duplicate_ids() {
        let text = "[[server]]\nid = 1\naddress = \"a\"\n[[server]]\nid = 1\naddress = \"b\"\n";
        assert!(ClusterConfig::parse(text).is_err());
    }
}
