//! The data-plane client: routes writes and searches straight to shard primaries using a
//! cached shard map (`docs/DESIGN.md` §6.5).
//!
//! - A write for vector `id` goes to the primary of shard `shard_of(id, shards)`.
//! - A search goes to every shard's primary, and the results are merged.
//! - When a data node refuses a request, for example because the map is stale, the client
//!   reads the map from the control-plane leader (a leader-confirmed read) and retries with
//!   backoff.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rpc::proto::data_node_client::DataNodeClient;
use rpc::proto::{SearchRequest, WriteRequest};
use rpc::{
    Neighbor, SearchQuery, SearchResult, ShardInfo, ShardKey, ShardMap, Write, WriteOp,
    WriteResult, shard_of,
};
use tonic::transport::{Channel, Endpoint};

use crate::control::{ControlClient, ControlError};

/// Why a data-plane request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorError {
    /// The collection does not exist.
    NoCollection(String),
    /// No primary accepted the request before the client gave up.
    GaveUp,
    /// The control plane could not be reached.
    Control(ControlError),
}

impl std::fmt::Display for VectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VectorError::NoCollection(name) => write!(f, "no collection {name}"),
            VectorError::GaveUp => write!(f, "no primary accepted the request in time"),
            VectorError::Control(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for VectorError {}

/// An acknowledged write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Acknowledged {
    /// The shard's epoch when the write was acknowledged.
    pub epoch: rpc::Epoch,
    /// The write's offset in the shard log.
    pub offset: u64,
}

/// A client of the vector store.
pub struct VectorClient {
    control: ControlClient,
    map: ShardMap,
    nodes: BTreeMap<String, DataNodeClient<Channel>>,
    /// How long to keep retrying a request.
    pub give_up: Duration,
}

impl VectorClient {
    /// A client that reads the shard map through `control`.
    pub fn new(control: ControlClient) -> Self {
        VectorClient {
            control,
            map: ShardMap::default(),
            nodes: BTreeMap::new(),
            give_up: Duration::from_secs(30),
        }
    }

    /// Reads the shard map again.
    ///
    /// # Errors
    ///
    /// Returns an error if the control plane cannot be reached.
    pub async fn refresh(&mut self, linearizable: bool) -> Result<(), VectorError> {
        let map = self
            .control
            .shard_map(linearizable)
            .await
            .map_err(VectorError::Control)?;
        if map.version >= self.map.version || linearizable {
            self.map = map;
        }
        Ok(())
    }

    fn node(&mut self, address: &str) -> Option<DataNodeClient<Channel>> {
        if let Some(client) = self.nodes.get(address) {
            return Some(client.clone());
        }
        let endpoint = Endpoint::from_shared(format!("http://{address}"))
            .ok()?
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_millis(200));
        let client = DataNodeClient::new(endpoint.connect_lazy());
        self.nodes.insert(address.to_string(), client.clone());
        Some(client)
    }

    /// The shard of `id` in `collection`, with its current entry in the cached map.
    fn route(
        &self,
        collection: &str,
        id: u64,
    ) -> Result<(ShardKey, Option<ShardInfo>), VectorError> {
        let spec = self
            .map
            .collections
            .get(collection)
            .ok_or_else(|| VectorError::NoCollection(collection.to_string()))?;
        let key = ShardKey {
            collection: collection.to_string(),
            shard: shard_of(id, spec.shards),
        };
        let info = self.map.shards.get(&key).cloned();
        Ok((key, info))
    }

    /// The address of a shard's primary in the cached map.
    fn primary(&self, info: &ShardInfo) -> Option<String> {
        let node = info.primary?;
        self.map.nodes.get(&node).map(|n| n.address.clone())
    }

    /// Writes `op` to `collection` and waits until every in-sync replica holds it.
    ///
    /// # Errors
    ///
    /// Returns an error if no primary acknowledges the write before the client gives up.
    pub async fn write(
        &mut self,
        collection: &str,
        op: WriteOp,
    ) -> Result<Acknowledged, VectorError> {
        let started = Instant::now();
        let mut backoff = Duration::from_millis(20);
        if self.map.collections.is_empty() {
            self.refresh(false).await?;
        }
        loop {
            if started.elapsed() > self.give_up {
                return Err(VectorError::GaveUp);
            }
            let (shard, info) = match self.route(collection, op.id()) {
                Ok(route) => route,
                // The cached map may predate the collection; read it from the leader.
                Err(VectorError::NoCollection(_)) if started.elapsed() <= self.give_up => {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(500));
                    self.refresh(true).await?;
                    continue;
                }
                Err(err) => return Err(err),
            };
            let attempt = match info
                .as_ref()
                .and_then(|i| self.primary(i).map(|a| (i.epoch, a)))
            {
                Some((epoch, address)) => match self.node(&address) {
                    Some(mut client) => {
                        let request = WriteRequest {
                            write: rpc::encode(&Write {
                                shard: shard.clone(),
                                epoch,
                                op: op.clone(),
                            }),
                        };
                        match tokio::time::timeout(Duration::from_secs(5), client.write(request))
                            .await
                        {
                            Ok(Ok(reply)) => rpc::decode::<WriteResult>(&reply.into_inner().result)
                                .ok()
                                .map(|result| (epoch, result)),
                            _ => None,
                        }
                    }
                    None => None,
                },
                None => None,
            };
            if let Some((epoch, WriteResult::Acknowledged { offset })) = attempt {
                return Ok(Acknowledged { epoch, offset });
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(500));
            // The map may be stale; read it from the leader.
            let _ = self.refresh(true).await;
        }
    }

    /// Inserts or replaces a vector.
    ///
    /// # Errors
    ///
    /// Returns an error if no primary acknowledges the write before the client gives up.
    pub async fn upsert(
        &mut self,
        collection: &str,
        id: u64,
        vector: Vec<f32>,
    ) -> Result<Acknowledged, VectorError> {
        self.write(collection, WriteOp::Upsert { id, vector }).await
    }

    /// The `k` vectors nearest to `vector` across every shard of `collection`.
    ///
    /// # Errors
    ///
    /// Returns an error if some shard's primary does not answer before the client gives up.
    pub async fn search(
        &mut self,
        collection: &str,
        vector: &[f32],
        k: u32,
    ) -> Result<Vec<Neighbor>, VectorError> {
        let started = Instant::now();
        if self.map.collections.is_empty() {
            self.refresh(false).await?;
        }
        if !self.map.collections.contains_key(collection) {
            self.refresh(true).await?;
        }
        let shards = self
            .map
            .collections
            .get(collection)
            .ok_or_else(|| VectorError::NoCollection(collection.to_string()))?
            .shards;
        let mut found: BTreeMap<u32, Vec<Neighbor>> = BTreeMap::new();
        let mut backoff = Duration::from_millis(20);
        while (found.len() as u32) < shards {
            if started.elapsed() > self.give_up {
                return Err(VectorError::GaveUp);
            }
            let mut calls = Vec::new();
            for number in 0..shards {
                if found.contains_key(&number) {
                    continue;
                }
                let key = ShardKey {
                    collection: collection.to_string(),
                    shard: number,
                };
                let Some(info) = self.map.shards.get(&key).cloned() else {
                    continue;
                };
                let Some(address) = self.primary(&info) else {
                    continue;
                };
                let Some(mut client) = self.node(&address) else {
                    continue;
                };
                let request = SearchRequest {
                    query: rpc::encode(&SearchQuery {
                        shard: key,
                        epoch: info.epoch,
                        vector: vector.to_vec(),
                        k,
                    }),
                };
                calls.push(async move {
                    let reply =
                        tokio::time::timeout(Duration::from_secs(5), client.search(request)).await;
                    let result = match reply {
                        Ok(Ok(reply)) => {
                            rpc::decode::<SearchResult>(&reply.into_inner().result).ok()
                        }
                        _ => None,
                    };
                    (number, result)
                });
            }
            let mut refused = false;
            for (number, result) in futures_join(calls).await {
                match result {
                    Some(SearchResult::Found(hits)) => {
                        found.insert(number, hits);
                    }
                    _ => refused = true,
                }
            }
            if refused || (found.len() as u32) < shards {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(500));
                let _ = self.refresh(true).await;
            }
        }
        Ok(merge(found.into_values(), k as usize))
    }
}

/// Runs futures concurrently and returns their outputs in order.
async fn futures_join<F>(futures: Vec<F>) -> Vec<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handles: Vec<_> = futures.into_iter().map(tokio::spawn).collect();
    let mut outputs = Vec::new();
    for handle in handles {
        if let Ok(output) = handle.await {
            outputs.push(output);
        }
    }
    outputs
}

/// Merges per-shard results into the overall `k` nearest, closest first.
pub fn merge(results: impl IntoIterator<Item = Vec<Neighbor>>, k: usize) -> Vec<Neighbor> {
    let mut all: Vec<Neighbor> = results.into_iter().flatten().collect();
    all.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
    all.truncate(k);
    all
}
