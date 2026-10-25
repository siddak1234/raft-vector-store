//! Client library and command-line interface: caches the shard map, routes writes to
//! shard primaries, and merges search results across shards.
//!
//! [`ControlClient`] talks to the control plane: it proposes commands, following the
//! leader, and reads the shard map. [`VectorClient`] uses the shard map to send writes and
//! searches straight to the data nodes. [`Dataset`] reads ANN-Benchmarks files.
//!
//! See `docs/PLAN.md` §4.5.

pub mod control;
pub mod data;
pub mod dataset;

pub use control::{Committed, ControlClient, ControlError, RetryPolicy};
pub use data::{Acknowledged, VectorClient, VectorError};
pub use dataset::Dataset;
