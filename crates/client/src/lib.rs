//! Client library and command-line interface: caches the shard map, routes writes to
//! shard primaries, and merges search results across shards.
//!
//! [`ControlClient`] talks to the control plane: it proposes commands, following the
//! leader, and reads the shard map.
//!
//! See `docs/PLAN.md` §4.5.

pub mod control;

pub use control::{Committed, ControlClient, ControlError, RetryPolicy};
