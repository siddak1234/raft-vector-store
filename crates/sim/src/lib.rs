//! Deterministic simulator and safety checkers.
//!
//! A [`Simulation`] runs a cluster of servers that implement [`consensus::Consensus`] on a
//! virtual clock. Every source of randomness (message delays, faults, client behavior, and
//! each server's election timeouts) is drawn from its own stream derived from one seed, so a
//! run is exactly repeatable: the same seed always produces the same sequence of events.
//!
//! The simulator is the driver described in the `Consensus` driver contract: it delivers
//! one input at a time, syncs storage after every input, and then carries out the outputs.
//! Of the workspace's crates it depends only on `consensus`, so it cannot treat Raft and
//! Multi-Paxos differently.
//!
//! While it runs, the simulator checks the safety properties in `docs/RESEARCH.md` §9 and
//! counts the metrics that must be measured in shared code (`docs/RESEARCH.md` §5).
//!
//! See `docs/PLAN.md` §4.6.

mod checks;
mod client;
mod faults;
mod metrics;
mod network;
mod queue;
mod seeds;
mod simulation;
mod storage;
mod time;

pub use checks::{Check, Violation};
pub use faults::{ChaosConfig, Fault, chaos_schedule};
pub use metrics::{Counts, Metrics};
pub use network::{DropRule, PPM};
pub use seeds::{Stream, stream_rng, stream_seed};
pub use simulation::{Confirmation, Observer, Reply, ServerView, SimConfig, Simulation};
pub use time::SimTime;
