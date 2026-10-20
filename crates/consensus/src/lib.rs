//! Interfaces shared by both consensus algorithms.
//!
//! [`Consensus`] is the trait that Raft and Multi-Paxos implement, and [`Storage`] is the
//! persistent state they read and write. Implementations perform no I/O: a driver delivers
//! messages and timer events and carries out the returned [`Output`]s. The simulator, the
//! model checker, and the networked server are all drivers, so the same algorithm code runs
//! in each.
//!
//! [`Config`] and [`Rng`] are shared so that both algorithms use the same timing settings
//! and the same source of randomness (`docs/PLAN.md` §5).
//!
//! See `docs/PLAN.md` §4.1.

mod config;
mod protocol;
mod rng;
mod storage;
mod types;

pub use config::Config;
pub use protocol::{Consensus, NotLeader, Output, RequestId, Role, Status, Timer};
pub use rng::Rng;
pub use storage::{HardState, MemStorage, Storage};
pub use types::{Command, Entry, LogIndex, NodeId, Payload, Term};
