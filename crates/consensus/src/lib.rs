//! Interfaces shared by both consensus algorithms.
//!
//! This crate defines the `Consensus` and `Storage` traits and the types they use: node
//! IDs, terms, log indexes, commands, timers, and outputs. Implementations perform no I/O;
//! callers deliver messages and timer events and carry out the returned outputs.
//!
//! See `docs/PLAN.md` §4.1.
