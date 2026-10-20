//! Deterministic simulator and safety and liveness checkers.
//!
//! The simulator is generic over the `Consensus` trait and does not depend on either
//! algorithm, so both run under identical, replayable conditions.
//!
//! See `docs/PLAN.md` §4.6.
