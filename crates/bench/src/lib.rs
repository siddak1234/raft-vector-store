//! Experiment runner.
//!
//! Runs the validation campaign (`docs/RESEARCH.md` §9) and the experiments in
//! `docs/RESEARCH.md` §6, and writes raw results with a manifest to `experiments/results/`.
//! In the simulator each trial is one simulation with its own seed; trials run in parallel,
//! and results are written in seed order, so the output does not depend on thread timing.
//! On the real cluster each trial is a set of processes, and trials run one at a time in a
//! shuffled order (`docs/RESEARCH.md` amendments A4.1 and A4.2). Model checking runs one
//! search per configuration.
//!
//! The runner refuses to write official results while the repository has uncommitted
//! changes, so every result maps to an exact commit (`experiments/README.md`).

pub mod algorithm;
pub mod checks;
pub mod cluster;
pub mod complexity;
pub mod config;
pub mod dataplane;
pub mod demo;
pub mod load;
pub mod real;
pub mod results;
pub mod runner;
pub mod scenarios;
pub mod vectors;
