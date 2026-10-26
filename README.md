# Raft vs. Multi-Paxos on a Vector Store Control Plane

ECE 382N – Distributed Systems, Fall 2026 course project.

**Team:** Siddak Bhatia (sb56878) & Harrison Held (HWH492)

## Overview

A sharded vector store needs a coordinator that tracks which collections exist, which
data nodes are alive, and which nodes hold each shard. A single coordinator is a single
point of failure, so we replicate it across 3 or 5 servers using a consensus algorithm.

We build this control plane twice, once on Raft and once on Multi-Paxos, in Rust. Both
versions share all code except the consensus module. This lets us compare the two
algorithms directly and test the Raft paper's claims about efficiency, decomposition, and
coherency.

Only metadata goes through consensus. Vector data goes from clients straight to the data
nodes, where each shard is replicated from a primary to its backups.

## Getting Started

1. Install Rust with [rustup](https://rustup.rs) and [uv](https://docs.astral.sh/uv/).
2. Clone the repository outside any synced folder, such as iCloud Drive.
3. Run `cargo xtask ci` to build the workspace and run every check.

`cargo xtask help` lists all project commands. To reproduce the committed results:

```sh
cargo xtask validate                    # Raft: seeds 1-10000 of the random-fault campaign
cargo xtask validate --algorithm paxos  # the same campaign for Multi-Paxos
cargo xtask experiment AA               # A/A test of the harness in the simulator
cargo xtask experiment AA-real          # A/A test on a real cluster of processes
cargo xtask experiment E2               # Raft paper Figure 16 reproduction (also E2-lan, E2-real)
cargo xtask experiment E1               # steady state; then E1-load, E3, E4, E5, E6, E8
cargo xtask complexity                  # E7: protocol size and code complexity
cargo xtask dataset glove-25            # download the GloVe-25 dataset (also: sift)
cargo xtask dataplane                   # the Phase 4 data-plane checks, both algorithms
cargo xtask demo --consensus raft       # live failover demonstration (also: paxos)
cargo xtask figures                     # summaries and figures from the latest results
```

`docs/RESULTS.md` lists the results folder of every run, and each run's `manifest.toml`
records the commit it ran at. A full rerun of Phase 5 takes about 18 hours on one machine.

To try the system by hand, `cargo xtask cluster 3 --consensus raft --data-nodes 4` starts
a local cluster and prints its cluster file; `target/release/rvs --cluster FILE status`
and the other `rvs` commands drive it.

## Documents

- [Project plan](docs/PLAN.md): research questions, design, metrics, experiments, and
  phases
- [Research protocol](docs/RESEARCH.md): hypotheses, procedures, sample sizes, tests, and
  dated amendments, frozen before data collection
- [Multi-Paxos specification](docs/PAXOS_SPEC.md): the variant compared with Raft
- [Data plane design](docs/DESIGN.md): leases, replication, and their correctness
  arguments
- [Contributing](CONTRIBUTING.md): repository rules and workflow
- [Results](docs/RESULTS.md): every result, with the commands that produce them
- [Bug log](docs/BUGLOG.md): bugs found during development
- [Report](docs/REPORT.md) and [slides](docs/SLIDES.md): the final write-up

## License

[MIT](LICENSE)

## References

- Raft website: https://raft.github.io/
- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended
  Version)*: https://raft.github.io/raft.pdf
- Howard & Mortier, *Paxos vs Raft: Have We Reached Consensus on Distributed Consensus?*
  (PaPoC 2020): https://arxiv.org/abs/2004.05074
