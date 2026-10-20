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

## Documents

- [Project plan](docs/PLAN.md): research questions, design, metrics, experiments, and
  phases

## References

- Raft website: https://raft.github.io/
- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended
  Version)*: https://raft.github.io/raft.pdf
- Howard & Mortier, *Paxos vs Raft: Have We Reached Consensus on Distributed Consensus?*
  (PaPoC 2020): https://arxiv.org/abs/2004.05074
