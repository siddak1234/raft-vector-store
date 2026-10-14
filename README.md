# Raft-Based Control Plane for a Fault-Tolerant Vector Store

ECE 382N – Distributed Systems, Fall 2026 course project.

**Team:** Siddak Bhatia (sb56878) & Harrison Held (HWH492)

## Overview

We implement the [Raft consensus algorithm](https://raft.github.io/) and use it to
replicate the coordinator of a sharded vector store. One coordinator server is a single
point of failure, so we run it on 3 or 5 servers that form a fault-tolerant control
plane.

The control plane tracks the cluster's metadata:

- which collections exist
- which shard lives on which data node
- which data nodes are alive

Every metadata change (for example, moving a shard to a new node) is appended to the
leader's log, replicated to a majority, and applied in the same order on every server.
If the leader crashes, the remaining servers elect a new one and no committed change is
lost.

As in Kafka and Qdrant, only metadata goes through Raft. Vector data is sent directly to
the data nodes, so the leader never becomes a bottleneck for large writes.

## Components

1. **Raft:** leader election, log replication, and safety.
2. **Control-plane state machine:** collections, shard-to-node assignments, and node
   liveness, driven by the committed Raft log.
3. **Data nodes:** store vector shards and answer similarity searches.

## Evaluation

- Crash the leader and partition the network, then check that every shard always has
  exactly one owner.
- Measure leader failover time in 3-node and 5-node clusters.

## References

- Raft website: https://raft.github.io/
- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (USENIX ATC 2014)
