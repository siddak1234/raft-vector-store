# Project Plan

This is the working plan for building a Raft-replicated control plane for a sharded,
replicated vector store in Rust. It covers the architecture, the design rules that keep
the system correct, the build phases, and how we will evaluate the result.

## 1. Architecture

```
clients ──(shard map, admin commands)──▶ control plane: 3 or 5 Raft servers
   │                                          ▲
   │ vector writes and searches               │ heartbeats, in-sync set changes
   ▼                                          │
data nodes: each shard = 1 primary + backups ─┘
```

- **Control plane:** 3 or 5 servers that run Raft and replicate the cluster metadata:
  collections, data nodes, and which data nodes hold each shard.
- **Data nodes:** store vector shards in memory and answer nearest-neighbor searches.
  Each shard has one primary and one or more backups.
- **Clients:** fetch the shard map from the control plane, then send vector writes and
  searches straight to the data nodes.

### Terms

- **Epoch:** a per-shard number that increases every time the shard's primary changes.
  Data nodes reject requests that carry an old epoch.
- **In-sync set:** the replicas of a shard that have every acknowledged write. Recorded
  in the control plane.
- **Lease:** a time-limited grant from the control plane. A data node acts as primary
  only while its lease is valid.
- **Incarnation:** a number a data node gets each time it starts. A restarted node is
  treated as a new replica.

## 2. Key decisions

1. **We implement Raft ourselves.** The course asks for it. tikv/raft-rs and openraft are
   good to read but are not dependencies.
2. **The Raft core has no networking, threads, or clocks.** It is a plain struct that
   takes events and returns actions:

   ```rust
   // crates/raft: no networking, no threads, no clocks
   pub enum Output { Send { to: NodeId, msg: Message }, Apply(Entry) }

   impl<S: Storage> RaftNode<S> {
       pub fn tick(&mut self) -> Vec<Output> { ... }                    // one unit of logical time
       pub fn step(&mut self, from: NodeId, msg: Message) -> Vec<Output> { ... }
       pub fn propose(&mut self, cmd: Vec<u8>) -> Result<LogIndex, NotLeader> { ... }
   }
   ```

   In tests, a deterministic simulator drives it with seeded randomness: it drops, delays,
   and reorders messages, partitions the network, and crashes nodes. Any failing run can
   be replayed from its seed. In the real cluster, a thin tokio and gRPC layer drives
   the same core.
3. **Only metadata goes through Raft.** Vectors go from clients straight to data nodes,
   as in Kafka and Qdrant. Sending vector data through one leader would make it the
   bottleneck.
4. **Vectors are replicated primary-to-backup, using Kafka's in-sync set model.** The
   control plane records each shard's primary, in-sync set, and epoch. We rejected
   running a separate Raft group per shard: it pushes vectors through Raft, which the
   proposal avoids, and it is much more work.
5. **Data-node heartbeats do not go through the Raft log.** Data nodes heartbeat the
   control-plane leader directly. Only the resulting state changes, such as marking a
   node dead, are logged. Logging every heartbeat would flood the log.
6. **Fixed membership.** The control plane has 3 or 5 servers, set in a config file.
7. **Fixed shard count per collection.** Vectors are routed to a shard by hashing the
   vector ID.
8. **Data nodes keep vectors in memory.** A restarted data node comes back empty with a
   new incarnation number and is re-replicated from the shard's primary.

## 3. Correctness rules

These are the rules that are easy to get wrong. Each one has a reason, and breaking any
of them causes a real bug.

### Raft

- **Figure 2 of the paper is the spec.** Implement it rule by rule.
- **Vote restriction (§5.4.1):** only vote for a candidate whose log is at least as up to
  date as yours.
- **Commit rule (§5.4.2, Figure 8):** a leader commits by counting replicas only for
  entries from its own term.
- **No-op on election (§8):** a new leader immediately appends an empty entry. Without
  it, the leader cannot commit leftover entries from earlier terms, and leader-confirmed
  reads can return stale data.
- **Persist before replying:** the current term, vote, and log entries are fsynced before
  the server answers any RPC.
- **Deterministic apply:** every server must reach the same state from the same log.
  Rust's `HashMap` iterates in a different random order in each process, so the state
  machine uses `BTreeMap`. Code inside `apply` never reads the clock or a random number
  generator. When a decision is needed, such as which node gets a new replica, the leader
  makes it and logs it as an explicit command.
- **The Raft core runs on its own OS thread.** If it ran inside a tokio task, each fsync
  would block a worker thread and delay heartbeats, which causes needless elections.

### Data-node leases

1. A data node starts its lease clock when it *sends* a heartbeat, not when the reply
   arrives.
2. The control-plane leader answers lease renewals only after a heartbeat round confirms
   that a majority still follows it. This is the same check used for leader-confirmed
   reads in §8. Pending renewals are batched into one round.
3. A newly elected leader waits one full lease period, plus a margin, before marking any
   data node dead.

**Why:** the leader tracks leases in memory. After a leader change, the new leader does
not know when leases were last renewed, and an old leader stuck in a minority partition
may still be renewing them. Rule 2 means any renewal was requested before a majority
moved to a newer term, so before the new leader was elected. Rule 3 then guarantees that
lease has expired before the new leader reassigns anything. The margin covers clocks that
run at slightly different rates.

### Vector replication

- A write is acknowledged only after every member of the in-sync set has it.
- A new primary is chosen only from the in-sync set, so it already has every acknowledged
  write. Each promotion increases the shard's epoch.
- **Removing a replica from the in-sync set:** the primary keeps waiting for that
  replica's acks until the control plane has committed the removal.
- **Adding a replica to the in-sync set:** the primary starts waiting for that replica's
  acks *before* proposing the addition.
- **Why these two rules:** getting either one backwards lets a primary acknowledge a write
  that a replica eligible for promotion does not have. If that replica is promoted, the
  acknowledged write is lost.
- If the in-sync set is smaller than the minimum (default 2 of 3), the shard rejects
  writes. It does not accept writes that have too few copies.
- Each replica keeps an ordered write log per shard. When a new primary takes over,
  backups drop any writes the new primary does not have. These writes were never
  acknowledged, so dropping them is safe, and it keeps replicas from diverging.
- A planned primary change, not caused by a failure, works in two steps. The old primary
  steps down and confirms before the new one is promoted. If the old primary cannot be
  reached, the control plane waits for its lease to expire.
- When a data node dies, the control plane assigns a replacement replica. The replacement
  copies the shard from the primary, receives new writes while catching up, and joins the
  in-sync set once it has everything.

### Routing

- Any control-plane server can serve the shard map from its own copy. A stale map is
  caught by the epoch check at the data node, and the client then refreshes its map.
  Admin reads go through the Raft log.
- Searches go to each shard's primary, which serves them only while its lease is valid.
  The client merges the top-k results from all shards.

## 4. Phases

### Phase 0: Foundation

- [ ] Cargo workspace, crate skeletons, and CI (`cargo fmt`, `cargo clippy`, `cargo test`)
- [ ] Add `target/` to `.gitignore`. Point the build output (`CARGO_TARGET_DIR`) and node
      data directories outside iCloud Drive. Otherwise iCloud tries to sync gigabytes of
      build output and can touch Raft data files during tests.
- [ ] Core interfaces: `RaftNode`, `Storage`, `StateMachine`, transport
- [ ] Design doc for the replication protocol and the lease timing argument, written
      before any data-plane code

**Done when:** CI builds the workspace, and we both agree on the design doc.

### Phase 1: Raft core in simulation

- [ ] Leader election with the vote restriction
- [ ] Log replication and the commit rule
- [ ] No-op entry on election, and leader-confirmed reads
- [ ] Persistence through `Storage`. After a crash, a node rebuilds its state by replaying
      the log.
- [ ] Simulator faults: dropped, delayed, and reordered messages; full and one-way
      partitions; crashes and restarts
- [ ] Safety checker for the Figure 3 properties: election safety, leader append-only,
      log matching, leader completeness, and state machine safety
- [ ] Liveness checker: once partitions heal and crashes stop, a leader is elected and
      new commands commit within a bounded number of ticks

**Done when:** thousands of random seeds pass both checkers.

### Phase 2: Metadata state machine

- [ ] Collections: dimension, distance metric, shard count, replication factor
- [ ] Data nodes: address, incarnation, status
- [ ] Shards: replicas, primary, in-sync set, epoch
- [ ] Commands: `CreateCollection`, `DropCollection`, `RegisterNode`, `MarkNodeDead`,
      `AddReplica`, `RemoveReplica`, `PromotePrimary`, `ShrinkInSync`, `ExpandInSync`.
      Every ownership change carries the epoch it expects and fails if the epoch has moved.
- [ ] Deterministic apply, following the rules in section 3
- [ ] Client sessions (client ID and sequence number), so a retried command is applied
      only once

**Done when:** unit tests pass against a stub that applies commands immediately, with no
Raft involved.

### Phase 3: Real control-plane cluster

- [ ] File-backed storage that fsyncs before replying
- [ ] gRPC transport (tonic), with the Raft core on its own thread
- [ ] Static cluster config and leader redirect
- [ ] Shard map served by any server
- [ ] Fault-injection admin command in every binary that drops traffic to chosen peers,
      one way or both ways
- [ ] Script to launch a 3-node or 5-node cluster locally
- [ ] Structured logs for each node

**Done when:** after a `kill -9` of the leader, writes resume once a new leader is
elected, and a restarted node catches up.

### Phase 4: Data plane with one copy per shard

- [ ] Data node: vectors in memory, brute-force nearest-neighbor search, registration
      with an incarnation number, heartbeats with leases
- [ ] Control plane: failure detection and shard reassignment, including the wait after
      a leader change
- [ ] Client: caches the shard map, routes by hashing the vector ID, checks epochs and
      refreshes the map, sends searches to all shards and merges the top-k results
- [ ] CLI for the demo

**Done when:** the end-to-end demo works: create a collection, insert vectors, search,
and kill the control-plane leader mid-workload.

### Phase 5: Vector replication

- [ ] Replica sets and the primary-to-backup write path with the in-sync rules
- [ ] Minimum in-sync size
- [ ] Promotion when a primary fails
- [ ] Per-shard write log, with backups dropping writes the new primary does not have
- [ ] Planned primary changes with step-down
- [ ] Replacement replicas copied from the primary

**Done when:** killing any single data node loses no acknowledged write, the shard keeps
accepting writes, and re-replication brings it back to full replication.

### Phase 6: Evaluation

Fault scenarios, run on the real cluster:

- [ ] Control-plane leader crash
- [ ] Minority and majority partitions
- [ ] One-way partitions
- [ ] Data-node crash
- [ ] Primary crash in the middle of a write

Checks, run against the recorded logs:

- [ ] At most one primary serves each shard at any moment. All processes run on one
      machine and share a clock, so lease intervals from different processes can be
      compared directly.
- [ ] No acknowledged write is lost.
- [ ] Every control-plane server applied the same commands in the same order.

Measurements:

- [ ] Control-plane failover time for 3 vs 5 servers, measured at the client from the
      moment the leader is killed to the first committed write
- [ ] Election time across election timeout ranges, rerunning the paper's Figure 16
      experiment (§9.3)
- [ ] Primary failover time
- [ ] Time to re-replicate a shard after a data node dies

## 5. Scope

- **In scope:** everything in phases 0 through 6.
- **Stretch goals:** snapshots and log compaction (§7), PreVote and CheckQuorum,
  membership changes (§6), shard splitting, approximate search indexes such as HNSW.
- **Out of scope:** using an existing Raft library as a dependency.

## 6. Repo layout

```
crates/
  raft/          Raft core (no networking, no tokio)
  sim/           deterministic simulator + checkers
  controlplane/  metadata state machine + server binary
  datanode/      shard storage, replication, search
  client/        routing library + CLI
  chaos/         fault-injection runner + log checkers
proto/           gRPC definitions
scripts/         local cluster launch
docs/            design doc, this plan
```

Main dependencies: tokio, tonic and prost for gRPC (requires `protoc`, installed with
`brew install protobuf`), serde, tracing, clap, and rand with fixed seeds.

## 7. References

- Raft website: https://raft.github.io/
- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended
  Version)*: https://raft.github.io/raft.pdf
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD dissertation; covers
  PreVote and client sessions): https://github.com/ongardie/dissertation
- Jon Gjengset, *Students' Guide to Raft* (common implementation bugs):
  https://thesquareplanet.com/blog/students-guide-to-raft/
- Apache Kafka documentation, replication design (in-sync replica sets):
  https://kafka.apache.org/documentation/#replication
