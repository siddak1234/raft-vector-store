# Raft vs. Multi-Paxos on a Vector Store Control Plane: Project Plan

**Course:** ECE 382N Distributed Systems, Fall 2026
**Team:** Siddak Bhatia (sb56878), Harrison Held (HWH492)

## 1. Objective

We build the control plane of a sharded, replicated vector store twice: once on Raft and
once on Multi-Paxos. The two versions share all code except the consensus module, so any
measured difference comes from the algorithm.

We use the system to test claims from the Raft paper (Ongaro & Ousterhout, 2014):

- Raft is as efficient as Paxos.
- Raft separates leader election, log replication, and safety.
- Raft enforces stronger coherency, which reduces the number of states that must be
  considered.

## 2. Research Questions

| ID | Question | Hypothesis | Basis |
|---|---|---|---|
| RQ1 | Do Raft and Multi-Paxos differ in commit latency or throughput with a stable leader? | No. They are equivalent within a ±5% margin. | Both commit with one round trip from the leader to a majority (Raft abstract, §9.3) |
| RQ2 | How do they differ when the leader fails? | Paxos election traffic grows with the number of uncommitted entries; Raft's does not. Which one recovers faster is an open question. | The algorithms differ only in leader election (Howard & Mortier) |
| RQ3 | Does a new Raft leader re-send fewer log entries? | Yes | Raft §5.4.2 |
| RQ4 | Does Raft keep logs more coherent and reach fewer states? | Yes: fewer reachable states, no log holes, no entries sent to the leader | Raft §4, §5.3, §5.4.1 |
| RQ5 | Is Raft's implementation smaller, with less overlap between election, replication, and safety code? | Yes: fewer message types and lower code complexity | Raft §4, §10 |
| RQ6 | Does the choice of algorithm change how fast the vector store recovers from failures? | Only when the control-plane leader fails at the same time | Follows from RQ1 and RQ2 |

## 3. System Overview

```
clients ──(shard map, admin commands)──▶ control plane: 3 or 5 servers
   │                                     consensus: Raft | Multi-Paxos
   │ vector writes and searches               ▲
   ▼                                          │ heartbeats, replica changes
data nodes: each shard = 1 primary + backups ─┘
```

- **Control plane:** replicates cluster metadata: collections, data nodes, and which data
  nodes hold each shard.
- **Data nodes:** store vector shards in memory and answer nearest-neighbor searches.
- **Clients:** read the shard map, then send writes and searches directly to data nodes.

Only metadata goes through consensus. Vector data never does, so the vector store's data
path is the same in both builds.

## 4. Design

### 4.1 Consensus Interface

Both algorithms implement one trait. The consensus code does no I/O and never reads the
clock. The caller delivers messages and timer events, then carries out the returned
outputs. The deterministic simulator, the model checker, and the networked server all
drive the same code.

```rust
/// A consensus module with no I/O. Callers deliver inputs and execute the returned outputs.
pub trait Consensus {
    type Message: Clone + Serialize + DeserializeOwned;

    /// Handles a message from another server.
    fn step(&mut self, from: NodeId, msg: Self::Message) -> Vec<Output<Self::Message>>;

    /// Handles an expired timer (election timeout or heartbeat).
    fn on_timer(&mut self, timer: Timer) -> Vec<Output<Self::Message>>;

    /// Submits a client command. Returns `NotLeader` if this server is not the leader.
    fn propose(&mut self, cmd: Command) -> Result<Vec<Output<Self::Message>>, NotLeader>;
}

pub enum Output<M> {
    Send { to: NodeId, msg: M },
    SetTimer { timer: Timer, after: Duration },
    Apply { index: LogIndex, cmd: Command },
}
```

Persistent state goes through a shared `Storage` trait. It flushes to disk before any
output that depends on that state is returned.

### 4.2 Raft

Implemented from Figure 2 of the Raft paper. These rules need particular care:

- A server votes only for a candidate whose log is at least as up to date as its own
  (§5.4.1).
- A leader commits by counting replicas only for entries from its current term (§5.4.2).
- A new leader immediately appends a no-op entry (§8).
- The current term, vote, and log are flushed to disk before replying to any RPC.

### 4.3 Multi-Paxos

Our variant is based on Howard & Mortier's description, which uses Raft's terminology so
that the comparison isolates leader election. Remaining details come from van Renesse &
Altinbuken, *Paxos Made Moderately Complex*.

- **Election:** any server can become leader. A candidate sends a prepare message with a
  higher term (ballot) and its commit index.
- **Promises:** each server that receives it promises to reject lower terms and returns
  the entries it holds after the candidate's commit index.
- **Taking over:** with promises from a majority, the candidate becomes leader. For each
  index it keeps the entry with the highest term and re-proposes it under its own term.
  Indexes with no entry are filled with no-ops.
- **Steady state:** the leader sends each batch to all followers and commits it once a
  majority accept it. This is the same message pattern as Raft.
- **Holes:** followers accept each index on its own, without Raft's consistency check, as
  in classic Multi-Paxos. A follower's log can therefore have holes. Entries are applied
  in index order.

The exact rules are written as a one-page summary in the style of Raft's Figure 2
(`docs/PAXOS_SPEC.md`). Both team members review it before coding starts. After
experiments begin, only bug fixes are allowed.

### 4.4 Control-Plane State Machine

- **State:**
  - collections: dimension, distance metric, shard count, replication factor
  - data nodes: address, incarnation, status
  - shards: replicas, primary, in-sync set, epoch
- **Commands:** `CreateCollection`, `DropCollection`, `RegisterNode`, `MarkNodeDead`,
  `AddReplica`, `RemoveReplica`, `PromotePrimary`, `ShrinkInSync`, `ExpandInSync`.
- **Epoch checks:** every ownership change carries the epoch it expects. It fails if the
  epoch has changed.
- **Deterministic apply:**
  - The state machine uses `BTreeMap`, because `HashMap` iteration order differs between
    processes.
  - It never reads the clock or a random number generator.
  - The leader makes placement decisions and logs them as explicit commands.
- **Client sessions:** each command carries a client ID and sequence number, so a retried
  command is applied once (Raft §8). Both builds use the same session logic.

### 4.5 Data Plane

- **Storage:** data nodes keep vectors in memory and answer k-nearest-neighbor queries by
  brute-force search. A restarted node gets a new incarnation number and is treated as a
  new replica.
- **Liveness:** data nodes send heartbeats to the control-plane leader. Heartbeats are not
  written to the log. Leases follow three rules:
  1. A data node starts its lease clock when it sends a heartbeat.
  2. The leader answers renewals only after a heartbeat round confirms that a majority
     still follows it.
  3. A newly elected leader waits one lease period plus a margin before marking any data
     node dead.

  These rules guarantee that an old lease has expired before a new leader reassigns its
  shards. The full argument goes in `docs/DESIGN.md`.
- **Replication:**
  - A write is acknowledged once every replica in the shard's in-sync set has it.
  - New primaries are chosen only from the in-sync set, and each promotion increases the
    shard's epoch.
  - **Removing a replica from the in-sync set:** the primary keeps waiting for that
    replica's acks until the removal is committed.
  - **Adding a replica:** the primary starts waiting for its acks before proposing the
    addition.
  - The shard rejects writes if fewer than 2 of 3 replicas are in sync.
  - When a new primary takes over, backups discard writes it does not have. Those writes
    were never acknowledged.
  - When a data node dies, a replacement replica copies the shard from the primary and
    joins the in-sync set once it has caught up.
- **Routing:**
  - Clients fetch the shard map from any control-plane server and hash each vector ID to
    a shard.
  - Data nodes reject requests that carry an old epoch, and the client then refreshes its
    map.
  - A search goes to every shard's primary, which serves it only while its lease is valid,
    and the client merges the top-k results.

### 4.6 Verification

- **Simulator:** deterministic and seeded.
  - **Faults:** dropped, delayed, reordered, and duplicated messages; full and one-way
    partitions; crashes and restarts.
  - **Separate random streams:** one for the environment (fault schedule, message delays,
    workload) and one for each algorithm instance (election timeouts).
  - **Replayable schedules:** schedules are saved to files so both algorithms replay
    identical inputs.
- **Shared checkers:**
  - No two servers apply different commands at the same index.
  - There is at most one leader per term or ballot.
  - No acknowledged command is missing from the committed log.
  - Once faults stop, a leader is elected and commands commit within a bounded time.
- **Raft-only checkers:** log matching and leader completeness (Figure 3).
- **Model checking:** a Stateright adapter wraps both algorithms. It counts every state
  each algorithm can reach under the same limits.
- **Real cluster:** `kill -9` crashes, and fault injection in the transport layer.

## 5. Experimental Method

These rules make the comparison apples to apples:

1. **One variable:**
   - A single binary built from one codebase, with `--consensus raft|paxos` choosing the
     algorithm.
   - Only `crates/raft` and `crates/paxos` differ.
   - Messages, bytes, fsyncs, and timings are counted in shared code (transport, storage,
     client), never inside an algorithm.
2. **Matched settings:**

   | Setting | Value for both |
   |---|---|
   | Tick length, heartbeat interval, election timeout range | Same |
   | Quorum | Majority |
   | Batch size, max entries in flight | Same |
   | fsync | Once per batch, before replying |
   | No-op on new leader | Yes |
   | Optimized follower catch-up (§5.3) | Off |
   | PreVote, CheckQuorum, read leases, flexible quorums | Off |
   | Message encoding | Same serde format |

3. **Frozen Paxos spec:** reviewed before coding, as described in Section 4.3.
4. **Same correctness bar:**
   - Both algorithms pass the same checkers on the same seeds before any measurement.
   - A bug fix in either algorithm means rerunning every experiment for both.
5. **Paired inputs:** for each seed, both algorithms replay the same schedule file.
6. **Controlled environment:**
   - Same machine, release build, same compiler and `Cargo.lock`.
   - Fresh data directories for every run.
   - Raft and Paxos runs interleaved in random order.
   - The warm-up period of each run is discarded.
7. **A/A validation:** before any A/B run, Raft is compared against itself through the
   full pipeline. The analysis must report no difference.
8. **Analysis fixed in advance:**
   - **Sample sizes:** 1000 simulator trials per configuration, matching the paper, and
     100 real-cluster runs per configuration.
   - **Tests:**
     - Paired simulator runs: Wilcoxon signed-rank.
     - Real-cluster runs: Mann–Whitney U.
     - Equivalence claims: two one-sided tests (TOST) with a ±5% margin.
   - **Reporting:** full distributions, medians, and p99, with bootstrap 95% confidence
     intervals.
   - **Timing:** sample sizes, tests, and margins are committed in `docs/RESEARCH.md`
     before any data is collected.
9. **Two environments:**
   - **Simulator:** exact message counts and a controlled delay model.
   - **Real cluster:** localhost with delay injected in the transport, plus real fsync.

   Delay is set so the broadcast time is about 1 ms (LAN) or about 15 ms (the setup
   behind the paper's Figure 16).

## 6. Metrics

**Leader election**

- **M1 Time without a leader:** from the leader crash until a new leader is elected.
- **M2 Unavailability:** from the crash until a client observes a new commit.
- **M3 Election rounds:** terms or ballots started per failover.
- **M4 Election traffic:** messages and bytes during an election, including any log
  entries sent.
- **M5 Stale-log rejections:** votes denied because the candidate's log is behind (Raft
  only).
- **M6 Unnecessary leader changes:** leader changes not caused by a crash.

**Log replication**

- **M7 Commit latency:** p50 and p99, from the client sending a command until it receives
  the committed reply.
- **M8 Throughput:** commits per second at saturation.
- **M9 Cost per commit:** messages and bytes per committed command.
- **M10 Re-sent entries:** entries re-sent after a leader change, before the first new
  commit.
- **M11 Catch-up time:** time for a follower to catch up after a restart or a healed
  partition.

**Safety** (pass/fail)

- **M12 Safety violations:** must be zero (see the checkers in Section 4.6).
- **M13 Evidence:** seeds and simulated hours checked.

**Coherency**

- **M14 Reachable states:** unique reachable states (model checker, same limits for
  both).
- **M15 Log holes:** holes observed.
- **M16 Entries sent to the leader:** entries transferred from followers to a leader.
- **M17 Log divergence:** the largest number of entries a follower must discard.

**Implementation**

- **M18 Protocol size:** message types and persistent state fields.
- **M19 Code size and complexity:** lines of code, cyclomatic and cognitive complexity per
  subproblem (election, replication, safety/commit), measured with
  `rust-code-analysis-cli`.
- **M20 Bugs:** bugs caught by the checkers during development, by subproblem (from
  `docs/BUGLOG.md`).

**System**

- **M21 Shard recovery time:** from a data-node primary crash until the shard accepts
  writes again.

## 7. Experiments

| ID | RQ | Setup | Metrics |
|---|---|---|---|
| E1 Steady state | RQ1 | 1 (reference), 3, and 5 servers; ~1 ms and ~15 ms broadcast time; 1, 4, 16, and 64 clients | M7–M9 |
| E2 Figure 16 reproduction (Raft) | Validation | 5 servers, ~15 ms broadcast time, the paper's timeout ranges, leader crashed at a random point in its heartbeat interval, servers with different log lengths, 1000 trials per range | M1 |
| E3 Failover | RQ2 | E2 setup on both algorithms, 3 and 5 servers, 0/10/100/1000 uncommitted entries at crash time | M1–M5 |
| E4 Log recovery | RQ3 | Leader crashes while entries from earlier terms are uncommitted | M10, M11 |
| E5 Partitions | RQ2 | Minority, majority, one-way, and flapping partitions | M2, M6 |
| E6 Coherency | RQ4 | Model checking at the same limits; counters collected during E3–E5 | M14–M17 |
| E7 Implementation | RQ5 | Static analysis of the frozen code; bug log | M18–M20 |
| E8 Vector store recovery | RQ6 | Data-node primary crash, with and without a simultaneous control-plane leader crash | M21 |

M12 and M13 are recorded in every experiment.

## 8. Phases

### Phase 0: Foundation and Research Protocol

- [ ] Cargo workspace, crate skeletons, CI (`fmt`, `clippy`, `test`)
- [ ] Build output (`target/`) and node data directories outside iCloud Drive
- [ ] `Consensus` and `Storage` traits and shared types
- [ ] `docs/RESEARCH.md`: research questions, hypotheses, metrics, experiments, sample
      sizes, tests, margins
- [ ] `docs/PAXOS_SPEC.md`: one-page Multi-Paxos specification
- [ ] `docs/DESIGN.md`: lease and replication protocol with correctness argument
- [ ] `docs/BUGLOG.md` started

**Exit:** CI passes. Both team members have reviewed and committed the three design
documents.

### Phase 1: Simulator and Raft

- [ ] Simulator with delay model, fault injection, schedule files, separate random
      streams, and message and byte accounting
- [ ] Shared and Raft-specific checkers
- [ ] Raft: election, replication, commit rule, no-op, persistence, restart
- [ ] A/A validation of the harness
- [ ] E2 (Figure 16 reproduction) in the simulator

**Exit:** thousands of seeds pass with zero violations. The A/A run shows no difference,
and E2 results are recorded.

### Phase 2: Multi-Paxos and State-Space Analysis

- [ ] Multi-Paxos implemented from `docs/PAXOS_SPEC.md`
- [ ] Passes the shared checkers on the same seed set as Raft
- [ ] Stateright adapter for both algorithms; state counts at the agreed limits

**Exit:** zero violations across the shared seed set, and state counts recorded for both
algorithms.

### Phase 3: Control Plane on a Real Cluster

- [ ] Metadata state machine and client sessions, unit-tested without consensus
- [ ] File-backed storage with fsync; gRPC transport (tonic); consensus core on its own
      thread
- [ ] `--consensus` flag, static cluster config, leader redirect, shard map served by any
      server
- [ ] Fault-injection and delay-injection admin commands
- [ ] Launch scripts for 1, 3, and 5 servers; structured logs; metrics export

**Exit:** for both algorithms, writes resume after a `kill -9` of the leader, and a
restarted server catches up.

### Phase 4: Vector Store Data Plane

- [ ] **Milestone A, one copy per shard:**
  - data node, brute-force search
  - registration, heartbeats, leases
  - routing client, CLI
  - ANN-Benchmarks loader
- [ ] **Milestone B, replication:**
  - in-sync set rules, minimum in-sync size, promotion
  - discarding unacknowledged writes on promotion
  - re-replication

**Exit:**

- Search results match the dataset's ground truth.
- Under both algorithms, killing any one data node loses no acknowledged write.
- The shard keeps accepting writes, and re-replication restores 3 copies.

### Phase 5: Experiments

- [ ] E1–E8 run as specified in `docs/RESEARCH.md`
- [ ] Raw results, configs, and seeds stored in `experiments/`

**Exit:** every configuration has reached its planned sample size, with zero safety
violations in any run.

### Phase 6: Analysis and Report

- [ ] Analysis scripts (Python: pandas, SciPy, matplotlib) that regenerate every figure
      and table from raw results
- [ ] Final report: background, design, method, results by research question, threats to
      validity, conclusion
- [ ] Demo of live failover under both algorithms, and presentation slides

**Exit:** report and slides are complete, and one script regenerates all figures from the
repository.

## 9. Repository Layout

```
crates/
  consensus/     Consensus and Storage traits, shared types
  raft/          Raft
  paxos/         Multi-Paxos
  sim/           deterministic simulator and checkers
  modelcheck/    Stateright adapter
  controlplane/  metadata state machine and server binary
  datanode/      shard storage, replication, search
  client/        routing library and CLI
  bench/         experiment runner
proto/           gRPC definitions
scripts/         cluster launch and fault scripts
experiments/     configs, schedules, raw results
analysis/        statistics and plotting
docs/            plan, research protocol, specs, bug log
```

## 10. Datasets

Vector data comes from ANN-Benchmarks. It provides standard train/test splits and the
true top-100 neighbors for each query, in HDF5 format.

| Use | Dataset | Size | Distance |
|---|---|---|---|
| Development | GloVe-25 | 1,183,514 × 25 (121 MB) | Angular |
| Evaluation | SIFT | 1,000,000 × 128 (501 MB) | Euclidean |

Brute-force search returns exact neighbors, so results must match the ground truth
exactly. This checks that routing and the top-k merge are correct. The dataset does not
affect the Raft vs. Multi-Paxos results, because vector data never passes through
consensus.

## 11. Scope

- **In scope:** everything in Phases 0–6.
- **Stretch goals:**
  - snapshots and log compaction (§7)
  - PreVote and CheckQuorum, as an additional experiment on unnecessary leader changes
  - membership changes (§6)
  - shard splitting
  - approximate search indexes such as HNSW
- **Out of scope:** existing Raft or Paxos libraries as dependencies, Byzantine faults,
  and deployment across real networks.

## 12. Risks

| Risk | Mitigation |
|---|---|
| Multi-Paxos is underspecified (Raft §3), so results depend on our variant | The spec is frozen in Phase 0, and results are reported as applying to that variant |
| The two implementations differ in quality | Shared code, shared checkers, matched settings, A/A validation |
| The simulator and localhost differ from a real network | Delay injection, results reported for both environments, limitation stated in the report |
| The data plane is the largest build item | It does not affect RQ1–RQ5, so it is built after both consensus modules pass verification |

## 13. References

1. D. Ongaro and J. Ousterhout. *In Search of an Understandable Consensus Algorithm
   (Extended Version)*. 2014. https://raft.github.io/raft.pdf
2. H. Howard and R. Mortier. *Paxos vs Raft: Have We Reached Consensus on Distributed
   Consensus?* PaPoC 2020. https://arxiv.org/abs/2004.05074
3. R. van Renesse and D. Altinbuken. *Paxos Made Moderately Complex.* ACM Computing
   Surveys, 2015.
4. L. Lamport. *Paxos Made Simple.* ACM SIGACT News, 2001.
5. D. Ongaro. *Consensus: Bridging Theory and Practice.* PhD thesis, Stanford University,
   2014.
6. Apache Kafka documentation, Replication.
   https://kafka.apache.org/documentation/#replication
7. ANN-Benchmarks. https://github.com/erikbern/ann-benchmarks
8. Stateright. https://github.com/stateright/stateright
