---
marp: true
title: Raft vs. Multi-Paxos on a Vector Store Control Plane
paginate: true
---

<!-- Status: the results slides (marked "Pending") are filled in from the Phase 5 runs at
data-v3 (docs/RESEARCH.md amendments A7 and A8). Every other slide is complete. -->

# Raft vs. Multi-Paxos on a Vector Store Control Plane

ECE 382N Distributed Systems, Fall 2026
Siddak Bhatia · Harrison Held

---

## The question

The Raft paper claims Raft is

1. **as efficient as Paxos**,
2. **decomposed**: leader election, log replication, and safety are separate, and
3. **more coherent**: fewer states to consider.

We test these claims by building the same system twice, once per algorithm, with the
algorithm as the only difference.

---

## Six research questions

| | Question | Hypothesis |
|---|---|---|
| RQ1 | Latency and throughput, stable leader | Equivalent within ±5% |
| RQ2 | Leader failure | Paxos election traffic grows with uncommitted entries |
| RQ3 | Entries re-sent by a new leader | Raft re-sends fewer |
| RQ4 | Coherency | Raft: fewer states, no gaps, nothing sent toward a leader |
| RQ5 | Implementation | Raft: fewer message types, lower complexity |
| RQ6 | Vector store recovery | Differs only if the control-plane leader also fails |

---

## The system

```
clients ──(shard map, admin)──▶ control plane: 3 or 5 servers
   │                             consensus: Raft | Multi-Paxos
   │ writes, searches                ▲
   ▼                                 │ heartbeats, leases, replica changes
data nodes: shard = primary + backups
```

- Only **metadata** goes through consensus: collections, data nodes, shard ownership.
- Vectors go straight to data nodes; each shard is replicated primary-to-backup with an
  in-sync set and epochs, as in Kafka.
- Leases (`L` = 3 s) guarantee **at most one serving primary**; the in-sync set
  guarantees **no lost acknowledged write**.

---

## One codebase, two algorithms

- One `Consensus` trait: no I/O, no clock, seeded randomness.
- Three drivers run the **same algorithm code**: a deterministic simulator, a model
  checker (Stateright), and the real server (gRPC, fsync).
- Only `crates/raft` and `crates/paxos` differ; messages, bytes, fsyncs, and timings are
  counted in shared code.
- `cargo xtask check-repo` enforces the separation.

---

## Our Multi-Paxos

Following Howard & Mortier: Paxos in Raft's terms, differing in **leader election**.

- Each term belongs to one server (`t mod N`).
- A candidate sends `Prepare` with its commit index; voters return every entry above it.
- The new leader keeps the highest-term entry per index, fills gaps with no-ops, and
  re-proposes everything in its own term.
- Followers accept each index on its own: **logs can have gaps**.

Same timers, batching, no-op on election, and leadership confirmation as Raft.
Frozen as a one-page spec before coding (`docs/PAXOS_SPEC.md`).

---

## Method: pre-registered

- Hypotheses, metrics, procedures, sample sizes, and tests fixed **before data**;
  changes only as dated amendments.
- Same `Config`, same seeds, same fault schedules for both algorithms.
- Same correctness bar: both must pass 10,000 random-fault seeds with zero violations
  before any measurement.
- A/A test (Raft vs. Raft) before any comparison, in the simulator and on the real cluster.
- Wilcoxon (paired), Mann–Whitney (unpaired), ±5% equivalence, Holm correction.
- 1,000 paired simulator trials and 100 real-cluster trials per configuration.

---

## Experiments

| | What | Where |
|---|---|---|
| E1 | Steady state: latency, throughput, cost per commit | sim + real |
| E1-load | Latency against offered load (exploratory) | real |
| E2 | Raft's Figure 16 (validation) | sim + real |
| E3 | Leader crash with `k` uncommitted entries | sim + real |
| E4 | Consecutive crashes; follower catch-up | sim |
| E5 | Partitions P1–P5 | sim + real |
| E6 | Model checking: reachable states | Stateright |
| E7 | Message types, code size, complexity, bugs | static analysis |
| E8 | Shard recovery after a data-node crash | real |

---

## Found during data collection

- An exploratory look at one E4 result (catch-up after a partition) didn't fit.
- Cause: after a reply, **each leader sent at most one message** of 64 entries, so a
  backlog moved one batch per round trip, not up to the 1,024-entry window.
- Raft needed about **16 round trips** to catch a follower up on 1,000 entries.
- Every checker passed: the bug cost only time.
- Protocol rule: fix, tag `data-v2`, **rerun everything** for both algorithms. The
  superseded run is kept and reported.

---

## Results: validation

*Pending: checkers, A/A tests, and E2 at `data-v3`.*

---

## RQ1: Steady state (E1)

*Pending.*

---

## RQ2: Leader failure and partitions (E3, E5)

*Pending.*

---

## RQ3: Re-sent entries and catch-up (E3, E4)

*Pending.*

---

## RQ4: Coherency (E6)

*Pending.*

---

## RQ5: Implementation (E7)

*Pending.*

---

## RQ6: Vector store recovery (E8)

*Pending.*

---

## Demo: live failover

`cargo xtask demo --consensus raft` (or `paxos`):

1. 3 control-plane servers, 4 data nodes, 20,000 GloVe-25 vectors, 4 writers.
2. `kill -9` the control-plane leader: a new leader is elected and writes continue.
3. `kill -9` the primary of shard 0: after its lease expires, a backup is promoted at a
   new epoch, and re-replication restores 3 in-sync copies.
4. A search, then the checks: no acknowledged write lost, never two serving primaries.

---

## Threats to validity

- **Our** Multi-Paxos variant, built close to Raft; optimizations off in both.
- Same two authors; Raft written first.
- Simulator and localhost, not real networks; one machine, one SSD; `fsync(2)`.
- Code complexity stands in for understandability.
- Exploratory analyses are labeled and never decide a hypothesis.

---

## Conclusion

*Pending.*

---

# Questions

Code, protocol, raw results, and analysis:
`docs/REPORT.md` · `docs/RESEARCH.md` · `experiments/results/`
