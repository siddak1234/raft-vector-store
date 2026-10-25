# Results

Results so far, by phase, with the commands that reproduce them. Every number below comes
from a committed results folder under `experiments/results/`; `cargo xtask figures`
recomputes it. The research protocol (`docs/RESEARCH.md`) defines every metric and
criterion used here.

## Phase 1: Simulator and Raft

### Validation campaign (`docs/RESEARCH.md` §9.1)

`cargo xtask validate`, results in `experiments/results/validation/20261020T173107Z-a1944ad`.

| | |
|---|---|
| Seeds | 1–10,000 (odd seeds: 3 servers, even seeds: 5) |
| Simulated time | 166.7 hours (60 s per seed) |
| Faults per seed | 82.9 on average |
| Elections per seed | 76.4 on average (highest term reached) |
| Commits per seed | 3,311 on average |
| Seeds with any violation (M12) | **0** |

Every seed passed election safety, state machine safety, apply order, unique entries per
index and term, log matching, leader completeness, liveness (a new command commits within
10 s after faults stop), and durability (every acknowledged command applied by every
server).

**Checker sensitivity.** To confirm the checks catch real bugs, Raft was broken on purpose
in two ways and the campaign was rerun. These scratch runs were not committed.

| Deliberate bug | Seeds with violations | Caught by |
|---|---|---|
| Vote for any candidate, ignoring the election restriction (§5.4.1) | 1,982 of 2,000 | Leader completeness |
| Commit entries from earlier terms by counting replicas (§5.4.2) | 0 of 10,000 | Not caught by the campaign; caught by the unit test `a_leader_does_not_commit_an_old_entry_by_counting_replicas` |

The second bug needs a precise history: an old entry reaches a majority under one leader,
and a leader of a later term overwrites it. Raft's no-op makes that history rare, because
a new leader sends old entries and its no-op together. Exhaustive model checking (Phase 2)
is the tool for such cases.

### A/A test (`docs/RESEARCH.md` §9.2)

`cargo xtask experiment AA`, results in `experiments/results/AA/20261020T173210Z-c3d33af`.
Raft against itself on seeds 1–1000 (arm A) and 1001–2000 (arm B), 3 servers, paper-like
network.

| E1 metric (16 clients) | Arm A | Arm B | Ratio B/A, 90% CI |
|---|---|---|---|
| Median commit latency (M7) | 31.70 ms | 31.70 ms | [1.000, 1.001] |
| p99 commit latency (M7) | 43.52 ms | 43.51 ms | [0.999, 1.001] |
| Throughput (M8) | 502.5 /s | 502.6 /s | [1.000, 1.000] |
| Messages per commit (M9) | 4.356 | 4.361 | [1.000, 1.002] |
| Bytes per commit (M9) | 1,025 | 1,027 | [1.000, 1.003] |

| E3 metric (k = 10) | Arm A | Arm B | Holm p |
|---|---|---|---|
| Time without a leader (M1) | 241.2 ms | 240.8 ms | 1.00 |
| Unavailability (M2) | 545.4 ms | 543.6 ms | 0.09 |
| Election messages (M4) | 18 | 18 | 1.00 |

**Verdict: pass.** Every E1 interval lies within [0.95, 1.05], and no Holm-corrected test
on E1 or E3 is significant.

Unavailability (M2) includes the clients' 500 ms reply timeout: a client waiting on the
crashed leader times out before it retries elsewhere. This applies equally to both
algorithms.

### E2: Raft paper Figure 16 (`docs/RESEARCH.md` §6)

`cargo xtask experiment E2`, results in `experiments/results/E2/20261020T173556Z-61c89dd`.
Five servers, paper-like network (mean round trip 15 ms), 1,000 trials per timeout range.
Figure: `analysis/figures/E2_figure16.png`.

| Timeout range | Median time without a leader | Paper |
|---|---|---|
| 150–150 ms | 60 s (capped; 550 of 998 trials censored) | longer than 10 s |
| 150–151 ms | 3,930 ms | |
| 150–155 ms | 632 ms | median 287 ms |
| 150–175 ms | 187 ms | |
| 150–200 ms | 188 ms (maximum 746 ms) | maximum 513 ms |
| 150–300 ms | 211 ms | |
| 12–24 ms | 73.3 ms | mean 35 ms |
| 25–50 ms | 69.2 ms | |
| 50–100 ms | 91.2 ms | |
| 100–200 ms | 149.9 ms | |

- **R1 passes.** With no randomness, the median is 95 times the 150–155 ms median.
- **R2 fails** between the two shortest ranges. At 12–24 ms, the 15 ms round trip is
  longer than the shortest timeouts, which breaks Raft's timing requirement (§5.6).
  Elections split: only 7.4% finish in the first round, against 33% at 25–50 ms.
- **Exploratory check.** On the LAN-like network (`E2-lan`), where the requirement holds,
  the medians increase as expected: 36.1, 40.6, 67.7, 126.3, 186.5 ms.

Amendment A2 in `docs/RESEARCH.md` records the decision: the simulator is valid, R2 stays
reported as failed, and the comparison experiments use 150–300 ms, where the requirement
holds.

### Bugs found (metric M20)

Three bugs, recorded in `docs/BUGLOG.md`:

- a message storm in Raft replication
- a durability check run too early in tests
- trials without a leader at the crash instant being marked ready

## Phase 2: Multi-Paxos and State-Space Analysis

### Validation campaign for Multi-Paxos (`docs/RESEARCH.md` §9.1)

`cargo xtask validate --algorithm paxos`, results in
`experiments/results/validation/20261025T003825Z-7fc69f0`. The same campaign as Raft's,
seed for seed: the fault schedules are generated from each seed in their own random
stream, so both algorithms faced the same faults.

| | Raft (Phase 1) | Multi-Paxos |
|---|---|---|
| Seeds | 1–10,000 | 1–10,000 |
| Simulated time | 166.7 hours | 166.7 hours |
| Faults per seed | 82.9 on average | 82.9 on average |
| Highest term per seed | 76.4 on average | 207.3 on average |
| Commits per seed | 3,311 on average | 3,436 on average |
| Seeds with any violation (M12) | **0** | **0** |

Multi-Paxos terms advance faster because each server may campaign only in the terms it
owns (`t mod N`), so a server skips the terms of the others.

Every Multi-Paxos seed passed election safety, state machine safety, apply order, unique
entries per index and term, liveness, and durability. Log matching and leader completeness
are Raft properties and do not apply to Multi-Paxos, whose logs may have gaps.

**Checker sensitivity.** As for Raft in Phase 1, Multi-Paxos was broken on purpose and the
campaign rerun on seeds 1–10,000. These scratch runs were not committed.

| Deliberate bug | Seeds with violations | Caught by |
|---|---|---|
| A new leader keeps the lowest-term entry from the promises instead of the highest | 2,438 of 10,000 | State machine safety |
| A follower accepts an `Accept` from an earlier term | 2,591 of 10,000 | State machine safety (2,585), durability (6) |
| A follower commits held entries from any term, not only the leader's | 6,073 of 10,000 | State machine safety |

No bug was found in the Multi-Paxos implementation: it passed every check on its first
run, so `docs/BUGLOG.md` has no Multi-Paxos entry. Details the specification left open are
recorded as its amendment A2.

### Model checking

`crates/modelcheck` runs either algorithm under Stateright: 3 servers that never crash, an
unordered network that never duplicates a message, timers that fire in any order, and
bounds on client commands, elections per server, and heartbeat timer firings
(`docs/RESEARCH.md` amendment A5). Two properties are checked in every state: election
safety (at most one leader per term) and state machine safety (every two servers hold the
same payload at every index both have committed). A third, that some state commits a client
command, shows a search with commands is not vacuous; the adapter's test in CI
(`crates/modelcheck/tests/small.rs`, 2 servers, 1 command) finds such a state for both
algorithms.

The bounds of §6 (2 commands, 2 elections per server) are far beyond the experiment
machine: while sizing the search, the smallest variant already had about 37 million states
per algorithm, and breadth-first search ran out of its 16 GB. Amendment A5 replaces them
with a fixed ladder of bounds searched depth-first; the reported E6 runs at the
data-collection tag.

The Phase 2 check of the adapter, `cargo xtask experiment E6-check`, results in
`experiments/results/E6-check/20261025T030655Z-a352325`, ran the smallest rung of the
ladder: no client commands, one election per server, no heartbeat timer firings.

| | Raft | Multi-Paxos |
|---|---|---|
| Complete | yes | yes |
| Distinct compared states (M14) | 37,125,265 | 35,753,384 |
| Distinct search states | 37,389,340 | 36,494,539 |
| States generated | 219,455,251 | 226,623,590 |
| Deepest path | 32 steps | 28 steps |
| Time (8 threads) | 127 s | 170 s |
| Election safety, state machine safety | held in every state | held in every state |

At this rung Raft reaches about 4% more compared states than Multi-Paxos. This is a check of
the adapter, not E6: the reported E6 runs at the data-collection tag across the ladder
(amendment A5.5). Raft's compared state includes the vote, which Multi-Paxos does not
persist.

**The commit-rule bug under model checking.** Phase 1's random-fault campaign did not catch
a Raft that commits entries from earlier terms by counting replicas (§5.4.2). That broken
Raft was model-checked at the rung with one client command, one election per server, and no
heartbeat timer firings, stopping at the first safety violation; this scratch run was not
committed. With 64 entries per message, the search reached its cap of 1.2 billion generated
states (193,916,972 distinct) in 1,596 s without a violation; with one entry per message,
which allows partial replication, it reached the cap (181,730,717 distinct) in 1,092 s
without one. The searches did not complete, so they do not show the bug is unreachable
within these bounds; they show that model checking within what this machine can search does
not catch it either. The unit test
`a_leader_does_not_commit_an_old_entry_by_counting_replicas` remains the check that does.

## Phase 3: Control Plane on a Real Cluster

### Writes resume after `kill -9` of the leader

`cargo test -p controlplane --test cluster` starts 3 server processes, commits commands,
kills the leader with `SIGKILL`, and checks that writes resume, that a server leads a
higher term, and that the killed server, restarted with its data, catches up to the last
committed index. It passes for both algorithms and runs in CI on every push.

### E2 on the real cluster (`docs/RESEARCH.md` amendment A1.6)

`cargo xtask experiment E2-real`, results in
`experiments/results/E2-real/20261025T051053Z-4033616`. Five server processes, the
paper-like delay injected in the transport, 150–300 ms election timeouts, 100 trials of the
Figure 16 procedure (amendment A4.8), with the harness of amendment A6.

| | Real cluster | Simulator (Phase 1) |
|---|---|---|
| Trials (ready, censored) | 100 (100, 0) | 1,000 |
| Median time without a leader | 208.9 ms | 210.8 ms |
| Mean | 237.5 ms | 228.8 ms |
| p99 | 450.2 ms | 461.3 ms |
| Maximum | 492.5 ms | 728.7 ms |
| Election rounds (median) | 1 | 1 |

Figure: `analysis/figures/E2_real.png`. The real cluster's distribution matches the
simulator's at the median. The first run, with `F_FULLFSYNC`
(`experiments/results/E2-real/20261025T015147Z-a4fea84`), had a median of 242.6 ms; amendment
A6 explains why it was rerun.

### The A/A test on the real cluster (`docs/RESEARCH.md` §9.2, amendments A1.9 and A6)

`cargo xtask experiment AA-real`. Raft against itself, 3 servers, paper-like delay, 16
clients, seeds 1–100 (arm A) against 101–200 (arm B), interleaved.

**First run, failed** (`experiments/results/AA-real/20261025T015302Z-b0bd0c9`, storage with
`F_FULLFSYNC`):

| E1 metric | Arm A | Arm B | Ratio B/A, 90% CI | Within [0.95, 1.05] | Holm p |
|---|---|---|---|---|---|
| Median commit latency (M7) | 173.3 ms | 173.0 ms | [0.989, 1.007] | yes | 1.00 |
| p99 commit latency (M7) | 264.2 ms | 254.6 ms | [0.942, 0.991] | **no** | 0.40 |
| Throughput (M8) | 90.25 /s | 90.85 /s | [0.997, 1.014] | yes | 1.00 |
| Messages per commit (M9) | 4.897 | 4.893 | [0.995, 1.003] | yes | 1.00 |
| Bytes per commit (M9) | 654.2 | 654.5 | [0.990, 1.014] | yes | 1.00 |

The p99 of a trial was about its 9th-slowest of some 900 commits, because every commit
waited for a 4 ms `F_FULLFSYNC` on all three servers, whose flushes queued on the machine's
one SSD. It varied widely between trials (standard deviation 42–70 ms) and did not drift
with the run order. Amendment A6 records the cause and the fix: storage calls `fsync(2)`.

**Rerun with the fixed harness, passed**
(`experiments/results/AA-real/20261025T051159Z-c538caf`):

| E1 metric | Arm A | Arm B | Ratio B/A, 90% CI | Within [0.95, 1.05] | Holm p |
|---|---|---|---|---|---|
| Median commit latency (M7) | 34.20 ms | 34.12 ms | [0.995, 1.001] | yes | 1.00 |
| p99 commit latency (M7) | 47.90 ms | 48.11 ms | [0.996, 1.015] | yes | 1.00 |
| Throughput (M8) | 465 /s | 466 /s | [0.999, 1.004] | yes | 1.00 |
| Messages per commit (M9) | 4.326 | 4.332 | [0.999, 1.004] | yes | 1.00 |
| Bytes per commit (M9) | 977.7 | 979.3 | [0.996, 1.006] | yes | 1.00 |

**Verdict: pass.** Every E1 interval lies within [0.95, 1.05], and no Holm-corrected test
is significant. The real cluster now behaves much like the simulator, whose A/A run had a
31.7 ms median latency and 502 commits per second.

## Phase 4: Vector Store Data Plane

`cargo xtask dataplane`, results in `experiments/results/dataplane/20261025T023443Z-462b1fb`.
Every check ran on real processes, 3 control-plane servers and 4 data nodes, once per
algorithm. The collection has 4 shards with 3 replicas each.

### Search against ground truth

All training vectors were loaded and the first test queries searched, top 10, by brute
force; results were compared with the dataset's true neighbors.

| Dataset | Vectors | Queries | Raft | Multi-Paxos |
|---|---|---|---|---|
| GloVe-25, angular | 1,183,514 | 1,000 | 1,000 exact | 1,000 exact |
| SIFT, Euclidean | 1,000,000 | 200 | 189 exact, 11 ties | 189 exact, 11 ties |

A *tie* returns the same 10 distances as the ground truth with neighbors at equal distance
in another order. No query returned a wrong distance.

### Faults under load

Each test loaded 20,000 vectors, ran 4 clients that upserted new vectors throughout, and
injected one fault 3 s in. Recovery is the time from the fault until shard 0 acknowledged
a write at a newer epoch, which only a new primary can do.

| Test | Raft | Multi-Paxos |
|---|---|---|
| Kill the primary of shard 0 (`SIGKILL`) | recovered in 3.65 s | recovered in 3.66 s |
| Cut the control-plane leader off with a minority for 10 s | writes never stopped (31,156 to shard 0 during the partition) | writes never stopped (30,262) |
| Cut the primary of shard 0 off from the control plane for 12 s | recovered in 3.41 s | recovered in 3.38 s |

After every fault, re-replication restored 3 in-sync copies of every shard. In every test,
under both algorithms:

- **No acknowledged write was lost (C2):** every one of the 153,044–245,202 acknowledged
  writes was on every in-sync replica.
- **At most one primary served each shard (C1)** over 138,878–245,202 served requests.

Recovery after a primary fails is bounded below by the lease: the control plane may not
reassign a shard until `L + δ = 3.5 s` after the last renewal, and the last renewal came up
to `h = 1 s` before the fault, so `docs/DESIGN.md` §7 predicts at least `L + δ − h = 2.5 s`.
