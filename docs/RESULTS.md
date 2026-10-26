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
recorded as its amendment A2. (This held through Phase 4. Data collection later found a
replication bug in both algorithms; see `docs/BUGLOG.md`, 2026-10-25.)

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

## Phase 5: Experiments

Every run below was made at `data-v4` (`docs/RESEARCH.md` amendments A7–A9), one at a
time on the experiment machine, with each run's results committed before the next began:
`cargo xtask validate`, `cargo xtask experiment <ID>`, and `cargo xtask complexity`.
Every configuration reached its planned sample size, and no run had a safety violation.
`cargo xtask figures` recomputes every number in this section.

### Validation and A/A tests at `data-v4`

The validation campaign and both A/A tests were rerun because the code under test changed
(amendment A7).

| | Raft | Multi-Paxos |
|---|---|---|
| Results | `validation/20261025T175740Z-d727054` | `validation/20261025T175857Z-d83d7c3` |
| Seeds, simulated time | 1–10,000, 166.7 hours | 1–10,000, 166.7 hours |
| Faults per seed | 82.9 on average | 82.9 on average |
| Highest term per seed | 76.5 on average | 207.7 on average |
| Commits per seed | 3,311 on average | 3,439 on average |
| Seeds with any violation (M12) | **0** | **0** |

| A/A test | Results | Median latency, A vs. B | Throughput, A vs. B | Verdict |
|---|---|---|---|---|
| Simulator | `AA/20261025T180046Z-945b707` | 31.7 vs. 31.7 ms, ratio [1.000, 1.001] | 502.5 vs. 502.6 per s, [1.000, 1.000] | **PASS** |
| Real cluster | `AA-real/20261025T180114Z-3f86731` | 35.97 vs. 35.98 ms, [0.998, 1.003] | 442.2 vs. 441.7 per s, [0.996, 1.002] | **PASS** |

In both, no Holm-corrected test was significant and every E1 ratio interval lay within
[0.95, 1.05].

### E2: Figure 16 at `data-v4` (validation, Raft only)

Results: `E2/20261025T184239Z-ca4f6dd`, `E2-lan/20261025T184241Z-7f0677e` (exploratory),
and `E2-real/20261025T184242Z-7435d60`. The outcome is the same as in Phase 1: R1 passes
(150–150 ms median 60,000 ms, censored at the cap, against 632.0 ms for 150–155 ms) and R2
fails between the two shortest ranges (medians 73.3, 69.2, 91.2, 149.9, and 210.8 ms). On
the LAN-like network the medians increase with the minimum timeout (36.1, 40.6, 67.7,
126.3, 186.5 ms). On the real cluster at 150–300 ms the median is 214.1 ms, against the
simulator's 210.8 ms.

### E1: Steady state (H1)

Results: `E1/20261025T211254Z-1c04074`. 48,000 simulator and 1,600 real-cluster trials,
all ready.

**H1 is not supported** in either environment: 24 of 32 decision intervals lie within
[0.95, 1.05] in the simulator, and 5 of 16 on the real cluster.

Simulator, ratio of medians, Multi-Paxos over Raft. With 1,000 paired trials every 90%
interval lies within 0.003 of its ratio, so only the ratio is shown; an asterisk marks a
decision interval outside [0.95, 1.05].

| Servers | Network | Clients | Median latency | Throughput | p99 latency |
|---|---|---|---|---|---|
| 3 | LAN | 1 / 4 / 16 / 64 | 1.000 / 0.969 / 0.925\* / 1.032 | 1.000 / 1.045 / 1.086\* / 0.971 | 1.000 / 0.838 / 0.865 / 0.971 |
| 3 | paper | 1 / 4 / 16 / 64 | 1.000 / 0.968 / 0.926\* / 1.031 | 1.000 / 1.045 / 1.085\* / 0.972 | 0.997 / 0.843 / 0.867 / 0.972 |
| 5 | LAN | 1 / 4 / 16 / 64 | 1.000 / 0.964 / 0.905\* / 0.991 | 1.000 / 1.046 / 1.106\* / 1.009 | 1.000 / 0.861 / 0.865 / 0.961 |
| 5 | paper | 1 / 4 / 16 / 64 | 0.998 / 0.964 / 0.907\* / 0.990 | 1.003 / 1.046 / 1.105\* / 1.011 | 0.999 / 0.867 / 0.867 / 0.961 |

The p99 latency is reported, not decided. The algorithms are equivalent with 1, 4, and 64
clients. With 16 clients Multi-Paxos has a 7–10% lower median latency and an 8–11% higher
throughput, and with 4 and 16 clients a 13–16% lower p99.

Real cluster, ratio of medians (90% interval):

| Servers | Network | Clients | Median latency | Throughput |
|---|---|---|---|---|
| 3 | LAN | 1 | 1.001 [0.997, 1.007] | 1.001 [0.993, 1.006] |
| 3 | LAN | 16 | 0.943 [0.925, 0.962] | 1.058 [1.037, 1.093] |
| 3 | paper | 1 | 0.990 [0.943, 1.036] | 1.018 [0.963, 1.074] |
| 3 | paper | 16 | 0.946 [0.905, 0.974] | 1.038 [1.026, 1.115] |
| 5 | LAN | 1 | 1.004 [1.001, 1.014] | 0.999 [0.980, 1.004] |
| 5 | LAN | 16 | 1.011 [0.986, 1.025] | 0.974 [0.946, 1.008] |
| 5 | paper | 1 | 0.995 [0.949, 1.014] | 1.012 [0.986, 1.059] |
| 5 | paper | 16 | 0.886 [0.881, 0.923] | 1.128 [1.083, 1.137] |

With 1 client on the paper-like network the intervals are too wide for equivalence, but no
Holm-corrected test is significant there. With 16 clients Multi-Paxos is faster in three of
the four configurations, as in the simulator.

**Descriptive measures (amendment A8).** Raft syncs storage less often per commit: with 16
clients, 1.60 times on 3 servers and 2.23–2.25 on 5 in the simulator, against 3.01–3.05
and 5.01–5.11 for Multi-Paxos; with 64 clients, 1.21–1.22 and 1.40 against 3.00–3.01 and
5.00–5.03. The real cluster agrees (1.59–2.33 against 3.01–5.12 with 16 clients). Bytes
synced per commit are within 2% on the LAN-like network and 15–19% higher for Multi-Paxos
on the paper-like one. On the real cluster with 16 clients, Multi-Paxos's median CPU time
per commit was 4–27% higher than Raft's (ratios 1.04–1.27; the interval for 3 servers on
the paper-like network includes 1), and peak memory per server was 8.5–34 MB for both,
with no consistent direction.

### E1-load: latency against offered load (exploratory)

Results: `E1-load/20261026T060646Z-04f2dd5`. 720 trials, all ready. Medians across the 30
trials at each rate:

| Servers | Algorithm | Highest sustained rate | Median latency, 1,000–6,000 per s | At the next rate |
|---|---|---|---|---|
| 3 | Raft | 8,000 per s | 48.7–51.2 ms | 10,000 per s: throughput 7,338 per s, 27% of commands late |
| 3 | Multi-Paxos | 10,000 per s (the highest tested) | 51.0–53.8 ms | — |
| 5 | Raft | 6,000 per s | 49.9–52.5 ms | 8,000 per s: throughput 56 per s, every command late |
| 5 | Multi-Paxos | 10,000 per s (the highest tested) | 51.4–57.1 ms | — |

Raft had the lower CPU time per commit at every rate it sustained, and the lower median
latency from 2,000 per s up to its limit; at 1,000 per s Multi-Paxos's was lower by about
1 ms. Multi-Paxos kept up at every tested rate, with its median latency rising to
227–231 ms on 5 servers at 8,000–10,000 per s. Past its limit, Raft's throughput fell far
below the offered rate instead of leveling off.

### E3: Failover (H2a, H2b)

Results: `E3/20261025T203312Z-bf01783`. 16,000 simulator and 800 real-cluster trials, all
ready, none censored.

**H2a: Multi-Paxos part supported, Raft part not supported.** Election bytes (M4), median,
k = 0 against k = 1000:

| Servers | Multi-Paxos | Raft |
|---|---|---|
| 3 | 1,236 → 161,079 bytes, grows (p (Holm) 3.7 × 10⁻¹⁰⁹) | 59 → 8,054 bytes, not equivalent (ratio 136.5) |
| 5 | 2,888 → 322,678 bytes, grows (p (Holm) 5.9 × 10⁻¹³³) | 121 → 16,105 bytes, not equivalent (ratio 133.1) |

Raft's election messages grew from 8 to 1,008 with 3 servers. The added messages average 8
bytes, so they carry no entries; which messages they are is examined in Phase 6.

**H2b: time without a leader differs in the simulator, unavailability barely.** Multi-Paxos
elected a new leader sooner in every simulator configuration (M1 ratios 0.89–0.98, every
p (Holm) below 10⁻⁶). Unavailability (M2) differed by less than 0.6%: significantly on
3 servers (ratios 0.995–0.997), not on 5. On the real cluster no difference in M1 or M2 was
significant (p (Holm) ≥ 0.074). Both algorithms needed a median of one election round.

**Re-sent entries (M10).** Raft's new leader sent a median of 0–4 entries at every k.
Multi-Paxos's sent 39 and 939 with 3 servers, and 117 and 2,880 with 5, at k = 100 and
k = 1000, but a median of 0 at k = 10, which is examined in Phase 6.

### E4: Recovery and catch-up (H3)

Results: `E4/20261025T184350Z-08c7b52`. 24,000 trials, all ready, none censored.

**H3 is supported in every configuration.** Re-sent entries (M10) by the third leader in
E4a, one-sided Wilcoxon, Holm-corrected:

| Servers | k | Raft | Multi-Paxos | Ratio, 90% CI | p (Holm) |
|---|---|---|---|---|---|
| 3 | 10 | 3 | 66 | 22.0 [21.3, 29.3] | 3.7 × 10⁻¹⁶⁵ |
| 3 | 100 | 3 | 189 | 63.0 [60.2, 65.5] | 4.8 × 10⁻¹⁶⁵ |
| 5 | 10 | 4 | 64 | 16.0 [12.8, 16.0] | 8.3 × 10⁻¹⁴⁰ |
| 5 | 100 | 4 | 254 | 63.5 [50.2, 77.0] | 8.3 × 10⁻¹⁶⁰ |

Catch-up time (M11), two-sided, median in ms:

| Procedure | Servers | n = 100, Raft / Multi-Paxos | n = 1,000, Raft / Multi-Paxos |
|---|---|---|---|
| E4b, restart | 3 | 70.1 / 62.7 | 86.9 / 272.1 |
| E4b, restart | 5 | 68.2 / 63.1 | 84.3 / 271.9 |
| E4c, partition | 3 | 70.1 / 62.8 | 96.2 / 271.5 |
| E4c, partition | 5 | 68.1 / 63.0 | 93.6 / 272.1 |

Every difference is significant (p (Holm) below 10⁻⁶⁴). With 1,000 missing entries Raft
caught up 2.8–3.2 times faster; with 100, Multi-Paxos was 7–11% faster.

### E5: Partitions (H2b)

Results: `E5/20261025T194640Z-12cd11e`. 18,000 simulator and 400 real-cluster trials, all
ready.

Unavailability (M2) in the simulator differed by at most 3.5% (ratios 0.965–0.999). The
P3 and P5 values are identical for each algorithm and size. Leader changes (M6) were a
median of 1 for both algorithms in every scenario except P5, the flapping link, where
Multi-Paxos changed leaders a median of 15 times against Raft's 2 (3 servers) and 1
(5 servers), p (Holm) below 10⁻¹⁶³. On the real cluster, P1 and P3 with 5 servers showed no
significant difference in M2 or M6 (p (Holm) ≥ 0.17).

### E6: Coherency (H4)

Results: `E6/20261025T184407Z-6213e9b`. Safety held in every state of every search.

| Rung (commands, elections, heartbeats) | Raft | Multi-Paxos |
|---|---|---|
| (0, 1, 0) | complete, 37,125,265 states | complete, 35,753,384 states |
| (0, 1, 1) | 140,560,578 states when the budget of 1.2 billion generated states ran out | 126,650,931 states, budget reached |
| (1, 1, 0) | 185,188,016 states, budget reached | 157,968,830 states, budget reached |
| Five larger rungs | skipped (amendment A5.4) | skipped |

The skipped searches are recorded with 0 states; they found no violation because they did
not run.

**H4 is partly supported.**

- **M14:** at the only rung complete for both, Multi-Paxos reached 3.7% fewer states than
  Raft (0.963×). Not as predicted.
- **M15:** across the 29,000 E3–E5 simulator trials of each algorithm, Raft's logs never
  had a gap and Multi-Paxos's had gaps in every trial (mean maximum 225.7). As predicted.
- **M16:** entries were delivered to a candidate or leader in 10,418 Raft trials (mean 4.8)
  and 21,048 Multi-Paxos trials (mean 223.9). The prediction was zero for Raft.
- **M17:** two-sided, with no single direction. Raft discarded more entries in E4a (a
  median of k against 0), in P2 and P4, and in P1 with 3 servers; Multi-Paxos discarded
  more in P5 (26–27 against 2–3).

### E7: Implementation (H5)

Results: `E7/20261025T194639Z-3387830`, at `data-v4`.

| | Raft | Multi-Paxos |
|---|---|---|
| Message types (M18) | 4 | 5 |
| Persistent state fields (M18) | 3 (currentTerm, votedFor, log) | 2 (currentTerm, log) |
| SLOC / LLOC (M19) | 860 / 210 | 1,008 / 237 |
| Cyclomatic / cognitive complexity (M19) | 123 / 62 | 139 / 68 |
| Bugs logged (M20) | 2, both replication | 1, replication |

Raft's totals are lower on every measure, and Raft has fewer SLOC in each of the four
modules. The largest gap is in election (SLOC 128 against 183, cyclomatic 17 against 27).
In replication the cyclomatic complexity is equal (39), and Raft's cognitive complexity is
one higher there (33 against 32) and in `lib.rs` (10 against 9). `docs/BUGLOG.md` also
holds 10 shared bugs.

### E8: Vector store recovery (H6)

Results: `E8/20261026T091130Z-7939154`. 800 trials, all ready; no acknowledged write was
lost and no two data nodes served a shard as primary at once, in any trial.

| Scenario | Servers | Raft | Multi-Paxos | Ratio, 90% CI | Verdict |
|---|---|---|---|---|---|
| S1, data node | 3 | 3,978 ms | 4,001 ms | 1.006 [0.995, 1.131] | not equivalent |
| S1, data node | 5 | 3,518 ms | 3,994 ms | 1.135 [0.998, 1.142] | not equivalent |
| S2, with the control-plane leader | 3 | 4,558 ms | 4,546 ms | p (Holm) 0.36 | no difference detected |
| S2, with the control-plane leader | 5 | 4,558 ms | 4,551 ms | p (Holm) 1 | no difference detected |

**H6 is not supported:** S1's intervals are too wide for equivalence, and S2 shows no
difference. The shortest recovery took 3,385 ms, above the minimum of 2.5 s that
`docs/DESIGN.md` §7 predicts.

### Superseded and stopped runs

- **E4 at `data-v1`** (`E4/20261025T055422Z-d6e3425`) is superseded by the run above
  (amendment A7). The bug it exposed made Raft catch up slowly: with 1,000 missing entries
  its medians were 215.9–314.5 ms, against 84.3–96.2 ms at `data-v4`; Multi-Paxos's were
  271.5–272.1 ms in both. With 100 missing entries Raft's medians rose from 63.1–63.8 ms
  to 68.1–70.1 ms. In a scratch check of one seed the messages exchanged were identical, and
  only the wait for the leader's next heartbeat changed (`docs/REPORT.md` §5). M10 in E4a
  changed only for 3 servers with k = 100 (Multi-Paxos median 195.5, now 189).
- **E6 at `data-v1`** was stopped during its second rung and was not committed (amendment
  A7).
- **Earlier validation, A/A, and E2 runs** remain in `experiments/results/` and are reported
  in the sections above; the `data-v4` runs are the ones the analysis uses.
