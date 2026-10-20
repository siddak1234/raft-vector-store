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
