# Research Protocol

**Status:** draft for review. Frozen with the tag `protocol-v1` before any data is
collected. After that, changes are made only in the Amendments section, with the date
and the reason.

## 1. Purpose

This protocol fixes, before any data exists, how we test the research questions in
`docs/PLAN.md` §2: the systems, hypotheses, metrics, experimental procedures, sample sizes,
and analysis. Where this document is more specific than the plan, this document governs.

## 2. Systems Under Test

| System | Specification |
|---|---|
| Raft | Figure 2 of the Raft paper [1], plus §8: a no-op entry on election and leader-confirmed reads |
| Multi-Paxos | `docs/PAXOS_SPEC.md` |
| Reference | Each algorithm running on a single server |

Both algorithms implement `consensus::Consensus` and run under the same driver code, with
the same storage, transport, state machine, and workload.

## 3. Fixed Settings

These apply to every experiment unless the experiment varies them.

| Setting | Value |
|---|---|
| Election timeout | 150–300 ms, drawn uniformly per timeout |
| Heartbeat interval | Half the minimum election timeout (75 ms by default), as in the paper's Figure 16 setup |
| Entries per message | At most 64 |
| Unacknowledged entries per follower | At most 1024 |
| Persistence | The driver calls `Storage::sync` after every input, before any output |
| Client command size | 128 bytes |
| Leadership confirmation | The leader sends heartbeats immediately and confirms after a majority replies |
| Disabled optimizations | PreVote, CheckQuorum, read leases, flexible quorums, Raft's faster log backtracking (§5.3) |
| Message encoding | One serde binary format, shared by both algorithms |

**Network model.** One-way message delay is uniform in `[0.5d, 1.5d]`. Messages are not
lost unless an experiment injects faults.

| Environment | `d` | Mean round trip | Purpose |
|---|---|---|---|
| Paper-like | 7.5 ms | 15 ms | Matches the broadcast time of about 15 ms in the paper's Figure 16 |
| LAN-like | 0.5 ms | 1 ms | A fast local network |

**Environments.**

- **Simulator:** deterministic, with the delay model above. Storage writes take no
  simulated time.
- **Real cluster:**
  - Localhost on one machine, one process per server.
  - The same delay distribution is injected in the transport, and storage uses real fsync.
  - Same release build for every run, with the machine prevented from sleeping.
  - Raft and Multi-Paxos trials are interleaved in random order.

## 4. Hypotheses

| ID | Research question | Hypothesis | Decision rule |
|---|---|---|---|
| H1 | RQ1 | With a stable leader, Multi-Paxos's median commit latency and throughput are within ±5% of Raft's | Equivalence (§8) in every E1 configuration with 3 or 5 servers |
| H2a | RQ2 | Multi-Paxos election traffic grows with the number of uncommitted entries; Raft's does not | Multi-Paxos: M4 bytes at k = 1000 exceed k = 0 (one-sided test). Raft: M4 bytes at k = 1000 equivalent to k = 0 |
| H2b | RQ2 | Time without a leader and unavailability differ between the algorithms (no direction predicted) | Two-sided tests on M1 and M2 in E3 |
| H3 | RQ3 | After a leader change, Raft re-sends fewer entries than Multi-Paxos | One-sided test on M10 in E4 |
| H4 | RQ4 | Raft reaches fewer states under the same model-checking bounds. Raft logs never have gaps and never send entries toward a leader; Multi-Paxos logs do both | M14 compared directly. M15 and M16 are zero for Raft and above zero for Multi-Paxos. M17 tested two-sided |
| H5 | RQ5 | Raft has fewer message types and lower code complexity | Descriptive (E7) |
| H6 | RQ6 | Shard recovery time is equivalent between the algorithms when only a data node fails, and differs when the control-plane leader fails at the same time | Equivalence for scenario S1, two-sided test for S2 (E8) |

**Validation.** Experiment E2 must reproduce the shape of the paper's Figure 16 before the
comparison experiments are run (§9).

## 5. Metrics

Each metric is measured in shared code (driver, transport, storage wrapper, or client)
unless noted. "Crash" means the instant the simulator or runner stops the process.

| ID | Metric | Definition | Unit |
|---|---|---|---|
| M1 | Time without a leader | From the crash until a server becomes leader in a term higher than the crashed leader's | ms |
| M2 | Unavailability | From the crash or fault until a client receives the commit reply for a command proposed after it | ms |
| M3 | Election rounds | Number of distinct terms in which a server became candidate, from the crash until the new leader is elected | count |
| M4 | Election traffic | Messages and bytes sent by all servers from the crash until the new leader is elected | count, bytes |
| M5 | Stale-log rejections | Votes refused because the candidate's log is behind. Raft only; counted by Raft-specific checker code | count |
| M6 | Unnecessary leader changes | Leader changes while the leader was running and connected to a majority | count |
| M7 | Commit latency | From a client sending a command until it receives the commit reply. Per trial: median and p99 | ms |
| M8 | Throughput | Commit replies per second during a trial's measurement window | commits/s |
| M9 | Cost per commit | Messages and bytes sent by all servers during the measurement window, divided by commits | count, bytes |
| M10 | Re-sent entries | Log entries carried (`Consensus::entry_count`) in messages from a new leader, from its election until its no-op is committed | entries |
| M11 | Catch-up time | From a follower's restart or reconnection until its commit index reaches the leader's commit index at that instant | ms |
| M12 | Safety violations | Violations found by the checkers (§9). Must be zero | count |
| M13 | Evidence | Seeds and simulated hours checked | count, hours |
| M14 | Reachable states | Unique states found by Stateright under the bounds in E6 | count |
| M15 | Log gaps | Indexes below a server's last index that hold no entry, checked after every input. Per trial: maximum | count |
| M16 | Entries sent toward a leader | Entries carried in messages delivered to a server whose role is candidate or leader | entries |
| M17 | Discarded entries | Entries a server overwrites with a different payload or truncates | entries |
| M18 | Protocol size | Message types (`Consensus::message_kind`) and persistent state fields | count |
| M19 | Code size and complexity | SLOC, LLOC, cyclomatic and cognitive complexity per module, from `rust-code-analysis-cli` | count |
| M20 | Bugs | Entries in `docs/BUGLOG.md`, by algorithm and subproblem | count |
| M21 | Shard recovery time | From killing a shard's primary until a client receives the first acknowledged write to that shard | ms |

**Module layout for M19.** Both `crates/raft` and `crates/paxos` are organized into the same
modules, so complexity can be compared per subproblem:

- `election.rs`
- `replication.rs`
- `commit.rs`
- `lib.rs`, which holds the shared state and the trait implementation

## 6. Experiments

Seeds `1..=1000` are used in the simulator for every configuration. Both algorithms run the
same seeds, so trials are paired. The real cluster uses seeds `1..=100`.

### E1: Steady state (H1)

- **Configurations:** algorithm × servers {1, 3, 5} × environment {LAN-like, paper-like}
  × clients {1, 4, 16, 64}. That is 48 configurations.
- **Procedure:**
  1. Start the cluster and wait until a leader's no-op is committed.
  2. Run 2 s of warm-up, which is discarded.
  3. Measure for 10 s.
  4. Each client is closed-loop: it sends one command, waits for the commit reply, then
     sends the next.
- **Trials:**
  - Simulator: 1000 per configuration.
  - Real cluster: 100 per configuration, for servers {3, 5} × both environments × clients
    {1, 16}.
- **Metrics:** M7, M8, M9.

### E2: Figure 16 reproduction (validation, Raft only)

- **Configurations:** 5 servers, paper-like environment, heartbeat at half the minimum
  timeout. Election timeout ranges (ms):
  - 150–150, 150–151, 150–155, 150–175, 150–200, 150–300
  - 12–24, 25–50, 50–100, 100–200
- **Procedure** (paper §9.3):
  1. With a stable leader, the leader appends one command and delivers it to a random
     subset of followers, so that log lengths differ.
  2. At a uniformly random time within one heartbeat interval, the leader sends heartbeats
     to all followers and crashes immediately afterward.
  3. Each trial is capped at 60 s of simulated time. Capped trials are reported as
     censored.
- **Trials:**
  - Simulator: 1000 per range.
  - Real cluster: 100 for 150–300 ms.
- **Metric:** M1.
- **Pass criteria:**
  - **R1:** the median for 150–150 ms is at least 10 times the median for 150–155 ms.
  - **R2:** for 12–24, 25–50, 50–100, 100–200, and 150–300 ms, the median increases with
    the minimum timeout.
- **Reported against the paper's values:**
  - 150–155 ms: median 287 ms
  - 150–200 ms: maximum 513 ms
  - 12–24 ms: mean 35 ms, maximum 152 ms

### E3: Failover (H2a, H2b; also M10 for H3)

- **Configurations:** algorithm × servers {3, 5} × uncommitted entries
  `k ∈ {0, 10, 100, 1000}`. Paper-like environment, 150–300 ms timeouts. That is 16
  configurations.
- **Procedure:**
  1. Run 4 clients in steady state.
  2. At a uniformly random time, a separate client proposes `k` commands.
  3. The leader's messages carrying them reach a random subset of followers, whose size is
     uniform in `0..=N−1`.
  4. The leader crashes before sending anything further, so no follower learns that the
     entries were committed.

  The same seed produces the same choices for both algorithms.
- **Trials:**
  - Simulator: 1000 per configuration.
  - Real cluster: 100 per configuration, for `k ∈ {0, 100}`.
- **Metrics:** M1–M5, M10, M15–M17.

### E4: Log recovery and catch-up (H3)

- **E4a, consecutive crashes:**
  1. Run E3's procedure with `k ∈ {10, 100}`.
  2. After the new leader is elected, it receives `k` more commands, delivered to a
     minority, and crashes the same way.
  3. M10 is measured for the third leader, which must handle entries from two earlier
     terms.
- **E4b, catch-up after a restart:** one follower is stopped, the leader commits
  `n ∈ {100, 1000}` commands, and the follower restarts.
- **E4c, catch-up after a partition:** as E4b, but the follower is partitioned instead of
  stopped, so it keeps its log.
- **Configurations:** servers {3, 5}, paper-like environment.
- **Trials:** 1000 per configuration in the simulator.
- **Metrics:** M10 (E4a), M11 (E4b and E4c).

### E5: Partitions (H2b)

Each scenario runs with 4 clients, 150–300 ms timeouts, and the paper-like environment.
Partitions last 2 s and then heal.

| Scenario | Fault | Servers |
|---|---|---|
| P1 | Leader isolated alone | 3, 5 |
| P2 | Leader isolated with one follower (a minority) | 5 |
| P3 | One follower isolated; the leader keeps its majority | 3, 5 |
| P4 | Messages from the leader to every follower dropped; messages to the leader delivered | 3, 5 |
| P5 | The link between the leader and one follower alternates down and up, each phase uniform in 100–500 ms, for 10 s | 3, 5 |

- **Trials:**
  - Simulator: 1000 per scenario and size.
  - Real cluster: 100 each for P1 and P3 with 5 servers.
- **Metrics:**
  - M2 for every scenario.
  - M6 for P3 and P5, where the leader keeps a majority, so every leader change is
    unnecessary.
  - M15–M17.

### E6: Coherency (H4)

- **Model checking:** Stateright breadth-first search over every reachable state.
  - **Bounds:**
    - 3 servers
    - at most 2 client commands
    - each server starts at most 2 elections
    - Stateright's unordered, non-duplicating network
    - timers fire in any order
    - no crashes
  - **State compared:** for each server, its role, term, vote (Raft), log entries, and
    commit index, plus the messages in flight.
  - **Excluded from the state:** random-number-generator state, timer durations, and
    measurement counters. Both adapters use the same state definition.
  - **Sensitivity check:** if each run finishes within 24 hours, repeat with at most 3
    elections per server.
- **Simulator counters:** M15–M17 from every E3–E5 trial.
- **Metrics:** M14–M17.

### E7: Implementation (H5)

- **Tool:** `rust-code-analysis-cli` runs on `crates/raft/src` and `crates/paxos/src` at
  the commit tagged for data collection. Metrics are reported per module (§5).
- **M18:** message types are counted from each algorithm's message type, and persistent
  fields from its use of `Storage`.
- **M20:** taken from `docs/BUGLOG.md`.

### E8: Vector store recovery (H6, real cluster only)

- **Setup:**
  - 3 or 5 control-plane servers, for each algorithm.
  - 4 data nodes.
  - One collection with 4 shards and 3 replicas each, loaded with 100,000 GloVe-25
    vectors.
  - 4 clients continuously upserting vectors.
- **Scenarios:**
  - **S1:** kill the primary of shard 0.
  - **S2:** kill the primary of shard 0 and the control-plane leader at the same instant.
- **Trials:** 100 per configuration and scenario.
- **Metric:** M21.

  `docs/DESIGN.md` §7 predicts M21 of at least `L + δ − h`, about 2.5 s.

## 7. Sample Sizes and Stopping

Sample sizes are fixed above:

- 1000 paired trials per configuration in the simulator
- 100 trials per configuration on the real cluster

Data collection never stops early, and trials are never added after looking at results.

## 8. Analysis

- **Unit of analysis:** one trial. For E1, a trial contributes its median and p99
  latency, its throughput, and its cost per commit.
- **Paired comparisons** (simulator; same seed for both algorithms): Wilcoxon signed-rank
  test.
- **Unpaired comparisons** (real cluster): Mann–Whitney U test.
- **Tests are two-sided** unless the hypothesis states a direction.
- **Equivalence (H1, H2a for Raft, H6 for S1):**
  - Compute the ratio of medians, Multi-Paxos over Raft (or k = 1000 over k = 0 for
    H2a).
  - Compute its 90% bootstrap percentile interval with 10,000 resamples. Paired designs
    resample seeds as pairs.
  - The two are equivalent if the interval lies within `[0.95, 1.05]`. This is the two
    one-sided tests procedure at α = 0.05.
- **Significance:** α = 0.05, with Holm–Bonferroni correction across the configurations of
  each experiment.
- **Effect sizes:**
  - Paired: the median of paired differences.
  - Unpaired: the difference of medians.

  Each with a 95% bootstrap interval.
- **Figures:** distribution plots for every failover metric, in the style of the paper's
  Figure 16.
- **Censored trials (E2):** counted at the cap value. A sensitivity analysis excludes them.
- **Software:** Python 3.13 with NumPy, SciPy, pandas, and matplotlib, pinned in
  `analysis/uv.lock`. The bootstrap uses a fixed seed (0).

## 9. Validation Before Data Collection

1. **Checkers:** both algorithms pass every shared checker on seeds `1..=10000` of a
   randomized fault schedule. Each run lasts 60 s of simulated time with message loss,
   delays, partitions, and crashes. The checkers are:
   - State machine safety: no two servers apply different entries at an index.
   - At most one leader per term.
   - No acknowledged command missing from the committed log.
   - Liveness after faults stop: a leader is elected and commands commit within 10 s of
     simulated time.

   Raft additionally passes the log matching and leader completeness checks.
2. **A/A test:** Raft is compared with itself, analyzed as unpaired, using:
   - **Simulator:** seeds `1..=1000` against `1001..=2000`, on E1 (3 servers, paper-like,
     16 clients) and E3 (3 servers, `k = 10`).
   - **Real cluster:** the same E1 configuration, 100 trials against 100 interleaved.

   The test passes if no Holm-corrected test is significant and the E1 ratios fall within
   `[0.95, 1.05]`. A failure means the harness is biased or noisy, and it must be fixed
   before going further.
3. **E2 pass criteria** (§6) are met.

## 10. Exclusions and Failures

- **Harness failures:** a trial may be excluded only if the harness failed (a process
  failed to start, the runner crashed, or the machine slept). The reason is recorded in
  the analysis code's exclusion list, and the trial is rerun with the same seed.
- **Safety violations:** a violation stops data collection.
  1. The bug is recorded in `docs/BUGLOG.md` and fixed.
  2. Every experiment is rerun for both algorithms.

## 11. Threats to Validity

- **Construct:**
  - Results describe our Multi-Paxos variant, not Paxos in general.
  - Code complexity (M18, M19) stands in for understandability; we do not repeat the
    paper's user study (§9.1).
- **Internal:**
  - The two implementations may differ in quality. Mitigations:
    - shared code, settings, and checkers
    - the same module layout
    - the A/A test
  - The same two people wrote both implementations.
- **External:**
  - The simulator and localhost are not real networks, and clusters have at most five
    servers.
  - The real cluster runs on a single machine.
- **Statistical:**
  - Multiple comparisons are corrected with Holm–Bonferroni.
  - Paired seeds remove variation caused by different fault schedules.

## Amendments

### A1. 2026-10-20: Implementation details fixed before data collection

These details were not specified above. They were fixed in code before any official data
was collected, and they apply to both algorithms.

1. **Clients.** Closed-loop clients send 128-byte commands over the simulated network with
   the delay model of §3. Partitions, loss, and duplication affect only links between
   servers.
   - A client waits 500 ms for a reply, then retries at a random server.
   - A server that knows the leader redirects the client to it, and the client resends
     immediately.
   - If the server does not know the leader, the client waits 50 ms and retries at a
     random server.
2. **Message counts.** M4, M9, and M10 count consensus messages between servers. Client
   traffic is the same for both algorithms and is not counted.
3. **Heartbeat rounds.** Messages that carry heartbeats, and their replies, include a round
   number in both algorithms (Raft: `AppendEntries`; Multi-Paxos: `Accept` and
   `Accepted`). The leader uses it to tell which replies answer heartbeats sent after a
   leadership confirmation request.
4. **Raft flow control.** The leader tracks each follower in one of two modes, as in etcd's
   Raft library:
   - **Probing:** one `AppendEntries` at a time. After a rejection it moves back one entry,
     as in Figure 2, or to the follower's last index + 1 if that is lower.
   - **Replicating:** entries are sent ahead of acknowledgments, up to the in-flight limit.

   Each reply echoes the `prev_log_index` it answers, so stale rejections are ignored. The
   term-based backtracking of §5.3 stays disabled. Without this, a single lost message
   caused a storm of resends (`docs/BUGLOG.md`).
5. **Delivering entries to a subset (E2, E3).** The procedure drops, from the leader to
   followers outside the subset, only messages that carry log entries
   (`Consensus::entry_count > 0`). Messages without entries, such as heartbeats, still
   reach every follower.
6. **E2 details.**
   - Each follower is in the subset independently with probability 1/2.
   - At the random instant, the leader appends the command, sends heartbeats to every
     follower, and crashes, all at the same simulated time.
   - Servers first start at times spread uniformly over 0–300 ms. With no randomness in
     the timeouts (150–150 ms), servers that start at the same instant split their votes
     forever and never elect a first leader. Only the election after the crash is
     measured.
   - The real-cluster run of E2 is done in Phase 3, when the real cluster exists.
7. **E3 details.**
   - The leader crashes at a time drawn uniformly from 1–2 s after its no-op is committed.
   - The new leader's no-op is identified as the last entry in its log at the moment it
     is elected; both algorithms append it there.
8. **Validation campaign (§9.1).** The run parameters:
   - **Cluster sizes:** odd seeds use 3 servers, even seeds 5.
   - **Fault window:** faults run from 1 s to 45 s. A new fault starts after a gap
     uniform in 0–2 s, and each fault lasts 100 ms to 5 s.
   - **Fault types:**
     - a crash between inputs
     - a crash during an input
     - a crash of the current leader
     - a two-sided partition
     - a one-way link failure
     - 5%, 20%, or 50% message loss
     - 10% duplication
     - delays three or ten times normal
   - **After the faults:**
     - Liveness requires a command sent after 45 s to commit by 55 s.
     - Clients stop at 57 s, and durability is checked at 60 s.
   - **Log matching:** checked every 5 s and at the end.
9. **A/A test (§9.2).** The simulator part is done in Phase 1. The real-cluster part is done
   in Phase 3, when the real cluster exists.

### A2. 2026-10-20: E2 outcome and decision

**Outcome.** R1 passed. R2 failed between the two shortest ranges.

| Timeout range | Median time without a leader |
|---|---|
| 12–24 ms | 73.3 ms |
| 25–50 ms | 69.2 ms |
| 50–100 ms | 91.2 ms |
| 100–200 ms | 149.9 ms |
| 150–300 ms | 210.8 ms |

Results: `experiments/results/E2/20261020T173556Z-61c89dd`. This rerun has the same
measurements as the first run (`20261020T173244Z-99054ab`). The only difference is that
trials with no leader at the crash instant are now marked not ready (`docs/BUGLOG.md`).

**Cause.** On the paper-like network, a round trip takes 7.5–22.5 ms (mean 15 ms). That is
longer than the shortest election timeouts in the 12–24 ms range. This breaks Raft's
timing requirement, broadcastTime ≪ electionTimeout (§5.6), as the paper notes for this
range (§9.3). Candidates time out before their vote requests are answered, so elections
split. Leadership is also unstable before the crash: in 67 of the 1000 trials at 12–24 ms,
an election was already in progress at the instant chosen for the crash, so those trials
have no measurement.

| Timeout range | Trials that elected a leader in the first round | Median rounds |
|---|---|---|
| 12–24 ms | 7.4% | 4 |
| 25–50 ms | 33.2% | 2 |
| 150–300 ms | 66.4% | 1 |

**Check of the simulator.** An exploratory run repeated E2 on the LAN-like network (mean
round trip 1 ms), where the timing requirement holds. It was decided after seeing the
result and is not part of the pre-registered protocol (config:
`experiments/configs/E2-lan.toml`; results:
`experiments/results/E2-lan/20261020T173608Z-066e4c1`). There the medians increase with
the minimum timeout: 36.1, 40.6, 67.7, 126.3, 186.5 ms. The 12–24 ms median is close to the paper's 35 ms
mean.

**Decision.**

- The simulator is treated as valid. It reproduces the paper's pattern wherever the timing
  requirement holds, and the failure follows from the pre-registered network model, not
  from a defect.
- R2 is reported as failed, not redefined.
- The comparison experiments (E1, E3–E5) use 150–300 ms timeouts, where the requirement
  holds, so they are unaffected.


### A3. 2026-10-24: Status, the data-collection tag, and checks during E8

1. **Status.** The status line at the top was written before the freeze and was not
   updated. This document has been frozen since the tag `protocol-v1`.
2. **The data-collection tag.** "The commit tagged for data collection" (E7) is the commit
   tagged `data-v1`. A bug fix after data collection begins is tagged `data-v2`, and so
   on. By §10, every experiment is then rerun for both algorithms, and E7 uses the latest
   such tag.
3. **Checks during E8.** Every E8 trial also runs two checks over logs that the data nodes
   and clients write: at most one data node serves each shard as primary at any moment
   (`docs/DESIGN.md` C1), and no acknowledged write is lost (C2). A violation of either
   is a safety violation under §10.


### A4. 2026-10-24: Real-cluster details fixed before real-cluster data

These details of the real cluster were not specified above. They were fixed in code before
any real-cluster data was collected, and they apply to both algorithms.

1. **Processes.** Each server is one `controlplane` process on `127.0.0.1` with a fresh
   temporary data folder, deleted after the trial. Trials run one at a time.
2. **Order.** All `(run, seed)` pairs of an experiment are shuffled once, with a seed
   derived from the experiment's ID, and run in that order, so the algorithms (or the A/A
   arms) are interleaved.
3. **Delay.** The receiving server holds each consensus message for a one-way delay drawn
   uniformly from `[0.5 d, 1.5 d]`, so a message sent just before its sender crashes still
   arrives, as on a real network. Clients delay each request and each reply by the same
   distribution. Waits use a timed sleep and spin for the last 300 µs.
4. **fsync.** Storage calls `File::sync_data`, which on macOS issues `F_FULLFSYNC` (about
   4 ms on the experiment machine). Every server's storage is on the machine's one SSD, so
   flushes from different servers queue on one device; this bounds throughput and applies
   equally to both algorithms.
5. **Clients.** Clients follow A1.1. Each command is a `Bench` request whose encoding is
   128 bytes.
6. **Ready.** A trial starts once a client command has committed, which implies that the
   leader's no-op has committed.
7. **Measurement.** Every process reads the same clock. Commit latency comes from the
   clients' timestamps; messages and bytes from the servers' logs of every consensus
   message sent; M1 from the servers' logs of role and term changes, from the crash time
   to the first record of a leader in a higher term.
8. **The crash procedure (E2, E3).** The leader receives a crash request. It drops
   messages that carry entries to the followers outside the chosen subset, appends the
   commands, sends a heartbeat round (E2 only), records the crash time, and exits 50 ms
   later without handling any further input. The 50 ms let messages already sent leave its
   sockets. The subset is drawn as in A1.6 (E2) and §6 (E3), from the trial's seed.
9. **The A/A test (§9.2).** Arm A uses seeds 1–100 and arm B seeds 101–200.
10. **Environment.** The machine is kept awake with `caffeinate -i`, and nothing else
    heavy runs during real-cluster experiments.

### A5. 2026-10-25: E4, E5, E6, and E8 details fixed before their data

These details were not specified above, or, for E6, could not be carried out as written.
They were fixed before any data of these experiments was collected, and they apply to
both algorithms.

1. **E4a.** E3's procedure crashes the first leader. As soon as the second leader is
   elected, the first leader restarts with its log; otherwise a 3-server cluster could not
   elect a third leader. The second leader then appends `k` commands whose messages reach
   a random subset of followers that, with the leader, is a minority (size uniform in
   `0..=⌊N/2⌋ − 1`), and crashes in the same instant. M10 is measured for the third leader,
   as in E3. E4a runs with 4 clients.
2. **E4b and E4c.** No clients run. Once a leader's no-op is committed, a random follower
   is stopped (E4b) or partitioned from every other server (E4c). The leader appends `n`
   commands and runs until they commit. The follower then restarts or the partition heals;
   M11 runs from that instant until the follower's commit index reaches the leader's
   commit index at that instant. Trials are capped at 60 s and recorded as censored.
3. **E5.** As in E3, the fault starts at a uniformly random time 1–2 s after a leader's
   no-op commits. P2's minority is the leader and one random follower. P3 isolates one
   random follower. P4 blocks every link from the leader to a follower. P5 starts with the
   link down and alternates. M6 counts the terms with a new leader from the fault until
   3 s after it heals. M2 is capped at 60 s after the fault. On the real cluster, P1 and P3
   isolate the server by dropping its messages in both directions in every server's
   transport.
4. **E6, the bounds.** The bounds of §6 cannot be searched exhaustively on the experiment
   machine. While the search was sized during development, the smallest variant (no client
   commands, one election per server, no heartbeat timer firing) already had about 37
   million reachable states per algorithm, and a variant that also let each server's
   heartbeat timer fire once ran for more than two hours with 4.6 GB of memory and did not
   finish. These development counts are not reported as results. Three changes are made:
   1. **A heartbeat bound.** Each server's heartbeat timer fires at most `H` times.
      Without a bound the state space is infinite, because every heartbeat round carries
      a new round number (amendment A1.3).
   2. **Depth-first search.** Stateright searches depth-first. Run to completion, it
      visits the same reachable states as breadth-first search, which keeps every
      frontier state in memory and does not fit.
   3. **A ladder of bounds.** The search runs, for both algorithms, at these bounds
      (commands, elections per server, heartbeats per server), in order: (0, 1, 0),
      (0, 1, 1), (1, 1, 0), (1, 1, 1), (2, 1, 1), (1, 2, 1), (2, 2, 1), (2, 2, 2). Each search
      has a budget of 6 hours and 1.2 billion generated states. A rung is skipped if a
      smaller rung, one with every bound at most as large and at least one smaller,
      did not complete for both algorithms. Every rung that completes for both
      algorithms is reported; none is dropped. This replaces the sensitivity check of §6.
   M14 counts the distinct states of the compared fields of §6. The search state also
   holds each algorithm's internal state, its pending timers, and the bound counters, so
   successors are computed exactly. Random number generators compare equal whatever their
   position, so states that differ only in the numbers a server will draw next are one
   state; both algorithms draw random numbers only for timer durations, which the model
   does not model. The model uses the default `Config` (64 entries per
   message), 3 servers, no crashes, and an unordered network that never duplicates a
   message; a message never delivered models a lost one.
5. **E6 in Phase 2.** The run of E6 in Phase 2 is a check of the adapter. The reported E6
   is the run at the data-collection tag.
6. **E8.** Both control-plane sizes, 3 and 5, run for each algorithm. Control-plane traffic
   uses the paper-like delay; data-plane traffic has no injected delay. The 100,000
   vectors are the first 100,000 GloVe-25 training vectors, loaded by 64 concurrent
   writers before the 4 clients start. The fault comes 2 s after the clients start. M21
   ends at the first acknowledgment to shard 0 at an epoch newer than the killed primary's,
   so an acknowledgment already in flight from the killed primary does not count. Trials
   are capped at 60 s.

## References

1. D. Ongaro and J. Ousterhout. *In Search of an Understandable Consensus Algorithm
   (Extended Version)*. 2014. https://raft.github.io/raft.pdf
2. H. Howard and R. Mortier. *Paxos vs Raft: Have We Reached Consensus on Distributed
   Consensus?* PaPoC 2020. https://arxiv.org/abs/2004.05074
3. Stateright. https://github.com/stateright/stateright
4. rust-code-analysis. https://mozilla.github.io/rust-code-analysis/
