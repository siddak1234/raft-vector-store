# Raft vs. Multi-Paxos on a Vector Store Control Plane

**ECE 382N Distributed Systems, Fall 2026: Final Report**
**Siddak Bhatia (sb56878), Harrison Held (HWH492)**

> **Status:** sections 1–3, 5, and the references are complete. Section 4 (results) and
> section 6 (conclusion) are written from the Phase 5 runs at `data-v4`
> (`docs/RESEARCH.md` amendments A7–A9), which have not been made yet.

## Abstract

We built the control plane of a sharded, replicated vector store twice, once on Raft and
once on Multi-Paxos, in Rust. The two builds share every line of code except the
consensus module: the driver, storage, network transport, state machine, leases, data
plane, clients, simulator, model checker, and experiment runner are common. This lets us
test three claims from the Raft paper (Ongaro and Ousterhout, 2014) with the algorithm as
the only variable: that Raft is as efficient as Paxos, that it separates leader election,
log replication, and safety, and that its stronger coherency reduces the number of states
to consider. The research questions, metrics, experiments, sample sizes, and statistical
tests were fixed in a research protocol before any data was collected, and every later
change is recorded as a dated amendment.

*Findings: pending the `data-v4` runs.*

## 1. Background

A vector store answers nearest-neighbor queries over embeddings. Once a collection is too
large for one machine it is split into shards on several data nodes, and something must
track which collections exist, which data nodes are alive, and which nodes hold each
shard. That coordinator is a single point of failure, so we replicate it on 3 or 5
servers with a consensus algorithm. Only this metadata goes through consensus; vectors go
from clients straight to the data nodes, where each shard is replicated from a primary to
its backups, as in Kafka's in-sync replica model.

Raft and Multi-Paxos both replicate a log through a leader, and with a stable leader both
commit an entry in one round trip to a majority. Howard and Mortier (2020) describe Paxos
in Raft's terms and argue that the two differ mainly in leader election: a Raft candidate
must already hold every committed entry, while any Paxos server may become leader and then
learns the uncommitted suffix from a majority's promises and re-proposes it. Our
Multi-Paxos variant follows that description, with remaining details from van Renesse and
Altinbuken (2015). Its exact rules are in `docs/PAXOS_SPEC.md`, written in the style of
Raft's Figure 2 and frozen before coding.

The Raft paper makes three claims that this project tests:

1. **Efficiency.** Raft is as efficient as Paxos.
2. **Decomposition.** Raft separates leader election, log replication, and safety.
3. **Coherency.** Raft's stronger rules, that logs never have gaps and entries flow only
   from leader to followers, reduce the number of states to consider.

The six research questions (`docs/PLAN.md` §2):

| ID | Question | Hypothesis |
|---|---|---|
| RQ1 | Do the algorithms differ in commit latency or throughput with a stable leader? | No: equivalent within ±5% |
| RQ2 | How do they differ when the leader fails? | Multi-Paxos election traffic grows with the number of uncommitted entries and Raft's does not; which recovers faster is open |
| RQ3 | Does a new Raft leader re-send fewer log entries? | Yes |
| RQ4 | Does Raft keep logs more coherent and reach fewer states? | Yes: fewer reachable states, no log gaps, no entries sent toward a leader |
| RQ5 | Is Raft's implementation smaller? | Yes: fewer message types and lower code complexity |
| RQ6 | Does the algorithm change how fast the vector store recovers? | Only when the control-plane leader fails at the same time |

## 2. System

### 2.1 One codebase, two algorithms

Both algorithms implement one trait, `consensus::Consensus`. An implementation performs no
I/O, never reads the clock, and draws random numbers only from a seeded generator. A
*driver* delivers one input at a time (a message, a timer, a proposal, or a leadership
check), calls `Storage::sync` after the input, and only then carries out the outputs
(messages, timers, applied entries). Three drivers run the same algorithm code:

- the deterministic **simulator**, which runs a cluster on a virtual clock with seeded
  message delays, loss, duplication, partitions, and crashes, and checks safety after every
  step;
- the **model checker**, which explores every interleaving within fixed bounds with
  Stateright;
- the **server**, which runs the algorithm on its own OS thread with fsync-backed storage
  and a gRPC transport.

Only `crates/raft` and `crates/paxos` differ. Messages, bytes, fsyncs, and timings are
counted in shared code. `cargo xtask check-repo` enforces the separation: only the server
and the experiment runner may depend on an algorithm crate, and the consensus, algorithm,
simulator, and model-checking crates may not use I/O libraries.

### 2.2 The algorithms

**Raft** follows Figure 2 of the paper, with a no-op entry on election and leader-confirmed
reads (§8). Two details beyond Figure 2 are recorded in `docs/RESEARCH.md` amendment A1.
The first is flow control as in etcd's Raft library: the leader probes a follower one
message at a time until their logs match, then sends entries ahead of acknowledgments up to
an in-flight limit. The second is a round number on heartbeats, used for leadership
confirmation. The term-based log backtracking of §5.3 is disabled, as are PreVote,
CheckQuorum, and read leases.

**Multi-Paxos** (`docs/PAXOS_SPEC.md`): each term belongs to one server (term `t` to the
server of rank `t mod N`), so each term has at most one candidate. A candidate moves to its
next own term and sends `Prepare` with its commit index; each voter that promises returns
every entry above that index. With a majority of promises, the new leader keeps the
highest-term entry at each index, fills missing indexes with no-ops, re-proposes all of
them in its own term, and appends a no-op. Followers accept each index on its own, so their
logs can have gaps; a follower commits an index it holds in the leader's term once the
leader's commit index passes it, and a follower missing chosen entries receives them in a
`Commit` message, at most one unanswered per follower.

Both algorithms take the same `Config`: election timeouts of 150–300 ms, a 75 ms heartbeat,
at most 64 entries per message, and at most 1,024 unacknowledged entries per follower. Both
append a no-op on election and confirm leadership the same way: the leader sends a round of
heartbeats and confirms once a majority has answered it.

### 2.3 The control plane

The state machine holds collections, data nodes (address, incarnation, status), and shards
(replicas, primary, in-sync set, epoch). Commands carry a client ID and sequence number, so
a retried command is applied once, and every ownership change carries the epoch it
expects. The leader makes every placement decision and logs it in the command, so applying
the log is deterministic.

### 2.4 The data plane

Data nodes keep vectors in memory and answer k-nearest-neighbor queries by brute force
(`docs/DESIGN.md`). The data plane uses only two operations of the `Consensus` trait,
`propose` and `confirm_leadership`, so it is identical under both algorithms.

- **Leases.** A data node heartbeats the control-plane leader every second (`h` = 1 s) and
  starts each lease, of `L` = 3 s, from the moment it sent the heartbeat. The leader grants
  renewals only after a leader-confirmed read. It marks a node dead only once `L + δ`
  (`δ` = 0.5 s) has passed since both its last grant to that node and the start of its own
  leadership, so an old lease has always expired before a shard is reassigned.
- **Replication.** Each shard has 3 replicas. A primary acknowledges a write once every
  replica in its ack set holds it. The ack set always contains the committed in-sync set,
  and a primary accepts writes only while its ack set has at least 2 members. New primaries come
  only from the in-sync set, and each promotion increases the shard's epoch. A new primary
  aligns backups with `NewEpoch`. During implementation we found that deleting only the
  entries above the new primary's last offset, as first designed, can keep entries from an
  epoch the new primary never followed, and replaced it with an epoch comparison like
  Kafka's leader-epoch truncation (`docs/DESIGN.md` §6.4).
- **Re-replication.** When a node is marked dead, the control-plane leader assigns each of
  its shards a replacement; the primary copies the shard to it and adds it to the in-sync
  set once it has caught up.
- **Routing.** Clients hash a vector's ID to a shard and send writes to its primary;
  searches go to every shard's primary and are merged.

Two properties are checked in every data-plane test and every E8 trial: at most one data
node serves a shard as primary at any moment (C1), and no acknowledged write is lost (C2).

## 3. Method

### 3.1 Pre-registration

The research protocol (`docs/RESEARCH.md`) fixed the hypotheses, metrics, procedures,
sample sizes, and tests before any data existed. It was tagged `protocol-v1`; later changes
are dated amendments (§3.6). The comparison rules (`docs/PLAN.md` §5):

- **One variable.** One binary with `--consensus raft|paxos`; only the algorithm crates
  differ; both use the same `Config` and the same random-number generator.
- **Same correctness bar.** Before any measurement, both algorithms pass the same checkers
  on seeds 1–10,000 of a randomized fault campaign (§3.3). A bug fix in either algorithm
  means rerunning every experiment for both.
- **Paired inputs.** In the simulator, both algorithms run the same seeds; every source of
  randomness has its own stream, so they face the same fault schedules.
- **A/A validation.** Raft is compared with itself, in the simulator and on the real
  cluster, before any comparison.
- **Fixed analysis.** Wilcoxon signed-rank tests for paired simulator trials, Mann–Whitney
  U for real-cluster trials, two-sided unless a hypothesis states a direction. Equivalence
  means the 90% bootstrap interval of the ratio of medians (10,000 resamples, seed 0) lies
  within [0.95, 1.05], the two one-sided tests procedure at α = 0.05. Holm–Bonferroni
  correction across the configurations of each experiment.
- **Fixed sample sizes.** 1,000 paired trials per configuration in the simulator and 100
  per configuration on the real cluster. Data collection never stops early, and no trial is
  added after looking at results.

### 3.2 Environments

- **Simulator.** A one-way message delay uniform in [0.5 d, 1.5 d], with d = 7.5 ms (a mean
  round trip of 15 ms, the broadcast time behind the paper's Figure 16) or d = 0.5 ms (a
  LAN-like 1 ms). Storage writes take no simulated time.
- **Real cluster** (`docs/RESEARCH.md` amendments A4 and A6). One `controlplane` process
  per server on `127.0.0.1`, each with a fresh data folder. The receiving server holds each
  consensus message for a delay drawn from the same distribution, and clients delay each
  request and reply the same way. Storage calls `fsync(2)` after every input. Trials run one
  at a time, in an order shuffled once per experiment, so the algorithms are interleaved,
  and the machine is kept awake with `caffeinate -i`.
- **Machine.** One Apple M1 Pro laptop (8 logical CPUs, macOS) for every run; the same
  release build, compiler, and `Cargo.lock`. Each run's manifest records the commit, start
  and end time, machine, and Rust version.

### 3.3 Validation before data collection

1. **Checkers.** Each seed of the fault campaign runs 60 s of simulated time on 3 servers
   (odd seeds) or 5 (even seeds). From 1 s to 45 s it injects faults: crashes between and
   during inputs, leader crashes, partitions, one-way link failures, message loss of 5–50%,
   duplication, and delays of three or ten times normal. Every step is checked for state
   machine safety and at most one leader per term; at the end, a command sent after 45 s
   must have committed by 55 s (liveness) and no acknowledged command may be missing
   (durability). Raft also passes log matching and leader completeness. The checkers were
   themselves tested by breaking each algorithm on purpose and confirming that the campaign
   catches the bug.
2. **A/A test.** Raft against Raft, analyzed as unpaired: seeds 1–1,000 against
   1,001–2,000 in the simulator on E1 (3 servers, paper-like, 16 clients) and E3
   (3 servers, k = 10); and 100 trials against 100 interleaved on the real cluster on the
   same E1 configuration. The test passes if no Holm-corrected test is significant and the
   E1 ratios lie within [0.95, 1.05].
3. **Figure 16 reproduction (E2).** The simulator must reproduce the shape of the Raft
   paper's Figure 16 before the comparison experiments run.

### 3.4 Experiments

| ID | Question | Procedure | Configurations | Trials | Metrics |
|---|---|---|---|---|---|
| E1 | RQ1 | Closed-loop clients with a stable leader; 2 s warm-up discarded, 10 s measured | Servers {1, 3, 5} × network {LAN, paper} × clients {1, 4, 16, 64} | 1,000 sim; 100 real for 3 and 5 servers × both networks × 1 and 16 clients | Commit latency (median, p99), throughput, messages and bytes per commit. Descriptive (A8): p99.9 and maximum latency, fsyncs and bytes synced per commit, and on the real cluster CPU time per commit and peak memory |
| E1-load | RQ1, exploratory | Commands sent at a fixed rate whatever the replies (open loop); latency runs from when each command was due | 3 and 5 servers × the offered rates of A8, paper-like network | 30 real | Throughput, latency percentiles, late commands, the E1 costs, CPU time, and memory |
| E2 | Validation | Raft only: the leader delivers one command to a random subset, then crashes at a random point of a heartbeat interval | 5 servers, 10 timeout ranges from 12–24 to 150–300 ms | 1,000 sim per range; 100 real at 150–300 ms | Time without a leader |
| E3 | RQ2, RQ3 | 4 clients; a client proposes `k` commands that reach a random subset of followers, then the leader crashes | Servers {3, 5} × k {0, 10, 100, 1000} | 1,000 sim; 100 real for k {0, 100} | Time without a leader, unavailability, election rounds and traffic, re-sent entries, log gaps, entries toward a leader, discarded entries |
| E4 | RQ3 | E4a: two consecutive crashes with `k` uncommitted entries each. E4b: a follower restarts after `n` commits. E4c: a partitioned follower reconnects after `n` commits | E4a: k {10, 100}; E4b and E4c: n {100, 1000}; servers {3, 5} | 1,000 sim | Re-sent entries by the third leader (E4a), catch-up time (E4b, E4c) |
| E5 | RQ2 | 4 clients; a 2 s fault, then heal | P1 leader alone, P2 leader with one follower, P3 one follower isolated, P4 leader's outgoing links dropped, P5 one link flapping for 10 s; servers 3 and 5 (P2: 5) | 1,000 sim; 100 real for P1 and P3 with 5 servers | Unavailability, unnecessary leader changes (P3, P5), log gaps, entries toward a leader, discarded entries |
| E6 | RQ4 | Stateright depth-first search over every reachable state | A ladder of 8 bounds (§3.6) | One search per rung and algorithm | Reachable states |
| E7 | RQ5 | `rust-code-analysis-cli` on both algorithm crates at the data-collection tag | Per module: election, replication, commit, shared state | — | Message types, persistent fields, SLOC, LLOC, cyclomatic and cognitive complexity, bugs logged |
| E8 | RQ6 | 4 data nodes, one collection of 4 shards × 3 replicas loaded with 100,000 GloVe-25 vectors, 4 writing clients. S1: kill the primary of shard 0. S2: kill it and the control-plane leader at once | 3 and 5 control-plane servers × S1, S2 | 100 real | Shard recovery time, with checks C1 and C2 on every trial |

The decision rules (`docs/RESEARCH.md` §4): H1 is equivalence in every E1 configuration
with 3 or 5 servers. H2a is a one-sided test that Multi-Paxos election bytes at k = 1000
exceed those at k = 0, together with equivalence of the two for Raft. H2b is two-sided tests
of time without a leader and unavailability in E3. H3 is a one-sided test on re-sent
entries in E4. H4 compares reachable states directly; log gaps and entries sent toward a
leader must be zero for Raft and above zero for Multi-Paxos, and discarded entries are
tested two-sided. H5 is descriptive. H6 is equivalence for S1 and a two-sided test for S2.

### 3.5 Datasets

Both datasets come from ANN-Benchmarks (Aumüller et al., 2020). `cargo xtask dataset`
downloads them from `ann-benchmarks.com` into a folder outside the repository and checks
the SHA-256 of each file against the value recorded on 2026-10-24.

| Dataset | Vectors | Distance | Used in | License |
|---|---|---|---|---|
| `glove-25-angular.hdf5` (GloVe, Pennington et al., 2014) | 1,183,514 training, 10,000 test, 25 dimensions | angular | E8, the Phase 4 checks, the demo | Public Domain Dedication and License 1.0 |
| `sift-128-euclidean.hdf5` (SIFT1M, Jégou et al., 2011) | 1,000,000 training, 10,000 test, 128 dimensions | Euclidean | the Phase 4 checks only | none stated by the source that we could find |

SHA-256: GloVe `51004cb0ae962159f0db507a51fec2b395de14b166f55976c89f16bd2f8b6391`,
SIFT `dd6f0a6ed6b7ebb8934680f861a33ed01ff33991eaee4fd60914d854a0ca5984`.

### 3.6 Changes after the protocol was frozen

Every change is a dated amendment in `docs/RESEARCH.md` or `docs/PAXOS_SPEC.md`, made
before the data it affects unless it records an outcome:

- **A1** fixes implementation details the protocol left open: client retry rules, which
  messages are counted, heartbeat round numbers, Raft's flow control, and the details of
  E2, E3, and the validation campaign.
- **A2** records E2's outcome and the decision that followed (section 4).
- **A3** names the data-collection tag `data-v1`, with `data-v2` and so on after a bug
  fix, and adds the C1 and C2 checks to every E8 trial.
- **A4** fixes the real-cluster details: processes, run order, delay injection, the crash
  procedure, and the A/A seeds.
- **A5** fixes the E4, E5, and E8 procedures. It also replaces E6's bounds, which could not
  be searched exhaustively on the experiment machine: the smallest variant already had
  about 37 million states per algorithm. The search runs depth-first, each server's
  heartbeat timer fires at most `H` times, and the bounds (commands, elections per server,
  heartbeats per server) form a ladder searched in order: (0, 1, 0), (0, 1, 1), (1, 1, 0),
  (1, 1, 1), (2, 1, 1), (1, 2, 1), (2, 2, 1), (2, 2, 2). Each search has a budget of
  6 hours and 1.2 billion generated states; a rung is skipped if a smaller one did not
  complete for both algorithms, and every rung that completes for both is reported.
- **A6** records the first real-cluster A/A failure. `F_FULLFSYNC` on the machine's one
  SSD made trials too noisy, so storage now calls `fsync(2)`, and the test was rerun.
- **A7** restarts data collection at `data-v2`. While data collection ran at `data-v1`, an
  exploratory check of one E4 result found a performance bug in both algorithms: after a
  reply, a leader sent at most one message of up to 64 entries, so a backlog moved one
  batch per round trip instead of up to the in-flight limit. Raft took about 16 round trips
  to catch a follower up on 1,000 entries. Both leaders now fill the window. Every
  experiment, the validation campaign, and both A/A tests are rerun. The `data-v1` E4
  results are kept and reported as superseded.
- **A8**, made before any run at `data-v2`, adds measures that industry comparisons
  commonly report and the protocol lacked: p99.9 and maximum latency, disk syncs and bytes
  per commit, and CPU time and memory, all descriptive and untested, and the exploratory
  E1-load sweep. Measurement code changed, not the algorithms.
- **A9**, made before any run at `data-v3`, fixes an analysis bug: a comparison without data
  would have read as significant or as no difference. The code that runs the experiments is
  unchanged, and every run is made at `data-v4`.
- **A10**, made in the analysis, fixes a measurement bug: re-sent entries (M10) left out the
  messages a new leader sends in the input that elects it. The runs that measure M10, the
  simulator A/A test, E3, and E4, are rerun at `data-v5`; every other run stays at
  `data-v4`, after checks showed the fix cannot change it.

`docs/BUGLOG.md` records every bug found in either algorithm or in shared code, with the
checker that found it; it is the data for metric M20.

## 4. Results

*Pending: written from the `data-v4` runs, with every number taken from
`docs/RESULTS.md` and the committed results in `experiments/results/`.*

### 4.1 Validation: checkers, A/A tests, and E2

*Pending.*

### 4.2 RQ1: Steady state (E1)

*Pending.*

### 4.3 RQ2: Leader failure and partitions (E3, E5)

*Pending.*

### 4.4 RQ3: Re-sent entries and catch-up (E3, E4)

*Pending.*

### 4.5 RQ4: Coherency (E6, and simulator counters from E3–E5)

*Pending.*

### 4.6 RQ5: Implementation (E7)

*Pending.*

### 4.7 RQ6: Vector store recovery (E8)

*Pending.*

### 4.8 Superseded results

*Pending: the `data-v1` E4 run, and what changed at `data-v2`.*

## 5. Threats to Validity

**Construct.**

- The results describe our Multi-Paxos variant, not Paxos in general. It follows Howard
  and Mortier's description in Raft's terms, with the same timers, batching, no-op on
  election, and leadership confirmation as Raft, so it is closer to Raft than many Paxos
  deployments. Optimizations either algorithm could use, such as PreVote, read leases, or
  flexible quorums, are disabled in both.
- Code size and complexity (E7) stand in for understandability; we do not repeat the Raft
  paper's user study.
- Reachable states (E6) depend on what the compared state includes. Both adapters use one
  definition, but Raft's state includes its vote, which Multi-Paxos does not persist.
- E1's clients are closed-loop: each waits for its reply before sending again, which can
  hide tail latency, because a slow cluster also slows the load. E1-load sends at fixed
  rates instead, but it is exploratory, with 30 trials per point.
- Three metrics measure less, or more, than their names suggest; each was checked in the
  analysis (section 4):
  - **Election traffic (M4)** counts every message sent between the crash and the
    election, including the followers' replies to the crashed leader's last messages. Those
    replies, not election messages, are why Raft's M4 grows with `k`.
  - **Entries toward a leader (M16)** counts by the receiver's role when a message arrives.
    For Raft these are almost all a new leader's `AppendEntries` reaching a candidate of
    the same term, not entries flowing toward a leader.
  - **Unavailability (M2)** times only the first command after the fault. In P3 and P5 that
    command commits before P5's link first changes, so M2 cannot tell them apart, and the
    flapping shows only in leader changes (M6).
- Every message in both environments gets its own random delay (§3.2), so messages arrive
  out of order. Raft's followers reject an `AppendEntries` whose predecessor has not
  arrived, about once per commit at 16 clients and more, while Multi-Paxos accepts each
  index on its own. A Raft implementation that delivers each follower's messages in order,
  over one connection, would see fewer rejections; our steady-state comparison reflects
  this network model.
- Catch-up time (E4b, E4c) includes the wait for the leader's next heartbeat after the
  follower returns. Because each trial starts right after a leader is ready, that wait
  depends on how long the trial's commands took to commit. This applies to both algorithms,
  but small differences in catch-up time can come from it.

**Internal.**

- The two implementations may differ in quality. Shared code, settings, and checkers, the
  same module layout, and the A/A tests reduce this, but cannot rule it out. The bug that
  restarted data collection (amendment A7) passed every checker because it cost only time,
  and it showed only in procedures that leave a leader with a backlog. Defects of that kind
  may remain.
- The same two people wrote both implementations. Raft was written first (Phase 1) and
  Multi-Paxos second (Phase 2), so the second may benefit from lessons learned on the
  first.
- The flow-control settings (64 entries per message, 1,024 in flight) were chosen once and
  shared. Raft's probing mode and Multi-Paxos's one-`Commit`-at-a-time rule are each
  algorithm's own way of repairing a lagging follower, and other choices would change the
  catch-up results.

**External.**

- The simulator and localhost are not real networks: delays are drawn from one uniform
  distribution, and clusters have at most five servers.
- The real cluster runs on one machine, so its servers share CPU cores and one SSD with
  each other and with the clients, and each server's delay injection spins for the last
  300 µs of each held message. CPU time per commit leaves out that thread, but the cores it
  takes are missing from the servers, so the saturation points of E1-load belong to this
  harness as much as to the algorithms. Storage calls `fsync(2)` (amendment A6), which
  survives process crashes, the only faults injected, but not necessarily power loss.
- E8's vector store differs from production use. It holds 100,000 GloVe vectors of 25
  dimensions, where current embeddings have 768–3,072 (VectorDBBench). Clients write one
  vector per request and wait for it, where vector databases recommend batches: 64–256
  points per request for Qdrant, and up to 1,000 records or 2 MB for Pinecone. No searches
  run during the trials, and search is brute force rather than an approximate index such as
  HNSW. E8 compares recovery time, which lease timing dominates (section 4.7), so these
  differences limit how far E8 generalizes more than they change its comparison.
- Outside the comparison: reads (both algorithms confirm leadership the same way),
  snapshots and log compaction, membership changes, wide-area networks, clusters of more
  than five servers, and durability against power loss.

**Statistical.**

- Holm–Bonferroni correction is applied within each experiment, not across experiments.
- Paired seeds remove variation caused by different fault schedules in the simulator; the
  real cluster is analyzed unpaired.
- The ±5% equivalence margin was chosen before the data, but it is a judgment: smaller
  differences are reported as equivalent.
- Analyses decided after seeing data, such as E2 on the LAN-like network and the E4c check
  that found the bug, are labeled exploratory and are not used to decide any hypothesis.

## 6. Conclusion

*Pending the `data-v4` runs.*

## References

1. D. Ongaro and J. Ousterhout. *In Search of an Understandable Consensus Algorithm
   (Extended Version)*. 2014. https://raft.github.io/raft.pdf
2. H. Howard and R. Mortier. *Paxos vs Raft: Have We Reached Consensus on Distributed
   Consensus?* PaPoC 2020. https://arxiv.org/abs/2004.05074
3. R. van Renesse and D. Altinbuken. *Paxos Made Moderately Complex*. ACM Computing Surveys
   47(3), 2015.
4. etcd Raft library. https://github.com/etcd-io/raft
5. Apache Kafka, KIP-101: *Alter Replication Protocol to use Leader Epoch rather than High
   Watermark for Truncation*.
   https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation
6. Stateright. https://github.com/stateright/stateright
7. rust-code-analysis. https://mozilla.github.io/rust-code-analysis/
8. M. Aumüller, E. Bernhardsson, and A. Faithfull. *ANN-Benchmarks: A Benchmarking Tool for
   Approximate Nearest Neighbor Algorithms*. Information Systems 87, 2020.
9. J. Pennington, R. Socher, and C. Manning. *GloVe: Global Vectors for Word
   Representation*. EMNLP 2014.
10. H. Jégou, M. Douze, and C. Schmid. *Product Quantization for Nearest Neighbor Search*.
    IEEE Transactions on Pattern Analysis and Machine Intelligence 33(1), 2011.
11. Zilliz. *Announcing VDBBench 1.0*.
    https://zilliz.com/blog/vdbbench-1-0-benchmarking-with-your-real-world-production-workloads
12. Qdrant. *Bulk upload vectors*. https://qdrant.tech/documentation/manage-data/bulk-upload/
13. Pinecone. *Upsert data*. https://docs.pinecone.io/guides/data/upsert-data
