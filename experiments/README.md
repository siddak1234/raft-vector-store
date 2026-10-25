# Experiments

Definitions and raw results for the experiments in `docs/PLAN.md` §7.

## `configs/`

One file per experiment, such as `E2.toml`, versioned like code. Each config lists runs:
a scenario (`steady`, `figure16`, `failover`, `partition`, or `modelcheck`), where it runs
(`environment = "simulator"`, the default, or `"real"`), the algorithm, the cluster size,
the network, the seed range, and the scenario's settings. `cargo xtask experiment <id>`
runs `configs/<id>.toml`.

| Config | Purpose |
|---|---|
| `AA.toml` | A/A test of the harness (`docs/RESEARCH.md` §9.2) |
| `E2.toml` | Raft paper Figure 16 reproduction |
| `E2-lan.toml` | Exploratory: E2 on the LAN-like network (`docs/RESEARCH.md` A2) |
| `AA-real.toml` | A/A test on the real cluster (`docs/RESEARCH.md` §9.2, A1.9) |
| `E2-real.toml` | E2 on the real cluster at 150–300 ms (`docs/RESEARCH.md` A1.6) |
| `E1.toml` | Steady state, simulator and real cluster (H1) |
| `E3.toml` | Failover with `k` uncommitted entries, simulator and real cluster (H2a, H2b) |
| `E4.toml` | Log recovery after consecutive crashes, and catch-up (H3; amendment A5) |
| `E5.toml` | Partitions P1–P5, simulator, and P1 and P3 on the real cluster (H2b; A5) |
| `E6-check.toml` | Phase 2 check of the model-checking adapter: the smallest rung of E6 (A5.5) |
| `E6.toml` | Model checking at the ladder of bounds of amendment A5 (H4) |
| `E8.toml` | Vector store recovery, real cluster only (H6; A3.3, A5) |

E7 has no config: `cargo xtask complexity` runs it (`docs/RESEARCH.md` §6).

## `results/`

Written only by the experiment runner (`cargo xtask experiment <id>`, added in Phase 1).
Each run gets its own folder:

```
results/<experiment>/<UTC timestamp>-<commit>/
  manifest.toml   experiment ID, config file, git commit, start and end time, number of
                  trials, machine, operating system, Rust version
  config.toml     an exact copy of the config that was run
  *.csv           one row per trial: steady.csv, figure16.csv, failover.csv,
                  partition.csv, recovery.csv, and, for real-cluster runs,
                  real-steady.csv, real-figure16.csv, real-failover.csv,
                  real-partition.csv, real-vectors.csv; one row per
                  search in modelcheck.csv; runs.csv for validation; checks.csv for the
                  Phase 4 data-plane checks (`cargo xtask dataplane`); for E7,
                  protocol.json and the JSON output of rust-code-analysis-cli
```

Fault schedules are not stored: they are generated from each trial's seed, so the seed and
the commit reproduce a trial exactly. A trial marked `ready = false` could not apply its
procedure (no leader was elected, or none existed at the crash instant) and has no
measurement.

## Rules

- The runner refuses to start if the working tree has uncommitted changes, so every result
  maps to an exact commit.
- Existing results are never edited, moved, or deleted. `cargo xtask check-repo` rejects
  any such change.
- To exclude a bad run, record it and the reason in the analysis code.
