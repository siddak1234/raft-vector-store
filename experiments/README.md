# Experiments

Definitions and raw results for the experiments in `docs/PLAN.md` §7.

## `configs/`

One file per experiment, such as `E1.toml`, reviewed in a pull request like code. Each
config lists the parameters to sweep, the number of trials, and the seed range.

## `results/`

Written only by the experiment runner (`cargo xtask experiment <id>`, added in Phase 1).
Each run gets its own folder:

```
results/<experiment>/<UTC timestamp>-<commit>/
  manifest.toml   experiment ID, config file, git commit, seeds, start and end time,
                  machine, operating system, Rust version
  schedules/      the fault schedule replayed for each seed
  *.csv           raw measurements
```

## Rules

- The runner refuses to start if the working tree has uncommitted changes, so every result
  maps to an exact commit.
- Existing results are never edited, moved, or deleted. `cargo xtask check-repo` rejects
  any such change.
- To exclude a bad run, record it and the reason in the analysis code.
