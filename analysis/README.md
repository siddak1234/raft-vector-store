# Analysis

Statistics and figures for the experiments (`docs/PLAN.md` §5 and Phase 6).

- Reads raw results from `experiments/results/` and never writes there.
- Writes figures to `analysis/figures/`, which is not committed. Every figure is
  regenerated from raw results.
- Dependencies are pinned in `uv.lock`. Run `uv sync` in this folder to create the
  environment.
- `cargo xtask figures` runs every script below from the repository root.

| Script | Reads | Prints |
|---|---|---|
| `validation.py` | `results/validation` | Seeds, simulated hours, faults, violations (M12, M13), for the latest run of each algorithm |
| `aa.py` | `results/AA`, `results/AA-real` | The A/A comparison and its pass or fail verdict, in the simulator and on the real cluster |
| `e2.py` | `results/E2`, `results/E2-lan`, `results/E2-real` | Figure 16 summary, criteria R1 and R2, the real-cluster run at 150–300 ms, and figures |

`stats.py` holds the pre-registered statistical procedures (`docs/RESEARCH.md` §8) and
`results.py` finds the latest results folder of each experiment.
