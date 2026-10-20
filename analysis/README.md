# Analysis

Statistics and figures for the experiments (`docs/PLAN.md` §5 and Phase 6).

- Reads raw results from `experiments/results/` and never writes there.
- Writes figures to `analysis/figures/`, which is not committed. Every figure is
  regenerated from raw results.
- Dependencies are pinned in `uv.lock`. Run `uv sync` in this folder to create the
  environment.
- Analysis code is added with the first experiment (E2, Phase 1).
