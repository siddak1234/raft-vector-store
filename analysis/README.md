# Analysis

Statistics and figures for the experiments (`docs/PLAN.md` §5 and Phase 6).

- Reads raw results from `experiments/results/` and never writes there.
- Writes figures to `analysis/figures/`, which is not committed. Every figure is
  regenerated from raw results.
- Dependencies are pinned in `uv.lock`. Run `uv sync` in this folder to create the
  environment.
- `cargo xtask figures` runs every script below from the repository root.
- `tests/` checks the statistical procedures and runs every script on the committed results:
  `uv run pytest` in this folder. `cargo xtask ci` and CI run them.

| Script | Reads | Prints |
|---|---|---|
| `validation.py` | `results/validation` | Seeds, simulated hours, faults, violations (M12, M13), for the latest run of each algorithm |
| `aa.py` | `results/AA`, `results/AA-real` | The A/A comparison and its pass or fail verdict, in the simulator and on the real cluster |
| `e1.py` | `results/E1` | H1: equivalence of latency and throughput per configuration, the M9 costs, and the descriptive measures of amendment A8, in the simulator and on the real cluster |
| `e1_load.py` | `results/E1-load` | Exploratory (A8): latency, throughput, late commands, and CPU per commit against offered load, the highest sustained rate, and a figure |
| `e2.py` | `results/E2`, `results/E2-lan`, `results/E2-real` | Figure 16 summary, criteria R1 and R2, the real-cluster run at 150–300 ms, and figures |
| `e3.py` | `results/E3` | H2a (election traffic against `k`), H2b (time without a leader, unavailability), and M10 |
| `e4.py` | `results/E4`, `results/E3` | H3 (re-sent entries, one-sided, in E4a and E3) and catch-up time (M11) |
| `e5.py` | `results/E5` | Unavailability (M2) in every partition scenario, unnecessary leader changes (M6), and M15–M17 |
| `e6.py` | `results/E6`, plus the simulator counters of E3–E5 | H4: reachable states per rung (M14), log gaps (M15), entries toward a leader (M16), and discarded entries (M17) |
| `e7.py` | `results/E7`, `docs/BUGLOG.md` | M18 protocol size, M19 code size and complexity per module, and M20 bugs |
| `e8.py` | `results/E8` | H6: shard recovery time (M21) for S1 and S2, and checks C1 and C2 |

`stats.py` holds the pre-registered statistical procedures (`docs/RESEARCH.md` §8),
`compare.py` the Multi-Paxos-against-Raft comparisons that several scripts share, and
`results.py` finds the latest results folder of each experiment.
