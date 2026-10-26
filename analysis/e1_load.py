"""E1-load: commit latency against offered load on the real cluster (docs/RESEARCH.md
amendment A8). Exploratory: it decides no hypothesis and has no tests.

For each cluster size, algorithm, and offered rate, the medians across trials of the
achieved throughput, the median and p99 latency, the fraction of commands that were late
(no reply within 2 s of when they were due), and the CPU time per commit, with the number
of trials that stalled (no commit in the measurement window) and that had a late command.
A rate is sustained when the median throughput is at least 95% of the offered rate and the
median trial has no late command. A trial's latency percentile is empty when it falls among
its late commands; such a trial counts as slower than every other here, so a median across
trials can be infinite, and the figure leaves those points out. The figure plots latency
against achieved throughput, one line per algorithm.
"""

import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from results import describe, figure_path, latest, load  # noqa: E402

SUSTAINED = 0.95


def summary(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.assign(
        late_fraction=frame["late"] / frame["offered"].where(frame["offered"] > 0),
        # An empty percentile fell among late commands: slower than any reply.
        median_latency_ms=frame["median_latency_ms"].fillna(np.inf),
        p99_latency_ms=frame["p99_latency_ms"].fillna(np.inf),
    )
    columns = {
        "throughput_per_s": "throughput",
        "median_latency_ms": "median ms",
        "p99_latency_ms": "p99 ms",
        "late_fraction": "late",
        "cpu_ms_per_commit": "CPU ms/commit",
    }
    grouped = frame.groupby(["servers", "algorithm", "offered_per_s"])
    table = grouped[list(columns)].median().rename(columns=columns)
    table.insert(0, "trials", grouped.size())
    table.insert(1, "stalled", grouped["commits"].apply(lambda commits: int((commits == 0).sum())))
    table.insert(2, "with late", grouped["late"].apply(lambda late: int((late > 0).sum())))
    table["sustained"] = (table["throughput"] >= SUSTAINED * table.index.get_level_values("offered_per_s")) & (
        table["late"] == 0
    )
    return table.reset_index()


def plot(table: pd.DataFrame) -> str:
    sizes = sorted(table["servers"].unique())
    figure, axes = plt.subplots(1, len(sizes), figsize=(6 * len(sizes), 4), squeeze=False)
    for axis, servers in zip(axes[0], sizes):
        for algorithm, group in table[table["servers"] == servers].groupby("algorithm"):
            group = group.sort_values("offered_per_s").replace(np.inf, np.nan)
            axis.plot(group["throughput"], group["median ms"], marker="o", label=f"{algorithm} median")
            axis.plot(group["throughput"], group["p99 ms"], marker="x", linestyle="--", label=f"{algorithm} p99")
        axis.set_xlabel("achieved throughput (commits/s, median of trials)")
        axis.set_ylabel("latency (ms, median of trials)")
        axis.set_yscale("log")
        axis.set_title(f"E1-load, {servers} servers (exploratory)")
        axis.legend(fontsize=7)
    figure.tight_layout()
    path = figure_path("E1_load.png")
    figure.savefig(path, dpi=150)
    plt.close(figure)
    return str(path)


def main() -> int:
    folder = latest("E1-load")
    if folder is None:
        print("E1-load: no results yet (run `cargo xtask experiment E1-load`)")
        return 0
    print(f"E1-load results (exploratory): {describe(folder)}")
    frame = load(folder, "real-load.csv")
    print(f"  {len(frame)} trials, {int((~frame['ready']).sum())} not ready")
    frame = frame[frame["ready"]]
    table = summary(frame)
    print(table.to_string(index=False, float_format=lambda v: f"{v:.4g}"))
    print("\nHighest sustained offered rate:")
    for (servers, algorithm), group in table.groupby(["servers", "algorithm"]):
        sustained = group.loc[group["sustained"], "offered_per_s"]
        best = f"{int(sustained.max())} commands/s" if len(sustained) else "none of the rates"
        print(f"  {servers} servers, {algorithm}: {best}")
    print(f"Figure: {plot(table)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
