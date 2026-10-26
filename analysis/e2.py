"""E2: reproduction of the Raft paper's Figure 16 (docs/RESEARCH.md §6).

Pass criteria, fixed before data collection:
- R1: the median time without a leader for 150-150 ms is at least 10 times the median for
  150-155 ms.
- R2: for 12-24, 25-50, 50-100, 100-200, and 150-300 ms, the median increases with the
  minimum timeout.

Censored trials (no leader within 60 s) count at the cap value. The sensitivity analysis of
§8 repeats the summary and the criteria with the censored trials excluded. The paper's
reported values are shown alongside ours; they are not expected to match exactly, because
the network and implementation differ.

If the exploratory E2-lan results exist (docs/RESEARCH.md amendment A2), their medians are
printed after the pre-registered results, clearly labeled. The real-cluster run at
150-300 ms (amendment A1.6) is summarized next to the simulator's run at that range.
"""

import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from results import describe, figure_path, latest, load  # noqa: E402

TOP = ["150-150", "150-151", "150-155", "150-175", "150-200", "150-300"]
BOTTOM = ["12-24", "25-50", "50-100", "100-200", "150-300"]
PAPER = {
    "150-150": "longer than 10 s",
    "150-155": "median 287 ms",
    "150-200": "maximum 513 ms",
    "12-24": "mean 35 ms, maximum 152 ms",
}


def summarize(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for label, group in frame.groupby("label", sort=False):
        times = group["time_without_leader_ms"]
        rows.append({
            "range (ms)": label,
            "trials": len(group),
            "censored": int(group["censored"].sum()),
            "median": times.median(),
            "mean": times.mean(),
            "p99": times.quantile(0.99),
            "max": times.max(),
            "rounds (median)": group["election_rounds"].median(),
            "paper": PAPER.get(label, ""),
        })
    return pd.DataFrame(rows).set_index("range (ms)")


def plot(frame: pd.DataFrame, name: str, title: str) -> str:
    figure, (top, bottom) = plt.subplots(2, 1, figsize=(7, 7))
    for axis, labels, log in [(top, TOP, True), (bottom, BOTTOM, False)]:
        for label in labels:
            times = np.sort(frame.loc[frame["label"] == label, "time_without_leader_ms"].dropna())
            if len(times) == 0:
                continue
            share = np.arange(1, len(times) + 1) / len(times) * 100
            axis.step(times, share, where="post", label=f"{label} ms")
        axis.set_ylabel("cumulative percent")
        axis.set_ylim(0, 100)
        axis.legend(loc="lower right", fontsize=8)
        if log:
            axis.set_xscale("log")
        else:
            axis.set_xlim(left=0)
    bottom.set_xlabel("time without leader (ms)")
    top.set_title(title)
    figure.tight_layout()
    path = figure_path(name)
    figure.savefig(path, dpi=150)
    plt.close(figure)
    return str(path)


def main() -> int:
    folder = latest("E2")
    if folder is None:
        print("E2: no results yet (run `cargo xtask experiment E2`)")
        return 0
    frame = load(folder, "figure16.csv")
    not_ready = int((~frame["ready"]).sum())
    violations = int(frame["violations"].sum())
    frame = frame[frame["ready"]]

    summary = summarize(frame)
    print(f"E2 results: {describe(folder)}")
    print(f"  trials not ready: {not_ready}; violations: {violations}\n")
    print(summary.to_string(float_format=lambda v: f"{v:.1f}"))
    print()
    criteria(summary["median"])
    sensitivity(frame)
    figure = plot(frame, "E2_figure16.png",
                  "Time to replace a crashed leader (5 servers, ~15 ms round trip)")
    print(f"\nFigure: {figure}")
    exploratory()
    real(frame)
    return 0


def criteria(medians: pd.Series) -> None:
    """Print R1 and R2 for the median time without a leader of each range. A range without
    trials has no median, and a criterion that needs it fails."""
    medians = medians.reindex(list(dict.fromkeys(TOP + BOTTOM)))
    r1 = bool(medians["150-150"] >= 10 * medians["150-155"])
    bottom = [medians[label] for label in BOTTOM]
    r2 = all(a < b for a, b in zip(bottom, bottom[1:]))
    print(f"R1 (150-150 median >= 10 x 150-155 median): "
          f"{medians['150-150']:.0f} vs {medians['150-155']:.0f} ms -> {'PASS' if r1 else 'FAIL'}")
    print("R2 (bottom-range medians increase with the minimum timeout): "
          + " < ".join(f"{m:.1f}" for m in bottom) + f" -> {'PASS' if r2 else 'FAIL'}")


def sensitivity(frame: pd.DataFrame) -> None:
    """The sensitivity analysis of docs/RESEARCH.md §8: the summary and the criteria with the
    censored trials excluded instead of counted at the cap."""
    kept = frame[~frame["censored"]]
    summary = summarize(kept)
    print(f"\nSensitivity analysis (§8): {len(frame) - len(kept)} censored trials excluded")
    print(summary[["trials", "median", "mean", "p99", "max"]].to_string(float_format=lambda v: f"{v:.1f}"))
    print()
    criteria(summary["median"])


def real(simulated: pd.DataFrame) -> None:
    """Summarize the real-cluster E2 run against the simulator's 150-300 ms range."""
    folder = latest("E2-real")
    if folder is None:
        return
    frame = load(folder, "real-figure16.csv")
    not_ready = int((~frame["ready"]).sum())
    frame = frame[frame["ready"]]
    times = frame["time_without_leader_ms"]
    sim = simulated.loc[simulated["label"] == "150-300", "time_without_leader_ms"]
    print(f"\nReal cluster, 150-300 ms ({describe(folder)}): {len(frame)} ready trials, "
          f"{not_ready} not ready, {int(frame['censored'].sum())} censored")
    print(f"  median {times.median():.1f} ms, mean {times.mean():.1f}, p99 {times.quantile(0.99):.1f}, "
          f"max {times.max():.1f}; rounds (median) {frame['election_rounds'].median():.0f}")
    print(f"  simulator at 150-300 ms: median {sim.median():.1f} ms, mean {sim.mean():.1f}, "
          f"p99 {sim.quantile(0.99):.1f}, max {sim.max():.1f}")
    figure, axis = plt.subplots(figsize=(7, 3.5))
    for values, label in [(sim, "simulator"), (times, "real cluster")]:
        values = np.sort(values.dropna())
        axis.step(values, np.arange(1, len(values) + 1) / len(values) * 100, where="post", label=label)
    axis.set_xlabel("time without leader (ms)")
    axis.set_ylabel("cumulative percent")
    axis.set_title("E2 at 150-300 ms: simulator and real cluster")
    axis.legend()
    figure.tight_layout()
    path = figure_path("E2_real.png")
    figure.savefig(path, dpi=150)
    plt.close(figure)
    print(f"  figure: {path}")


def exploratory() -> None:
    """Print the exploratory E2 run on the LAN-like network, if it exists."""
    folder = latest("E2-lan")
    if folder is None:
        return
    frame = load(folder, "figure16.csv")
    frame = frame[frame["ready"]]
    medians = frame.groupby("label")["time_without_leader_ms"].median()
    print(f"\nExploratory, not pre-registered: E2 on the LAN-like network ({describe(folder)})")
    print("  bottom-range medians: " + " < ".join(f"{label} {medians[label]:.1f}" for label in BOTTOM))
    figure = plot(frame, "E2_lan_figure16.png",
                  "Exploratory: time to replace a crashed leader (~1 ms round trip)")
    print(f"  figure: {figure}")


if __name__ == "__main__":
    sys.exit(main())
