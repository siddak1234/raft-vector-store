"""E2: reproduction of the Raft paper's Figure 16 (docs/RESEARCH.md §6).

Pass criteria, fixed before data collection:
- R1: the median time without a leader for 150-150 ms is at least 10 times the median for
  150-155 ms.
- R2: for 12-24, 25-50, 50-100, 100-200, and 150-300 ms, the median increases with the
  minimum timeout.

Censored trials (no leader within 60 s) count at the cap value. The paper's reported values
are shown alongside ours; they are not expected to match exactly, because the network and
implementation differ.

If the exploratory E2-lan results exist (docs/RESEARCH.md amendment A2), their medians are
printed after the pre-registered results, clearly labeled.
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
            times = np.sort(frame.loc[frame["label"] == label, "time_without_leader_ms"])
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

    medians = summary["median"]
    r1 = medians["150-150"] >= 10 * medians["150-155"]
    bottom = [medians[label] for label in BOTTOM]
    r2 = all(a < b for a, b in zip(bottom, bottom[1:]))
    print(f"\nR1 (150-150 median >= 10 x 150-155 median): "
          f"{medians['150-150']:.0f} vs {medians['150-155']:.0f} ms -> {'PASS' if r1 else 'FAIL'}")
    print("R2 (bottom-range medians increase with the minimum timeout): "
          + " < ".join(f"{m:.1f}" for m in bottom) + f" -> {'PASS' if r2 else 'FAIL'}")
    figure = plot(frame, "E2_figure16.png",
                  "Time to replace a crashed leader (5 servers, ~15 ms round trip)")
    print(f"\nFigure: {figure}")
    exploratory()
    return 0


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
