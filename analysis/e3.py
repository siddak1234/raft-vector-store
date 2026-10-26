"""E3: failover with k uncommitted entries (docs/RESEARCH.md §6; H2a, H2b, and M10 for H3).

- H2a: Multi-Paxos election traffic (M4 bytes) at k = 1000 exceeds k = 0 (one-sided
  Wilcoxon signed-rank, paired by seed), while Raft's at k = 1000 is equivalent to k = 0
  (ratio of medians within [0.95, 1.05]).
- H2b: time without a leader (M1) and unavailability (M2) differ between the algorithms.
- Every metric below, M1 and M2 among them, is compared two-sided in every configuration
  (cluster size and k), in one Holm family per environment. On the real cluster the
  comparison covers M1-M4 and M10.
- M10, re-sent entries, is also tested one-sided in E4 (H3).
"""

import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from compare import ALGORITHMS, COLORS, NAMES, algorithms, cdf, interval, paired, show  # noqa: E402
from results import describe, figure_path, latest, load  # noqa: E402
from stats import ALPHA, effect_size, equivalent, holm, ratio_of_medians, wilcoxon  # noqa: E402

METRICS = [
    ("time_without_leader_ms", "M1 time without leader (ms)"),
    ("unavailability_ms", "M2 unavailability (ms)"),
    ("election_rounds", "M3 election rounds"),
    ("election_messages", "M4 election messages"),
    ("election_bytes", "M4 election bytes"),
    ("resent_entries", "M10 re-sent entries"),
    ("max_log_gaps", "M15 log gaps (max)"),
    ("entries_to_leaders", "M16 entries toward leaders"),
    ("discarded_entries", "M17 discarded entries"),
]
# The figures docs/REPORT.md shows; the others stay in analysis/figures/.
REPORT = {"E3_time_without_leader_ms.png", "E3_election_bytes.png", "E3_resent_entries.png"}
# Metrics whose values span orders of magnitude between the algorithms, drawn on a
# logarithmic axis that is linear around 0, since a count can be 0.
WIDE = {"election_messages", "election_bytes", "resent_entries", "entries_to_leaders", "max_log_gaps"}


def h2a(frame) -> None:
    print("\nH2a: election bytes (M4) at k = 1000 against k = 0, paired by seed")
    rows = []
    for servers in sorted(frame["servers"].unique()):
        for algorithm in ["paxos", "raft"]:
            group = frame[(frame["servers"] == servers) & (frame["algorithm"] == algorithm)]
            a, b = paired(group, "election_bytes", group["uncommitted"] == 0, group["uncommitted"] == 1000)
            ratio, low, high = ratio_of_medians(a, b, paired=True)
            effect = effect_size(a, b, paired=True)
            alternative = "greater" if algorithm == "paxos" else "two-sided"
            m0 = float(np.median(a)) if len(a) else float("nan")
            m1 = float(np.median(b)) if len(b) else float("nan")
            rows.append((servers, algorithm, len(a), m0, m1, ratio, low, high, effect,
                         wilcoxon(a, b, alternative=alternative)))
    adjusted = holm([r[9] for r in rows])
    for (servers, algorithm, n, m0, m1, ratio, low, high, effect, _), p in zip(rows, adjusted):
        if n == 0:
            result = "no data"
        elif algorithm == "paxos":
            result = "grows (supported)" if p < ALPHA and m1 > m0 else "does not grow"
        else:
            result = "equivalent (supported)" if equivalent(low, high) else "not equivalent"
        print(f"  {servers} servers, {algorithm:5}: n={n}, median {m0:.0f} -> {m1:.0f} bytes, "
              f"ratio {ratio:.3f} [{low:.3f}, {high:.3f}], paired difference {effect[0]:.0f} bytes "
              f"{interval(effect[1], effect[2])}, p (Holm) {p:.3g}: {result}")


def plot(frame, column: str, label: str, name: str, title: str) -> str:
    """The distribution of one failover metric, one panel per k, one line per algorithm and
    cluster size, in the style of the Raft paper's Figure 16 (docs/RESEARCH.md §8)."""
    ks = sorted(frame["uncommitted"].unique())
    figure, axes = plt.subplots(1, len(ks), figsize=(3.2 * len(ks), 3), squeeze=False)
    for axis, k in zip(axes[0], ks):
        for servers, linestyle in zip(sorted(frame["servers"].unique()), ["-", "--"]):
            for algorithm in ALGORITHMS:
                group = frame[(frame["uncommitted"] == k) & (frame["servers"] == servers)
                              & (frame["algorithm"] == algorithm)]
                cdf(axis, group[column], f"{NAMES[algorithm]}, {servers} servers", color=COLORS[algorithm],
                    linestyle=linestyle)
        if column in WIDE:
            axis.set_xscale("symlog", linthresh=1)
        axis.set_title(f"k = {k}", fontsize=9)
        axis.set_xlabel(label, fontsize=8)
        axis.set_ylabel("cumulative %", fontsize=8)
        axis.legend(fontsize=6)
    figure.suptitle(title)
    figure.tight_layout()
    path = figure_path(name, report=name in REPORT)
    figure.savefig(path, dpi=150)
    plt.close(figure)
    return str(path)


def plot_all(frame, metrics: list[tuple[str, str]], prefix: str, environment: str) -> None:
    for column, label in metrics:
        name = f"{prefix}_{column}.png"
        print(f"Figure: {plot(frame, column, label, name, f'E3 {environment}: {label}')}")


def main() -> int:
    folder = latest("E3")
    if folder is None:
        print("E3: no results yet (run `cargo xtask experiment E3`)")
        return 0
    print(f"E3 results: {describe(folder)}")
    sim = load(folder, "failover.csv")
    print(f"  simulator: {len(sim)} trials, {int((~sim['ready']).sum())} not ready, "
          f"{int(sim['censored'].sum())} censored, {int(sim['violations'].sum())} violations")
    sim = sim[sim["ready"] & ~sim["censored"]]
    show("H2b and the other metrics: Multi-Paxos against Raft (Wilcoxon, paired by seed)",
         algorithms(sim, ["servers", "uncommitted"], METRICS, True))
    h2a(sim)
    plot_all(sim, METRICS, "E3", "simulator")
    try:
        real = load(folder, "real-failover.csv")
    except FileNotFoundError:
        return 0
    print(f"\n  real cluster: {len(real)} trials, {int((~real['ready']).sum())} not ready, "
          f"{int(real['censored'].sum())} censored")
    real = real[real["ready"] & ~real["censored"]]
    real_metrics = METRICS[:6]
    show("Real cluster: Multi-Paxos against Raft (Mann-Whitney)",
         algorithms(real, ["servers", "uncommitted"], real_metrics, False))
    plot_all(real, real_metrics, "E3_real", "real cluster")
    return 0


if __name__ == "__main__":
    sys.exit(main())
