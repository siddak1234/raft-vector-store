"""E1: steady state (docs/RESEARCH.md §6, hypothesis H1).

H1: with a stable leader, Multi-Paxos's median commit latency and throughput are within
+/-5% of Raft's. Decision rule: equivalence (§8) in every E1 configuration with 3 or 5
servers. For each configuration and metric, the ratio of medians (Multi-Paxos over Raft) is
bootstrapped; simulator trials are paired by seed, real-cluster trials are not. The
two-sided tests (Wilcoxon signed-rank for paired, Mann-Whitney U for unpaired) are
Holm-corrected across the configurations of the experiment.

Single-server configurations are the reference and are reported but not tested.
"""

import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from results import describe, figure_path, latest, load  # noqa: E402
from stats import equivalent, holm, mann_whitney, ratio_of_medians, wilcoxon  # noqa: E402

DECISION = [
    ("median_latency_ms", "M7 median latency"),
    ("throughput_per_s", "M8 throughput"),
]
REPORTED = [
    ("p99_latency_ms", "M7 p99 latency"),
    ("messages_per_commit", "M9 messages per commit"),
    ("bytes_per_commit", "M9 bytes per commit"),
]


def compare(frame: pd.DataFrame, paired: bool) -> pd.DataFrame:
    """One row per configuration and metric: medians, ratio, interval, and p-value."""
    rows = []
    keys = ["servers", "network", "clients"]
    for config, group in frame.groupby(keys):
        raft = group[group["algorithm"] == "raft"].set_index("seed")
        paxos = group[group["algorithm"] == "paxos"].set_index("seed")
        for column, name in DECISION + REPORTED:
            if paired:
                both = raft[[column]].join(paxos[[column]], lsuffix="_raft", rsuffix="_paxos").dropna()
                a = both[f"{column}_raft"].to_numpy()
                b = both[f"{column}_paxos"].to_numpy()
                p = wilcoxon(a, b)
            else:
                a = raft[column].dropna().to_numpy()
                b = paxos[column].dropna().to_numpy()
                p = mann_whitney(a, b)
            ratio, low, high = ratio_of_medians(a, b, paired=paired)
            rows.append({
                "servers": config[0],
                "network": config[1],
                "clients": config[2],
                "metric": name,
                "decision": column in dict(DECISION),
                "n": len(a) if paired else f"{len(a)}/{len(b)}",
                "raft": float(np.median(a)) if len(a) else float("nan"),
                "paxos": float(np.median(b)) if len(b) else float("nan"),
                "ratio": ratio,
                "low": low,
                "high": high,
                "equivalent": equivalent(low, high),
                "p": p,
            })
    table = pd.DataFrame(rows)
    table["p (Holm)"] = holm(table["p"].tolist())
    return table


def verdict(table: pd.DataFrame, label: str) -> bool:
    tested = table[(table["servers"] > 1) & table["decision"]]
    passed = bool(tested["equivalent"].all())
    failing = tested[~tested["equivalent"]]
    print(f"\nH1 ({label}): {'SUPPORTED' if passed else 'NOT SUPPORTED'} — "
          f"{int(tested['equivalent'].sum())} of {len(tested)} decision intervals lie within [0.95, 1.05]")
    for _, row in failing.iterrows():
        print(f"  not equivalent: {row['servers']} servers, {row['network']}, {row['clients']} clients, "
              f"{row['metric']}: ratio {row['ratio']:.3f} [{row['low']:.3f}, {row['high']:.3f}]")
    return passed


def plot(table: pd.DataFrame, name: str, title: str) -> str:
    tested = table[(table["servers"] > 1) & table["decision"]].reset_index(drop=True)
    figure, axis = plt.subplots(figsize=(8, 0.3 * len(tested) + 1.5))
    y = np.arange(len(tested))
    axis.errorbar(tested["ratio"], y, xerr=[tested["ratio"] - tested["low"], tested["high"] - tested["ratio"]],
                  fmt="o", capsize=3)
    axis.axvspan(0.95, 1.05, alpha=0.15)
    axis.axvline(1.0, linewidth=0.8)
    axis.set_yticks(y)
    axis.set_yticklabels([f"{r.servers}s {r.network} {r.clients}c {r.metric}" for r in tested.itertuples()], fontsize=7)
    axis.set_xlabel("Multi-Paxos / Raft, ratio of medians (90% CI)")
    axis.set_title(title)
    figure.tight_layout()
    path = figure_path(name)
    figure.savefig(path, dpi=150)
    plt.close(figure)
    return str(path)


def show(table: pd.DataFrame) -> None:
    printable = table.drop(columns=["decision", "p"]).copy()
    printable["90% CI"] = [f"[{lo:.3f}, {hi:.3f}]" for lo, hi in zip(table["low"], table["high"])]
    printable = printable.drop(columns=["low", "high"])
    print(printable.to_string(index=False, float_format=lambda v: f"{v:.4g}"))


def main() -> int:
    folder = latest("E1")
    if folder is None:
        print("E1: no results yet (run `cargo xtask experiment E1`)")
        return 0
    print(f"E1 results: {describe(folder)}")
    sim = load(folder, "steady.csv")
    print(f"  simulator: {len(sim)} trials, {int((~sim['ready']).sum())} not ready, "
          f"{int(sim['violations'].sum())} violations")
    sim = sim[sim["ready"]]
    table = compare(sim, paired=True)
    show(table)
    verdict(table, "simulator")
    print(f"Figure: {plot(table, 'E1_ratios.png', 'E1 simulator: Multi-Paxos / Raft')}")
    try:
        real = load(folder, "real-steady.csv")
    except FileNotFoundError:
        return 0
    print(f"\n  real cluster: {len(real)} trials, {int((~real['ready']).sum())} not ready")
    real = real[real["ready"]]
    table = compare(real, paired=False)
    show(table)
    verdict(table, "real cluster")
    print(f"Figure: {plot(table, 'E1_real_ratios.png', 'E1 real cluster: Multi-Paxos / Raft')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
