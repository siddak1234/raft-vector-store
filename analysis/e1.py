"""E1: steady state (docs/RESEARCH.md §6, hypothesis H1).

H1: with a stable leader, Multi-Paxos's median commit latency and throughput are within
+/-5% of Raft's. Decision rule: equivalence (§8) in every E1 configuration with 3 or 5
servers. For each configuration and metric, the ratio of medians (Multi-Paxos over Raft) is
bootstrapped; simulator trials are paired by seed, real-cluster trials are not. The
two-sided tests (Wilcoxon signed-rank for paired, Mann-Whitney U for unpaired) are
Holm-corrected across the configurations and metrics of each environment. Each comparison
also has its effect size, Multi-Paxos minus Raft, with a 95% bootstrap interval.

Single-server configurations are the reference. They are compared like the others and are
part of the simulator's Holm family, but H1's decision uses only 3 and 5 servers.

Amendment A8 adds descriptive measures: p99.9 and maximum latency, storage syncs and bytes
synced per commit, and, on the real cluster, CPU time per commit and peak memory. They are
reported as medians and ratios of medians with 90% bootstrap intervals, without tests, so
the Holm family of the pre-registered tests is unchanged.
"""

import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import NullLocator  # noqa: E402

from compare import ALGORITHMS, COLORS, NAMES, interval  # noqa: E402
from results import describe, figure_path, latest, load  # noqa: E402
from stats import effect_size, equivalent, holm, mann_whitney, ratio_of_medians, wilcoxon  # noqa: E402

DECISION = [
    ("median_latency_ms", "M7 median latency"),
    ("throughput_per_s", "M8 throughput"),
]
REPORTED = [
    ("p99_latency_ms", "M7 p99 latency"),
    ("messages_per_commit", "M9 messages per commit"),
    ("bytes_per_commit", "M9 bytes per commit"),
]
DESCRIPTIVE = [
    ("p999_latency_ms", "p99.9 latency"),
    ("max_latency_ms", "max latency"),
    ("fsyncs_per_commit", "fsyncs per commit"),
    ("storage_bytes_per_commit", "bytes synced per commit"),
    ("cpu_ms_per_commit", "CPU ms per commit"),
    ("max_rss_mb", "peak memory MB"),
]


def compare(frame: pd.DataFrame, paired: bool) -> pd.DataFrame:
    """One row per configuration and metric: medians, ratio and its interval, effect size
    (Multi-Paxos minus Raft) and its interval, and p-value."""
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
            effect, effect_low, effect_high = effect_size(a, b, paired=paired)
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
                "paxos-raft": effect,
                "95% CI": interval(effect_low, effect_high),
                "p": p,
            })
    table = pd.DataFrame(rows)
    table["p (Holm)"] = holm(table["p"].tolist())
    return table


def descriptive(frame: pd.DataFrame, paired: bool) -> pd.DataFrame:
    """The amendment A8 measures: medians and the ratio of medians (Multi-Paxos over Raft)
    with its 90% bootstrap interval, for each configuration with data. No tests."""
    rows = []
    for config, group in frame.groupby(["servers", "network", "clients"]):
        raft = group[group["algorithm"] == "raft"].set_index("seed")
        paxos = group[group["algorithm"] == "paxos"].set_index("seed")
        for column, name in DESCRIPTIVE:
            if column not in frame.columns:
                continue
            if paired:
                both = raft[[column]].join(paxos[[column]], lsuffix="_raft", rsuffix="_paxos").dropna()
                a = both[f"{column}_raft"].to_numpy(float)
                b = both[f"{column}_paxos"].to_numpy(float)
            else:
                a = raft[column].dropna().to_numpy(float)
                b = paxos[column].dropna().to_numpy(float)
            if len(a) == 0 or len(b) == 0:
                continue
            ratio, low, high = ratio_of_medians(a, b, paired=paired)
            rows.append({
                "servers": config[0],
                "network": config[1],
                "clients": config[2],
                "metric": name,
                "n": len(a) if paired else f"{len(a)}/{len(b)}",
                "raft": float(np.median(a)),
                "paxos": float(np.median(b)),
                "ratio": ratio,
                "90% CI": f"[{low:.3f}, {high:.3f}]",
            })
    return pd.DataFrame(rows)


def show_descriptive(table: pd.DataFrame, label: str) -> None:
    print(f"\nDescriptive measures ({label}; amendment A8, not tested):")
    if table.empty:
        print("  (none in these results)")
        return
    print(table.to_string(index=False, float_format=lambda v: f"{v:.4g}"))


def verdict(table: pd.DataFrame, label: str) -> bool:
    tested = table[(table["servers"] > 1) & table["decision"]]
    passed = bool(tested["equivalent"].all())
    failing = tested[~tested["equivalent"]]
    print(f"\nH1 ({label}): {'SUPPORTED' if passed else 'NOT SUPPORTED'} — "
          f"{int(tested['equivalent'].sum())} of {len(tested)} decision intervals lie within [0.95, 1.05]")
    for _, row in failing.iterrows():
        where = f"{row['servers']} servers, {row['network']}, {row['clients']} clients, {row['metric']}"
        if np.isnan(row["ratio"]):
            print(f"  no data: {where}")
        else:
            print(f"  not equivalent: {where}: ratio {row['ratio']:.3f} [{row['low']:.3f}, {row['high']:.3f}]")
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


PANELS = [
    (3, "lan", "3 servers, LAN-like"),
    (3, "paper", "3 servers, paper-like"),
    (5, "lan", "5 servers, LAN-like"),
    (5, "paper", "5 servers, paper-like"),
]
SIMULATOR = {"marker": "o", "linestyle": "-"}
# The real cluster ran 1 and 16 clients only, so its points are not joined by lines.
REAL = {"marker": "s", "linestyle": "none", "markerfacecolor": "none", "markersize": 7}
SPEED = [
    ("median_latency_ms", "median latency (ms)", False),
    ("p99_latency_ms", "p99 latency (ms)", False),
    ("throughput_per_s", "throughput (commits/s)", True),
]
COST = [
    ("messages_per_commit", "messages per commit", False),
    ("bytes_per_commit", "bytes per commit", False),
    ("fsyncs_per_commit", "fsyncs per commit", False),
]
RESOURCES = [
    ("cpu_ms_per_commit", "CPU time per commit (ms)", False),
    ("max_rss_mb", "peak memory per server (MB)", False),
]


def against_clients(environments: list, metrics: list, name: str, title: str) -> str:
    """The median across trials of each metric against the number of clients: one row per
    metric, one column per cluster size and network, one line per algorithm and
    environment. `environments` lists (trials, name, line style)."""
    figure, axes = plt.subplots(len(metrics), len(PANELS), figsize=(3.3 * len(PANELS), 2.5 * len(metrics)),
                                squeeze=False)
    clients = sorted(set().union(*(frame["clients"].unique() for frame, _, _ in environments)))
    for row, (column, label, log) in enumerate(metrics):
        for col, (servers, network, heading) in enumerate(PANELS):
            axis = axes[row][col]
            for frame, environment, style in environments:
                group = frame[(frame["servers"] == servers) & (frame["network"] == network)]
                for algorithm in ALGORITHMS:
                    medians = group[group["algorithm"] == algorithm].groupby("clients")[column].median().dropna()
                    if len(medians):
                        axis.plot(medians.index, medians.to_numpy(), color=COLORS[algorithm],
                                  label=f"{NAMES[algorithm]}, {environment}", **style)
            axis.set_xscale("log", base=2)
            axis.xaxis.set_minor_locator(NullLocator())
            axis.set_xticks(clients, [str(c) for c in clients])
            if log:
                axis.set_yscale("log")
            axis.tick_params(labelsize=7)
            if row == 0:
                axis.set_title(heading, fontsize=9)
            if row == len(metrics) - 1:
                axis.set_xlabel("clients", fontsize=8)
            if col == 0:
                axis.set_ylabel(label, fontsize=8)
    entries = {}
    for axis in axes.flat:
        for handle, label in zip(*axis.get_legend_handles_labels()):
            entries.setdefault(label, handle)
    figure.legend(entries.values(), entries.keys(), loc="lower center", ncol=len(entries), fontsize=8)
    figure.suptitle(title)
    figure.tight_layout(rect=(0, 0.06, 1, 1))
    path = figure_path(name, report=True)
    figure.savefig(path, dpi=150)
    plt.close(figure)
    return str(path)


def show(table: pd.DataFrame) -> None:
    printable = table.drop(columns=["decision", "p"]).copy()
    printable.insert(printable.columns.get_loc("ratio") + 1, "90% CI",
                     [f"[{lo:.3f}, {hi:.3f}]" for lo, hi in zip(table["low"], table["high"])])
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
    show_descriptive(descriptive(sim, paired=True), "simulator")
    print(f"Figure: {plot(table, 'E1_ratios.png', 'E1 simulator: Multi-Paxos / Raft')}")
    environments = [(sim, "simulator", SIMULATOR)]
    try:
        real = load(folder, "real-steady.csv")
    except FileNotFoundError:
        real = None
    if real is not None:
        print(f"\n  real cluster: {len(real)} trials, {int((~real['ready']).sum())} not ready")
        real = real[real["ready"]]
        table = compare(real, paired=False)
        show(table)
        verdict(table, "real cluster")
        show_descriptive(descriptive(real, paired=False), "real cluster")
        print(f"Figure: {plot(table, 'E1_real_ratios.png', 'E1 real cluster: Multi-Paxos / Raft')}")
        environments.append((real, "real cluster", REAL))
    figures = [
        (environments, SPEED, "E1_speed.png", "E1: latency and throughput"),
        (environments, COST, "E1_cost.png", "E1: messages, bytes, and fsyncs per commit"),
    ]
    if real is not None:
        figures.append((environments[1:], RESOURCES, "E1_resources.png", "E1 real cluster: CPU time and memory"))
    for arguments in figures:
        print(f"Figure: {against_clients(*arguments)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
