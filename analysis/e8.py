"""E8: vector store recovery (docs/RESEARCH.md §6, H6; details in amendments A3.3 and A5).

H6: shard recovery time (M21) is equivalent between the algorithms when only a data node
fails (S1: the ratio of medians, Multi-Paxos over Raft, has a 90% bootstrap interval within
[0.95, 1.05]), and differs when the control-plane leader fails at the same time (S2:
two-sided Mann-Whitney U). The Mann-Whitney tests of all four configurations, S1 and S2 on
3 and 5 servers, form one Holm family, which S2's verdict uses. Every trial also checks C1
and C2.
"""

import math
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from compare import ALGORITHMS, COLORS, NAMES, algorithms, cdf, show  # noqa: E402
from results import describe, figure_path, latest, load  # noqa: E402
from stats import ALPHA, equivalent, ratio_of_medians  # noqa: E402

TITLES = {"S1": "S1: a data node is killed", "S2": "S2: a data node and the control-plane leader"}


def plot(frame) -> str:
    """The distribution of shard recovery time in each scenario, one line per algorithm and
    control-plane size."""
    scenarios = sorted(frame["scenario"].unique())
    figure, axes = plt.subplots(1, len(scenarios), figsize=(6 * len(scenarios), 3.6), squeeze=False)
    for axis, scenario in zip(axes[0], scenarios):
        group = frame[frame["scenario"] == scenario]
        for servers, linestyle in zip(sorted(group["servers"].unique()), ["-", "--"]):
            for algorithm in ALGORITHMS:
                values = group.loc[(group["servers"] == servers) & (group["algorithm"] == algorithm), "recovery_ms"]
                cdf(axis, values, f"{NAMES[algorithm]}, {servers} servers", color=COLORS[algorithm],
                    linestyle=linestyle)
        axis.set_title(f"E8 {TITLES.get(scenario, scenario)}", fontsize=9)
        axis.set_xlabel("M21 shard recovery time (ms)", fontsize=8)
        axis.set_ylabel("cumulative %", fontsize=8)
        axis.legend(fontsize=7)
    figure.tight_layout()
    path = figure_path("E8_recovery.png", report=True)
    figure.savefig(path, dpi=150)
    plt.close(figure)
    return str(path)


def main() -> int:
    folder = latest("E8")
    if folder is None:
        print("E8: no results yet (run `cargo xtask experiment E8`)")
        return 0
    print(f"E8 results: {describe(folder)}")
    frame = load(folder, "real-vectors.csv")
    print(f"  {len(frame)} trials, {int((~frame['ready']).sum())} not ready; "
          f"lost acknowledged writes in {int((frame['lost_writes'] > 0).sum())} trials; "
          f"two serving primaries in {int((frame['double_primaries'] > 0).sum())} trials")
    frame = frame[frame["ready"]].assign(scenario=lambda f: f["label"].str.split("-").str[0])
    table = algorithms(frame, ["scenario", "servers"], [("recovery_ms", "M21 shard recovery (ms)")], False)
    show("M21: Multi-Paxos against Raft (Mann-Whitney)", table)
    for (scenario, servers), group in frame.groupby(["scenario", "servers"]):
        if scenario != "S1":
            continue
        a = group.loc[group["algorithm"] == "raft", "recovery_ms"].dropna().to_numpy(float)
        b = group.loc[group["algorithm"] == "paxos", "recovery_ms"].dropna().to_numpy(float)
        if len(a) == 0 or len(b) == 0:
            print(f"  S1, {servers} servers: no data")
            continue
        ratio, low, high = ratio_of_medians(a, b, paired=False)
        print(f"  S1, {servers} servers: ratio {ratio:.3f} [{low:.3f}, {high:.3f}] -> "
              f"{'equivalent' if equivalent(low, high) else 'not equivalent'}")
    s2 = table[table["scenario"] == "S2"]
    for _, row in s2.iterrows():
        if math.isnan(row["p (Holm)"]):
            print(f"  S2, {row['servers']} servers: no data")
            continue
        print(f"  S2, {row['servers']} servers: p (Holm) {row['p (Holm)']:.3g} -> "
              f"{'differs' if row['p (Holm)'] < ALPHA else 'no difference detected'}")
    print(f"Figure: {plot(frame)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
