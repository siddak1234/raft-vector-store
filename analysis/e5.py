"""E5: partitions (docs/RESEARCH.md §6, H2b; details in amendment A5).

In the simulator, M2 (unavailability), M6 (unnecessary leader changes), and M15-M17 are
compared two-sided in every scenario and cluster size, in one Holm family. The mean of M6
is also printed for P3 and P5, where the leader keeps a majority. On the real cluster, M2
and M6 are compared in a Holm family of their own.
"""

import sys

from compare import algorithms, show
from results import describe, latest, load

METRICS = [
    ("unavailability_ms", "M2 unavailability (ms)"),
    ("leader_changes", "M6 leader changes"),
    ("max_log_gaps", "M15 log gaps (max)"),
    ("entries_to_leaders", "M16 entries toward leaders"),
    ("discarded_entries", "M17 discarded entries"),
]


def main() -> int:
    folder = latest("E5")
    if folder is None:
        print("E5: no results yet (run `cargo xtask experiment E5`)")
        return 0
    print(f"E5 results: {describe(folder)}")
    sim = load(folder, "partition.csv")
    print(f"  simulator: {len(sim)} trials, {int((~sim['ready']).sum())} not ready, "
          f"{int(sim['violations'].sum())} violations")
    sim = sim[sim["ready"]].assign(scenario=lambda f: f["label"].str.split("-").str[0])
    show("Simulator: Multi-Paxos against Raft (Wilcoxon, paired by seed)",
         algorithms(sim, ["scenario", "servers"], METRICS, True))
    unnecessary = sim[sim["scenario"].isin(["P3", "P5"])]
    print("\nM6, unnecessary leader changes (P3 and P5): mean per trial")
    print(unnecessary.groupby(["scenario", "servers", "algorithm"])["leader_changes"].mean().to_string())
    try:
        real = load(folder, "real-partition.csv")
    except FileNotFoundError:
        return 0
    real = real[real["ready"]].assign(scenario=lambda f: f["label"].str.split("-").str[1])
    show("Real cluster: Multi-Paxos against Raft (Mann-Whitney)",
         algorithms(real, ["scenario", "servers"], METRICS[:2], False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
