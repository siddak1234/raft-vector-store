"""E4: log recovery and catch-up (docs/RESEARCH.md §6, H3; details in amendment A5).

H3: after a leader change, Raft re-sends fewer entries than Multi-Paxos. One-sided Wilcoxon
signed-rank test on M10 (Multi-Paxos greater), paired by seed, in E4a and in E3. M11,
catch-up time, is compared two-sided in E4b and E4c.
"""

import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from compare import algorithms, boxes, legend_handles, show  # noqa: E402
from results import describe, figure_path, latest, load  # noqa: E402
from stats import ALPHA  # noqa: E402

PANELS = [
    ("E4a", "resent_entries", "M10 re-sent entries", "E4a: entries re-sent by the third leader"),
    ("E4b", "catch_up_ms", "M11 catch-up time (ms)", "E4b: a follower restarts"),
    ("E4c", "catch_up_ms", "M11 catch-up time (ms)", "E4c: a partition heals"),
]


def plot(frame) -> str:
    """Box plots of re-sent entries in E4a and catch-up time in E4b and E4c, by cluster size
    and k or n, Raft and Multi-Paxos side by side."""
    figure, axes = plt.subplots(1, len(PANELS), figsize=(4.5 * len(PANELS), 3.6), squeeze=False)
    for axis, (procedure, column, label, title) in zip(axes[0], PANELS):
        group = frame[frame["procedure"] == procedure]
        group = group.assign(configuration=group["label"].str.split("-", n=1).str[1])
        boxes(axis, group, "configuration", column, sorted(group["configuration"].unique()))
        if column == "resent_entries":
            axis.set_yscale("symlog", linthresh=1)
            axis.set_ylim(bottom=0)
        axis.set_title(title, fontsize=9)
        axis.set_ylabel(label, fontsize=8)
        axis.set_xlabel("servers and k" if procedure == "E4a" else "servers and missing entries n", fontsize=8)
    figure.legend(handles=legend_handles(), loc="lower center", ncol=2, fontsize=8)
    figure.tight_layout(rect=(0, 0.07, 1, 1))
    path = figure_path("E4_recovery.png", report=True)
    figure.savefig(path, dpi=150)
    plt.close(figure)
    return str(path)


def main() -> int:
    folder = latest("E4")
    if folder is None:
        print("E4: no results yet (run `cargo xtask experiment E4`)")
        return 0
    print(f"E4 results: {describe(folder)}")
    frame = load(folder, "recovery.csv")
    print(f"  {len(frame)} trials, {int((~frame['ready']).sum())} not ready, "
          f"{int(frame['censored'].sum())} censored, {int(frame['violations'].sum())} violations")
    frame = frame[frame["ready"] & ~frame["censored"]]
    frame = frame.assign(procedure=frame["label"].str.split("-").str[0])
    e4a = frame[frame["procedure"] == "E4a"]
    table = algorithms(e4a, ["servers", "uncommitted"], [("resent_entries", "M10 re-sent entries")], True,
                       alternative="greater")
    show("H3 (E4a): M10, one-sided (Multi-Paxos re-sends more), paired by seed", table)
    if len(table):
        supported = all(p < ALPHA and pm > rm for p, pm, rm in
                        zip(table["p (Holm)"], table["paxos median"], table["raft median"]))
        print(f"  H3 in E4a: {'SUPPORTED' if supported else 'NOT SUPPORTED'} in every configuration")
        for _, row in table[table["p (Holm)"].isna()].iterrows():
            print(f"  no data: {row['servers']} servers, k = {row['uncommitted']}")
    catch = frame[frame["procedure"].isin(["E4b", "E4c"])]
    show("E4b and E4c: M11 catch-up time (ms), two-sided, paired by seed",
         algorithms(catch, ["procedure", "servers", "label"], [("catch_up_ms", "M11 catch-up (ms)")], True))
    print(f"Figure: {plot(frame)}")
    e3_folder = latest("E3")
    if e3_folder is None:
        return 0
    e3 = load(e3_folder, "failover.csv")
    e3 = e3[e3["ready"] & ~e3["censored"]]
    table = algorithms(e3, ["servers", "uncommitted"], [("resent_entries", "M10 re-sent entries")], True,
                       alternative="greater")
    show("H3 (E3): M10, one-sided, paired by seed", table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
