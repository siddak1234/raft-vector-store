"""E4: log recovery and catch-up (docs/RESEARCH.md §6, H3; details in amendment A5).

H3: after a leader change, Raft re-sends fewer entries than Multi-Paxos. One-sided Wilcoxon
signed-rank test on M10 (Multi-Paxos greater), paired by seed, in E4a and in E3. M11,
catch-up time, is compared two-sided in E4b and E4c.
"""

import sys

from compare import algorithms, show
from results import describe, latest, load
from stats import ALPHA


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
    try:
        e3 = load(__import__("results").latest("E3"), "failover.csv")
        e3 = e3[e3["ready"] & ~e3["censored"]]
        table = algorithms(e3, ["servers", "uncommitted"], [("resent_entries", "M10 re-sent entries")], True,
                           alternative="greater")
        show("H3 (E3): M10, one-sided, paired by seed", table)
    except (FileNotFoundError, TypeError):
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
