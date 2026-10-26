"""E6: coherency (docs/RESEARCH.md §6, H4; bounds in amendment A5).

H4: Raft reaches fewer states under the same model-checking bounds (M14, compared
directly), its logs never have gaps (M15) and never send entries toward a leader (M16),
while Multi-Paxos logs do both; discarded entries (M17) are tested two-sided. M15-M17 come
from every E3-E5 simulator trial.
"""

import sys

import pandas as pd

from compare import algorithms, show
from results import describe, latest, load


def outcome(search: pd.Series) -> str:
    """A search completed, stopped at its budget, or was skipped (amendment A5.4) because a
    smaller rung did not complete. A skipped search generated no state, so its safety
    columns say nothing."""
    if search["complete"]:
        return "complete"
    return "skipped" if search["generated"] == 0 else "budget reached"


def counters() -> pd.DataFrame:
    frames = []
    for experiment, name in [("E3", "failover.csv"), ("E4", "recovery.csv"), ("E5", "partition.csv")]:
        folder = latest(experiment)
        if folder is None:
            continue
        frame = load(folder, name)
        frame = frame[frame["ready"]]
        frames.append(frame.assign(experiment=experiment)[
            ["experiment", "label", "algorithm", "servers", "seed", "max_log_gaps", "entries_to_leaders",
             "discarded_entries"]])
    return pd.concat(frames) if frames else pd.DataFrame()


def main() -> int:
    folder = latest("E6")
    if folder is None:
        print("E6: no results yet (run `cargo xtask experiment E6`)")
        return 0
    print(f"E6 results: {describe(folder)}")
    searches = load(folder, "modelcheck.csv").assign(outcome=lambda f: f.apply(outcome, axis=1))
    columns = ["label", "algorithm", "outcome", "states", "search_states", "generated", "max_depth",
               "seconds", "election_safety", "state_machine_safety", "command_committed"]
    print(searches[columns].to_string(index=False))
    complete = searches[searches["complete"]]
    print("\nM14, distinct compared states, for rungs complete for both algorithms:")
    for label, group in complete.groupby("label", sort=False):
        if set(group["algorithm"]) == {"raft", "paxos"}:
            raft = int(group.loc[group["algorithm"] == "raft", "states"].iloc[0])
            paxos = int(group.loc[group["algorithm"] == "paxos", "states"].iloc[0])
            fewer = "Raft fewer" if raft < paxos else ("equal" if raft == paxos else "Multi-Paxos fewer")
            print(f"  {label}: Raft {raft:,}, Multi-Paxos {paxos:,} ({paxos / raft:.3f} x): {fewer}")
    ran = searches[searches["outcome"] != "skipped"]
    unsafe = ran[~(ran["election_safety"] & ran["state_machine_safety"])]
    print(f"\nSearches with a safety violation: {len(unsafe)} of {len(ran)} that ran "
          f"({len(searches) - len(ran)} skipped, amendment A5.4)")

    frame = counters()
    if frame.empty:
        return 0
    print("\nM15-M17 over E3-E5 simulator trials, by algorithm:")
    summary = frame.groupby("algorithm")[["max_log_gaps", "entries_to_leaders", "discarded_entries"]].agg(
        ["mean", "max", lambda s: (s > 0).mean()])
    summary.columns = [f"{a} {('share > 0' if b == '<lambda_0>' else b)}" for a, b in summary.columns]
    print(summary.to_string(float_format=lambda v: f"{v:.4g}"))
    raft = frame[frame["algorithm"] == "raft"]
    paxos = frame[frame["algorithm"] == "paxos"]
    print(f"\n  Raft trials with log gaps: {int((raft['max_log_gaps'] > 0).sum())} of {len(raft)}")
    print(f"  Raft trials with entries sent toward a leader: {int((raft['entries_to_leaders'] > 0).sum())} of {len(raft)}")
    print(f"  Multi-Paxos trials with log gaps: {int((paxos['max_log_gaps'] > 0).sum())} of {len(paxos)}")
    print(f"  Multi-Paxos trials with entries sent toward a leader: "
          f"{int((paxos['entries_to_leaders'] > 0).sum())} of {len(paxos)}")
    show("M17 discarded entries, two-sided, paired by seed",
         algorithms(frame, ["experiment", "label"], [("discarded_entries", "M17 discarded entries")], True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
