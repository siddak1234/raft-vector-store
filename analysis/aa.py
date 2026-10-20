"""A/A validation (docs/RESEARCH.md §9.2).

Raft is compared with itself on disjoint seeds (arm A: seeds 1-1000, arm B: 1001-2000),
analyzed as unpaired samples. The harness passes if no Holm-corrected test is significant
and every E1 ratio of medians is equivalent within the 5% margin. A failure means the
harness is biased or too noisy to detect real differences.
"""

import sys

import numpy as np
import pandas as pd

from results import describe, latest, load
from stats import ALPHA, equivalent, holm, mann_whitney, ratio_of_medians

STEADY_METRICS = [
    ("median_latency_ms", "M7 median latency (ms)"),
    ("p99_latency_ms", "M7 p99 latency (ms)"),
    ("throughput_per_s", "M8 throughput (commits/s)"),
    ("messages_per_commit", "M9 messages per commit"),
    ("bytes_per_commit", "M9 bytes per commit"),
]

FAILOVER_METRICS = [
    ("time_without_leader_ms", "M1 time without leader (ms)"),
    ("unavailability_ms", "M2 unavailability (ms)"),
    ("election_rounds", "M3 election rounds"),
    ("election_messages", "M4 election messages"),
    ("election_bytes", "M4 election bytes"),
    ("stale_log_rejections", "M5 stale-log rejections"),
    ("resent_entries", "M10 re-sent entries"),
]


def compare(frame: pd.DataFrame, metrics, require_equivalence: bool) -> tuple[list[dict], bool]:
    """Compare arms A and B on each metric; return rows for printing and whether it passed."""
    rows = []
    for column, name in metrics:
        a = frame.loc[frame["arm"] == "A", column].dropna().to_numpy()
        b = frame.loc[frame["arm"] == "B", column].dropna().to_numpy()
        ratio, low, high = ratio_of_medians(a, b, paired=False)
        rows.append({
            "metric": name,
            "n": f"{len(a)}/{len(b)}",
            "median A": float(pd.Series(a).median()),
            "median B": float(pd.Series(b).median()),
            "ratio B/A": ratio,
            "90% CI": "n/a" if np.isnan(low) else f"[{low:.3f}, {high:.3f}]",
            "p": mann_whitney(a, b),
            "equivalent": equivalent(low, high) if require_equivalence else None,
        })
    adjusted = holm([row["p"] for row in rows])
    passed = True
    for row, p in zip(rows, adjusted):
        row["p (Holm)"] = p
        if p < ALPHA:
            passed = False
        if require_equivalence and not row["equivalent"]:
            passed = False
    return rows, passed


def show(title: str, rows: list[dict]) -> None:
    print(f"\n{title}")
    table = pd.DataFrame(rows).drop(columns=["p"])
    if table["equivalent"].isna().all():
        table = table.drop(columns=["equivalent"])
    print(table.to_string(index=False, float_format=lambda v: f"{v:.4g}"))


def main() -> int:
    folder = latest("AA")
    if folder is None:
        print("A/A: no results yet (run `cargo xtask experiment AA`)")
        return 0
    steady = load(folder, "steady.csv")
    failover = load(folder, "failover.csv")
    print(f"A/A results: {describe(folder)}")
    for name, frame in [("E1", steady), ("E3", failover)]:
        not_ready = int((~frame["ready"]).sum())
        violations = int(frame["violations"].sum())
        print(f"  {name}: {len(frame)} trials, {not_ready} not ready, {violations} violations")

    e1_rows, e1_passed = compare(steady[steady["ready"]], STEADY_METRICS, True)
    ready = failover[failover["ready"] & ~failover["censored"]]
    e3_rows, e3_passed = compare(ready, FAILOVER_METRICS, False)
    show("E1 (3 servers, paper-like network, 16 clients)", e1_rows)
    show("E3 (3 servers, paper-like network, k = 10)", e3_rows)

    passed = e1_passed and e3_passed
    print(f"\nA/A test: {'PASS' if passed else 'FAIL'}")
    print("  Criteria: no Holm-corrected p below 0.05; every E1 ratio interval within [0.95, 1.05].")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
