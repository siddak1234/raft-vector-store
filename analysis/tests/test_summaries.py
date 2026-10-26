"""E1, E1-load, E2, and E6 summaries (e1.py, e1_load.py, e2.py, e6.py) on small, hand-made
results."""

import math

import pandas as pd

import e1
import e1_load
import e2
import e6


def test_descriptive_measures_skip_columns_the_results_do_not_have():
    rows = [
        {"servers": 3, "network": "paper", "clients": 16, "algorithm": algorithm, "seed": seed,
         "fsyncs_per_commit": (1.6 if algorithm == "raft" else 3.0) + seed / 100}
        for algorithm in ("raft", "paxos")
        for seed in range(1, 11)
    ]
    table = e1.descriptive(pd.DataFrame(rows), paired=True)
    assert list(table["metric"]) == ["fsyncs per commit"]
    assert table.loc[0, "ratio"] > 1.8


def load_rows(rate: int, throughput: float, late: int, median: float, seeds=(1, 2, 3)) -> list[dict]:
    return [
        {"servers": 3, "algorithm": "raft", "offered_per_s": rate, "seed": seed,
         "offered": rate * 10, "commits": int(throughput * 10), "late": late, "throughput_per_s": throughput,
         "median_latency_ms": median, "p99_latency_ms": median, "cpu_ms_per_commit": 0.3}
        for seed in seeds
    ]


def test_a_rate_is_sustained_only_without_late_commands():
    frame = pd.DataFrame(load_rows(1000, 1000.0, 0, 50.0) + load_rows(8000, 0.0, 80000, float("nan")))
    table = e1_load.summary(frame).set_index("offered_per_s")
    assert table.loc[1000, "sustained"]
    assert table.loc[1000, "median ms"] == 50.0
    assert not table.loc[8000, "sustained"]
    # Empty percentiles fell among late commands, so the median across trials is unbounded.
    assert math.isinf(table.loc[8000, "median ms"])


def test_stalled_trials_and_trials_with_late_commands_are_counted():
    frame = pd.DataFrame(load_rows(1000, 1000.0, 0, 50.0, seeds=(1, 2))
                         + load_rows(1000, 0.0, 10000, float("nan"), seeds=(3,))
                         + load_rows(1000, 990.0, 5, 60.0, seeds=(4,)))
    row = e1_load.summary(frame).set_index("offered_per_s").loc[1000]
    assert row["trials"] == 4
    assert row["stalled"] == 1
    assert row["with late"] == 2


def figure16_rows() -> pd.DataFrame:
    """Three trials per range. Two of the three 150-150 trials are censored at the cap."""
    medians = {"150-155": 300.0, "12-24": 50.0, "25-50": 60.0, "50-100": 90.0, "100-200": 150.0,
               "150-300": 210.0}
    rows = []
    for label in dict.fromkeys(e2.TOP + e2.BOTTOM):
        if label == "150-150":
            trials = [(60_000.0, True), (60_000.0, True), (500.0, False)]
        else:
            trials = [(medians.get(label, 300.0), False)] * 3
        rows += [{"label": label, "time_without_leader_ms": time, "censored": censored, "election_rounds": 1}
                 for time, censored in trials]
    return pd.DataFrame(rows)


def test_e2_counts_censored_trials_at_the_cap(capsys):
    e2.criteria(e2.summarize(figure16_rows())["median"])
    assert "60000 vs 300 ms -> PASS" in capsys.readouterr().out


def test_e2_sensitivity_analysis_excludes_censored_trials(capsys):
    e2.sensitivity(figure16_rows())
    out = capsys.readouterr().out
    assert "2 censored trials excluded" in out
    # Without the censored trials, the 150-150 median is the one trial that elected a leader.
    assert "500 vs 300 ms -> FAIL" in out
    assert "50.0 < 60.0 < 90.0 < 150.0 < 210.0 -> PASS" in out


def test_e6_labels_skipped_searches_apart_from_those_that_ran():
    rows = pd.DataFrame([
        {"complete": True, "generated": 219_455_251},
        {"complete": False, "generated": 1_200_054_459},
        {"complete": False, "generated": 0},
    ])
    assert [e6.outcome(row) for _, row in rows.iterrows()] == ["complete", "budget reached", "skipped"]
