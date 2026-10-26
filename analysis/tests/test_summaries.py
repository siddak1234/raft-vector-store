"""E1, E1-load, and E6 summaries (e1.py, e1_load.py, e6.py) on small, hand-made results."""

import math

import pandas as pd

import e1
import e1_load
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


def load_rows(rate: int, throughput: float, late: int, median: float) -> list[dict]:
    return [
        {"servers": 3, "algorithm": "raft", "offered_per_s": rate, "seed": seed,
         "offered": rate * 10, "late": late, "throughput_per_s": throughput,
         "median_latency_ms": median, "p99_latency_ms": median, "cpu_ms_per_commit": 0.3}
        for seed in (1, 2, 3)
    ]


def test_a_rate_is_sustained_only_without_late_commands():
    frame = pd.DataFrame(load_rows(1000, 1000.0, 0, 50.0) + load_rows(8000, 0.0, 80000, float("nan")))
    table = e1_load.summary(frame).set_index("offered_per_s")
    assert table.loc[1000, "sustained"]
    assert table.loc[1000, "median ms"] == 50.0
    assert not table.loc[8000, "sustained"]
    # Empty percentiles fell among late commands, so the median across trials is unbounded.
    assert math.isinf(table.loc[8000, "median ms"])


def test_e6_labels_skipped_searches_apart_from_those_that_ran():
    rows = pd.DataFrame([
        {"complete": True, "generated": 219_455_251},
        {"complete": False, "generated": 1_200_054_459},
        {"complete": False, "generated": 0},
    ])
    assert [e6.outcome(row) for _, row in rows.iterrows()] == ["complete", "budget reached", "skipped"]
