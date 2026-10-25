"""Multi-Paxos-against-Raft comparisons (compare.py): a configuration without data gets no
result and does not change the correction of the others."""

import math

import pandas as pd
import pytest

from compare import algorithms


def results(missing_algorithm: str) -> pd.DataFrame:
    """Two configurations of 30 seeds; in the second, `missing_algorithm` has no values."""
    rows = []
    for seed in range(1, 31):
        rows += [
            {"config": "both", "algorithm": "raft", "seed": seed, "value": float(seed)},
            {"config": "both", "algorithm": "paxos", "seed": seed, "value": seed + 40.0 + seed % 7},
            {"config": "one", "algorithm": "raft", "seed": seed, "value": float(seed)},
            {"config": "one", "algorithm": "paxos", "seed": seed, "value": float(seed)},
        ]
    frame = pd.DataFrame(rows)
    gone = (frame["config"] == "one") & (frame["algorithm"] == missing_algorithm)
    frame.loc[gone, "value"] = float("nan")
    return frame


@pytest.mark.parametrize("is_paired", [True, False])
def test_a_configuration_without_data_has_no_p_value(is_paired):
    table = algorithms(results("paxos"), ["config"], [("value", "value")], is_paired).set_index("config")
    assert math.isnan(table.loc["one", "p"])
    assert math.isnan(table.loc["one", "p (Holm)"])
    assert table.loc["one", "90% CI"] == "n/a"
    # The other configuration is a family of one, so its p-value is not adjusted.
    assert table.loc["both", "p (Holm)"] == pytest.approx(table.loc["both", "p"])
    assert table.loc["both", "p (Holm)"] < 0.05


def test_medians_are_paxos_and_raft_in_their_own_columns():
    table = algorithms(results("raft"), ["config"], [("value", "value")], True).set_index("config")
    assert table.loc["both", "raft median"] == pytest.approx(15.5)
    assert table.loc["both", "paxos median"] > table.loc["both", "raft median"]
    assert math.isnan(table.loc["one", "raft median"])
