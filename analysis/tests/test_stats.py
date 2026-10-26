"""The pre-registered statistical procedures (stats.py), including comparisons without data."""

import math

import numpy as np
import pytest

from stats import effect_size, equivalent, holm, mann_whitney, ratio_of_medians, wilcoxon


def test_holm_follows_the_step_down_procedure():
    # Sorted: 0.005 x 4 = 0.02, 0.01 x 3 = 0.03, 0.03 x 2 = 0.06, then 0.04 x 1 = 0.04,
    # raised to 0.06 so the adjusted values never decrease.
    assert holm([0.01, 0.04, 0.03, 0.005]) == pytest.approx([0.03, 0.06, 0.06, 0.02])
    assert holm([0.6, 0.9]) == pytest.approx([1.0, 1.0])
    assert holm([]) == []


def test_holm_leaves_a_comparison_without_data_out_of_the_family():
    adjusted = holm([0.01, float("nan"), 0.04])
    # A family of two: 0.01 x 2 and 0.04 x 1.
    assert adjusted[0] == pytest.approx(0.02)
    assert math.isnan(adjusted[1])
    assert adjusted[2] == pytest.approx(0.04)
    assert math.isnan(holm([float("nan")])[0])


def test_a_test_without_data_has_no_p_value():
    assert math.isnan(wilcoxon([], []))
    assert math.isnan(mann_whitney([], [1.0, 2.0]))
    assert math.isnan(mann_whitney([1.0, 2.0], []))


def test_identical_samples_show_no_difference():
    assert wilcoxon([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == 1.0
    assert mann_whitney([5.0, 5.0], [5.0, 5.0]) == 1.0


def test_the_one_sided_wilcoxon_test_asks_whether_b_is_greater():
    a = np.arange(1, 21, dtype=float)
    b = a + np.arange(1, 21)
    assert wilcoxon(a, b, alternative="greater") < 0.001
    assert wilcoxon(b, a, alternative="greater") > 0.99


def test_the_ratio_is_b_over_a_with_a_reproducible_interval():
    rng = np.random.default_rng(1)
    a = rng.normal(100, 5, 200)
    b = a * 1.2 + rng.normal(0, 1, 200)
    ratio, low, high = ratio_of_medians(a, b, paired=True)
    assert ratio == pytest.approx(np.median(b) / np.median(a))
    assert low <= ratio <= high
    assert ratio_of_medians(a, b, paired=True) == (ratio, low, high)


def test_a_ratio_without_data_or_to_a_zero_median_is_undefined():
    for result in (
        ratio_of_medians([], [1.0], paired=False),
        ratio_of_medians([1.0], [], paired=False),
        ratio_of_medians([0.0, 0.0, 1.0], [1.0, 2.0, 3.0], paired=True),
    ):
        assert all(math.isnan(value) for value in result)


def test_paired_samples_must_have_the_same_length():
    with pytest.raises(ValueError):
        ratio_of_medians([1.0, 2.0], [1.0], paired=True)


def test_the_paired_effect_is_the_median_of_the_differences():
    a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    b = np.array([5.0, 1.0, 9.0, 2.0, 8.0])
    effect, low, high = effect_size(a, b, paired=True)
    # The differences are 4, -1, 6, -2, and 3, with median 3; the difference of the
    # medians would be 5 - 3 = 2.
    assert effect == 3.0
    assert low <= effect <= high
    assert effect_size(a, b, paired=True) == (effect, low, high)


def test_the_unpaired_effect_is_the_difference_of_medians():
    a = np.array([1.0, 2.0, 3.0, 100.0, 200.0])
    b = np.array([10.0, 11.0, 12.0, 13.0, 14.0])
    effect, low, high = effect_size(a, b, paired=False)
    assert effect == pytest.approx(12.0 - 3.0)
    assert low <= effect <= high


def test_an_effect_without_data_is_undefined():
    for result in (effect_size([], [1.0], paired=False), effect_size([1.0], [], paired=True)):
        assert all(math.isnan(value) for value in result)
    with pytest.raises(ValueError):
        effect_size([1.0, 2.0], [1.0], paired=True)


def test_equivalence_needs_the_whole_interval_within_five_percent():
    assert equivalent(0.95, 1.05)
    assert equivalent(0.99, 1.01)
    assert not equivalent(0.94, 1.01)
    assert not equivalent(0.99, 1.06)
    assert not equivalent(float("nan"), float("nan"))
