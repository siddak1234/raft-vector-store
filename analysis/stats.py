"""Statistical procedures fixed in advance in docs/RESEARCH.md §8.

- Paired comparisons (simulator, same seed for both arms): Wilcoxon signed-rank test.
- Unpaired comparisons: Mann-Whitney U test.
- Equivalence: the 90% percentile bootstrap interval of a ratio of medians must lie within
  [0.95, 1.05]; this is the two one-sided tests procedure at alpha = 0.05.
- Multiple comparisons: Holm-Bonferroni within each experiment.
- Effect sizes: the median of paired differences (paired) or the difference of medians
  (unpaired), each with a 95% percentile bootstrap interval.
- Bootstrap: 10,000 resamples with a fixed seed.

A comparison without data has no result: its p-value, ratio, and effect size are NaN, never
a number that reads as "significant" or "no difference", and Holm leaves it out of the
family.
"""

import numpy as np
from scipy import stats

ALPHA = 0.05
EQUIVALENCE_MARGIN = 0.05
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 0


def ratio_of_medians(a, b, paired: bool, level: float = 0.90) -> tuple[float, float, float]:
    """Return median(b) / median(a) and its percentile bootstrap interval, or NaNs if either
    sample is empty.

    Paired samples are resampled as pairs, so a and b must then have the same length and
    order (one value per seed).
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    if len(a) == 0 or len(b) == 0:
        return float("nan"), float("nan"), float("nan")
    if np.median(a) == 0:
        # A ratio to a zero median is undefined; such metrics are compared by test only.
        return float("nan"), float("nan"), float("nan")
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    if paired:
        if len(a) != len(b):
            raise ValueError("paired samples must have the same length")
        picks = rng.integers(0, len(a), size=(BOOTSTRAP_RESAMPLES, len(a)))
        medians_a = np.median(a[picks], axis=1)
        medians_b = np.median(b[picks], axis=1)
    else:
        medians_a = np.median(a[rng.integers(0, len(a), (BOOTSTRAP_RESAMPLES, len(a)))], axis=1)
        medians_b = np.median(b[rng.integers(0, len(b), (BOOTSTRAP_RESAMPLES, len(b)))], axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        ratios = medians_b / medians_a
    tail = (1 - level) / 2
    low, high = np.nanquantile(ratios, [tail, 1 - tail])
    return float(np.median(b) / np.median(a)), float(low), float(high)


def effect_size(a, b, paired: bool, level: float = 0.95) -> tuple[float, float, float]:
    """Return the effect of b against a and its percentile bootstrap interval, or NaNs if
    either sample is empty: the median of the paired differences b - a for paired samples,
    and median(b) - median(a) for unpaired ones.

    Paired samples are resampled as pairs, so a and b must then have the same length and
    order (one value per seed).
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    if len(a) == 0 or len(b) == 0:
        return float("nan"), float("nan"), float("nan")
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    if paired:
        if len(a) != len(b):
            raise ValueError("paired samples must have the same length")
        differences = b - a
        estimate = np.median(differences)
        picks = rng.integers(0, len(differences), size=(BOOTSTRAP_RESAMPLES, len(differences)))
        resampled = np.median(differences[picks], axis=1)
    else:
        estimate = np.median(b) - np.median(a)
        medians_a = np.median(a[rng.integers(0, len(a), (BOOTSTRAP_RESAMPLES, len(a)))], axis=1)
        medians_b = np.median(b[rng.integers(0, len(b), (BOOTSTRAP_RESAMPLES, len(b)))], axis=1)
        resampled = medians_b - medians_a
    tail = (1 - level) / 2
    low, high = np.quantile(resampled, [tail, 1 - tail])
    return float(estimate), float(low), float(high)


def equivalent(low: float, high: float, margin: float = EQUIVALENCE_MARGIN) -> bool:
    """Whether an interval for a ratio lies within [1 - margin, 1 + margin]."""
    return 1 - margin <= low and high <= 1 + margin


def mann_whitney(a, b) -> float:
    """Two-sided Mann-Whitney U test p-value for unpaired samples; NaN if either is empty."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    if len(a) == 0 or len(b) == 0:
        return float("nan")
    if np.all(a == a[0]) and np.all(b == a[0]):
        return 1.0
    return float(stats.mannwhitneyu(a, b, alternative="two-sided").pvalue)


def wilcoxon(a, b, alternative: str = "two-sided") -> float:
    """Wilcoxon signed-rank test p-value for paired samples (b - a); NaN without pairs."""
    differences = np.asarray(b, dtype=float) - np.asarray(a, dtype=float)
    if len(differences) == 0:
        return float("nan")
    if np.all(differences == 0):
        return 1.0
    return float(stats.wilcoxon(differences, alternative=alternative).pvalue)


def holm(p_values: list[float]) -> list[float]:
    """Holm-Bonferroni adjusted p-values, in the original order. A NaN p-value, a comparison
    without data, is not part of the family and stays NaN."""
    tested = [i for i, p in enumerate(p_values) if not np.isnan(p)]
    order = sorted(tested, key=lambda i: p_values[i])
    adjusted = [float("nan")] * len(p_values)
    running = 0.0
    for rank, i in enumerate(order):
        running = max(running, min(1.0, (len(tested) - rank) * p_values[i]))
        adjusted[i] = running
    return adjusted
