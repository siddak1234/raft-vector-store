"""Statistical procedures fixed in advance in docs/RESEARCH.md §8.

- Paired comparisons (simulator, same seed for both arms): Wilcoxon signed-rank test.
- Unpaired comparisons: Mann-Whitney U test.
- Equivalence: the 90% percentile bootstrap interval of a ratio of medians must lie within
  [0.95, 1.05]; this is the two one-sided tests procedure at alpha = 0.05.
- Multiple comparisons: Holm-Bonferroni within each experiment.
- Bootstrap: 10,000 resamples with a fixed seed.
"""

import numpy as np
from scipy import stats

ALPHA = 0.05
EQUIVALENCE_MARGIN = 0.05
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 0


def ratio_of_medians(a, b, paired: bool, level: float = 0.90) -> tuple[float, float, float]:
    """Return median(b) / median(a) and its percentile bootstrap interval.

    Paired samples are resampled as pairs, so a and b must then have the same length and
    order (one value per seed).
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
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


def equivalent(low: float, high: float, margin: float = EQUIVALENCE_MARGIN) -> bool:
    """Whether an interval for a ratio lies within [1 - margin, 1 + margin]."""
    return 1 - margin <= low and high <= 1 + margin


def mann_whitney(a, b) -> float:
    """Two-sided Mann-Whitney U test p-value for unpaired samples."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    if np.all(a == a[0]) and np.all(b == a[0]):
        return 1.0
    return float(stats.mannwhitneyu(a, b, alternative="two-sided").pvalue)


def wilcoxon(a, b, alternative: str = "two-sided") -> float:
    """Wilcoxon signed-rank test p-value for paired samples (b - a)."""
    differences = np.asarray(b, dtype=float) - np.asarray(a, dtype=float)
    if np.all(differences == 0):
        return 1.0
    return float(stats.wilcoxon(differences, alternative=alternative).pvalue)


def holm(p_values: list[float]) -> list[float]:
    """Holm-Bonferroni adjusted p-values, in the original order."""
    count = len(p_values)
    order = sorted(range(count), key=lambda i: p_values[i])
    adjusted = [0.0] * count
    running = 0.0
    for rank, i in enumerate(order):
        running = max(running, min(1.0, (count - rank) * p_values[i]))
        adjusted[i] = running
    return adjusted
