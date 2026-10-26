"""Comparisons (docs/RESEARCH.md §8) and figure helpers shared by the experiment analyses."""

import numpy as np
import pandas as pd
from matplotlib.patches import Patch

from stats import effect_size, holm, mann_whitney, ratio_of_medians, wilcoxon


def paired(frame: pd.DataFrame, column: str, a_mask, b_mask) -> tuple[np.ndarray, np.ndarray]:
    """Values of `column` for the two selections, matched by seed, dropping missing ones."""
    a = frame[a_mask].set_index("seed")[[column]]
    b = frame[b_mask].set_index("seed")[[column]]
    both = a.join(b, lsuffix="_a", rsuffix="_b", how="inner").dropna()
    return both[f"{column}_a"].to_numpy(float), both[f"{column}_b"].to_numpy(float)


def algorithms(frame: pd.DataFrame, keys: list[str], metrics: list[tuple[str, str]], is_paired: bool,
               alternative: str = "two-sided") -> pd.DataFrame:
    """Multi-Paxos against Raft for each configuration and metric, Holm-corrected, with the
    effect size: Multi-Paxos minus Raft, as the median of paired differences or the
    difference of medians, with its 95% bootstrap interval. A configuration without data
    gets NaN for its p-value and is left out of the correction."""
    rows = []
    for config, group in frame.groupby(keys):
        config = config if isinstance(config, tuple) else (config,)
        for column, name in metrics:
            if is_paired:
                a, b = paired(group, column, group["algorithm"] == "raft", group["algorithm"] == "paxos")
                p = wilcoxon(a, b, alternative=alternative)
            else:
                a = group.loc[group["algorithm"] == "raft", column].dropna().to_numpy(float)
                b = group.loc[group["algorithm"] == "paxos", column].dropna().to_numpy(float)
                p = mann_whitney(a, b)
            ratio, low, high = ratio_of_medians(a, b, paired=is_paired)
            effect, effect_low, effect_high = effect_size(a, b, paired=is_paired)
            row = dict(zip(keys, config))
            row.update({
                "metric": name,
                "n": len(a) if is_paired else f"{len(a)}/{len(b)}",
                "raft median": float(np.median(a)) if len(a) else np.nan,
                "paxos median": float(np.median(b)) if len(b) else np.nan,
                "paxos/raft": ratio,
                "90% CI": "n/a" if np.isnan(low) else f"[{low:.3f}, {high:.3f}]",
                "paxos-raft": effect,
                "95% CI": interval(effect_low, effect_high),
                "p": p,
            })
            rows.append(row)
    table = pd.DataFrame(rows)
    if len(table):
        table["p (Holm)"] = holm(table["p"].tolist())
    return table


def interval(low: float, high: float) -> str:
    """An effect-size interval for printing, or "n/a" without data."""
    return "n/a" if np.isnan(low) else f"[{low:.4g}, {high:.4g}]"


def show(title: str, table: pd.DataFrame) -> None:
    print(f"\n{title}")
    if table.empty:
        print("  (no data)")
        return
    print(table.drop(columns=["p"]).to_string(index=False, float_format=lambda v: f"{v:.4g}"))


def cdf(axis, values, label: str, **style) -> None:
    values = np.sort(np.asarray(values, dtype=float))
    values = values[~np.isnan(values)]
    if len(values) == 0:
        return
    axis.step(values, np.arange(1, len(values) + 1) / len(values) * 100, where="post", label=label, **style)


ALGORITHMS = ["raft", "paxos"]
NAMES = {"raft": "Raft", "paxos": "Multi-Paxos"}
COLORS = {"raft": "C0", "paxos": "C1"}


def boxes(axis, frame: pd.DataFrame, category: str, column: str, categories: list) -> None:
    """Box plots of `column` for each value of `category`, Raft and Multi-Paxos side by side.
    Boxes span the quartiles and whiskers the 5th to 95th percentiles; the line in a box is
    the median, and points beyond the whiskers are not drawn."""
    width = 0.38
    for offset, algorithm in zip((-width / 2, width / 2), ALGORITHMS):
        data = [frame.loc[(frame[category] == value) & (frame["algorithm"] == algorithm), column]
                .dropna().to_numpy(float) for value in categories]
        parts = axis.boxplot(data, positions=np.arange(len(categories)) + offset, widths=width * 0.9,
                             whis=(5, 95), showfliers=False, patch_artist=True, manage_ticks=False)
        for box in parts["boxes"]:
            box.set_facecolor(COLORS[algorithm])
            box.set_alpha(0.6)
        for median in parts["medians"]:
            median.set_color("black")
    axis.set_xticks(np.arange(len(categories)))
    axis.set_xticklabels([str(value) for value in categories], fontsize=7)


def legend_handles() -> list:
    """Legend entries for figures drawn with `boxes`."""
    return [Patch(facecolor=COLORS[a], alpha=0.6, label=NAMES[a]) for a in ALGORITHMS]
