"""Locating and loading raw results written by the experiment runner.

Results live in experiments/results/<experiment>/<UTC time>-<commit>/ (see
experiments/README.md). Analysis always uses the most recent folder of an experiment.
"""

from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "experiments" / "results"
FIGURES = Path(__file__).resolve().parent / "figures"


def latest(experiment: str) -> Path | None:
    """Return the most recent results folder of an experiment, or None if there is none."""
    folder = RESULTS / experiment
    if not folder.is_dir():
        return None
    runs = sorted(p for p in folder.iterdir() if p.is_dir())
    return runs[-1] if runs else None


def load(folder: Path, name: str) -> pd.DataFrame:
    """Load one CSV file from a results folder."""
    return pd.read_csv(folder / name, keep_default_na=True)


def describe(folder: Path) -> str:
    """Return the results folder relative to the repository, for printing."""
    return str(folder.relative_to(REPO))


def figure_path(name: str) -> Path:
    """Return the path for a generated figure, creating analysis/figures/ if needed."""
    FIGURES.mkdir(exist_ok=True)
    return FIGURES / name
