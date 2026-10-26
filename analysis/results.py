"""Locating and loading raw results written by the experiment runner.

Results live in experiments/results/<experiment>/<UTC time>-<commit>/ (see
experiments/README.md). Analysis always uses the most recent folder of an experiment.
"""

import tomllib
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "experiments" / "results"
FIGURES = Path(__file__).resolve().parent / "figures"
REPORT_FIGURES = REPO / "docs" / "figures"


def latest(experiment: str) -> Path | None:
    """Return the most recent results folder of an experiment, or None if there is none."""
    folder = RESULTS / experiment
    if not folder.is_dir():
        return None
    runs = sorted(p for p in folder.iterdir() if p.is_dir())
    return runs[-1] if runs else None


def manifest(folder: Path) -> dict:
    """Return a results folder's manifest.toml."""
    with open(folder / "manifest.toml", "rb") as file:
        return tomllib.load(file)


# Validation runs written before manifests named their algorithm. All of them checked Raft:
# the runner could not run anything else when they were made.
RAFT_ONLY_RUNS = {"20261020T173107Z-a1944ad"}


def algorithm(folder: Path) -> str:
    """Return the algorithm a single-algorithm results folder checked."""
    named = manifest(folder).get("algorithm")
    if named is not None:
        return named
    if folder.name in RAFT_ONLY_RUNS:
        return "raft"
    raise ValueError(f"{folder} does not name its algorithm")


def latest_by_algorithm(experiment: str) -> dict[str, Path]:
    """Return the most recent results folder of a single-algorithm experiment, such as the
    validation campaign, for each algorithm."""
    folder = RESULTS / experiment
    if not folder.is_dir():
        return {}
    found: dict[str, Path] = {}
    for run in sorted(p for p in folder.iterdir() if p.is_dir()):
        found[algorithm(run)] = run
    return found


def load(folder: Path, name: str) -> pd.DataFrame:
    """Load one CSV file from a results folder."""
    return pd.read_csv(folder / name, keep_default_na=True)


def describe(folder: Path) -> str:
    """Return the results folder relative to the repository, for printing."""
    return str(folder.relative_to(REPO))


def figure_path(name: str, report: bool = False) -> Path:
    """Return the path for a generated figure, creating its folder if needed. Figures that
    docs/REPORT.md and docs/SLIDES.md show go to docs/figures/, which is committed; the
    others go to analysis/figures/, which is not."""
    folder = REPORT_FIGURES if report else FIGURES
    folder.mkdir(exist_ok=True)
    return folder / name
