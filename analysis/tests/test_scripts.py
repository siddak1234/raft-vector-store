"""Every analysis script runs on the results committed in the repository, and `cargo xtask
figures` runs every one of them."""

import importlib
from pathlib import Path

import pytest

import results

ANALYSIS = Path(__file__).resolve().parent.parent
HELPERS = {"compare", "results", "stats"}
SCRIPTS = sorted(path.stem for path in ANALYSIS.glob("*.py") if path.stem not in HELPERS)


@pytest.mark.parametrize("name", SCRIPTS)
def test_the_script_runs_on_the_committed_results(name, monkeypatch, tmp_path):
    monkeypatch.setattr(results, "FIGURES", tmp_path / "analysis")
    monkeypatch.setattr(results, "REPORT_FIGURES", tmp_path / "report")
    assert importlib.import_module(name).main() == 0
    # The report's figures are committed, and `cargo xtask check-repo` rejects files over
    # 1 MiB.
    too_large = [p.name for p in (tmp_path / "report").glob("*") if p.stat().st_size > 1024 * 1024]
    assert too_large == []


def test_xtask_figures_runs_every_script():
    listed = (ANALYSIS.parent / "xtask" / "src" / "experiments.rs").read_text()
    assert [name for name in SCRIPTS if f'"{name}.py"' not in listed] == []
