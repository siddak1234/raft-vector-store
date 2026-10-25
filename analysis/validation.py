"""Validation campaign summary (docs/RESEARCH.md §9.1, metrics M12 and M13).

Each algorithm is validated by its own run of `cargo xtask validate --algorithm NAME`; the
latest run of each is summarized.
"""

import sys

from results import describe, latest_by_algorithm, load

SECONDS_PER_SEED = 60


def summarize(algorithm: str, folder) -> None:
    runs = load(folder, "runs.csv")
    failed = runs[runs["violations"] > 0]
    hours = len(runs) * SECONDS_PER_SEED / 3600

    print(f"Validation results for {algorithm}: {describe(folder)}")
    print(f"  seeds:               {len(runs)} ({runs['seed'].min()}-{runs['seed'].max()})")
    print(f"  simulated time:      {hours:.1f} hours")
    print(f"  servers:             {runs['servers'].value_counts().sort_index().to_dict()}")
    print(f"  faults per seed:     mean {runs['faults'].mean():.1f}")
    print(f"  elections per seed:  mean {runs['max_term'].mean():.1f} (highest term reached)")
    print(f"  commits per seed:    mean {runs['replies'].mean():.0f}")
    print(f"  seeds with violations (M12): {len(failed)}")
    for _, row in failed.head(10).iterrows():
        print(f"    seed {row['seed']}: {row['first_violation']}")


def main() -> int:
    folders = latest_by_algorithm("validation")
    if not folders:
        print("validation: no results yet (run `cargo xtask validate`)")
        return 0
    for algorithm in sorted(folders):
        summarize(algorithm, folders[algorithm])
    return 0


if __name__ == "__main__":
    sys.exit(main())
