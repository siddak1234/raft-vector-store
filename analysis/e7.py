"""E7: implementation (docs/RESEARCH.md §6, H5; descriptive).

M18, protocol size: message types and persistent state fields. M19, code size and
complexity per module, from rust-code-analysis-cli at the data-collection tag. M20, bugs
from docs/BUGLOG.md by algorithm and subproblem.
"""

import json
import sys
from pathlib import Path

import pandas as pd

from results import REPO, describe, latest

MODULES = ["election.rs", "replication.rs", "commit.rs", "lib.rs"]


def metrics(folder: Path) -> pd.DataFrame:
    rows = []
    for algorithm in ["raft", "paxos"]:
        for module in MODULES:
            path = folder / f"{algorithm}-{module}.json"
            if not path.is_file():
                continue
            data = json.loads(path.read_text())["metrics"]
            rows.append({
                "algorithm": algorithm,
                "module": module,
                "SLOC": data["loc"]["sloc"],
                "LLOC": data["loc"]["lloc"],
                "cyclomatic": data["cyclomatic"]["sum"],
                "cognitive": data["cognitive"]["sum"],
            })
    return pd.DataFrame(rows)


def bugs() -> pd.DataFrame:
    rows = []
    for line in (REPO / "docs" / "BUGLOG.md").read_text().splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) >= 7 and cells[0][:4].isdigit():
            rows.append({"algorithm": cells[1], "subproblem": cells[2]})
    return pd.DataFrame(rows, columns=["algorithm", "subproblem"])


def main() -> int:
    folder = latest("E7")
    if folder is None:
        print("E7: no results yet (run `cargo xtask complexity`)")
        return 0
    print(f"E7 results: {describe(folder)}")
    protocol = json.loads((folder / "protocol.json").read_text())
    print("\nM18, protocol size:")
    for algorithm, info in protocol.items():
        print(f"  {algorithm}: {len(info['message_types'])} message types ({', '.join(info['message_types'])}); "
              f"{len(info['persistent_fields'])} persistent state fields ({', '.join(info['persistent_fields'])})")
    table = metrics(folder)
    print("\nM19, code size and complexity per module:")
    print(table.to_string(index=False))
    totals = table.groupby("algorithm")[["SLOC", "LLOC", "cyclomatic", "cognitive"]].sum()
    print("\nTotals:")
    print(totals.to_string())
    log = bugs()
    print("\nM20, bugs in docs/BUGLOG.md by algorithm and subproblem:")
    if log.empty:
        print("  none")
    else:
        print(log.value_counts().sort_index().to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
