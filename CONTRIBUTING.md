# Contributing

These rules keep the code, the experiment data, and the documents we submit consistent and
reproducible. `cargo xtask check-repo` enforces the ones that can be checked automatically,
and CI runs it on every push to `main`.

## Setup

1. Clone the repository outside iCloud Drive, Dropbox, or any other synced folder (for
   example into `~/dev`). Sync tools can corrupt the `.git` folder.
2. Install Rust with [rustup](https://rustup.rs). The first build installs the exact
   version pinned in `rust-toolchain.toml`.
3. Install [uv](https://docs.astral.sh/uv/) for the analysis code.
4. Run `cargo xtask ci`. It runs every check that CI runs.

## Layout

| Path | Contents |
|---|---|
| `crates/` | All system code, one folder per crate (see `docs/PLAN.md` §9) |
| `xtask/` | Project commands, run as `cargo xtask <command>` |
| `experiments/configs/` | Experiment definitions, versioned like code |
| `experiments/results/` | Raw results, written only by the experiment runner |
| `analysis/` | Python statistics and figures |
| `docs/` | Plan, research protocol, specifications, bug log |

Nothing else belongs at the top level. Datasets, node data, and build output are kept out
of the repository.

## Commands

Every project task is a `cargo xtask` subcommand; `cargo xtask help` lists them. Do not add
shell scripts or one-off tools. If a task is worth repeating, add it to `xtask`. Python is
used only in `analysis/`.

## Workflow

- Commit directly to `main`. Run `cargo xtask ci` before every push; CI runs the same
  checks again on GitHub after the push.
- `main` cannot be force-pushed or deleted, so published history is never rewritten.
- Pull `main` before starting work (`git pull --ff-only`) so commits stay in a straight
  line.
- The project does not use branches or pull requests.

## Commit messages

- Write a summary line in the imperative mood, under 72 characters, such as
  `Add election timer to Raft`.
- Explain why in the body when the reason is not obvious.
- Commits list only team members as authors. Do not add co-author or generated-by lines.

## Dependencies

- Each external dependency's version is declared once, under `[workspace.dependencies]`
  in the root `Cargo.toml`. Crates refer to it with `name.workspace = true`.
- Add a dependency in the commit that first uses it.
- Dependency rules between crates:
  1. `consensus`, `raft`, `paxos`, `sim`, and `modelcheck` perform no I/O, so they may not
     use async or networking libraries.
  2. Only `controlplane` and `bench` may depend on `raft` or `paxos`. The simulator, the
     model checker, and the data plane cannot treat one algorithm differently.
  3. `datanode` and `client` may not depend on `consensus`.
  4. No crate may depend on `bench` or `xtask`.

  The full table is `ALLOWED_DEPS` in `xtask/src/check_repo.rs`. A new crate must be added
  to it.

## Experiment results

- Results are written only by the experiment runner (`cargo xtask validate` and
  `cargo xtask experiment <id>`). The runner refuses to start with uncommitted changes, so
  every result maps to an exact commit. Commit new results before starting the next run.
- To try something out without making an official result, pass `--scratch <dir>` with a
  folder outside the repository.
- Existing results are never edited, moved, or deleted. To exclude a bad run, record it and
  the reason in the analysis code.
- See `experiments/README.md` for the folder format.

## Frozen documents

`docs/RESEARCH.md` and `docs/PAXOS_SPEC.md` are tagged `protocol-v1` before any data is
collected. After that, they change only through a dated entry in their Amendments section
that states the reason.

## Bugs

Record every bug found in a consensus implementation or the test harness in
`docs/BUGLOG.md`, in the same commit as the fix. The bug log is metric M20.

## What `check-repo` enforces

1. Only the entries in the layout above exist at the top level.
2. Every folder in `crates/` is a workspace crate.
3. No file is larger than 1 MiB outside `experiments/results/`.
4. No datasets or archives (`.hdf5`, `.h5`, `.npy`, `.npz`, `.fvecs`, `.ivecs`, `.zip`,
   `.tar`, `.gz`, `.tgz`) anywhere.
5. No shell scripts or notebooks anywhere, and Python files only in `analysis/`.
6. `docs/` contains only Markdown, PNG, and SVG files.
7. Files already committed under `experiments/results/` are not modified, renamed, or
   deleted.
8. Crate dependencies follow the rules above.
