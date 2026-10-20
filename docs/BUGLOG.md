# Bug Log

Every bug found in a consensus implementation or the test harness is recorded here, in the
same pull request as the fix. The log is the data for metric M20 (`docs/PLAN.md` §6).

## Columns

- **Date:** date the bug was found (YYYY-MM-DD).
- **Algorithm:** `raft`, `paxos`, or `shared` (simulator, checkers, storage, transport).
- **Subproblem:** `election`, `replication`, `safety`, or `other`.
- **Found by:** the checker or test that caught it, such as `sim: state machine safety`,
  `modelcheck`, `unit test`, `real cluster`, or `review`.
- **Seed:** simulator seed that reproduces it, if any.
- **Description:** what went wrong, in one or two sentences.
- **Fix:** pull request number.

## Log

| Date | Algorithm | Subproblem | Found by | Seed | Description | Fix |
|---|---|---|---|---|---|---|
