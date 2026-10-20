# Bug Log

Every bug found in a consensus implementation or the test harness is recorded here, in the
same commit as the fix. The log is the data for metric M20 (`docs/PLAN.md` §6).

## Columns

- **Date:** date the bug was found (YYYY-MM-DD).
- **Algorithm:** `raft`, `paxos`, or `shared` (simulator, checkers, storage, transport).
- **Subproblem:** `election`, `replication`, `safety`, or `other`.
- **Found by:** the checker or test that caught it, such as `sim: state machine safety`,
  `modelcheck`, `unit test`, `real cluster`, or `review`.
- **Seed:** simulator seed that reproduces it, if any.
- **Description:** what went wrong, in one or two sentences.
- **Fix:** what changed, and the commit that changed it.

## Log

| Date | Algorithm | Subproblem | Found by | Seed | Description | Fix |
|---|---|---|---|---|---|---|
| 2026-10-20 | raft | replication | sim test: random faults (the run never finished) | 2 | The leader sent entries ahead of acknowledgments. After one lost message, the follower rejected every later message, and the leader resent entries for each stale rejection; duplicated messages multiplied the resends into a storm of millions of messages. | Followers are tracked as probing or replicating, and each reply names the message it answers so stale rejections are ignored (commit "Add Raft") |
| 2026-10-20 | shared | other | sim: durability | 2 | Tests checked durability while clients were still sending, before followers had learned the latest commit index, so the check reported violations that were not real. | Added `Simulation::stop_clients`; durability is checked after clients stop and servers catch up (commit "Add Raft") |
