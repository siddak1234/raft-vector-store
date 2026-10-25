# Multi-Paxos Specification

**Status:** draft for review. Frozen with the tag `protocol-v1`; after that, changes are
made only in the Amendments section.

## 1. Purpose

This document defines the Multi-Paxos variant compared against Raft (`docs/PLAN.md` §4.3).
Multi-Paxos has no single agreed specification (Raft paper §3), so our results apply to
this variant. It follows Howard & Mortier [2], who describe Paxos in Raft's terms so that
the two algorithms differ mainly in leader election. Remaining details follow van Renesse
& Altinbuken [3]. Where we had to choose, the choice matches Raft's behavior unless the
difference is part of the comparison.

## 2. Differences from Raft

| | Raft | This Multi-Paxos |
|---|---|---|
| Who can become leader | Only a candidate whose log is at least as up to date as a majority's (§5.4.1) | Any server |
| Terms | Any server may campaign in any term | Each term belongs to one server |
| Election messages | `RequestVote` carries the candidate's last log index and term | `Promise` carries the voter's entries above the candidate's commit index |
| New leader's log | Already contains every committed entry | Built from the promises, then every entry above the commit index is re-proposed in the new term |
| Follower log | Must match the leader's log up to the last new entry (consistency check) | Each index is accepted on its own, so a log can have gaps |
| Committing | Count replicas of an entry from the current term | Count acceptances in the current term |

## 3. State

**Persistent** (written through `Storage`; the driver syncs it before any output):

- `currentTerm`: the highest term this server has seen. A server promises a term by
  advancing `currentTerm` to it. Initially 0. Stored as `HardState::term`;
  `HardState::voted_for` is unused.
- `log[i]`: the entry this server accepted at index `i`, with the term in which it was
  accepted. Indexes may be missing.

**Volatile, all servers:**

- `commitIndex`: the highest index `c` such that every index `1..=c` is known to be chosen
  and this server stores the chosen entry at each of them. Initially 0.
- `lastApplied`: the highest index passed to the state machine. Initially 0.

**Volatile, candidates:** the promises received in the current term, including the
candidate's own.

**Volatile, leaders** (reset on election):

- `baseIndex`: `commitIndex` at the moment of election. The leader proposes every index
  above it in `currentTerm`.
- `accepted[i]`: the servers, including the leader, known to have accepted index `i` in
  `currentTerm`.
- `followerCommit[f]`: the latest `commitIndex` follower `f` has reported.

**Term ownership.** Let `N` be the cluster size and `rank(s)` the position of server `s` in
the cluster's IDs sorted in ascending order, starting at 0. Server `s` may campaign only in
terms `t` with `t mod N = rank(s)`. `nextOwnTerm(s)` is the smallest such `t` greater than
`currentTerm`. Each term therefore has at most one candidate and at most one leader.

## 4. Messages

| Message | Fields | Direction |
|---|---|---|
| `Prepare` | `term`, `commitIndex` | candidate → all servers |
| `Promise` | `term`, `granted`, `entries`: every `(i, log[i])` with `i` greater than the candidate's `commitIndex` | voter → candidate |
| `Accept` | `term`, `firstIndex`, `entries`, `commitIndex` | leader → follower; empty `entries` is a heartbeat |
| `Accepted` | `term`, `success`, `firstIndex`, `lastIndex`, `commitIndex` (the follower's) | follower → leader; also answers `Commit` |
| `Commit` | `term`, `firstIndex`, `entries` (chosen entries), `commitIndex` | leader → lagging follower |

Every entry in an `Accept` has `term` equal to the message's `term`. A `Promise` is sent
as one message regardless of how many entries it carries; that cost is what experiment E3
measures.

## 5. Rules

### All servers

1. If a message's `term` is greater than `currentTerm`, set `currentTerm` to it and
   become a follower.
2. If `commitIndex > lastApplied`, apply entries `lastApplied + 1 ..= commitIndex` in
   order.

### Followers

1. If the election timer fires before the follower receives an `Accept` or `Commit` from
   the current leader, or grants a promise, become a candidate.
2. On `Prepare(term, c)`:
   - If `term ≥ currentTerm`: set `currentTerm = term`, reply
     `Promise(term, granted = true, entries = every log[i] with i > c)`, and reset the
     election timer.
   - Otherwise reply `Promise(currentTerm, granted = false, [])`.
3. On `Accept(term, first, entries, leaderCommit)`:
   - If `term < currentTerm`, reply `Accepted(currentTerm, success = false)`.
   - Otherwise:
     1. Set `currentTerm = term` and record the sender as leader.
     2. Reset the election timer.
     3. Write each entry at `first, first + 1, …`, replacing any entry already there.
     4. Advance `commitIndex` while `commitIndex < leaderCommit` and
        `log[commitIndex + 1]` exists with term equal to `term`.
     5. Reply `Accepted(term, success = true, first, last, commitIndex)`.
4. On `Commit(term, first, entries, leaderCommit)` with `term ≥ currentTerm`:
   1. Accept the sender as leader, as for `Accept`.
   2. Write each entry whose index is above `commitIndex`.
   3. If `first ≤ commitIndex + 1`, set `commitIndex` to the larger of `commitIndex` and
      the message's last index, then advance it as in rule 3.
   4. Reply `Accepted(term, success = true, commitIndex)` with an empty range.

### Candidates

1. On becoming a candidate:
   1. Set `currentTerm = nextOwnTerm`.
   2. Record a promise from itself, holding its own entries above `commitIndex`.
   3. Send `Prepare(currentTerm, commitIndex)` to every other server.
   4. Set the election timer.
2. On a granted `Promise` for `currentTerm` from a server not yet counted, record its
   entries.
3. On promises from a majority, including its own, become leader.
4. If the election timer fires, start a new election.

### Becoming leader

1. Let `m` be the highest index in any recorded promise, or `commitIndex` if there is
   none.
2. For each index `i` in `commitIndex + 1 ..= m`, choose the entry with the highest term
   among the promises that contain `i`. If no promise contains `i`, choose a no-op.
3. Write each chosen entry at its index with its term set to `currentTerm`. This
   re-proposes it in the new term.
4. Append a no-op at `m + 1` in `currentTerm`, as Raft does (§8).
5. Set `baseIndex = commitIndex` and `accepted[i] = {self}` for every
   `i` in `baseIndex + 1 ..= m + 1`.
6. Cancel the election timer and send `Accept` messages, as described under Leaders.

### Leaders

1. On a client command, append it at `lastIndex + 1` in `currentTerm` and add itself to
   that index's `accepted` set.
2. On each heartbeat, send every follower an `Accept` with the oldest entries above
   `baseIndex` that the follower has not acknowledged. The message carries up to
   `max_entries_per_message` entries, and no more than `max_entries_in_flight`
   unacknowledged entries are outstanding per follower. With nothing to send, the
   `Accept` is empty. New client commands are sent the same way without waiting for the
   heartbeat.
3. On `Accepted(term = currentTerm, success = true, first, last, c)` from follower `f`:
   1. Add `f` to `accepted[i]` for each `i` in `first ..= last`, and set
      `followerCommit[f] = c`.
   2. An index whose `accepted` set is a majority is chosen. Advance `commitIndex` while
      the next index is chosen.
   3. If `c < commitIndex` and `f` has not accepted index `c + 1` in `currentTerm`, send
      `Commit` with the chosen entries starting at `c + 1`, up to
      `max_entries_per_message` of them.
4. **Leadership confirmation (Raft §8).** On `confirm_leadership(r)`:
   1. Send a round of heartbeats immediately.
   2. Confirm `r` once both hold: the leader's no-op from this term is committed, and a
      majority, including the leader, has replied with `Accepted` in `currentTerm` to
      heartbeats sent after the request.
   3. The confirmation reports `read_index`: the `commitIndex` when the request arrived,
      or when the no-op committed, whichever is later.
   4. If the leader steps down first, report `LeadershipLost(r)`.

## 6. Safety Argument

**Chosen.** An entry is chosen at index `i` in term `T` when a majority has accepted it at
`i` in `T`. A leader writes at most one entry per index per term, so `(i, T)` identifies
it.

**P1.** If entry `e` is chosen at index `i` in term `T`, every entry accepted at `i` in a
term `T' > T` has the same payload as `e`.

*Argument (induction on `T'`).* The leader of `T'` received promises from a majority `Q'`,
which shares at least one server `s` with the majority `Q` that accepted `e` in `T`. Server
`s` accepted `e` before promising `T'`, because after promising it rejects messages from
lower terms. The entry `s` reported at `i` therefore has a term in `[T, T')`, and by the
induction hypothesis its payload equals `e`'s. The leader keeps the highest-term entry at
`i` among the promises; its term is also in `[T, T')`, so its payload equals `e`'s. If
`i` is at or below the candidate's `commitIndex`, voters do not report it, but the
candidate already stores the chosen entry there by the definition of `commitIndex`.

**P2.** A server marks index `i` committed only if it stores the chosen entry there.

*Argument.* A follower advances `commitIndex` past `i` in one of two ways:

- **It accepted `(i, T)` from the leader of `T`, and that leader's `commitIndex` is at
  least `i`.** The leader sends `Accept` only above `baseIndex`, so `i > baseIndex`. A
  leader's `commitIndex` passes such an index only after a majority accepted it in `T`.
  The follower's entry is therefore the chosen one.
- **It installed the entry from a `Commit` message.** `Commit` carries entries at or below
  the leader's `commitIndex`, which the leader stores with their chosen payloads.

**State Machine Safety.** By P1, at most one payload can be chosen at an index. By P2,
every server that applies index `i` applies that payload. Entries are applied in index
order without gaps.

**Election Safety.** Each term belongs to one server, and that server becomes leader only
with promises from a majority. There is at most one leader per term.

**Liveness.** As in Raft, it depends on randomized election timeouts drawn from the same
distribution (`consensus::Config`).

## 7. Behavior Matched with Raft

Both implementations share these behaviors, so differences in results come from the
algorithms (`docs/PLAN.md` §5):

- A no-op entry appended on election.
- Heartbeats carry the leader's commit index; unacknowledged entries are resent on each
  heartbeat.
- `max_entries_per_message` and `max_entries_in_flight` from the shared `Config`.
- The same leadership-confirmation procedure.
- Persistence through the driver's `Storage::sync` after every input.
- Election timeouts drawn from the shared `Config` and `Rng`.
- No PreVote, CheckQuorum, read leases, or flexible quorums.

## 8. Not Included

Membership changes, log compaction, and snapshots. The Raft implementation omits them too.

## Amendments

### A1. 2026-10-20: Heartbeat round numbers

`Accept` and `Accepted` carry a `round` number, as Raft's `AppendEntries` and its reply do.
The leader starts a new round with every heartbeat and with every leadership confirmation
request. Rule 4 under Leaders counts only `Accepted` replies whose round is at least the
request's round. The research protocol records the same detail (`docs/RESEARCH.md`,
amendment A1.3).

### A2. 2026-10-24: Implementation details fixed before data collection

These details are not specified above. They were fixed in `crates/paxos` before any
Multi-Paxos data was collected.

1. **Empty ranges.** An `Accepted` acknowledges the indexes `firstIndex ..= lastIndex`; the
   range is empty when `lastIndex < firstIndex`. The reply to an empty `Accept` uses the
   `Accept`'s `firstIndex`, and the reply to a `Commit` uses
   `commitIndex + 1 ..= commitIndex`.
2. **`Commit` from an earlier term.** Rule 4 under Followers covers only
   `term ≥ currentTerm`. A `Commit` with a lower term is answered like an `Accept` with a
   lower term: `Accepted(currentTerm, success = false)`, so the old leader learns of the
   newer term.
3. **Rounds in replies to `Commit`.** An `Accepted` that answers a `Commit` carries round 0,
   so it never counts toward a leadership confirmation, which counts only replies to
   heartbeats (amendment A1).
4. **`Commit` flow control.** At most one `Commit` per follower is unanswered at a time.
   The leader sends the next one when the follower reports a commit index at or above the
   first index of the outstanding one, and may resend after its next heartbeat. This plays
   the role of Raft's probing mode (`docs/RESEARCH.md` amendment A1.4): replies to
   pipelined `Accept` messages do not each trigger the same `Commit`.
5. **Tracking acknowledgments.** For each follower the leader keeps the lowest index above
   `baseIndex` the follower has not accepted in `currentTerm`, the accepted indexes above
   it, and the next index never sent. A heartbeat sends the oldest unacknowledged entries,
   as rule 2 under Leaders states. New entries are sent from the next index never sent, and
   `max_entries_in_flight` counts from the lowest unacknowledged index.
6. **`followerCommit` is the latest report, not the highest.** A follower that restarts
   reports commit index 0, and the leader must then send it `Commit` messages again.
7. **Acceptance sets of chosen indexes are dropped.** Once an index is committed, the leader
   no longer needs `accepted[i]` for it. This saves memory only.
8. **Ranks.** `rank(s)` uses the cluster's IDs sorted in ascending order with duplicates
   removed. With IDs `1..=N`, the server with ID `r + 1` owns the terms `t` with
   `t mod N = r`.


### A3. 2026-10-24: Status

The status line at the top was written before the freeze and was not updated. This
specification has been frozen since the tag `protocol-v1`.

## References

1. D. Ongaro and J. Ousterhout. *In Search of an Understandable Consensus Algorithm
   (Extended Version)*. 2014. https://raft.github.io/raft.pdf
2. H. Howard and R. Mortier. *Paxos vs Raft: Have We Reached Consensus on Distributed
   Consensus?* PaPoC 2020. https://arxiv.org/abs/2004.05074
3. R. van Renesse and D. Altinbuken. *Paxos Made Moderately Complex.* ACM Computing
   Surveys, 2015.
