# Data Plane Design: Leases and Replication

**Status:** implemented in Phase 4, whose checks it passes. This document is not part of the
frozen protocol, because the data plane does not affect RQ1–RQ5. It changes like code:
commit the change to `main` with the reason in the commit message.

## 1. Scope

This document specifies how the control plane tracks data-node liveness (leases), how
data nodes replicate shards, and how clients route requests (`docs/PLAN.md` §4.5). The
design uses only two operations from the `Consensus` trait: `propose`, to commit metadata
changes, and `confirm_leadership`, for leader-confirmed reads. It is therefore identical
under Raft and Multi-Paxos.

## 2. Assumptions

- **Faults:**
  - Processes fail by crashing.
  - A data node keeps vectors in memory, so after a restart it is empty and registers with
    a new incarnation number.
  - Control-plane servers recover their state from disk.
- **Network:** messages may be delayed, lost, reordered, or duplicated.
- **Clocks:**
  - Each process measures durations with its own monotonic clock.
  - Clock rates differ by at most a factor `ρ`. We assume `ρ ≤ 10⁻³`, which is far looser
    than real hardware.
  - Nothing is assumed about clock offsets.
- **What depends on what:** safety depends only on the rate bound; liveness depends on
  timely message delivery.

## 3. Control-Plane State

Committed through consensus (`docs/PLAN.md` §4.4):

- **Data node:** ID, address, incarnation, and status (`Alive` or `Dead`).
- **Shard:**
  - `replicas`: the data nodes holding a copy, each with its incarnation
  - `primary`
  - `inSync`: replicas known to hold every acknowledged write; always contains `primary`
  - `epoch`: increases with every change of primary

## 4. Parameters

| Name | Value | Meaning |
|---|---|---|
| `h` | 1 s | Data-node heartbeat interval |
| `L` | 3 s | Lease duration |
| `δ` | 0.5 s | Safety margin |
| `a` | 1 s | How long a primary waits for a backup's acknowledgment before asking to remove it from `inSync` |
| `RF` | 3 | Replicas per shard |
| `minInSync` | 2 | Fewest in-sync replicas with which a shard accepts writes |

**The margin is sufficient.** A leader waits `L + δ` on its own clock, which is at least
`(L + δ)/(1 + ρ)` of real time. A data node's lease of `L` on its clock lasts at most
`L/(1 − ρ)` of real time. The first must not be shorter than the second, which holds when
`δ ≥ 2ρL/(1 − ρ)`. That is about 6 ms for `L = 3 s` and `ρ = 10⁻³`, well below 0.5 s.

## 5. Leases

### Data node `n` with incarnation `k`

1. Every `h`, send `Heartbeat(n, k)` to the control-plane leader, and record the local
   send time `s`.
2. When a reply grants the lease for the heartbeat sent at `s`, set
   `leaseExpiry = max(leaseExpiry, s + L)`.
3. Serve as primary only while the local time is before `leaseExpiry`.
4. If the shard map shows incarnation `k` marked `Dead`, drop every shard and start over
   with a new incarnation, as after a restart. The control plane never grants `(n, k)` a
   lease again (rule 4 below).

### Control-plane leader

1. **Collect heartbeats.** Add each `Heartbeat(n, k)` to the pending batch, then call
   `confirm_leadership` for the batch. If `n` is unknown or `k` is newer than the
   incarnation on record, first propose `RegisterNode(n, address, k)`.
2. **Grant renewals.** On `LeadershipConfirmed(read_index)`:
   1. Wait until the state machine has applied every entry up to `read_index`.
   2. Grant the lease to each heartbeat in the batch whose node is registered with
      incarnation `k` and is `Alive`.
   3. Record `lastGrant[n]`, the local time the grant is sent.
3. **On `LeadershipLost`,** refuse the batch. Data nodes retry with the new leader.
4. **Detect failures.** Let `leaderSince` be the local time this server became leader.
   When the local time exceeds `max(lastGrant[n], leaderSince) + L + δ`, propose
   `MarkNodeDead(n, k)`. Once the leader has proposed this, it never grants to `(n, k)`
   again.

### Claim L1

When `MarkNodeDead(n, k)` is proposed, every lease granted to `(n, k)` has expired.

**Argument.** Take any grant, answering a heartbeat sent at data-node time `s`. The leader
of term `T` sent the grant only after a majority acknowledged `T` in a heartbeat round
that began after the heartbeat arrived, so after `s`.

- **Case 1: the leader that sent the grant proposes `MarkNodeDead`.** It sent the grant
  after `s` and waited `L + δ` of its own clock after that. By the margin argument in
  §4, more than `L` of data-node time has passed since `s`.
- **Case 2: a leader of a later term `T′` proposes it.**
  1. Winning `T′` required votes or promises from a majority, and that majority shares a
     server with the majority that acknowledged `T`.
  2. That server acknowledged `T` before supporting `T′`, because afterwards it rejects
     messages from `T`. So `T′` began after the acknowledgment, and therefore after `s`.
  3. The new leader waits `L + δ` after it took over, so again more than `L` of data-node
     time has passed since `s`.
- **Older leaders.** A leader of a term before `T′` cannot commit anything once `T′` has
  begun, because consensus guarantees this.

Grants after the proposal are also excluded:

- **Same leader:** it stops granting to `(n, k)` once it proposes.
- **Later leaders:** a later leader grants only after applying entries up to its
  `read_index`, so it sees the committed `MarkNodeDead`.

## 6. Replication

### 6.1 Per-replica state

- **The shard log:** a sequence of `(epoch, offset, op)`. The vectors are the result of
  applying the log in offset order.
- **Operations:** `Upsert(id, vector)` and `Delete(id)`. Both are idempotent per `id`, so
  clients can retry safely.

The primary also keeps an `ackSet`: the replicas whose acknowledgments a write requires.
The rules below keep `ackSet ⊇ inSync` (the committed set) at all times.

### 6.2 Writes

1. The client sends `Write(shard, epoch e, op)` to the primary named in its shard map.
2. The primary rejects the write unless all of these hold:
   - it is the primary at epoch `e`
   - its lease is valid
   - `ackSet` has at least `minInSync` members
3. The primary assigns the next offset `o`, appends `(e, o, op)` to its log, and sends
   `Replicate(shard, e, o, op)` to every member of `ackSet` and to every replica that is
   catching up.
4. A backup handles `Replicate` by its epoch:

   | Message epoch | Action |
   |---|---|
   | Lower than the backup's epoch | Reject |
   | Higher | Refuse, reporting the backup's own epoch, so the primary aligns it with `NewEpoch` first (§6.4) |
   | Equal | See the offset rules below |

   Offset rules:
   - **Appending:** if `o` is the next offset, append it.
   - **Duplicate:** if `o` is already stored, acknowledge again.
   - **Gap:** otherwise, ask the primary to resend the missing offsets.

   After storing the write, reply `Ack(shard, e, o)`.
5. The primary acknowledges the client once every member of `ackSet` has acknowledged `o`
   and every earlier offset.
6. If a backup `b` has not acknowledged within `a`, the primary proposes
   `ShrinkInSync(shard, e, b)`. It keeps `b` in `ackSet` until it learns that the change
   is committed, then removes `b` and acknowledges the writes that the remaining members
   hold.

### 6.3 Re-replication

1. **Plan the replacement.** When `MarkNodeDead(n)` is committed, the control-plane leader
   proposes, for each shard on `n`:
   1. `PromotePrimary`, if `n` was the primary (§6.4).
   2. `RemoveReplica(shard, n)`.
   3. `AddReplica(shard, r, e)`. The replacement `r` is the `Alive` node with the fewest
      replicas, ties broken by lowest ID. The leader makes this choice and logs it, so
      applying the log stays deterministic.
2. **Copy the shard.** The primary sends `r` a copy of its log up to some offset, then
   streams later writes to it.
3. **Join the in-sync set.** When `r` has acknowledged every offset up to the primary's
   latest, the primary first adds `r` to `ackSet`, then proposes
   `ExpandInSync(shard, e, r)`.

### 6.4 Promotion

1. **Choose the new primary.** When `MarkNodeDead(n)` is committed and `n` is a shard's
   primary, the control-plane leader proposes `PromotePrimary(shard, p′, e)`, which
   commits epoch `e + 1`.
   - `p′` is the `Alive` member of `inSync` with the lowest ID.
   - If no member of `inSync` is alive, the shard stays unavailable. A replica outside
     `inSync` is never promoted.
2. **Align the backups.** `p′` learns of the promotion from its next heartbeat reply. It
   then sends `NewEpoch(shard, e + 1, endOffset, epochs)` to the other replicas, where
   `endOffset` is its last offset and `epochs` lists, for each epoch in its log, the last
   offset written in that epoch. Each replica:
   1. keeps the longest prefix of its log whose entries are at or below `endOffset` and
      carry the epoch that `p′`'s log has at the same offset, and deletes the rest
   2. adopts epoch `e + 1`
   3. acknowledges with its last offset, from which `p′` sends it the entries it lacks

   Deleting only the entries above `endOffset` is not enough. A replica that missed an
   earlier change of epoch can hold, below `endOffset`, entries from an older epoch that
   `p′` never had. Each epoch has one primary, which writes each offset once, so an epoch
   and an offset identify an entry, and comparing epochs finds where the logs part, as
   Kafka's leader-epoch truncation does (`docs/BUGLOG.md` records the earlier rule).
3. **Start serving.** `p′` accepts writes once its lease is valid and `ackSet` has at
   least `minInSync` members.

Primaries change only through this promotion, after the old primary's node is marked
dead. Planned primary changes are out of scope.

### 6.5 Searches and routing

- **Searches:** the client sends `Search(shard, epoch, query, k)` to each shard's primary.
  - A primary answers only if it is the primary at that epoch and its lease is valid.
  - It returns its local top `k`, and the client merges the results across shards.
- **Shard map:** clients may read it from any control-plane server, and it may be stale.
- **Stale epochs:** a data node rejects any request carrying an old epoch. The client then
  reads the map from the leader (a leader-confirmed read) and retries with backoff.

## 7. Correctness

**C1. At most one serving primary.** At any real time, at most one data node serves a
shard as primary.

*Argument.*

1. A data node serves a shard only if its latest map names it primary at the current
   epoch and its lease is valid.
2. A shard's primary changes from `p` to `p′` only through `PromotePrimary`.
   `PromotePrimary` is proposed only after `MarkNodeDead(p)` is committed.
3. By L1, `p`'s lease expired before that proposal, and `p` cannot renew it afterward.
4. So `p` stopped serving before `p′` was promoted.

**C2. No acknowledged write is lost** while at most `RF − minInSync` replicas of a shard
fail between re-replications.

*Argument.*

1. **Acknowledged writes are on every in-sync replica.** A write is acknowledged only when
   every member of `ackSet` holds it, and `ackSet` always contains the committed
   `inSync`. So every member of `inSync` holds every acknowledged write.
2. **Changes to `inSync` keep this true.** Members leave `inSync` only through committed
   shrinks. A replica joins only after it holds the primary's whole log.
3. **Promotion keeps the writes.** The new primary is chosen from `inSync`, so it holds
   every acknowledged write. A backup deletes only entries that differ from the new
   primary's log (§6.4), so it keeps every acknowledged write it holds.
4. **There is always a survivor.** `minInSync` copies exist for every acknowledged write.
   With at most `RF − minInSync` failures, one of them survives.

**C3. Replicas converge.** After a promotion, every replica that adopts the new epoch
holds a prefix of the new primary's log, and receives the rest from it in offset order.

**C4. Recovery time.** Suppose a majority of the control plane and at least `minInSync`
replicas of a shard are running and connected. After the shard's primary fails, the
shard accepts writes again within about `L + δ + h`, plus the time to commit the
promotion and align the backups. The control plane cannot reassign the shard until the
old lease has expired, so M21 in experiment E8 is expected to be at least `L + δ − h`.

## 8. Messages

| Message | Between |
|---|---|
| `Heartbeat`, `HeartbeatReply` (lease granted or refused, plus shard-map changes since the node's last known version) | Data node ↔ control-plane leader |
| `Write`, `WriteReply` | Client ↔ primary |
| `Replicate`, `ReplicateReply` | Primary ↔ backup |
| `NewEpoch`, `NewEpochReply` (with the new primary's epoch boundaries) | New primary ↔ backup |
| `CopyShard` (log chunks) | Primary → replacement replica |
| `Search`, `SearchReply` | Client ↔ primary |
| `Inspect`, `SetFaults` (tests only) | Test runner → data node: what a replica holds of a shard, for the no-lost-write check; cutting the node off from control-plane servers |

## 9. Out of Scope

Persistent data nodes, planned primary changes, shard splitting, cross-shard
transactions, and Byzantine faults.
