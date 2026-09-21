# MVCC deep-dive + usearch integration notes (2026-09-08)

Conversation log: how TypeDB's MVCC works, what's in the WAL, sequence number
lifecycle, and how a usearch HNSW sidecar index fits the MVCC model.
Companion to `hnsw-sketch.md`.

## Q1: How is MVCC implemented? What happens when two transactions commit at the same time?

### Storage layout

MVCC lives in the `storage` crate on top of RocksDB. Every version of a key is
a separate physical RocksDB key: `[KEY][SEQ][OP]` (`storage/storage.rs:638`).
`SEQ` is the commit sequence number stored **inverted** big-endian, so newer
versions sort first for a given logical key. `OP` is one byte: Insert or
Delete — deletes are tombstone records; nothing is updated in place.

### Reads

A snapshot opens at the current *watermark* sequence number and never blocks.
A `get` is a prefix scan (`storage.rs:481`); the first visible entry
(`version_seq <= open_seq`, `MVCCKey::is_visible_to`) is the newest allowed
version. Delete tombstone → key doesn't exist for you.

### Writes

Transactions buffer everything in an in-memory `OperationsBuffer`
(Insert / Put / Delete per key, plus explicit `LockType::Unmodifiable` /
`Exclusive` locks for higher-level constraints). Nothing touches RocksDB
until commit.

### Concurrent commit path (`snapshot_commit`, `storage.rs:259`)

Optimistic concurrency control, serialized by the WAL:

1. **Get a slot.** Commit record (write buffer + open sequence number) goes to
   the WAL via `sequenced_write`. The WAL atomically hands out monotonically
   increasing sequence numbers — the single serialization point. Two
   "simultaneous" commits become N and N+1. No global commit lock beyond this.
2. **Validate against all concurrent predecessors**
   (`isolation_manager.rs:validate_commit`). The `IsolationManager` keeps a
   `Timeline` of recent commits in fixed-size windows of atomic slots
   (`Empty → Pending → Validated/Aborted → Applied`). Committer N+1 checks
   against every commit in `(open_seq, N+1)`. Records evicted from the
   in-memory window are replayed from the WAL on disk.
3. **Conflict rules** (`record.rs:compute_dependency`) — not simple
   write-write overlap:
   - I delete a key the predecessor locked `Unmodifiable` → `DeletingRequiredKey`
   - I locked `Unmodifiable`, predecessor deleted it → `RequireDeletedKey`
   - Both hold `Exclusive` on the same key → `ExclusiveLock`

   Overlapping Puts don't conflict — the later committer's `reinsert` flag is
   fixed up (`DependentPut`) so it knows whether it still needs to physically
   write. Blind write-write overlaps generally don't abort.
4. **The racy bit.** If a concurrent predecessor is still `Pending`, the later
   committer spin-waits (`await_pending_status_commits`,
   `isolation_manager.rs:572`) until it resolves — but only when their key
   sets actually interact. Momentarily `Empty` slots also spin
   (`resolve_concurrent`, line 250).
5. **Outcome.** Success: wait for WAL fsync → apply write batch to RocksDB
   (keys stamped with commit seq) → mark Applied → durable
   `StatusRecord(true)`. Conflict: mark Aborted, `StatusRecord(false)`,
   isolation error to client. The **watermark** only advances over a
   contiguous prefix of Applied/Aborted commits (`may_increment_watermark`),
   so readers always see a gap-free prefix of history.

## Q2: What's in the WAL? Does it have MVCC info?

The WAL (`durability/wal.rs`) is a generic sequenced record log. Each entry:
header (sequence number u64, payload length, 1-byte record type) + payload
(lz4-compressed bincode). Three record types (`storage/record.rs`):

- **`CommitRecord` (type 2, sequenced)** — written at start of every commit:
  `operations: OperationsBuffer` (entire logical write set + locks),
  `open_sequence_number` (snapshot version read at), `commit_type`
  (Data/Schema), `snapshot_id` (for client commit-status queries after
  crash/disconnect via `commit_record_exists`).
- **`StatusRecord` (type 1, unsequenced)** — written after validation+apply:
  `{commit_record_sequence_number, was_committed}`. Durable record of whether
  commit N won.
- **`LegacyCommitRecordV1` (type 0)** — pre-upgrade WAL compatibility.

The WAL *is* the source of truth for MVCC:

1. **It assigns the versions** — a key's MVCC stamp is literally the WAL
   sequence number of its CommitRecord (`fetch_add` in `wal.rs:111`).
2. **It records the concurrency interval** — `open_sequence_number` defines
   which predecessors are concurrent; evicted records are replayed from the
   WAL during validation.
3. **Status is two-phase** — CommitRecord alone means "pending"; only a
   matching `StatusRecord(true)` makes it committed. Status records can be
   out of order, so recovery builds a status map first
   (`isolation_manager.rs:205`). Recovery (`recovery/commit_recovery.rs`)
   replays from last checkpoint: true → re-apply, false → skip, pending → 
   re-validate and persist the outcome.

Not in the WAL: the physical `[KEY][SEQ][OP]` keys — constructed at apply
time (`WriteBatches::from_operations`). WAL stores the logical write set;
RocksDB stores the versioned materialization.

## Q3: How/when is the sequence number decided? At transaction open?

Two numbers, decided at different times; only one is newly allocated.

- **Open: no allocation.** The snapshot *adopts* the watermark
  (`open_snapshot_write`, `storage.rs:208`): read
  `highest_committed_snapshot`, spin-wait until the watermark catches up
  (`wait_for_watermark`, `storage.rs:248` — external consistency: an acked
  commit is visible to transactions opened after), use that watermark as
  `open_sequence_number`. Many transactions share the same open number; it's
  a read timestamp, not an identity.
- **Commit: WAL allocates.** The commit sequence number doesn't exist until
  `snapshot_commit` writes the CommitRecord — atomic `fetch_add(1)` on the
  WAL counter (`wal.rs:111`). Read-only transactions never get one.
- Wrinkle: the counter is shared by all WAL records — unsequenced writes
  (StatusRecords) also consume numbers (`wal.rs:142`), so commit sequence
  numbers are monotonic but not contiguous. Timeline/watermark logic handles
  gaps.

Lifecycle: open at watermark W → buffer writes, read at ≤ W → commit: WAL
assigns N > W → validate against (W, N) → data becomes version N.

## Q4: usearch for vector attribute indexing — how to maintain MVCC?

usearch has no MVCC — thread-safe concurrent add/search (internal locking), a
`remove` that tombstones and reuses slots, own file serialization. No
versioning, no snapshots. And none is needed: MVCC stays in storage; the
index is a versionless candidate generator. Each candidate IID goes through a
normal snapshot `get` at `open_sequence_number` — that get *is* the MVCC
check. Invariant (from `hnsw-sketch.md`): **the index may contain too much,
never silently too little.**

Concrete rules, confirmed against the commit path:

1. **Apply to the index only after `snapshot_commit` returns `Ok`** (after
   the isolation manager marks Applied — hook after `storage.rs:314`). Never
   on WAL write (commit can still abort), never pre-commit from the buffer.
2. **Never call `usearch::remove` on attribute delete.** A delete at seq N is
   a tombstone; snapshots open at < N must still see the attribute, and the
   index entry is how they find it. Eager removal = silent recall loss for
   every open older snapshot. The re-check filters it for newer snapshots.
   Physical removal only safe once no open snapshot precedes the delete
   (timeline reader counts could tell you — phase-2 rebuild territory).
3. **Merge own writes.** Buffered vector inserts aren't in the index and the
   re-check can't fix false negatives — brute-force score the transaction's
   `OperationsBuffer` vector writes and merge with index results.
4. **Out-of-order apply across concurrent commits is safe** given 1–2:
   inserts commute (idempotent per key, IIDs value-derived), and nothing is
   removed, so no insert/remove race exists.

Gap not yet in the sketch: **usearch keys are `u64`; attribute IIDs are
variable-length byte strings.** Need a side map (u64 → IID, rebuilt with the
index — trivial while in-memory) or hash the IID to u64 and keep IID → vector
separately for the re-check.
