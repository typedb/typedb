# Durable usearch save — crash safety notes (2026-09-10)

Companion to `hnsw-sketch.md` (phase-2 persistence bullet) and
`mvcc-notes.md`. Covers the one crash mode those files assume away: crashing
*in the middle of* saving the index file.

## What usearch gives us (checked against v2.26.2 source)

Nothing. `save` is `fopen(path, "wb")` + `fwrite` + `fclose`
(`include/usearch/index.hpp:1905`, `output_file_t`):

- `"wb"` truncates the target on open — saving over the live index file means
  a crash mid-save destroys the **old** index too. Never save in place.
- No `fsync` anywhere — a returned-OK save may still vanish on power loss.
- No checksum. The format doc comment (`index_dense.hpp:39`) mentions one,
  but the actual 64-byte header has only magic/version/kinds/counts — nothing
  is verified on load. Truncated files usually fail deserialization because
  section sizes won't add up, but that's incidental, not a guarantee.

## Our protocol

Layout (from the data-layout discussion): one file per attribute type +
metric, keyed by concept ID (stable across relabels) and stamped with the WAL
watermark it reflects:

```
data/<db>/secondary-indices/{concept-id}-{metric}.{seqno}.usearch
       # e.g. 0x00001-cosine.184467.usearch
```

Save (standard tmp + fsync + rename):

1. `save` to `{concept-id}-{metric}.{new-seqno}.usearch.tmp`
2. `fsync` the file, then the directory
3. rename to the final name
4. delete the previous-seqno file

Load:

1. Pick the highest-seqno file that deserializes; ignore `*.tmp` (delete it).
2. If it fails to load, fall back to the previous-seqno file.
3. Replay committed WAL records after that file's seqno into the index
   (inserts only — see `mvcc-notes.md` Q4: nothing is ever removed).

## Why this is sufficient

Every torn-save outcome degrades into the already-handled "missed save" case
(`hnsw-sketch.md`, MVCC interaction, last bullet):

- Crash before rename → stale `.tmp`, old file intact → load old, replay WAL.
- Crash after rename, before delete → two valid files → load newest.
- New file torn/unfsynced → fails to load → fall back, replay WAL.
- Worst case (no file loads) → phase-1 behavior: full rebuild from storage.

Invariant preserved: the index may contain too much, never silently too
little — WAL replay from the loaded file's watermark closes any gap, and
replayed inserts are idempotent (IIDs are value-derived).

## Precedent: TypeDB storage already does exactly this (checked 2026-09-10)

Our protocol isn't novel — it's the same pattern TypeDB uses to make RocksDB
crash-safe. RocksDB's own WAL is disabled (`storage/keyspace/keyspace.rs:230`
`disable_wal(true)`); TypeDB's WAL is the only log, and recovery is
checkpoint + replay-from-watermark, not full WAL replay:

- **Checkpoint write** (`storage/recovery/checkpoint.rs`, every 60s via
  `IntervalRunner`, `CHECKPOINT_INTERVAL`): create `checkpoint/<ts>.tmp/` →
  per-keyspace RocksDB `Checkpoint::create_checkpoint` (hard-link SST
  snapshot) → write `STORAGE_METADATA` (watermark seqno) with `sync_all` →
  rename `.tmp` to final → delete previous checkpoints.
- **Recovery** (`CheckpointReader::recover_storage`): latest *complete*
  checkpoint (skip `.tmp`/incomplete) → restore keyspace files → read
  watermark W → replay WAL from W+1 (`load_commit_data_from` +
  `apply_recovered`, re-validating pending commits). Full-WAL replay only if
  no checkpoint has ever been written; panic if checkpoint > WAL.

Step for step: tmp + fsync + rename, watermark metadata, delete-old-after,
skip-incomplete-on-load. Our usearch protocol is the same design applied to
one file instead of a directory.

**Option: ride the existing checkpoint instead of a separate dir.**
`CheckpointWriter::add_extension` / `CheckpointAdditionalData`
(`checkpoint.rs:258,330`) already persists arbitrary named files inside a
checkpoint — tmp/rename/cleanup and watermark handled for us,
`get_additional_data` reads it back on recovery, and the index watermark can
never disagree with storage's. Tension: checkpoints fire every 60s, and
re-serializing a large usearch index each time is a very different cost from
hard-linking SSTs. Either dump into the checkpoint only when the index
changed enough, or keep `secondary-indices/` with a lazier cadence. Decide
when phase 2 starts; the recovery-side integration favors the checkpoint.

## Non-goals

- Incremental/streaming saves, our own checksum wrapper (WAL replay +
  fallback already bound the damage), storing the blob in RocksDB (loses
  mmap `view`, churns compaction, buys no transactionality).
