# How other databases handle vector index persistence (2026-09-10)

Companion to `hnsw-sketch.md` / `durability-notes.md`.

## ScyllaDB (checked against local checkout, `~/Repositories/etc/scylladb`)

**Punchline: ScyllaDB never durably saves a usearch index at all.** The
architecture makes the problem disappear instead of solving it.

### Where usearch lives

Not in the scylladb repo. The ScyllaDB side (`index/vector_index.cc`, ~370
lines) is schema metadata only — validates `USING 'vector_index'` and
serializes index targets. The HNSW/usearch code is an external service
("Vector Store", separate `scylladb/vector-store` Rust repo) that connects
back to ScyllaDB *as a regular CQL client*. The in-tree `vector_search/` dir
is just the per-shard HTTP client the coordinator uses to fetch candidates.

### Save mechanism (from `docs/dev/vector_search.md`)

- **Durability lives entirely in ScyllaDB's own storage.** Base table holds
  the vectors; CDC (mandatory, auto-enabled on vector-indexed tables,
  TTL >= 24h) is the incremental log. The Vector Store index is a disposable
  cache over that.
- **Recovery = rebuild, not load — and not replay.** The index is RAM-only:
  no file, no load path, no watermark. On every Vector Store restart (and on
  index creation): token-range-parallel full scan of the *current* base table
  (`SELECT pk, vector, writetime(vector) ... BYPASS CACHE`), CDC readers
  started *before* the scan so concurrent writes flow through the same
  channel; `writetime` timestamps dedupe scan rows against CDC rows. Old CDC
  history is never replayed — the scan reads current state, CDC covers only
  the tail concurrent with/after it. Until the scan completes, ANN queries
  get 503 + scan progress (a normal startup state, not a fallback — the API
  exposing scan progress confirms rebuild is the expected path).
- **CDC is not ScyllaDB's WAL.** The base database recovers via its
  **commitlog** (the actual WAL): sstables + short replay since last memtable
  flush — same checkpoint-bounded shape as TypeDB's recovery. CDC is a
  user-visible change feed stored as ordinary tables (durable via the same
  commitlog/sstable machinery, recovered as plain data); ScyllaDB never reads
  it to restore itself. The CDC TTL (>= 24h) exists to bound the log while a
  slow consumer catches up during normal operation, not to enable deep
  replay at restart.
- **Staleness = read-time re-check**, same as our snapshot re-check: index is
  eventually consistent; the base-table read is authoritative and silently
  drops stale candidates ("can remove stale candidates but cannot discover
  missing ones").

### Takeaways for us

- ScyllaDB is our "phase 1, forever": rebuild from source of truth on every
  restart. Live production validation that rebuild is a legitimate strategy,
  not a hack.
- Recovery-cost comparison: their restart is O(current table size) scan over
  CQL; our phase 2 is O(index file) load + O(commits since last save) WAL
  replay. CDC maps to our WAL replay only for *steady-state incremental
  updates* — for crash recovery they have no replay-from-watermark at all,
  they throw the index away and rescan.
- The two-log structure (commitlog for durability + CDC feed for the index)
  is what being external costs them. Our applier sits in-process at the
  post-commit hook, so TypeDB's WAL doubles as both — one log doing the job
  Scylla needs two for. No CDC-equivalent needed.
- Storage-efficiency note: they never pay the full-index-dump cost (no file),
  but pay instead in permanent CDC retention (every write to a vector-indexed
  table stored twice for >= 24h), full-rescan restarts, and 503 downtime
  during rebuild. Same trade-off surface as ours, opposite corner — right for
  a separately-scaled external service with rare restarts, wrong for an
  embedded index that restarts with the server.
- Same core bets as ours: index as candidate generator, authoritative
  re-check at read time, timestamp-based dedup when merging scan + CDC.
- Caveat: whether the external vector-store process internally snapshots
  usearch files isn't answerable from the scylladb repo; in-tree docs
  describe only scan+CDC rebuild.

## ClickHouse (checked against local checkout, `~/Repositories/etc/ClickHouse`)

**Punchline: ClickHouse never mutates or re-saves an index — it makes the
index as immutable as the data.** usearch is embedded in-process as a
MergeTree *skip index* (`src/Storages/MergeTree/MergeTreeIndexVectorSimilarity.{h,cpp}`).

### Save mechanism

- One HNSW index per index granule per **data part**. Built in memory while
  the part is written (`MergeTreeIndexAggregatorVectorSimilarity::update`),
  then streamed into the part's `skp_idx_<name>` file alongside the columns:
  `serializeBinary` writes own `FILE_FORMAT_VERSION` + dimensions, then
  usearch `save_to_stream` (`MergeTreeIndexVectorSimilarity.cpp:241`).
- They version the format **themselves**, explicitly not trusting usearch's
  own header (h:108-112): the index should be library-agnostic, and there is
  extra non-usearch data (dimensions) to version. Version mismatch on load →
  clean error "drop the index and create it again" — no migration.
- **Written once, never updated.** Inserts create new parts (each with its
  own small index); merges rebuild the index for the merged part from
  scratch by re-adding vectors. No in-place mutation exists anywhere.

### Lifecycle, precisely (clarified 2026-09-10)

- **Storage unit is the part, not the index.** The index has no standalone
  file — it's embedded in the part's `skp_idx_<name>` file next to the
  columns. That embedding is what makes its crash safety free.
- **A new index per INSERT, no accumulation threshold.** Every INSERT batch
  immediately becomes a new part; the HNSW is built in memory *during that
  part write* and serialized with it. Parts are never open for appending;
  there's no buffer where vectors wait to be dumped. Batching is the
  client's job (many small inserts = many tiny parts = ClickHouse
  anti-pattern, extra painful with vector indexes).
- **Merges are automatic and rebuild, not merge.** Background merges
  continuously fold parts into bigger ones — not an optional/manual
  compaction. The merged part's HNSW is rebuilt from scratch by re-inserting
  every vector: two HNSW graphs cannot be structurally merged. This is where
  the design pays its bill.
- **Query = fan-out over all parts.** k-NN searches every part's index
  independently and merges top-k across them. (Technically one index per
  GRANULARITY block within a part, but vector indexes are declared with a
  granularity so large it's effectively one per part.)
- **Net shape: Lucene's segment model** — immutable segments born
  fully-formed per write batch, background rebuild-merges, fan-out search.
  If we ever revisit our monolithic-index non-goal, this is the price list:
  per-write index-build latency, merge amplification, per-query fan-out — in
  exchange for never needing a durable-save protocol at all.

### Crash safety

Inherited entirely from the immutable-part lifecycle: parts are written to a
`tmp_*` directory and atomically renamed on commit; incomplete parts are
discarded at startup. The index file can never be torn independently of its
part — same tmp+rename shape as ours, at part granularity. No WAL for the
index because ClickHouse has no row-level WAL at all: per-part atomicity +
replication/insert-dedup is the whole durability story. Notably `fsync_after_insert`
and `fsync_part_directory` default to **false** — they accept losing the most
recent parts on power loss (replicas cover it).

### Takeaways for us

- This is the **segmented/immutable design** — the thing we declared a
  non-goal (Lucene/DiskANN-style). It eliminates the torn-save and
  incremental-persistence problems structurally: nothing mutable is ever
  saved. RocksDB's hard-link checkpoint trick works for the same reason.
- The cost surfaces at **merge time**: every merge re-builds HNSW for the
  merged part (re-inserting every vector) — a known pain point, and the
  price of immutability for a graph index that can't be merged structurally.
- Query cost: k-NN must search *every part's* index and merge results —
  fine for ClickHouse's few-large-parts steady state, worse with many parts.
- Their format-versioning decision is worth copying: wrap usearch's
  serialization in our own versioned envelope (version + dimensions +
  metric), don't rely on usearch's internal header. Cheap now, saves a
  migration story later. "Drop and re-create on version mismatch" == our
  "fall back to full rebuild" — we get it for free.

## Comparison summary

| | TypeDB (planned) | ScyllaDB | ClickHouse |
|---|---|---|---|
| usearch location | in-process | external service | in-process |
| index granularity | one per attribute type | one per index | one per part granule |
| mutability | mutable in RAM | mutable in RAM | immutable |
| persisted? | phase 2: checkpoint file | never | always, inside the part |
| crash safety | tmp+fsync+rename + WAL replay | rebuild by full scan | part atomicity (tmp+rename) |
| restart cost | O(file) + O(replay tail) | O(table scan) | O(load parts' indexes) |
| ongoing cost | periodic full dump | CDC retention >= 24h | HNSW rebuild on every merge |
