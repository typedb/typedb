# HNSW index for vector search — sketch

## Current state (branch `vector-search-integration`)

- `vector` value type exists (`encoding/value/vector_bytes.rs`).
- `VectorSearch` IR constraint + `vector_search_executor.rs` do a brute-force
  scan: iterate all attribute instances, score with simsimd cosine, sort.
- Fine for small data; O(n) per query. HNSW is the fix.

## Decision: library vs hand-rolled

| Option | Verdict |
|--------|---------|
| usearch | Fast path to working. C++ dep (Bazel pain, cf. simsimd SVE saga). In-memory index, own file serialization — sits *beside* storage, not in it. |
| hnsw_rs | Pure Rust, same "beside storage" problem. |
| hand-rolled on simsimd | Only option that can live inside the MVCC/WAL storage model. More work. |

**Phase 1: use a library as a sidecar index. Phase 2 (if ever): native.**
Don't hand-roll a graph before brute-force is actually the bottleneck for a
real user.

## Phase 1 sketch — sidecar HNSW

One HNSW index per (attribute type with `@index(vector)` annotation? TBD
syntax) per database.

- **Ownership**: lives in `ThingManager`/database level, `Arc<RwLock<Index>>`.
- **Build**: on server startup / index creation, scan attribute instances and
  insert (vector, attribute IID) pairs. IID is the label stored in the index.
- **Writes**: on commit, apply the transaction's vector-attribute puts/deletes
  to the index *after* the storage commit succeeds. Index is eventually
  consistent w.r.t. concurrent readers — acceptable because:
- **Reads (correctness)**: HNSW gives *candidate* IIDs; executor must re-check
  each candidate against the transaction's snapshot (exists? not deleted?)
  before yielding. This makes staleness safe: false candidates get filtered,
  missing ones only hurt recall. ANN is approximate anyway.
- **Deletes**: HNSW deletion is weak in most libs. Tombstone via the snapshot
  re-check; rebuild index when tombstone ratio passes a threshold (later).
- **Persistence**: none in phase 1 — rebuild on startup. Add library-native
  snapshot files keyed by WAL sequence number when startup cost hurts.

## Query integration

- Planner: when a `VectorSearch` constraint targets an indexed attribute type
  and the pattern doesn't already bind the attribute, emit
  `VectorSearchInstruction::Indexed` instead of the scan.
- Executor: `index.search(query_vec, k * overfetch)` → snapshot re-check →
  yield top-k. Overfetch factor covers tombstones/filtering (start at 2x,
  make it a knob).
- Keep the brute-force path as fallback (unindexed types, tiny data).

## Open questions

- Schema syntax for declaring the index (annotation on attribute type?).
- k / ef_search exposure in TypeQL vs server config.
- Metric: cosine only for now, or metric per index?
- Multi-valued attributes / ownership semantics: index attribute instances or
  (owner, attribute) pairs? Current executor yields attribute instances —
  keep that.
- Memory bound: HNSW holds all vectors in RAM. Fine until it isn't; note the
  ceiling, no action now.

## MVCC interaction (discussed 2026-09-07)

No MVCC inside usearch. Storage stays the source of truth; the index only
produces *candidate* IIDs, and the executor re-verifies each against the
transaction's snapshot (`snapshot.get` at `open_sequence_number`). That
re-check is the entire MVCC bridge.

Invariant to bias every choice toward: **the index may contain too much,
never silently too little.** Extra entries are filtered at read time; missing
entries are undetectable recall loss.

- **Conflicted commits**: apply index updates only after `snapshot_commit`
  returns `Ok`. Aborted transactions never touch the index — eager apply
  would leave permanent garbage (nothing ever deletes it; only a rebuild
  clears it). Post-commit apply is load-bearing, not just tidy.
- **Stale/not-yet-visible entries** (committed after a reader's snapshot, or
  deleted before it): filtered by the snapshot re-check. Correct; costs only
  overfetch.
- **Out-of-order apply across concurrent commits**: tolerable. Attribute IIDs
  are value-derived, so the worst interleaving loses an entry → recall loss,
  not wrong results.
- **Own writes**: a write transaction's buffered vector inserts are not in
  the index. The re-check can't fix false negatives, so brute-force score the
  transaction's `OperationsBuffer` vector writes and merge with index
  results.
- **Vector in RocksDB but not in index** (the false-negative case):
  - Crash between storage apply and index apply: self-healing in phase 1 —
    startup rebuild scans post-recovery storage. Second reason
    rebuild-on-startup is right for phase 1.
  - Process alive, hook silently skipped/failed: persists until restart,
    invisibly. The hook is an in-memory insert — nothing legitimate to
    catch. Never swallow errors; log-and-alarm or panic so a broken index
    announces itself.
- **Persistence (phase 2) is where real MVCC work appears**: once startup no
  longer rebuilds, a missed apply stops being self-healing. The persisted
  index must carry the watermark it reflects; startup replays the WAL from
  that watermark, turning "missed the apply" into "applied during recovery."
  Defer until rebuild cost actually hurts.

## Data layout (discussed 2026-09-10)

Schema:

```
define
    attribute my-embedding, value vector(f32)
```

Persisted indices (phase 2) sit beside the existing per-database dirs:

```
data/
    test-db/
        wal/
        rocksdb/
        secondary-indices/
            {concept-id}-{metric}.{seqno}.usearch
                # e.g. 0x00001-cosine.184467.usearch
                # (cosine is the only metric for now)
```

- Keyed by the attribute type's concept ID (the schema `TypeVertex` bytes,
  hex-encoded), not its label — labels can be renamed; the ID is stable.
- One file per (attribute type, metric); filename encodes both, so adding
  metrics later needs no format change.
- `{seqno}` is the WAL watermark the file reflects — carries the durable-save
  watermark (see MVCC section) and makes replace atomic. Crash-safety
  protocol: `durability-notes.md`.
- `secondary-indices/` is deliberately generic — room for future non-vector
  sidecar indices.
- Phase 1 rebuilds on startup and persists nothing; don't create the dir
  until phase 2 has a file to put in it.

## Non-goals (now)

- IVF/PQ/quantization, disk-resident ANN (DiskANN), index persistence,
  concurrent index rebuild, per-transaction index visibility (snapshot
  re-check covers correctness).
