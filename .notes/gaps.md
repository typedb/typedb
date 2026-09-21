# Vector primary-store: what's missing (2026-09-18)

Gap list after landing commit `e1a1ba714` (usearch as primary store) on
`vector-search-impl-indexing`. Companion to `primary-store.md`.

## Blocking for CI / merge

1. ~~**Bazel can't build yet**~~ — FIXED 2026-09-21. Deps `vector-search` branch
   pushed (now 3 commits: `7a186c4` usearch 2.26.2, `80c96ec` disable usearch
   default features — numkong's cargo:include path doesn't survive rules_rust
   build-script sandboxing; search unaffected since we re-score exactly with
   simsimd — and `1c06242` crate-annotation patch removing cxx-build's dangling
   OUT_DIR/cxxbridge symlink that failed Bazel output validation). typedb
   MODULE.bazel repointed to `1c06242`, `tool/rust/sync.sh` re-run (Cargo.tomls
   regenerated; needs `bazel clean --expunge` after a deps repin or `@crates`
   is served stale). Verified: `//:typedb_server_bin`, `//concept`, and all 4
   vector test targets (`test_vector_search`, `test_vector_durability`,
   `test_crate_concept`, `test_crate_executor`) build and pass.

## Correctness under adversity

3. **No fallback for a corrupt checkpoint extension.** If the `VECTOR_STORE` file
   fails to deserialize, `Database::open` fails hard. The safe recovery — full-WAL
   replay to rebuild the store — exists in principle (WAL is never pruned today)
   but isn't wired as an automatic path. Coupling to watch: if WAL pruning is ever
   added, checkpoint-extension integrity becomes the *only* copy and this fallback
   becomes impossible.
4. **Pre-existing databases silently lose search.** Data committed before this
   change has vectors in RocksDB values; reads still work (non-empty bytes decode
   fine) but the store is empty, and the executor no longer brute-forces — searches
   return nothing for that data. Fine on a wip branch (recreate test DBs), but
   there's no guard or migration.
5. **No crash-injection tests.** Fail points exist in the checkpoint/commit
   machinery (`UNFINISHED_CHECKPOINT`, `COMMIT_APPLIED_WITHOUT_PERSISTING_STATUS`)
   but nothing exercises: crash between KV apply and observer apply, torn
   extension, checkpoint racing concurrent commits. The durability test covers
   only clean restarts.
6. **No concurrency tests** — concurrent commits into the same type index, search
   during heavy insert, checkpoint during writes. Coarse locking is probably fine;
   "probably" is untested.

## Semantics changes nobody signed off on

7. **Search became approximate.** The old scan returned *exactly* everything above
   the threshold; ANN + k-widening can miss results (recall loss), and there's no
   way to request exact search (usearch has `exact_search` — could be used below
   some index-size threshold).
8. **Results are ordered by attribute ID, not similarity** — the tuple iterator
   requires sorted yield. A user expecting nearest-first must add an explicit sort
   stage; nothing documents this.

## Scale ceilings (deliberate, ponytail-marked)

9. Deleted vectors are never removed — unbounded growth under churn until a
   vacuum/rebuild exists (needs snapshot-aware reclamation).
10. Whole dataset in RAM, and the checkpoint re-serializes the entire store every
    60 seconds even when unchanged — CPU and memory spikes at scale (usearch
    `view()` mmap and dirty-tracking are the upgrades).
11. Own-writes merge scans the transaction's *entire* write buffer per target type
    instead of a range scan.
12. One `RwLock` per type index — adds serialize; usearch's internal lock-free
    adds are the upgrade if it contends.

## Features from the checklist / notes still open

13. Per-metric indexes (cosine hardcoded; the checkpoint envelope is versioned so
    adding a metric field is a format bump; extra metrics should be search-only
    sidecars, not more primary stores — the value's home stays the cosine index).
14. `k` / `ef_search` exposure in TypeQL; planner-provided k instead of
    k-widening.
15. Dropping an attribute type doesn't drop its index (garbage lives until
    reset/restart).
16. BDD feature files for vector search in typedb-behaviour (step definitions
    exist; blocked on Bazel anyway).

## Suggested order

5 (crash tests) — the cheapest way to find out whether
the durability story actually holds. Everything else is refinement.
