# usearch as PRIMARY store for vector values — decision + implementation (2026-09-18)

Supersedes the "sidecar index" phase-1 plan in `hnsw-sketch.md`. Decision: vector attribute
values live ONLY in usearch; RocksDB keeps everything else. Data is split:

- RocksDB: the attribute vertex KEY with an EMPTY value — existence, MVCC visibility,
  delete tombstones, isolation validation. This is what makes a versionless value store sound:
  attribute IDs are value-derived, so key -> vector is immutable; MVCC only ever asks "does
  this attribute exist for my snapshot", and RocksDB still answers that.
- usearch: one HNSW index per attribute type, entry {key = concept id, value = vector} —
  both the searchable index AND the single copy of the value.

## The key trick: the ID is the usearch key

`VectorAttributeID` = [1: category][7: hash][1: tail]. The disambiguated hash ([7 hash][1 tail])
is exactly 8 bytes = the usearch u64 key (`as_vector_index_key`/`from_vector_index_key`).
Derivable both ways, no side table, unique per type (indexes are per-type). multi=false.

## Write path (CommitObserver, storage crate)

`MVCCStorage` has an optional `CommitObserver` (implemented by `concept::thing::vector_store::VectorStore`):

- `owns_value(keyspace, key)`: byte-test for vector attribute vertices.
- `WriteBatches::from_operations` writes owned keys with an EMPTY value (the strip).
- The full value stays in the OperationsBuffer -> WAL CommitRecord (own-reads + replay source).
- `apply(seq, owned)` is called INSIDE `snapshot_commit`, after the KV write, BEFORE
  `isolation_manager.applied` — i.e. before the watermark can advance past seq. Load-bearing:
  a checkpoint reading watermark W is guaranteed the store already contains every commit <= W.
  Aborted commits never reach apply. Applies are idempotent (contains-check).
- usearch add failure panics: the store is the only copy; a broken index must announce itself.

## Read path

- `ThingManager::get_attribute_value(Vector)`: snapshot get -> non-empty bytes = own buffered
  write (decode); empty bytes = committed (fetch from VectorStore); None = doesn't exist.
- Hash-collision disambiguation (`find_existing_or_next_disambiguated_hash_vector`): equal-hash
  candidates with empty stored value are compared via a `committed_vector: Fn(u64) -> Option<Vec<f32>>`
  closure (thing_manager passes a store lookup). Without this, re-putting an existing vector
  would allocate a duplicate attribute.

## Search (executor)

Per target type: ANN search the store (threshold-only semantics via k-widening — start 128,
x4 until the tail drops below threshold or exhausted), then merge this transaction's buffered
vector writes (not in the index), dedup via BTreeSet (downstream iterators require sorted
yield), MVCC re-check each candidate against the snapshot KV key, re-score exactly with simsimd.

## Durability

Rides the existing checkpoint: `checkpoint_storage` calls `checkpoint.add_extension(vector_store)`
(CheckpointAdditionalData, name VECTOR_STORE) — tmp+rename+watermark+old-cleanup inherited.
Format: our own versioned envelope (format_version, per type: type id + dimensions + usearch
save_to_buffer blob) — not trusting usearch's unverified header (ClickHouse's lesson).

Recovery (Database::load): load the extension into a VectorStore FIRST, then pass it as the
commit observer into `MVCCStorage::load` — storage recovery replays the WAL tail through the
observer, so post-checkpoint vectors are re-applied by the same code path as live commits.
No checkpoint -> empty store + full WAL replay. One mechanism, no separate replay code.

`Database::reset` clears the store. usearch never removes entries (old snapshots may still
read deleted attributes' values) — vacuum is future work.

## Verified by

- `concept` unit: `thing::vector_store::test` (add/get/search/serialise roundtrip).
- `query/tests/vector_search.rs` e2e: commit -> store populated + ANN search via
  `let $x in cosine_similarity_search(type, vector([...], "float32"), t)`; value read-back from
  store; same-transaction buffered search; duplicate insert dedup.
- `database/tests/vector_durability.rs`: restart via WAL replay, restart via checkpoint extension.

## Loose ends

- typedb_dependencies pinned commit must be repointed after pushing the local `vector-search`
  branch commit (adds usearch 2.26.2) — until then Bazel builds fail on `@crates//:usearch`.
- typeql pin bumped to aafb485 (vector_literal grammar fix) in Cargo.toml + MODULE.bazel.
- ponytail ceilings: RwLock per type index (usearch internal locking if it contends);
  k-widening until planner exposes k; RAM-resident store (usearch `view` mmap later);
  no vacuum for deleted vectors; extension re-serialised every checkpoint even when unchanged.
