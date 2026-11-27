**Download from TypeDB Package Repository:**

[Distributions for 3.16.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.16.0-rc0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.16.0-rc0```


## New Features
- **Importer validation optimization**
  
  Large imports spend much less time on validation. Constraints that were checked again on every import commit by reading data committed earlier are now checked once when the import finishes and the original schema is restored.
  
  - `@unique` ownerships and the maximum of `plays` cardinalities are relaxed once at the start of import and validated once on restore. This is much faster, but defers cardinality and uniqueness violations to the end of the import, rather than earlier in the process.
  - For attribute-first ordered imports, `@key` is enforced as `@card(1..1)` and other `owns` cardinalities are not relaxed, so violations are reported during the import.
  - Schema transactions revalidate cardinalities _only_ for constraints that existing data could now violate, and the affected types are validated in parallel.
  
  
- **Introduce vector value type and HNSW-indexed cosine similarity search**
  
  TypeDB can now store vector embeddings as first-class attribute values and query them by cosine similarity, backed by a per-attribute-type HNSW index (usearch):
  
  **Define**:
  
  ```typeql
  define
    attribute embedding, value vector(1024, "float32") @index(hnsw:cosine);
    entity document owns embedding @card(0..);
  ```
  
  **Write**:
  
  ```typeql
  insert
    $d isa document, has embedding vector([0.12, -0.08, /* ... */ 0.91], "float32");
  ```
  
  **Read**:
  
  ```typeql
  match
    let $emb, $sim in cosine_similarity_search(embedding, vector([0.1, -0.1, /* ... */ 0.9], "float32"), 0.75);
    $doc isa document, has embedding $emb;
  sort $sim desc;
  limit 10;
  ```
  
  ### Storage layout
  
  #### What is stored in RocksDB
  
  RocksDB holds a reference to the actual vector instance stored in Usearch. The layout is as follows:
  
  ```
  RocksDB key:  [ prefix:1 | type id:2 | category:1 | hash:7 | tail:1 ]  ->  (empty value)
  usearch:        index for <type id>  ──  key = hash + tail as u64  ->  [f32; N]
  ```
  
  - **Reference in RocksDB:** an ordinary attribute vertex key, ending in a 7-byte hash and a 1-byte tail.
  - **Capacity**: Per attribute type, we can store 2^56 * 128 instances = 2^63
  - **Hash:** the first 7 bytes of the 64-bit seahash of the vector's encoded bytes.
  - **Tail:** a flag bit marking the ID as hashed, plus a 7-bit disambiguator (0-127) that separates different vectors with the same hash. An equal vector reuses the existing key, so equal vectors are one attribute.
  - **Finding it in usearch:** the type ID picks the per-attribute-type index, and the hash and tail together, read as a big-endian `u64`, are the key within it. No mapping table.
  
  #### What is stored in Usearch
  
  One usearch file per attribute type, holding every committed vector of that type together with its HNSW graph.
  
  ```
  <database>/vector-index/
  └── 0x0001/               one directory per attribute type (type id in hex)
      └── hot.usearch       key (u64) -> [f32; N], plus the HNSW graph
  ```
  
  - **One file per attribute type:** all vectors in a file have the same dimensions and use cosine distance with `f32` precision.
  - **Once checkpointed, this file is the only copy of a vector's values outside the WAL.**
  - **Written only at checkpoint,** and only if the index changed since the last one. Between checkpoints the index lives in memory.
  
  #### What is stored in Checkpoint
  
  A checkpoint is an on-disk snapshot of both stores plus the sequence number to replay the WAL from. Only the latest one is kept.
  
  ```
  <database>/checkpoint/<timestamp>/
  ├── STORAGE_METADATA      the sequence number the checkpoint is taken at
  ├── <keyspace>/           one RocksDB checkpoint per keyspace
  └── VECTOR_INDEX/
      ├── MANIFEST          "version 2" + one line per index file
      └── 0x0001/
          └── hot.usearch   hard link to the live file above
  ```
  
  - **MANIFEST line:** `<type id hex> cosine <dimensions> hot <file length>`. It is written and fsynced last, so a readable manifest means every file it names is durable.
  - **Index files are hard links** to the live files, so a checkpoint does not duplicate them on disk.
  - **Always present:** every checkpoint includes `VECTOR_INDEX`, even when there are no vectors.
  
  #### What is stored in WAL
  
  The WAL is unchanged in format. A commit record carries the transaction's writes, and for a vector attribute that is the key plus the full vector value.
  
  ```
  commit record:  [ vertex key (12 bytes) ]  ->  [ f32 | f32 | ... ]   N x 4 bytes, big-endian
  status record:  commit <sequence number> applied / rejected
  ```
  
  - **The only place the vector value is durable before the next checkpoint.** RocksDB receives the key with the value stripped; the index holds the value in memory.
  - **Deletes** are recorded as a delete of the key, with no value.
  - **Replayed on startup** from the checkpoint's sequence number onwards, into both RocksDB and the index.
  
  #### CRUD
  
  - **Create:** the value sits in the transaction's write buffer until commit; on commit the key goes to RocksDB (value stripped) and the vector is added to the index.
  - **Read:** from the write buffer if uncommitted, otherwise from the index by key.
  - **Update:** none in place; attributes are immutable, so it is a delete plus a create.
  - **Delete:** the key is deleted in RocksDB and the vector is removed from the index.
  
  #### Write path
  
  ```
  commit ─> index validate ─> WAL append ─> isolation validate ─> WAL fsync ─> RocksDB ─> usearch (memory) ─> visible
  checkpoint (every 60s if there were commits, and after a recovery that replayed WAL):  RocksDB + usearch ─> disk
  ```
  
  - **Index validate** (dimension checks, capacity) runs before anything is written, so an index problem aborts the commit cleanly.
  - **The WAL is the source of truth:** it holds the full vector, so both stores can be rebuilt from it.
  - **usearch is in memory between checkpoints;** its files are only written at a checkpoint.
  
  #### Recovery and reboot
  
  Both use the same load path: load the latest checkpoint (RocksDB plus index), replay the WAL after it into both stores, and write a fresh checkpoint if anything was replayed.
  
  - **Reboot or crash:** the checkpoint is up to 60s old; every commit since is replayed, rebuilding vectors that were only in memory. A clean shutdown does not write a final checkpoint, so both cases take the same path.
  - **Crash between the two stores:** the commit is in the WAL, so the replay completes it. Re-adding a key the index already has is a no-op.
  - **Checkpoint from before this feature:** it has no index and no vector attributes, so the index starts empty.
  
  #### Versioning (MVCC)
  
  - **The index is not versioned:** it holds only the latest committed state.
  - **RocksDB is:** the existence key is an MVCC key, so it carries the visibility.
  - **Search:** every candidate from the index is checked against the reader's snapshot before it is returned, so vectors committed later are filtered out.
  - **Deletes and older readers:** removed values are kept in memory until the next checkpoint, so a reader whose snapshot predates the delete can still read them.
  
  #### Limits
  - Vector length is capped at 65,535 (ie., `u16::MAX`)
  - For now, a vector attribute type must declare @index(hnsw:cosine); defining one without it fails schema validation at commit.
  
  
## Bugs Fixed
- **Don't count 'is' as a thing constraint when deciding if a query involves only types**
  This allows `is` constraints to be used in type queries, so an unsatisfiable query returns 0 answers instead of throwing a compilation error.
  
  
- **Fix server crash when an attribute variable is compared with itself**
  
  `match $n isa name; $n == $n;` crashed the server with an index-out-of-bounds panic in `executor/row.rs`. It now answers: `==`, `<=`, `>=` keep every row; `!=`, `<`, `>` keep none.
  
  Fixes https://github.com/typedb/typedb/issues/7725
  
  Reproduced on 3.12.3, 3.13.6 and 3.14.0 for all six comparators, on owned and independent attributes (matrix below).
  
  
## Code Refactors
- **Simplify workspace referencing**
  
  Each TypeDB dependency's commit or tag is now written once, in its `git_override` in `MODULE.bazel`. 
  
  Before, refs were duplicated in `workspace_refs`' commit and tag dicts, with commented-out alternatives for switching between the two. 
  
  
## Other Improvements
- **Sync cargo files**
  
- **Reduce bazel cache size limit to 40G from 100G**

    
