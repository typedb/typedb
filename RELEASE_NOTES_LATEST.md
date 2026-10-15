**Download from TypeDB Package Repository:**

[Distributions for 3.14.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.14.0-rc0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.14.0-rc0```


## New Features


## Bugs Fixed
- **Fix linux CI build**
  
  Update TypeDB Dependencies to fix all CI builds for Linux.
  
  
- **Fix attribute equality regression**
  
  Equality lookups on long strings, such as `match $n isa name; $n == "fixed_prefix_with_unique_tail_1234"` were tweaked to do scan rather than point lookup in #7869.  Strings longer than 16 bytes are stored under a hash rather than inline, scanning the entire bucket even when looking for a specific value rather than a range. Equality now uses the value's exact key again, while range comparisons keep using the order-preserving prefix-based search.
  
  While fixing this, value comparisons on attribute variables were reworked so that all comparisons on a variable are combined before the storage lookup, which fixed several related bugs:
  
  - **Missed answers when seeking with a bound of another value type.** When several `has` constraints were joined on an owner, the seek used the comparison bound in its own value type. A bound of a different type, such as a double against an integer attribute, could skip valid answers: `$p has age $a; $p has name $n; $a > 10.5;` could drop `age 11`. The same seek could skip long strings that sort after a bound, and datetime-tz values at the same instant in another time zone.
  - **Double bounds on decimal attributes** were converted incorrectly, so `match $x isa cost; $x == 10.5;` missed `10.5dec`.
  - **Sorting by values that have no ordering** (duration, struct) panicked at runtime. It is now a compile-time error, `UnorderedValueTypeForSortVariable`, for both attribute and computed value variables.
  - **Comparisons between attribute variables** are now type-checked per comparator: `<`, `<=`, `>`, `>=` only for value types that have an ordering, and `like`/`contains` only for strings. `contains` between non-string attributes could previously panic.
  - **Contradictory comparisons** on one variable (`$x > 10; $x < 5;`) are recognised up front and return no answers without scanning, in most lookup paths.
  
  We also change the behaviour durations, which now compare as equal using `==`, but are not range-orderable or sortable.
  
  
- **Avoid blocking the transaction reservation queue on lock timeouts**
  
  Fix a bug where a timed-out schema transaction opening request could get stuck in the queue, blocking new schema and write transactions from opening. 
  
  
- **Fix max and min object type prefixes**
  
  Fix `max_object_type_prefix` and `min_object_type_prefix` results, used for building `has` iterators in `ThingManager`. This was a latent bug in the usage paths with no current callers (for unbounded searches).
  
  
- **Don't clean up new, independent relations without players**
  
  Relations marked internally as 'independent' should survive at commit even without any linked players. The existing check at commit only looks at relations that lost a player in this transaction, not for newly written relations that never had one. A newly inserted relation may still get cleaned up at commit time. However, these relations' existence may be relied on, specifically by the database importer, in some cases.
  
  
- **Remove excessive memory consumption and fix bugs of uniqueness constraint checks**
  
  Optimise memory usage of schema transactions' operation-time validation for `@unique` annotations, triggered by type hierarchy, annotations, and ownership changes. While the previous algorithm stored all affected objects in RAM, uncontrolled (potentially gigabytes of data for huge datasets), the new approach uses the same pattern as write transactions' checks, depending only on a set of `has` edges for a single attribute at a time.
  
  Additionally, resolve a discovered bug in the shared operation-time suitability check: when an attribute (or role) type is moved under a new supertype, constraints declared above the new supertype were not applied to the moved subtree's existing instances, so any constraint violation could be committed through a set supertype. Covered by https://github.com/typedb/typedb-behaviour/pull/455.
  
  Solves #7138 
  
  
- **Console log outputs are best-effort**
  
  When the server's stderr is a pipe whose reader has gone away (the launching shell or SSH session exited, a CI step finished, a supervisor dropped the pipe), the first log line written to it panics with `failed printing to stderr: Broken pipe (os error 32)`. 
  
  Console output is a convenience for whoever launched the server, so it is now best-effort: a broken stdout or stderr loses console output only, while the log file remains the authoritative record and the server keeps serving. This is the behaviour of a traditional daemon, which ignores `SIGPIPE` and lets writes to a dead stderr fail silently.
  
  

## Code Refactors
- **Commit deltas**
  
  We introduce a `CommitDeltas` record that replaces full WAL replay during statistics updates, significantly reducing memory overhead and latency. This allows us to avoid reading from disk when performing live statistics updates, and reduces memory consumption during recovery.
  
  
- **Use RocksDB's HyperClockCache and build Linux releases on glibc 2.28**
  
  Update to the latest RocksDB (11.8) and switch to a new type of block cache (HyperClockCache) instead of the LRU cache. We also enable the newly available Ribbon filter API, which replaces the bloom filter with equivalent performance for approximately 30% less memory.
  
  For reference, a typical database would be about 1% index blocks and 7-9% bloom filter blocks. This change drops to 1% index blocks and 4-6% filter blocks. Optimal machine sizing is still recommended at around 10% of data size on disk to be available as RAM.
  
  RocksDB 11.8 requires C++20 - Linux releases are therefore now built on AlmaLinux 8, compatible with glibc 2.28 and newer. Systems with an older glibc, such as Amazon Linux 2 and Ubuntu 18.04, should use the Docker image.
  
  
- **Clean up warnings: private visibility leak**
  
  Clean up linter warnings about 'private interfaces', where the public interface transitively exposed narrower types. We clean these up by tightening or widening the types where appropriate.
  
  
- **Read an object's status in @card validation only when a constraint is checkable**
  
  Optimize the cardinality validation path on commit by removing one storage seek per pre-existing object whose type has no cardinality worth checking.
  
  Commit validation used to read every touched owner's, player's, and relation's `ConceptStatus` from storage before anything is established about whether its type had a cardinality worth checking. The plays default is `@card(0..)`, i.e. unchecked, so the read was usually wasted.
  
  
- **Stop copying the write buffer and optimize relation index qualification**
  
  Iteration over storage writes on finalisation cloned every buffered write into a `Vec`, then used the copy for two unrelated jobs. Each entry's copy doubled the footprint, and it grew without bound with commit size. Now, the iterators were redesigned to avoid cloning.
  
  Additionally, with each `links` write forcing a relation index regeneration check, a relatively expensive recalculation of indexing qualification for its type was performed. A cache in `TypeManager` was introduced to accelerate the decision making.
  
  
- **Reduce storage lookups and heap allocations for @card validations**
  
  Optimize the throughput of commit operations with new objects and changed types (operations affecting multiple interface instances) by reducing the number of type and constraint reads and collections. 
  
  
- **Introduce BTreeMapIntersectionIterator to use in isolation validation**
  Introduce `BTreeMapIntersectionIterator`, which iterates through the shared keys of two BTreeMaps, and their corresponding values. Replaces the iterate-and-check approach to checking dependency between two commits.
  
  
- **Optimize commit locking for capabilities**
  
  Let concurrent writes get faster by not creating cardinality and ordering locks for freshly inserted objects, which used to be purely excessive work, since freshly made objects cannot be referenced from multiple parallel transactions.
  
  This change showed a **+5.3% with 4 writers, +6.8% with 8 writers** speedup on parallel benchmarks. No behaviour changes introduced.
  
  
- **Replace CommitProfile::DISABLED const with a disabled() constructor**
  
  Replace an `&mut` reference to a const that generated hundreds of warnings with an inline constructor to silence them.
  
  

## Other Improvements
- **Update apple signing keys**
  Updates the apple signing keys newly issues ones.
  
  
- **Fix error message for value type mismatch in function arguments to include function name**
  
  We fix the error message for value type mismatch in function arguments to show the function name rather than an internal function ID. This required keeping the function name in the annotated function signature.
  
  
- **Add regression test for recursive functions crashing with "WouldBlock"**
  Adds a regression test for recursive functions crashing with "WouldBlock"
  
- **Fix crash caused in recursive call retries caused by bad assumption**
  Fixes a faulty assumption where the recursive function "retry" assumed all function executors would be "inactive" (i.e., not on the stack) and tried to reset them. 
  
  
- **Update BDD ref for HTTP integer display in documents**
  
  Update the BDD ref to point to the commit with additional tests for integer and decimal values in order to ensure that the HTTP API correctly handles their conversions to JSONs.
  
  BDD PR: https://github.com/typedb/typedb-behaviour/pull/456
  
- **Optimise Match stage executor to take a FixedBatch instead of a row as input**
  MatchStageExecutor takes a FixedBatch instead of a single input row, saving on allocations and getting the benefits of batching in multi-stage pipelines.
  
- **Upgrade Write::Put known_to_exist to 3 values, use when putting edges**
  We extend the `known_to_exist` field in `Write::Put` to be a three-value enum `KnownToExist` (`Exists`, `NonExistent`, `Unknown`).
  By setting the edges connected of newly inserted vertices as `KnownToExist::NonExistent`, we can optimise the commit-time `set_initial_put_status` step to avoid hitting RocksDB. This step could be a significant chunk of the commit (20%-40%).
  
- **fix: correct "occured" to "occurred" in error messages**
  
  Fixes a misspelling in five user-facing error messages: `occured` -> `occurred`.
  
  TypeDB surfaces these strings through its error API, so a user who hits a decimal-parse
  failure or an invalid given-row sees the typo verbatim. The same repository already
  spells the word correctly in messages of the same shape, including the sibling line in
  the very same enum:
  
  - `query/given_rows.rs:100` — `"An error occurred while decoding the provided concept."`
    (correct) sits two lines above `:102` and `:103` (misspelled)
  - `compiler/annotation/mod.rs:126` — `"An error occurred when trying to resolve ..."`
  - `query/define.rs:1419` — `"An error occurred by defining the function"`
  
  
- **Remove circleci synchronization to benchmark repo**
  
  Remove a synchronization job that relies on a bot that has direct push access to a master branch.
  
  
    
