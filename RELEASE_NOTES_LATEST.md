**Download from TypeDB Package Repository:**

[Distributions for 3.15.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.15.0-rc0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.15.0-rc0```


## New Features
- **Implement optimized database import**
  
  We introduce the possibility of **parallelized, read-free database imports**, when specific conditions are met. In test we achieve up to 100k relations/sec consistently up to billions of loaded relations.
  
  This path utilizes an updated typedb-protocol (backwards compatible). Old import files/old servers fall back to the existing, slower import path. This protocol is designed for an upcoming data loader that reads from specifically shaped tables or CSV files, and for importing TypeDB exports rapidly.
  
  We combine existing guarantees that are implicit and not explicitly marked, with additional constraints.
  
  _Existing implicit requirements_:
  - Entity or relation import messages _never re-uses global or type-label scoped IDs_ (therefore, every entity and relation is _complete and listed with all its components, exactly once_)
  - All attributes mentioned as owned in other entity or relation import messages are listed as _separate Attribute messages_
  - All references are correct (eg. any concept IDs refer to concepts that have their own import messages; attribute values that are referenced are imported)
  
  _Additional, optional guarantees, required for the optimized path_:
  1) When the data is imported in order of attributes, entities, then relations => This enables **parallelism** on import, since import items can never collide.
    a) Exception: hashed attributes _can_ collide when allocating a hash value, so we add a rarely used retry for isolation conflicts
    b) Exception: `plays` cardinality bounds. Expect slowdowns/failures when plays cardinality is set (future work: relax plays cardinality and revalidate once at the end)
  
  2) When guaranteed links to relation players by 'type-local ids' (which are u64), we can directly create the edges between the relation and the player without any reads into the importer mapping table. We skip creating the mapping table, mapping IDs, and existence checks when this is guaranteed and the AER order is guaranteed.
  
  3) When guaranteed that attribute ownerships link to attributes by value, rather than ID that has to be remapped, and that the AER order is guaranteed, we can skip recording new IDs for attributes whose ID is derivable from the value (in effect, any attribute that is not hashed). This skips the mapping table creation, mapping IDs, and attribute existence checks.
  
  4) When attribute messages are guaranteed to be unique, we also never read on `put` of the attribute - it's a straight write without read.
  
  In combination, we can freely **parallelize** and **avoid almost all reads during import**, skipping both mapping tables and reads on the write and commit path in transactions. **This is only as "safe" as the import data - dangling references will lead to a corrupt TypeDB dataset!**
  
  Two read operations are not yet skippable:
  
  - Unavoidable read into main storage: creating >16 byte string, struct, and vector values. These will always require a hash probe to avoid collisions). Loaded first when the dataset is smallest. Unavoidable in our architecture!
  - Unavoidable read into importer storage: Mapping a string, struct, and vector attribute ownership to the new database's IID space.
  
  In addition, we have one thing that we can skip with additional implementation work:
  - Relations playing roles in other relations validate at commit time by reading disk, and to regenerate the index.
  
  
  
- **Type-inference & optional dereference safety improvements**
  Large refactors to type-inference & optional dereference checks for better stability.
  
  **Stricter optional dereference rules**
  Previously, referencing an optional variable in a match-stage would simply discard the row. With this change, any reference must to an optional variable must explicitly address the optionality.
  * Within a match pattern, the variable must be tested using the newly introduced `isset` constraint ( e.g. `isset $x;`- this fails the row if the variable is empty.
  * A reference to an optional variable in an expression may be marked with `?` to  short-circuit the expression and assign the empty value to the result. ( e.g.  `let $y = $x?  + 1;`) . Note the assigned variable must then be optional.
  * When fetching the attributes of an optional variable, it may be marked with `?` to return an empty value for that field. e.g. `fetch { "opt": $opt?.* }`
  
  **Note:**  These are not strictly enforced in this commit. Older, unsafe dereferences will simply drop the row, but the server will emit a deprecation warning. These will be compile time errors in a future version.
  
  **if conditions in writes**
  To prevent the awkward reading of `isset` constraints in write stages  ( `insert try { isset $x;  ... }` or `delete try { isset $x; ... }`  ), we introduce the `if` constructs for use in write stages. Currently, only `isset` constraints are allowed in the condition.
  ```
  match
    try { $x isa person; };
  delete 
    if { isset $x;  } then {
       delete $x;
    };
  ```
  
  **Note:**  `try` blocks in write stages are still supported, with the server will emitting a deprecation warning. They will be removed in a future version.
  
  
  **Type inference refactors**
  Also refactors type-inference to include value-types and move towards a simpler, more stable approach.
  
  

## Bugs Fixed
- **Replace unimplemented panic with error for Cascade**
  Setting/Unsetting cascade annotation returns an error instead of panicking.
  
- **Fix serialization of deletes in commit record**
  
  Fix crash on loading an older commit record without delete resolution.
  
  

## Code Refactors
- **Avoid writing tombstones for keys deleted in concurrent commit**
  
  We add a check during concurrent commit dependency resolution to skip writing tombstones for a deleted concept or edge that was also deleted in parallel by another commit. This slightly reduces the number of dead keys in storage and prevents us from double counting deletes when calculating commit deltas.
  
  

## Other Improvements
- **Bump dependencies to merged commit**
  because I forgot
  
- **Make concept Bazel target public**
  
  `concept` package is sometimes require usages in dependent crates, and it's impossible to set it through Bazel without making the Bazel target public.
  
  
- **Bump behaviour for testing IsSet constraints in analyze responses**
  Bump behaviour for testing IsSet constraints in analyze responses
  
  
- **Microbenchmarks**
  Introduce a microbenchmark framework that allows running queries designed to stress different parts of the system.
  Reports are generated from the profiles, allowing quick comparison of where the time is being spent.
  
  
- **Make NotInitialised error classified as Unavailable**
  
  Make the `NotInitialised` error classified as `Unavailable`, which is specified in the gRPC error code and can be handled by TypeDB Drivers to retry automatically after a recovery (when the server may become initialised).
  
  
- **Update README to feature high availability**
  
  Feature high availability and update the used open-source software in `README.md`.
  
  
    
