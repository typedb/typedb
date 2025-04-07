**Download from TypeDB Package Repository:**

[Distributions for 3.1.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.1.0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.1.0```


## New Features
- **Better type-inference errors by pre-checking constraints for empty type annotations**
  Add a pre-check to see if type-seeding could not annotate some edge. Since such an error is localised to a pattern rather than an edge, it lets us have more descriptive errors. Such trivial cases are likely to account for most type-inference errors.
  Sample:
  ```
  mydb::read> match $c isa cat; $n isa dog-name; $c has $n;

  INF11
  [INF11] Type-inference was unable to find compatible types for the pair of variables 'c' & 'n' across the constraint '$0 has $1'. Types were:
  - c: [cat]
  - n: [dog-name]
  Caused: [QUA1] Type inference error while compiling query annotations.
  Caused: [QEX8] Error analysing query.
  Near 1:39
  -----
  --> match $c isa cat; $n isa dog-name; $c has $n;
                                            ^
  -----
  ```

  Since type-seeding is non-deterministic when annotating variables without explicit labels, the edge flagged may vary.
  ```
  INF11
  [INF11] Type-inference was unable to find compatible types for the pair of variables 'n' & 'dog-name' across the constraint '$1 isa dog-name'. Types were:
  - n: [name, cat-name]
  - dog-name: [dog-name]
  Caused: [QUA1] Type inference error while compiling query annotations.
  Caused: [QEX8] Error analysing query.
  Near 1:23
  -----
  --> match $c isa cat, has dog-name $n;
                            ^
  -----
  ```

  In cases where every edge (in isolation) can be annotated, the existing & less-descriptive "unsatisfiable pattern" error will still be returned.


- **Allow abstract relation types having zero role types.**
  Allow abstract relation types having zero role types. It is no longer a requirement for a relation type to declare or inherit role types if it's abstract.

  Non-abstract relation types are still required to have at least one role type.
  Notice that it can be a single abstract role type. However, such a schema does not allow preserving any data related to this relation type as its instances will get cleaned up on commits due to the absence of role players.

  Additionally, a bug in relation type deletion has been fixed. In some cases, deletion of a relation type could affect specialized role types of its supertype.


- **Match disjunction variables as optional**

  We treat variables that are only used in a branch of a disjunction as optionally matched. If the branch is not used by a given row, the column is left empty.


- **Introduce update queries**
  We introduce update queries as a shortcut for `delete + insert`. Using `update`, it is possible to replace an existing `has` or `links` edge of a specific attribute or role type by a new value in one step.

  If an old value exists, it is deleted. Then, the new value specified in the query is set.

  This feature is only available for `has` and `links`. Moreover, to make sure that no excessive data is unexpectedly deleted, it only works with types with `owns` and `relates` of cardinalities not exceeding 1: `card(0..1)` (the default value) or `card(1..1)` (card of `@key`).

  Usage examples:

  ```
  match
    $p isa person, has name "Bob";
  update
    $p has balance 1234.56789dec;
  ```

  ```
  match
    $alice isa person, has name "Alice";
    $bob isa person, has name "Bob";
    $e isa employment, links (employer: $alice);
  update
    $e links (employee: $bob);
  ```


- **Let attribute supertypes be not abstract and fix independent attributes behavior**
  Remove the constraint that attribute types can be subtyped only if they are abstract. Now, it is possible to build schemas like:
  ```
  define
    attribute name;
    attribute first-name sub name;
    attribute surname sub name;

    entity dog owns name;
    entity passport owns first-name, owns surname;
  ```

  Or be less restrictive than before with:
  ```
  define
    entity person owns name, owns first-name, owns surname;
  ```
  Be aware that annotations of `name` will still be applied to all instances of `first-name` and `surname`. Thus, the schema above will let a `person` have either one `name`, one `first-name`, or one `surname`. Use the `@card` annotation to override the default cardinality values for ownerships.

  Additionally, a couple of rare bugs related to `has` deletion and dependent attribute retrieval were fixed. This includes:
  * Dependent attributes without owners can no longer appear after the query affecting these attributes is run. While it can be a part of the query result (e.g. the returned attribute variable when deleting a `has`), it will no longer be accessible for further queries before and after the commit, thus deleted from the system.
  * Abstract attribute supertypes are now consistently returned as results for valid queries.



## Bugs Fixed
- **Make type annotations local to a conjunction**
  Making type annotations local to each subpattern leads to cleaner handling of variables which cross into subpatterns or subsequent stages.


- **Fix disjoint variable validation logic**

  Fix the condition for the disjoint variable reuse detection (`Variable 'var' is re-used across different branches of the query`).


- **Release Windows in published mode and introduce declared features for Cargo sync**
  Fix a bug when Windows release binaries are published in the development mode.


- **Extend Docker OS support and reduce TypeDB Docker image size**
  Update base Ubuntu images for Docker instead of generating intermediate layers through Bazel to potentially support more operating systems that expect additional configuration possibly missed in Bazel rules by default.

  This change is expected to allow more systems to run TypeDB Docker images, including Windows WSL, and significantly reduce the size of the final image.


- **Various fixes around variable usage**
  * Extend the check for empty type-annotations to include label vertices
  * Store function call arguments in a Vec, so they are ordered by argument index.
  * Flag assignments to variables which were already bound in previous stages
  * Flag anonymous variables in unexpected pipeline stages instead of assuming named & unwrapping.


- **Fix incorrect set of input variables to nested patterns**

  During query plan lowering, we separate the current output variables (which represent the output variables of the plan being lowered) from variables available to the pattern being lowered, dubbed the row variables.


- **Optimize unique constraints operation time checks for write queries**
  Modify the algorithm of attribute uniqueness validation at operation time, performed for each inserted `has` instance of ownerships with `@unique` and `@key` annotations.
  These changes boost the performance of insert operations for constrained `has`es by up to 35 times, resulting in an almost indiscernible effect compared to the operations without uniqueness checks.


- **Handle assignment to anonymous variables**

  We allow function assignment to discard values assigned to anonymous variables (such as in `let $_ = f($x);`) as there is no space allocated for those outputs. Previously the compilation would crash as it could not find the allocated slot for the value.


- **Fix write transactions left open and blocking other transactions after receiving errors**
  The gRPC service no longer leaves any of the transactions open after receiving transaction or query errors. This eliminates scenarios where schema transactions cannot open due to other hanging schema or write transactions and where read transactions can crash the server using incorrect operations (commits, rollbacks, ...). Additionally, it ensures the correctness of the load diagnostics data.


- **Filter out unnamed variables in expected variable positions of Put stages**
  Avoids the case where the insert stage sees a named role-type variable as both an input and a locally defined one.


- **Fix erroneously selected named variables**

  We adjust the computation of bound variables after each stage in query execution and keep better track of selected variables.


- **Fix concurrent schema and write transactions locking issues**
  Fix concurrent transactions locking issues, allowing multiple users to open schema and write transactions in parallel with guaranteed safety and availability whenever it's possible.
  * When a transaction opening failure originated from the schema lock being held by an existing schema transaction, a deadlock situation appeared, preventing new transactions from being opened after the schema lock release. This does not happen anymore.
  * Schema transaction opening requests could be ignored and rejected on timeout because of preceding write transactions in the request queue, even if they were closed in time. The retry mechanism has been corrected to prevent any request from being ignored.


- **Avoid writing tombstones for temporary relation indices**
  Fix relation index eager deletes to avoid writing tombstones for temporary relation indices and corrupting statistics.


- **Implement fixes for duplicate inserts & redundant deletes**
  Implement fixes for duplicate inserts & redundant deletes


- **Unsatisfiable type constraints do not cause type-inference errors; Enable more BDD tests**
  Queries containing unsatisfiable type-constraints no longer fail type-inference. Instead the (sub)pattern returns 0 answers.  We also enable BDD tests which were failing due to overly strict debug_asserts


- **Resolve ambiguity in duration arithmetic**

  We stabilize duration arithmetic with datetimes with time zones in cases where the resulting datetime is ambiguous or does not exist in the target time zone.


- **Handle zero-length rows in batch**

  Fix the bug where iteration over zero-length rows in a fixed batch caused a panic.


- **Avoid writing tombstones for temporary concepts**

  When deleting an entity or an attribute that was inserted in the same transaction (i.e. never committed), we remove the key from the write buffer rather than create a tombstone. This logic was already present for relations.


- **Allow duration literals without date component**

  We add support for duration literals without the date component in TypeQL (https://github.com/typedb/typeql/pull/394).



## Code Refactors
- **Compile only the functions referenced in the query**
  Updates the compiler to only compile those functions referenced by the query.


- **Improve readability of pattern executor**
  Improve readability of pattern executor


- **Simplify Batch by letting Vec do the work**
  Simplify Batch by letting Vec do the work


- **Make TypeDB logo configurable**
  No visible product changes


- **Remove server domain-leak in the main function**
  - The main and server module is refactored for better maintainability


- **Cyclic function retries at cycle entry**
  Refines cyclic function re-evaluation to happen at the entry of such a cycle. Identifies strongly connected components (SCCs) in the function dependency graph to determine the entry point.



## Other Improvements
- **Refactor sort stage to avoid code duplication**
  Moves the duplicated batch sorting code to the batch, rather than both the pipeline sort stage, and the function sort executor.


- **Update release notes and version for 3.1.0-rc2**

- **Add quotes to support spaces in windows launcher**
  Add quotes in typedb.bat to support spaces in the path to the TypeDB executable.


- **Update erorr message**

- **Sync behaviour tests transaction management based on query errors with the gRPC service**
  Behaviour tests now close the active transaction when a logical error is found in write and schema transactions, and leave transactions opened in case of a parsing error, which is how the real gRPC service acts.

- **Update README.md**

- **Implement put stage**
  Implements the `put` stage.
  A `put <pattern>;`will perform an insert if no answers matched `<pattern>`; else, it returns all answers matched by the pattern.
  **Note:** The put matches the entire pattern or inserts the entire pattern. Partial matching and partial inserts are not performed.

  **Examples**
  ```
  insert $p isa person, has name "Alice"; # Create some initial data.
  ```

  ```
  put $p1 isa person, has name "Alice"; # Does nothing
  ```
  ```
  put $p2 isa person, has name "Bob"; # Creates a person with name Bob
  ```

  ```
  put $p3 isa person, has name "Alice", has age 10; # Creates a new person with name "Alice" & age 10.
  ```
  The above statement (for $p3) creates a new person because the pattern fails to match, as the existing person with name "Alice" does not have age 10.
  To achieve the desired result of not re-creating the person if only the age attribute is missing, `put` stages can be pipelined.
  ```
  put $p4 isa person, has name "Bob"; # Will match the person with name "bob", if it exists.
  put $p4 isa person, has age 11; # Adds an attribute to the same person, since we use $p4 again.
  ```



- **Blueprints**

  Add blueprints for key pieces of TypeDB's database architecture. Blueprints are both meant to:

  * **record existing architecture** of TypeDB
  * **foresee and outline future architectural** changes


- **Fix retries for suspended recursive functions**
  Fixes a bug where recursive functions were not retried properly if called by a negation, or collecting stage such as 'sort' or 'reduce'.
  Also fixes a panic which wrongly assumed suspend points could not exist at certain modifiers.


- **Update dependencies for 3.1**
  Update dependencies prior to the 3.1 release


- **Allow multiple expression assignments in different branches**
  Allows multiple expression assignments for the same variable in different branches - essential for meaningful recursive functions.
  * Assignments in different branches of the same disjunction are allowed
  * Assignments in different constraints/nested-patterns of the same conjunction are not allowed.
  * Assignments across branches must have the same value type.

  Example isage:
  ```
    with
    fun factorial($i: integer) -> { integer }:
    match
        { $i <= 1; let $factorial = 1; } or
        { $i > 1; let $factorial = $i * factorial($i - 1); };
    return { $factorial };

    match
        let $f_5 in factorial(5);
  ```

- **Point dependencies to upstream**

- **Implement like & contains comparators**
  Implement like & contains comparators for strings.
  - The 'like' operator expects the right operand to be a string literal which is a valid rust regex. The left operand may be a string literal or a variable holding a string value.
  - The 'contains' operator first performs [case-folding](https://www.w3.org/International/wiki/Case_folding) on either operand, then checks whether the (folded) left operand contains the (folded) right operand. Both operands may be string literals or variables holding string values.


- **Rename missed OptimisedToUnsatisfiable to Unsatisfiable**

- **Remove erroneous INFO logging**

- **Introduce docker snapshot jobs**
  Introduce docker snapshot jobs.


- **Enable functions in write pipelines**
  Enable functions in write pipelines


- **Updates for restricted TypeQL**
  Updates the translation of TypeQL based on typedb/typeql#393 , which makes the grammar stricter.


- **Increase query plan cache flush statistics fraction from 0.25 to 5.0**

- **Improve query plan eviction**

  We improve the query plan eviction policy from a flat 1% change in total count of the data, to instead be a 25% change of _any individual_ statistic, such as a specific entity type's instances, or the count of `has` between two specific types, etc. This means the query plans will be much more accurate in rapidly changing data, in particular in the common case of building an initial dataset or benchmarking.

  Before, we'd get tpcc results for the PAYMENT action that look like this:
  ```

  Execution Results after 63 seconds
  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    Complete        Time (µs)         minLatMs        p50             p75             p90             p95             p99             maxLatMs        Aborts
  ...
    PAYMENT         20                 61.618         7.90            12.91           1244.86         15993.48        24360.62        24360.62        24360.62        0
  ...
  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  ```

  This shows that during a 60s execution time, a few pathological query executions would blow up the entire runtime! However, sometimes the query also executed quickly. This was fundamentally caused by the initial queries using the query plan based on old statistics, which would only get updated later in the benchmark when the total statistics updated sufficiently.


- **3.0 distinct and select for functions**

  We provide a way to de-duplicate rows in pipelines via a `distinct` stage.


- **Stackify ConceptReadError + Add Query Planner errors**

  We now have stack traces for concept read errors.


- **Add setup_remote_docker step to docker deployment jobs**
  Add the  'setup_remote_docker' step to docker deployment jobs



