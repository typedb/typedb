**Download from TypeDB Package Repository:**

[Distributions for 3.1.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.1.0-rc0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.1.0-rc0```


## New Features
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
- **Remove server domain-leak in the main function**
  - The main and server module is refactored for better maintainability


- **Cyclic function retries at cycle entry**
  Refines cyclic function re-evaluation to happen at the entry of such a cycle. Identifies strongly connected components (SCCs) in the function dependency graph to determine the entry point.



## Other Improvements

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



