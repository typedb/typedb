Install & Run: https://typedb.com/docs/typedb/2.x/installation


## New Features

- **Allow type variables in insert and delete clauses**

  We allow using type variables in delete and insert clauses. This allows much more expressivity when doing deletes, in particular allowing disjunctions over types in the `match`, which are then operated over in turn in the `delete` clause to clean up relations or attributes.

  This change resolves https://github.com/vaticle/typedb/issues/6755



- **Deletes are idempotent instead of throwing**

  We move partially towards the 'assert-style' deletion outlined in #6882 to allow idempotent deletion. Previously, deletes would throw when trying to delete a previously-removed/non-existent connection.

  We now guarantee that all deletes will continue even if trying to delete a relationship, ownership, role player, or concept that doesn't exist (ie. was previously deleted).

  For example, the following is now valid:
  ```
  match 
  $x has $a, has $b; $b = $a;
  delete
  $x has $a;
  ```
  Because the same attribute would be generated twice by the `match`, the second delete would fail and throw an error. This is now an idempotent operation, which will continue without an error.

  We also introduce a new operation to disallow a new class of semantically unsafe queries. We add an explicit 'runtime' type check before performing the idempotent delete operation. This means the following semantically invalid query will still throw an exception when any data matches:
  ```
  match
  $x isa person;
  $id isa company-id;
  delete
  $x has $id;
  ```
  

- **Role types are always concrete and inheritable**
  
  We address https://github.com/vaticle/typeql/issues/203 by making all role types in relations to be concrete - even when the relation type is abstract. The resulting behaviour is exactly as when using non-abstract relation type hierarchies.
  
  The following is now permissible:
  ```
  define
  membership sub relation, abstract, relates member;
  group-membership sub membership; // inherit 'member'
  
  <commit>
  
  insert
  (member: $x) isa group-membership;
  ```
  
  **Migration:** new abstract relation types defined will automatically have concrete relation roles. However, old databases with pre-existing abstract relation types will preserve role abstractness. The simple way to migrate to the new behaviour is to unset and set the abstractness of each abstract relation type via the Concept API:
  ```
  # pseudocode
  for rel_type in relation_types:
    if rel_type.is_abstract():
      rel_type.unset_abstract()
      rel_type.set_abstract()
  commit()
  ```
  
  **Warning:** there is a gap in the semantics of inheriting role types. Role types are considered independent types, and validated as such. In the above example, the role type `group-membership:member` doesn't actually exist, it aliases to `membership:member`.
  
  This means, if we write the following:
  ```
  define
  person plays group-membership:member;
  ```
  this actually implies `person plays membership:member`.
  
  Further, any relation types that inherit the `member` role type are automatically playable by the `person`:
  ```
  define
  person sub entity, plays membership:member;
  team-membership sub membership;
  
  <commit>
  
  insert
  (member: $x) isa team-membership;
  (member: $x) isa group-membership;
  (member: $x) isa membership;
  ```
  These are all legal inserts.
  
  This gap in the expressivity has always existed in the language when using non-abstract relation type hierarchies. Resolving this would imply solving https://github.com/vaticle/typeql/issues/274, either by automatically creating a new `member` sub-role for each relation sub-type, or by requiring the user do so explicitly. In the meantime, the workaround is to explicitly override the inherited role with a new name in the child:
  ```
  define
  team-membership sub membership, relates team-member as member;
  ```
  
  
- **Add configurable reasoner plan logging & periodic printing of counters**
  Adds logging of reasoner plans and reasoner performance counters. These can be toggled through the config file &  log-level of respective loggers.
  
  To enable & log reasoner plans:
  ```
  log:
    ...
    logger:
      ...
      reasoner-planner:
          filter: com.vaticle.typedb.core.reasoner.planner
          level: debug
          output: [ stdout, file ]
  
  ```
  
  To enable & log reasoner counters:
  ```
  log:
    ...
    logger:
      ...
      reasoner-perf-counters:
        filter: com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters
        level: debug
        output: [ stdout ]
    ...
    debugger:
      reasoner-tracer:
        enable: false
        type: reasoner-tracer
        output: file
      reasoner-perf-counters:
        enable: true
        type: reasoner-perf-counters        
  ```
  

## Bugs Fixed


## Code Refactors


## Other Improvements

- **Update readme file with the website content**
  
  Update the readme file to be in-line with the new typedb.com website.
  
  