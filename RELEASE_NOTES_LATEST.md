### Install

**Download from TypeDB Package Repository:**

[Distributions for 3.0.0-alpha-7](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.0-alpha-7)

**Pull the Docker image:**

```docker pull vaticle/typedb:3.0.0-alpha-7```


## New Features

- **Expression support**

  We add expression execution to match queries:

  ```php
  match
      $person_1 isa person, has age $age_1;
      $person_2 isa person, has age $age_2;
      $age_2 == $age_1 + 2;
  ```


- **Basic streaming functions**

  Implement function execution for non-recursive stream functions which return streams **only**.
  **Recursive functions & Non-stream functions will throw unimplemented.**

  These can currently only be used from the preamble. Sample:
  ```php
             with
              fun get_ages($p_arg: person) -> { age }:
              match
                  $p_arg has age $age_return;
              return {$age_return};
  
              match
                  $p isa person;
                  $z in get_ages($p);
  ```

- **Implement tabling machinery for cyclic functions**

  Introduces machinery needed to support finding the fixed-point of cyclic function calls. Cyclic functions now run, and return an incomplete set of results followed by an error. It is possible that the planner chooses a plan.

## Bugs Fixed

- **Minor query planner bug fixes**

  1. Fix the issue where an iterator would attempt to use a variable before assigning to it.
  2. Let role name constraint behave as a post-check.

## Code Refactors
- **Resolve warnings**
  

## Other Improvements
 
- **Update protocol to 3.0.0-alpha-7 release**

- **3.0 Rename Ok.Empty to Ok.Done. Add query_type to the Done response**

  We add a `query_type` field to all the `Query.InitialRes.Ok` protobuf messages to support retrieval of this information for any `QueryAnswer` on the client side.
  Additionally, the `Ok.Empty` message was renamed to `Ok.Done`.

- **Fix and add to CI concept BDD tests**

  We fix data commit error collection and checks and add `concept` package BDD tests to CI.

