### Install

**Download from TypeDB Package Repository:**

[Distributions for 3.0.0-alpha-6](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.0-alpha-6)

**Pull the Docker image:**

```docker pull vaticle/typedb:3.0.0-alpha-6```


## New Features
- **Disjunction support**
  
  We introduce support for disjunctions in queries:
  
  ```php
  match
      $person isa person;
      { $person has name $_; } or { $person has age $_; };
  ```


- **Fetch execution**

  We implement fetch execution, given an executable pipeline that may contain a Fetch terminal stage.

  Note: we have commented out `match-return` subqueries, fetching of expressions (`{ "val" : $x + 1 }`), and fetching of function outputs (`"val": mean_salary($person)`), as these require function-body evaluation under the hood - this is not yet implemented.



## Bugs Fixed

- **Fix document answers streaming for fetch**
  We fix document answers streaming for fetch in order to complete the first version of `fetch` queries execution.


## Code Refactors
- **Function compilation preparation & trivial plan implementation**
  Prepares higher level packages for compiling functions.


- **Fetch annotation, compilation, and executables**

  We implement Fetch annotation, compilation and executable building, rearchitecting the rest of the compiler to allow for tree-shaped nesting of queries (eg. functions or fetch sub-pipelines).

- **Fetch iii**

  We implement further refactoring, which pull Fetch into Annotations and Executables, without implementing any sub-methods yet.


## Other Improvements

- **Add database name validation for database creation**
  We add validation of names for created databases. Now, all database names should be valid TypeQL identifiers.

- **Enable connection BDDs in CI**
  We update some of the steps for BDDs to match the [updated definitions](https://github.com/typedb/typedb-behaviour/pull/303).
  Additionally, we add connection BDDs to the factory CI to make sure that this piece of the system is safe and stable.

- **Refactor compiler**
  
  We refactor the compiler to standardize naming, removing the usage of 'program', and introducing 'executable' as the second stage of compilation.
  
- **Refactor pipeline annotations**
  
  We implement the next step of Fetch implementation, which allows us to embed Annotated pipelines into Fetch sub-queries and into functions. We comment out the code paths related to type inference for functions, since functions are now enhanced to include pipelines.
  
    
