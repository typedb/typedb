Install & Run: https://typedb.com/docs/home/install

Download from TypeDB Package Repository: 

Server only: [Distributions for 2.27.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-server+version:2.27.0)

Server + Console: [Distributions for 2.27.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-all+version:2.27.0)


## New Features
- **Metrics and diagnostics service**
  
  We introduce a metrics and diagnostics service. The web endpoint is bound at port 4104 by default and exposes metrics formatted for prometheus (`http://localhost:4104/metrics?format=prometheus`) or as JSON (`http://localhost:4104/metrics?format=JSON`).
  
  Example metrics:
  ```
  # TypeDB version: TypeDB Core 2.26.6
  # Time zone: Europe/London
  # Java version: Azul Systems, Inc. 11.0.15
  # Platform: Mac OS X aarch64 13.6.3
  
  # TYPE typedb_attempted_requests_total counter
  typedb_attempted_requests_total{kind="CONNECTION_OPEN"} 625
  typedb_attempted_requests_total{kind="SERVERS_ALL"} 625
  typedb_attempted_requests_total{kind="USER_MANAGEMENT"} 0
  typedb_attempted_requests_total{kind="USER"} 0
  typedb_attempted_requests_total{kind="DATABASE_MANAGEMENT"} 2606
  typedb_attempted_requests_total{kind="DATABASE"} 252
  typedb_attempted_requests_total{kind="SESSION"} 1207
  typedb_attempted_requests_total{kind="TRANSACTION"} 2835
  
  # TYPE typedb_successful_requests_total counter
  typedb_successful_requests_total{kind="CONNECTION_OPEN"} 625
  typedb_successful_requests_total{kind="SERVERS_ALL"} 625
  typedb_successful_requests_total{kind="USER_MANAGEMENT"} 0
  typedb_successful_requests_total{kind="USER"} 0
  typedb_successful_requests_total{kind="DATABASE_MANAGEMENT"} 2606
  typedb_successful_requests_total{kind="DATABASE"} 252
  typedb_successful_requests_total{kind="SESSION"} 1207
  typedb_successful_requests_total{kind="TRANSACTION"} 2755
  
  # TYPE typedb_current_count gauge
  typedb_current_count{kind="DATABASES"} 2
  typedb_current_count{kind="SESSIONS"} 1
  typedb_current_count{kind="TRANSACTIONS"} 1
  typedb_current_count{kind="USERS"} 0
  
  # TYPE typedb_error_total counter
  typedb_error_total{code="TXN23"} 2
  typedb_error_total{code="QRY22"} 4
  typedb_error_total{code="TXN19"} 2
  typedb_error_total{code="THR10"} 19
  ```
  
  We also extract the metrics reporter into a separate module within the diagnostics package and redirect it to the dedicated diagnostics service.
  
- **Redesign schema modification capabilities**
  
  We redesign schema modification to allow much more flexible in-place changes to the database schema. We relax various schema invariants within a schema write transaction, to allow moving and editing schema types on the fly. However, the data is validated against the schema consistency at each step, allowing full and safe use of TypeDB's existing Concept and Query API. Before committing, we can restore schema invariants guided by TypeDB's exceptions API (`ConceptManager.getSchemaExceptions()`). 
  
  
  #### Expected schema migration workflow
  
  This change facilitates large-scale database schema migration. We expect the following workflow to be adopted:
  
  1. Open a schema session, and a write transaction. This blocks writes anywhere on the system.
  2. Mutate the schema incrementally. Mutations that _expand_ schema are always possible and cheap, mutations that _restrict_ the schema are validated against the existing data for conformance to the new schema. All schema states you move through must match the current state of the data.
    a. If your data does not fit the new schema state, in 2.x you will get an exception on commit and it will roll back. You must open a data session+transaction to mutate the data into the shape it is expected to be and commit this. Then go back into schema session+transaction and retry the schema mutation.
    b. In TypeDB 3.0 these operations will be possible all within one schema write transaction, smoothing out the schema migration workflow.
  3. To make schema migration simpler, some schema invariants are relaxed *within a schema write transaction*:
    a. **Dangling overrides are allowed**: overridden types (`... as TYPE`) are allowed to refer to types that are not overridable at that place in the schema. This is common when moving a type from one supertype to a different supertype.
    b. **Redeclarations are allowed**: Declarations of `owns`, `plays`,  or annotations, may be duplicated in child types. This facilitates moving types from one supertype to a different supertype, or moving declarations up or down the type hierarchy.
    c. **Relaxed abstract ownership**: Types may own abstract attribute types without themselves being abstract.
  4. All of these invariants **must be restored before commit**, or the transaction will fail and the changes will be rolled back. To retrieve the set of errors that must be fixed before commit, use the api `ConceptManager.getSchemaExceptions()` (`transaction.concepts().getSchemaExceptions()` in most drivers).
  
  Operations that _expand_ the schema capabilities:
  - Adding a new type
  - Adding a `plays` or `owns`
  - Removing an override
  - Removing an annotation
  - Removing abstractness
  
  Operations that _restrict_ the schema capabilities:
  - Removing a type
  - Removing a `plays` or `owns`
  - Adding an override
  - Adding an annotation
  - Adding abstractness
  

## Bugs Fixed
- **Disable monitoring in assembly tests**
  
  Failure to bind a port for the monitoring server for some reason results in a failure of assembly tests on Windows. Disabling monitoring should resolve the issue.
  

## Code Refactors


## Other Improvements
- **Fix Owns constraint toString()**

- **Update anonymous variable in delete error message**

- **Merge release branch back to development after 2.27.0-rc0**
  
  We merge changes in the master branch back to development.
  
- **Update docs/banner.png for the README file**
  
  Update the banner image in the README file.
  
- **Update maven artifacts**

- **Update typedb-console-runner version**

- **Clarify error message on illegal type specifier in query**

- **Rename TypeDB Enterprise error to TypeDB Cloud**
  
  Whenever an error message previously referred to TypeDB "Enterprise" edition, it now refers to "Cloud" instead.
  
  
    
