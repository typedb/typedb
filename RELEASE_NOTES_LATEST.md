Install & Run: https://typedb.com/docs/home/install

Download from TypeDB Package Repository: 

Server only: [Distributions for 2.27.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-server+version:2.27.0-rc0)

Server + Console: [Distributions for 2.27.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-all+version:2.27.0-rc0)


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
  
## Bugs Fixed


## Code Refactors


## Other Improvements
- **Clarify error message on illegal type specifier in query**

- **Rename TypeDB Enterprise to TypeDB Cloud**
  
  Whenever an error message previously referred to TypeDB "Enterprise" edition, it now refers to "Cloud" instead.
  
