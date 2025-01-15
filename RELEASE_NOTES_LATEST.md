**Download from TypeDB Package Repository:**

[Distributions for 3.0.2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.2)

**Pull the Docker image:**

```docker pull vaticle/typedb:3.0.2```


## New Features


## Bugs Fixed
- **Cartesian product fix**
  
  We fix the issue where a join on an indexed relation would skip all additional data within the join variable.
  
  
- **Fix rollback and remove dependency on typedb-common**
  We fix the `rollback` feature, which used to hang the transaction operated from an external driver, not allowing the operations to proceed correctly.
  
  Additionally, we remove an outdated Bazel dependency on `typedb-common`, which is no longer needed for TypeDB 3.0.
  
  

## Code Refactors
- **Improve TypeDB error macro**
  
  We refactor the `typedb_error!` macro to avoid the use of meaningful parentheses and position-dependence.
  
  
- **Update grouped reduction syntax and add reduction tests**
  
  We use the new grouped-reduction syntax. So we no longer use 'within':
  ```
  match $x...; $y ...;
  reduce $count = count($x) within $y;
  ```
  and we now write:
  ```
  match $x...; $y ...;
  reduce $count = count($x) groupby $y;
  ```
  
  
- **Remove action metrics reporting to Posthog if reporting is disabled**
  We remove action metrics reporting for all remote diagnostics endpoints if reporting is disabled. Previously, while not sharing confidential information, the server could still send the full action metrics snapshot to one of the endpoints, which contained information about user actions: numbers of opened connections, opened transactions, executed queries, etc. 
  
  

## Other Improvements
- **Extend query planning to functions & introduce IAM benchmark**
  Extends query planning to consider functions. Non-recursive functions add up planning cost estimates of every triggered function body. Recursive function planning currently just sets the recursive call cost to 1.
  
  
- **Implement release pipeline for windows**
  Implements a release pipeline for windows which uses cargo instead of bazel to build TypeDB.
  
  
- **Small cost model fix**
  
  When we, e.g., look up an attribute of specific type of a bound entity, then the cost should be proportional to the average number of attributes _of that type_ of the entity (_not_  average number of all attributes of the entity).
  
  
- **Add Sentry for critical error reporting and fix panics on diagnostics initialization**
  We add integration with Sentry to allow critical errors (e.g. `panic!`s) reporting. This information will help us see and eliminate unexpected TypeDB Server crashes.
  Please use the `-diagnostics.reporting.errors` boolean option to disable this feature if its work is undesirable (note that it will reduce the efficiency of our maintenance).
  
  Additionally, an overflow subtraction bug that sometimes affects diagnostics initialization and leads to crashes is fixed.
  
  
- **Add a more user-friendly taken address error**
  Introduce a more user-friendly and explicit "Address already in use" error while running a second instance of the server with the same addresses and monitoring enabled:
  ```
  Ready!
  WARNING: Diagnostics monitoring server could not get initialised on 0.0.0.0:4104: 'error creating server listener: Address already in use (os error 48)'
  Exited with error: [SRO7] Could not serve on 0.0.0.0:1729.
  Cause: 
           tonic::transport::Error(Transport, Os { code: 48, kind: AddrInUse, message: "Address already in use" })
  ```
  
  
- **Add more value types to typeql tests**

- **Return concept bdds, fix build**

    
