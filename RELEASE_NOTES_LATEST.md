**Download from TypeDB Package Repository:**

[Distributions for 3.12.2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.12.2)

**Pull the Docker image:**

```docker pull typedb/typedb:3.12.2```


## New Features
- **Add parsing and translation caches**
  
  With the introduction of the given stage, there is now massive benefit in avoiding re-parsing and translating queries that are string-identical - parameters are now generally expected to be extracted out of the query.
  
  We convert our old compilation query cache into a three-stage cache to take advantage of this fact.
  
  1) parse cache: string -> parsed data pipeline. Purely syntactic, so it is never invalidated. Only data pipelines are cached; schema queries (define/redefine/undefine) are not included
  2) translation cache: string -> translated IR. Translation resolves user-defined functions, so it is flushed on schema commits (but not on statistics-only changes). 
  3) compile cache: translated IR -> executable pipeline. Flushed on schema commits, and when statistics drift far enough to change query plans.
  
  Splitting parse from translate also lets a query be parsed with no transaction: queries arriving mid-write are parsed immediately and their translation deferred until the write completes. 


- **Implement ability to rename types**
  Entity, relation and attribute types can be renamed by doing `redefine old-label label new-label;`. Role types can be renamed by doing `redefine declaration-relation:old-role label new-role;`.



## Bugs Fixed
- **Fix 'given' HTTP api bugs**
  
  We fix bugs discovered by implementing BDD tests for HTTP requests using raw values for 'given' input rows.
  
  
- **Fix admin tool's RPC error representation**
  
  `RpcFailed` error of the admin tool used to swallow the details of the error message by ignoring the error details that are used excessively by the server.  
  
  Now, we print the errors with all the details nicely, following the usual TypeDB's error stack trace style.
  
  Before (totally inaccurate: there were no invalid arguments):
  ```
  [ADM2] Client specified an invalid argument: Request generated error
  ```
  
  Now:
  ```
  [ADM2] Request failed.
  [CSV5] Unable to register replica....
  ```
  
  

## Code Refactors
- **DefinitionKey holds Prefix & DefinitionID instead of raw bytes**
  Refactor `DefinitionKey` struct to hold Prefix & DefinitionID instead of raw bytes, in line with other stored objects.
  
- **Introduce macros for ExpressionOpCode enum & dispatching methods**
  Introduces macros for defining the `ExpressionOpCode`  enum & implementing the dispatch methods.


- **Move type inference logic into own module**
  Move type inference logic into own module




## Other Improvements
- **Validate conjunction can have valid plans in IR**
  We use the variable binding modes to ensure there are no subpatterns whose inputs cannot be satisfied.

- **Distribute typedb-all as signed mac installer**
  Publishes a signed mac installer to cloudsmith.

  
- **Only attempt to cleanup each attribute once when owner deleted**
  
  During a commit, we delete all dependent attributes that have no owners. To do that, we iterate through all deleted `has` edges and check whether the previously owned attribute has any remaining owners, and if not, delete it. This check involves querying the state of the DB on disk.
  This PR ensures each attribute is checked only once.
  

- **Introduce distribution tests for apt, brew, docker snapshots**
  Introduce distribution tests for apt, brew, docker snapshot into circleci.


- **BDD: Each feature runs on a separate TypeDB server instance for parallelisation**
  Initialise server instance in the test call, so we can run multiple features in parallel (not multiple scenarios)
  
  
    
