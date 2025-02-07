**Download from TypeDB Package Repository:**

[Distributions for 3.0.6](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.6)

**Pull the Docker image:**

```docker pull typedb/typedb:3.0.6```


## New Features
- **Database debug tools**
  
  We introduce the database/tools package, hosting small CLI tools to help investigate misbehaving or corrupted databases.
  
  Tools included in this commit:
  - read-wal: read and dump the contents of the WAL in the database at specified sequence numbers;
  - replay-wal: replay the records from one WAL to another, filtering by record type and sequence numbers.
  
  
- **Introduce multi-staged server termination**
  Make server termination through `CTRL-C` interrupt opened transactions to proceed with the resource deallocation.
  Introduce two stages of termination:
  * When `CTRL-C` is pressed, a graceful termination will be initiated, ensuring that all resources are freed securely and all open transactions finish preparing current batches of streamed answers (this might take some time to calculate answers for the whole batch, but will not wait until all the answers are sent).
  * When `CTRL-C` is pressed while a graceful termination is active, the server's process will be forcefully immediately terminated. This prevents the server from hanging because of long-running queries or other issues. 
  
  
- **Checkpoint databases at regular intervals**
  
  We checkpoint the database every minute, if changes to it were made.
  
  

## Bugs Fixed
- **Avoid persisting empty statistics deltas**
  
  We resolve the issue in which statistics would be written to the WAL more often than necessary, which could cause WAL (not the data) to become corrupted.
  
  
- **Fix getting annotations when argument is not referenced in body**
  Fixes a crash during function annotation when  a function argument is not used in the body of the function.
  
  
- **Include deleted concepts in structural equality for delete stage**
  Include deleted concepts in structural equality for delete stage. This avoids a bug where the query cache picks the wrong cached query and runs it - allowing the execution of a delete stage with a totally different set of deleted concepts.
  
  
- **Add flag to type-seeder for write stages**
  Adds a flag to type-seeder to indicate the stage is a write stage. This ensures variables constrained by `isa`, or labelled roles are seeded with the exact type, and not (transitive) subtypes. This fixes a bug where one could not insert a role-player for a role with a subtype.
  
  
- **Remove cardinalities operation time validation**
  Remove cardinalities operation time validation. Now, both schema and data modifications lead to cardinalities revalidations only on commits.
  This fix solves https://github.com/typedb/typedb/issues/7317.
  
  

## Code Refactors


## Other Improvements
- **Introduce brew and apt deployment**
  Introduce brew and apt deployment
  
  
- **Fixes for the is_write_stage type-seeder fix**

- **Activate diagnostics reporting for the Docker image**
  The TypeDB Docker image receives enough minimal dependencies to report diagnostics and error data for maintenance.
  
  
- **Syncing up planning and lowering**
  
  We synced up three subsystems:
  1. Planner logic for selection of pattern traversal direction 
  2. Planner logic for selection of sort (a.k.a. join) variables
  3. Lowering logic for both, which in same cases overwrote choices made by the planner leading to errors.
  
  
    
