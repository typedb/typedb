**Download from TypeDB Package Repository:**

[Distributions for 3.14.0-rc1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.14.0-rc1)

**Pull the Docker image:**

```docker pull typedb/typedb:3.14.0-rc1```


## New Features
- **Reject importing relations without roleplayers**
  
  TypeDB currently does not preserve relations without role players on disk, and it is impossible to export a database containing independent relations. However, external migration tools can produce migration streams with such relations, which will lead to future data loss, as these relations cannot exist after migration.
  
  To avoid this, we explicitly reject such relations until a possible analogue of explicit `@independent` is introduced for relations.
  
  

## Bugs Fixed
- **Checkpoint before serving imported databases**
  
  Finalising an import closed the staged database without a checkpoint, so the reopen replayed every commit since the last periodic checkpoint, all held in memory at once. During an import, that is up to a minute of the heaviest writes, and potentially several GB of memory, all to recover state the database already had.
  
  `DatabaseOpenError::StatisticsPersist` (DBO18) is removed: a statistics persistence failure at startup is now reported as `StatisticsInitialise` (DBO11).
  
  

## Code Refactors


## Other Improvements
- **Simplify inlined check functions to avoid Boxed functions as far as possible**
  The inlined checker had multiple levels of Box<dyn> functions. We rewrite it to use concrete structs to capture the required state. We also parametrise the outline checks on the "source" of what's being checked (used to be tuples, now generic to rows and/or tuples) ,allowing us to unify the implementation of inline and outline checks. 
  
    
