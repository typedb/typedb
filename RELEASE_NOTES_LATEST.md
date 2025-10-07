**Download from TypeDB Package Repository:**

[Distributions for 3.5.2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.5.2)

**Pull the Docker image:**

```docker pull typedb/typedb:3.5.2```


## New Features


## Bugs Fixed
- **Fix variable name unwrap when creating error message in insert executable**
  Fixes an unwrap when reading the variable name for returning an error message
  
  
- **SpillOverCache removes key before inserting**
  Fixes a bug causing in SpillOverCache where a key can be inserted in both the memory backed map, and the disk back mapped.
  
  
- **Fix relation indices recalculation on schema changes**
  Correctly regenerate and delete relation role player indices on schema changes. This fixes a bug when some query results could be ignored because of an incorrectly configured index used for optimization purposes.
  
  
- **Reenable Sentry crash reporting**
  Fix a bug when TypeDB Server did not send crash reports for diagnostics even when `--diagnostics.reporting.errors` was enabled.
  
  

## Code Refactors
- **Replace serializable_response macro with available serde annotations**
  Replace custom implementation of Serialize for concepts in the HTTP API with the derived serialize and serde annotations.
  
  

## Other Improvements

    
