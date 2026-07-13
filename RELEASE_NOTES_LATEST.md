**Download from TypeDB Package Repository:**

[Distributions for 3.12.1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.12.1)

**Pull the Docker image:**

```docker pull typedb/typedb:3.12.1```


## New Features


## Bugs Fixed
- **Fix writes deadlock for huge commits**
  
  Fix a deadlock in large (> configured RocksDB write buffer manager limit) commits, by undoing the `allow_stall` parameter in the write buffer manager. This means that write buffers can now temporarily exceed the configured memory budget.
  
  

## Code Refactors


## Other Improvements
- **String comparison fix**
  
  Fix the bug where during retrieval of a string attribute, the comparison bounds were applied incorrectly, discarding more answers than expected.
  
  
- **Fix runtime error handling in fetch stages executed in write queries**
  Fixes a niche bug (#7864) in handling of runtime errors when fetch queries are executed in write transactions by delaying `Arc::into_inner(snapshot)` till the iterator holding a copy of the Arc is dropped.
  
  
    
