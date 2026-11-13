**Download from TypeDB Package Repository:**

[Distributions for 3.15.0-rc1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.15.0-rc1)

**Pull the Docker image:**

```docker pull typedb/typedb:3.15.0-rc1```


## New Features


## Bugs Fixed
- **Limit database import commits by estimated write size**
  
  Database import becomes friendlier to small machines with data sets containing many huge string attributes: intermediate commits are limited by the estimated size of their writes, not only by their number.
  
  
- **Verify length of WAL records**
  
  We add a check that the compressed WAL record had the expected length, which prevents WAL corruption in adverse conditions.
  
  

## Code Refactors
- **Cut the per-key overhead of storage range iteration**
  
  Small encoding decode helpers are now inlined across crates (since Bazel doesn't enable LTO right now), and vertex and edge types return their stored prefix instead of re-encoding themselves. In addition, key extraction from the stored key + metadata no longer goes through constructing an `MVCCKey`, which appears responsible for the bulk of the speedup.
  
  `MVCCRangeIterator` now uses `Peekable<KeyspaceRangeIterator>` directly instead of unsafely storing the pointer to its held item.
  
  On mac (arm64) the benchmark's time for a scan dropped by ~20%, from ~120ms to ~100ms.
  
  
- **Sync cargo files to use latest typeql 3.15.0-rc0**
  Sync cargo files to use 3.15.0-rc0 
  
  

## Other Improvements
- **Fix TypeQL & protocol references in Cargo**
    
