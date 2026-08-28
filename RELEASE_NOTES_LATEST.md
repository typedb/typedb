**Download from TypeDB Package Repository:**

[Distributions for 3.13.0-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.13.0-rc0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.13.0-rc0```


## New Features
- **Allow evicting commits in isolation manager**
  
  Allow freeing memory held by old commits that are still being read, but cannot cause a conflict.
  
  
- **Add eager cleanup strategy of deleted keys**
  
  We implement an `eager` cleanup strategy, disabled by default, that scans the backing storage for inaccessible data (keys marked as deleted or shadowed by later writes) and deletes them for good.
  
  
- **Support clustered database import and export**
  
  Extend the database import and export functionalities to support these features in applications built on top of TypeDB, e.g., TypeDB Cluster.
  
  Before, both import and export operated on local data only, avoiding the new extensible state `Operator`s and keeping all the related information and handles private. Now, by becoming more open, both services support flexible extensions to their behavior.
  
  

## Bugs Fixed
- **Increase HTTP message size limit to 1GB**
  We increase the HTTP message size limit to match grpc (1GB). We also improve an the interrupted stream error message and some typos in error messages.
  
  

## Code Refactors
- **Enable networkless database export and import**
  
  Refactor database export and import logic by exposing intermediate `MigrationItems` to avoid conversions to proto messages while executing export/import in a single process.
  
  With this refactor, the core of the export and import has stronger and cleaner isolation from networking, making the gRPC services just thin and straightforward wrappers on top of the consolidated and strictly ordered logic.
  
  Additionally, fix `typedb-cluster` build by returning methods for converting `CommitIntent`s into serializable objects required for replication.
  
  

## Other Improvements
- **Cleanup record carries its commit's sequence number**
  
  Fix bug where a cleanup record would inherit the sequence number of the previous commit record in the WAL, rather than track the sequence number of the commit that it is associated with. This could lead to multiple cleanup records with the same sequence number, and conversely to some commit records lacking a corresponding cleanup record.
  
  
- **Bump amazonlinux-ci image for clang dependency & rules python**
  Bumps our amazon linux image to a new one that has clang & llvm installed.
  Bumps rules python to a version that doesn't have a silly print statement that breaks our deploy scripts on certain pythoon versions.
  
- **Fix build-breaking formatting**
  
  
  
- **Make query cache eviction less sensitive to relative changes in small databases**
  
  Reduce the impact on query cache eviction of large relative changes when the amount of data is small for the type considered.
  
  
- **Add "manual" tag to mac-installer targets**
  To stop local `build //...` on mac failing because of the signing keychain not existing.
  
  
- **Ban expressions in write stage**
  Checks for expressions in write stages (insert, put, update, delete). If found, we error instead of crashing downstream.
  
  
    
