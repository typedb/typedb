Install & Run: http://docs.vaticle.com/docs/running-typedb/install-and-run


## New Features


- **Configure logging to respect size and time limits**

  TypeDB's logger previously could over-run its expected archives size cap. This occurred because the logging framework expected to have both the time and size window configured for either to work correctly.

  TypeDB logger file configuration now has an updated set of options to control both archive retention policies for size and age.
  ```
  log:
    output:
      file:
        type: file
        base-dir: server/logs        // renamed from 'directory'
        file-size-limit: 50mb        // renamed from 'file-size-cap'
        archive-grouping: month      // new option, using: minute(s) | hour(s) | day(s) | week(s) | month(s) | year(s)
        archive-age-limit: 1 year    // new option, using: <N> minute(s) | hour(s) | day(s) | week(s) | month(s) | year(s)
        archives-size-limit: 1gb     // renamed from 'archives-size-cap'
  ```

  The `archive-grouping` option configures the rollover and naming policy of archives produced by the logger.
  The `archive-age-limit` option configures how long each archive files are kept. Note that old archives are only deleted when new ones are produced.

  The execution semantics are that every period of time defined by 'grouping', files exceeding the time limit are asynchronously deleted, and then oldest files are asynchronously deleted until the total size cap is respected.

  In the above example, log files would be compacted into archives monthly, with naming pattern like:
  ```
  typedb_202306.0.log.gz
  typedb_202307.0.log.gz
  typedb_202308.0.log.gz
  ...
  ```

  Where 1 year's worth of log archives are retained.




Resolves #6854.



## Bugs Fixed

- **Disable stamping to fix windows builds**
  
  As of #6853 we include an updated version of `rules_jvm_external`, which is used to transform maven dependencies into valid Bazel targets. However, the upgrade to version 5 also included a new default feature called 'stamping' (https://bazel.build/docs/user-manual#workspace-status), which works in general but breaks builds on Windows.
  
  This PR disables stamping by default (which supposedly also improves cache hits on remote caches), as well as shortens the Windows build directory to avoid potential long path issues in the future.
  
  
- **Fix NullPointerExceptions during reasoner stream graph construction**
  Fix a concurrency bug leading to rare NullPointerExceptions during the construction of the reasoner graph.
  
  
- **Fix server hang-ups during shutdown**
  
  Under exceptional circumstances, such as when the server runs out of memory, the server could fail to shut down. We modify the server shutdown process and unexpected exception handler to shutdown in several stages, in the most severe case halting the JVM runtime immediately.
  
  
- **Made datetime attribute insert and match time-zone invariant**
  
  Enables the BDD tests to check timezone-invariance of inserting and reading datetime attributes.
  
  

## Code Refactors

- **Expose number of outstanding tasks in ActorExecutor**
  We expose the number of outstanding tasks in an actor executor's queue. This allows us to drop retries messages on TypeDB enterprise to a pile-up of redundant work.


## Other Improvements
  
  
- **Bump vaticle dependencies for jNaCl**
  Bump vaticle/dependencies to one which includes jNaCL. This is needed for tests in TypeDB enterprise. 
  
  
- **Bump dependencies for typedb-enterprise encryption improvements**
  Bump netty dependencies for typedb-enterprise encryption changes. 
  
  
- **Merge for 2.19.1 release**
  
  Merge for 2.19.1 release.
  
    
