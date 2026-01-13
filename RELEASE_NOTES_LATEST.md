**Download from TypeDB Package Repository:**

[Distributions for 3.7.3](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.7.3)

**Pull the Docker image:**

```docker pull typedb/typedb:3.7.3```


## New Features


## Bugs Fixed
- **Cleaner snapshot drop**
  
  We eliminate `SnapshotDropGuard`, which was only protecting us half the time, in favor of a direct `drop` implementation on Snapshot. Misuse of the drop guard could lead to difficult to diagnose and sporadic memory leaks.
  
  
- **Simplify merge iterator seek**
  
  We simplify the implementation of merge iterators by removing explicit tracking of the iterator state. The iterator queue fully determines the state instead.
  
  We also resolve the issue where a seek on a just-initialized merge iterators would cause a crash.
  
  
- **Fix index usage in ImmediateExecutor::find_next**
  Fix mixed up array index in `ImmediateExecutor::find_next`
  
  
- **Detect empty iterator queue in intersection iterator**
  
  Keep the exhausted iterator in a `Done` state when seeking.
  
  Seeking on an exhausted intersection iterator just after retrieving its last element would put it in a `Used` state with no iterators in queue. Retrieving the next item from it would then make it panic as there is no next iterator in the queue, causing a server crash.
  
  

## Code Refactors
- **Speed up bazel build**
  
  Add `--remote_download_toplevel` to the bazel-rc for build jobs, which improves CI build times using the remote cache significantly (10-20% baseline) by not downloading intermediate rule outputs. We also allow parallelization of multiple checkstyle jobs.
  

## Other Improvements
- **Update README with resource links**

- **Update links in README for newsletter and learning center**

- **Update TypeDB site link in README**

- **Fix TypeDB banner image link in README**

- **Create CONTRIBUTING.md with contribution guidelines**

- **Remove Discussion Forum badge from README**

- **Reduce diagnostics noise from CI of TypeDB users**
  
  Add a requirement for `initial_delay` of diagnostics reports, which, if not met, forces the reporter to skip the first report cycle. This allows silencing CI jobs built by the users of TypeDB not using the `diagnostics.reporting.metrics` flag without significant harm to the data (the data will be added to the next report unless the server stops early).
  
  Additionally, clean up the diagnostics logic from the outdated code from TypeDB 2.x not used in TypeDB 3.x.
  
  
    
