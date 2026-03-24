**Download from TypeDB Package Repository:**

[Distributions for 3.8.2](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.8.2)

**Pull the Docker image:**

```docker pull typedb/typedb:3.8.2```


## New Features


## Bugs Fixed
- **Isolation: only fetch commit data when necessary**
  
  The data of an aborted commit is never examined again. During recovery, we don't bother putting it into the isolation manager timeline and discard the data immediately.
  
  In the (rare) event isolation manager fetches the status of an aborted commit during recovery, it expects the record of an aborted commit to exist even though it's not used. This change makes it so the commit record is only fetched when needed for status.
  
  
- **Ensure reduce results are unavailable to groupby**
  Translating groupby variables before reducers ensures the reduce results variables aren't available to groupby
  
  
- **Mark commits applied during recovery**
  
  Fix crash during recovery when the commit validation result was not persisted in the WAL.
  
  The isolation manager expects that when a commit is opened reading a sequence number, all the commits up to and including the opening number must have been persisted to storage.
  
  Previously the commits were only marked as validated in the isolation manager, and only persisted at the very end of recovery. This meant that if the commit corresponding to the open sequence number of the unvalidated commit was not present in the checkpointed storage, the isolation manager would consider opening the pending commit for validation a violation of the watermark invariant, a fatal error.
  
  
- **QueryCache replaces cached preamble function parameters on get**
  Fixes a bug where cached queries would use the cached parameters in preamble pipelines, rather than those from the actual query - leading to wrong results when only the parameters in the preamble had been updated (Fixes: #7702)
  
  

## Code Refactors
- **Reflect the TypeQL refactor introducing the Reducer::Collect variant**
  Update translation to reflect the TypeQL refactor introducing the `Reducer::Collect` variant for like lists.
  
  
- **Workspace dependencies**
  
  Collect all dependencies in a single list under cargo workspace that the individual crates can refer to.
  
  

## Other Improvements
- **Allow leading underscore in type labels**
  
  We update TypeQL, which permits leading underscores in type labels, matching other database systems:
  
  ```
  define
    relation _test, relates _tested;
    entity _person, plays _test:_tested;
  ```
  
  ```
  insert 
  _test (_tested: $x); 
  $x isa _person;
  ```
  
  
- **Delete incorrect randomized datetime arithmetic test**
  
  The `datetime_subtraction_always_produces_time_delta_less_than_a_day` is incorrect: subtracting across a DST time change can produce time deltas of over 24 hours which are still less than a day.
  
  For example, a DST change occurred in London on 2024-10-27 02:00:00 BST.
  
  ```
  2024-10-27T01:30:00 Europe/London - 2024-10-26T02:00:00 Europe/London == PT24H30M
  ```
  
  Adding 24 hours to the earlier date would undershoot the later by 30 minutes:
  
  ```
  2024-10-26T02:00:00 Europe/London + PT24H == 2024-10-27T01:00:00 Europe/London
  ```
  
  Adding 1 day overshoots by the same amount:
  
  ```
  2024-10-26T02:00:00 Europe/London + P1D == 2024-10-27T02:00:00 Europe/London
  ```
  
  
- **Optional inserts receive concepts created in parent as "input"**
  Reflecting how optional blocks in match statements see shared variables as "inputs", we pass any concept variables inserted in the root as input variables to optional insert blocks. 
  
  
- **Error if write stage has optional variables which are used outside try blocks**
  Checks that no optional variables are used outside try blocks in insert stages. Prevents crash in #7694
  
  
- **Improve start up resilience**
  
  We reduce the possibility of crashes during startup after an operating system failure by doing the following:
  
  - detect and discard corrupted database checkpoints on startup,
  - ignore extraneous (non-database) directories in the data directory,
  - separate checkpoints in progress from completed checkpoints. 
  
  
    
