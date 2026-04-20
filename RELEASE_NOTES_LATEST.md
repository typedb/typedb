**Download from TypeDB Package Repository:**

[Distributions for 3.10.0-rc1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.10.0-rc1)

**Pull the Docker image:**

```docker pull typedb/typedb:3.10.0-rc1```


## New Features
- **Optimize transaction open - +240% read-write throughput**
  
  TypeDB's history versioning architecture, combined with the desire to keep full external causality, means that opening a transaction may have to wait for transactions that committed to be 'caught up to' by the visible version number (`watermark`). 
  
  We optimize this by changing to a the earliest version number that retains causality without needing client coorperation (most recent fully committed snapshot) instead of the most recently validated (but not yet KV-persisted, or ack'ed) version number. This leads to shorter wait times while the watermark catches up, speeding up in particular highly parallelized data write transactions by about 5%.
  
  ```
    300k ops, 3 runs per branch (bench-opt-pureinsert / bench-master-pureinsert)
    Threads   Batch    opt mean    ±sd     master mean    ±sd      Delta      p
          2      10      7,759    420         7,695      416      +0.8%   0.85
          4      10     14,429    286        14,173    1,194      +1.8%   0.72
          8      10     24,343    557        22,851      415      +6.5%   <0.001  ***
         16      10     34,663  1,861        29,342    1,006     +18.1%   <0.001  ***
          2     100     18,026    337        17,821      523      +1.1%   0.57
          4     100     33,414    673        31,281    1,208      +6.8%   0.008   **
          8     100     55,590  2,131        52,981    2,002      +4.9%   0.12
         16     100     61,629  2,666        58,704    7,699      +5.0%   0.53
  ```
  
  ### Secondary throughput improvements
  
  The secondary consequence of this change are highly interesting. In a mixed 50/50 read and write workload, we increase **total system query throughput by 2.4x**. Note that this and all tests were on small scale data, so mostly were in-memory (larger scale test that hit disk would also be interesting to verify)
  
  ```
  (batch=1, 100k ops, 8 thread total)
  opt 4W+4R    master 4W+4R    Delta
    Write ops/s          1,056          1,119     -5.6%
    Read ops/s           6,881          2,117   +225.1%
    Total ops/s          7,937          3,236   +145.3%
  ```
  
  This occurs because the reader threads are now spending much less time blocking & waiting for the watermark, and much more time executing.
  
  If we increase thread counts further, we find that reader threads end up starving the writer threads, as the 10 core machine is fully subscribed:
  ```
  opt 8W+8R    master 8W+8R    Delta
    Write ops/s            707          1,892    -62.6%
    Read ops/s          11,191          4,073   +174.7%
    Total ops/s         11,898          5,965    +99.5%
  ```
  
  To confirm the hypothesis that the write performance reduction is due to total machine oversubscription, we test that pure reader threads are able to fully saturate the CPU. We did an experiment showing ops performed in a fixed time period (10 seconds) with an increasing number of threads:
  
  ```
  Reader CPU Saturation Test (10 CPU cores, 10s per config)
  
    Threads   Total reads/s   Per-thread reads/s   Scaling
          1          1,720              1,720        1.0x
          2          3,755              1,877        2.2x
          4          7,391              1,848        4.3
          8         14,475              1,809        8.4x
         10         16,361              1,636        9.5x  (peak)
         12         15,879              1,323        9.2x  (declining)
         16         12,073                755        7.0x  (worse than 8!)
  ```
  
  So we can confirm that pure reader threads can fully saturate the CPU, and over-parallelizing penalizes and drops throughput! We conclude that write throughput drop in the previous test with 8 read and 8 write threads is because the 8 reader threads are essentially fully saturating 8 cores on their own, and the 8 writer threads are competing for resources. We should theoretically see similar behaviour if we had a 2 core machine with 8 writer threads.
  
  ### Test for commit validation cost:
  
  Since we hypothesize that commit validation could take slightly longer, but we spend less time opening transactions, there is likely a point where larger commits can slow down commit validation which can slow down throughput overall. In this test, we write larger and larger batches of straight inserts, which still must be serialized and validated against each other to look for conflicts. (Note: without conflicts these validations can run and finish, without needing to depend on each others' validation results. However - testing with a commit that forces commit waiting doesn't show any different results).
  
  ```
  PureInsert COMPLETE — all batch sizes, 5 runs, 300k ops
  
     Batch  Thr   opt mean      ±sd   CV%   mst mean      ±sd   CV%       Δ%        p
    ==========================================================================================
         1    1        332        9  2.6%        339        2  0.6%    -2.1%   0.072
         1    4      1,198       48  4.0%      1,017      122 12.0%   +17.8%   0.002   **
         1    8      2,207      110  5.0%      1,822      230 12.6%   +21.1%   <0.001  ***
  
       100    1      9,342      399  4.3%      9,878      588  6.0%    -5.4%   0.091
       100    4     31,241    1,185  3.8%     31,931      566  1.8%    -2.2%   0.240
       100    8     47,889    6,881 14.4%     53,577    2,334  4.4%   -10.6%   0.080
  
      1000    1     10,851       63  0.6%     11,141      102  0.9%    -2.6%   <0.001  ***
      1000    4     37,543      516  1.4%     39,668      557  1.4%    -5.4%   <0.001  ***
      1000    8     57,808    5,816 10.1%     64,092    1,434  2.2%    -9.8%   0.019    *
  
      2000    1     13,450      177  1.3%     13,542       28  0.2%    -0.7%   0.253
      2000    4     45,045    1,581  3.5%     43,756      445  1.0%    +2.9%   0.079
      2000    8     75,975      934  1.2%     76,528    1,932  2.5%    -0.7%   0.564
  
      4000    1     13,563      118  0.9%     13,564       55  0.4%    -0.0%   0.978
      4000    4     37,011    1,111  3.0%     37,269      960  2.6%    -0.7%   0.694
      4000    8     65,881    4,023  6.1%     60,821    9,051 14.9%    +8.3%   0.253
  ```
  
  We see that for tiny commits (size 1), we see significant gains, but at around 1000 we see some reduction in throughput. After that, we go back to being within the noise.
  
  
  
- **Bump TypeQL to receive unicode unescaping**
  Escaped unicode characters can be used in TypeQL string literals. These must be of the form `\uXXXX` (exactly 4 hex digits), or `\u{XX...X}` (1 to 6 hex digits).
  
  
  

## Bugs Fixed
- **Error on assigning to existing variable in a reduce stage**
  Error on reduce assigning to existing variable. Source errors when building functions in a Fetch clause are now propagated.
  
  

## Code Refactors
- **Variable binding mode changes**
  Variable binding modes fully replace the use of scopes to determine variable binding & visibility.
  
  
- **Raise gRPC message size limits to 1 GB**
  
  The default tonic limit of 4 MB is too restrictive for database workloads. Query responses, fetch documents with many attributes, and export batches can legitimately exceed 4 MB. This caused client-side decode failures with: "decoded message length too large: found N bytes, the limit is: 4194304 bytes"
  
  Set both encoding and decoding limits to 1 GB on the TypeDbServer service, matching the approach taken by other database systems eg. CockroachDB.
  
  

## Other Improvements
- **Bump typeql for parser performance improvements**
  Bump typeql for parser performance improvements
  
  
- **Non recursive executable stages**
  
  Refactor to allow long query pipelines of several hundred or thousand clauses to succeed without stack overflows. 
  
  The common case here is users writing or generating a long sequence of `insert` clauses.
  
  Fixes https://github.com/typedb/typedb/issues/7719
  
  ### Caveats
  
  Using really long query pipelines causes really big slowdowns in compilation:
  
  For 2000 chained inserts:
    - translate: 1.5s
    - annotate: 3.1s
    - compile: 4.2s
    - extract_pipeline_structure: 0.4s
    - build_write_pipeline: 0.005s — negligible
  
    All three main phases show O(n^2) growth - likely because each insert stage's output row accumulates all variables from prior stages, and every stage needs to process this growing row mapping.
  
  Note: now at least, this is a slowdown not a crash. Follow up work should introduce a pipeline cap, for instance at say 1000 inserts and produce a useful error.
  
  
- **Fail point instrumentation tests**
  
  We add a suite of crash recovery tests where we ensure that TypeDB server can start back up after it crashes during a disk operation such as making a checkpoint.
  
  
- **Fetch TypeDB console using native_artifact_files module extension**
  Fetches the TypeDB console artifact using native_artifacts_files, close to how we used to with bazel 6.
  
    
