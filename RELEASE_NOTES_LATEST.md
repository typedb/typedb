**Download from TypeDB Package Repository:**

[Distributions for 3.8.1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.8.1)

**Pull the Docker image:**

```docker pull typedb/typedb:3.8.1```



## Bugs Fixed
- **Fix lowering so equality checks against variables can be inlined to iterators**
  Fixes a bug which prevented statements of the form `$a isa attribute-type == $value;` from efficiently retrieving attribute instances. It worked fine when a literal was used as value.
  
  
- **Fix output variables in analyzed structure**
  We were using `named_referenced_variables` of the last stage to determine the output variables of a pipeline. 
  We replace this with a method based on `variable_binding_modes` or the retained variables for select & reduce stages.
  
  


## Other Improvements
- **Optimize isolation checks - 5-20% multithreaded insert performance**
  
  We update one loop in the isolation manager on the hot path, which improves throughput under heavily contended write workloads with many threads by 5-20%, and is in the noise at other thread counts. 
  
  Benchmarked using an embedded TypeDB benchmark to exclude network and driver noise.
  
  
  ```
    PureInsert batch=1000                                       
  
  Query >>
  
  insert $p isa person, has name "person_{N}", has age {random};        
                                                                                                                                                                 
    ┌─────────┬──────────────┬────────────────┬────────────────────┐   
    │ Threads │ master       │ adaptive       │ adaptive vs master │                                
    ├─────────┼──────────────┼────────────────┼────────────────────┤                        
    │       1 │       11,739 │         11,520 │              -1.9% │                                                                                                                                                                                   
    ├─────────┼──────────────┼────────────────┼────────────────────┤
    │       2 │       21,880 │         22,160 │              +1.3% │
    ├─────────┼──────────────┼────────────────┼────────────────────┤
    │       4 │       41,510 │         39,779 │              -4.2% │
    ├─────────┼──────────────┼────────────────┼────────────────────┤
    │       8 │       67,245 │         64,519 │              -4.1% │
    ├─────────┼──────────────┼────────────────┼────────────────────┤
    │      16 │       60,863 │         65,620 │              +7.8% │
    ├─────────┼──────────────┼────────────────┼────────────────────┤
    │      32 │       57,827 │         72,347 │             +25.1% │
    └─────────┴──────────────┴────────────────┴────────────────────┘
  ```
  
  
- **Use Box in InitialIterator to shrink pipeline stage iterator sizes**
  Shrinks the size of InitialIterator by using a Box around its inner iterator. This reduces the size of the pipeline stage iterator enums, leading to smaller stack frames during execution and supporting longer pipelines.
  
  
