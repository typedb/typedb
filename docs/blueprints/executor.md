# Query qxecutor

> ***Morsel-driven multi-core***. Modern hardware will often have tens of cores and a single heavy analytical query should benefit from near-linear scaling on these. This means that graph query languages should parallelize well and the state-of-the art here is flexible morsel-driven scheduling where a fixed number of threads pinned to the cores steal morsels of work (typically 10-100K data items) from a queue, and exploit the scheduling flexibility provided by shared data structures (e.g., hash tables), for good load balancing.
>
> (_Efficient Property Graph Queries in an analytical RDBMS_, 2023)


***Must reads***

* For parallelism: _"Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age"_ (2014)
* For partitioning: _"The State of the Art in Distributed Query Processing"_ (2000)

***On this page***

## Compute graph


## Recursion and termination


## Batch-driven parallelism

_Note_: Terminology-wise "batch" = "morsel".

