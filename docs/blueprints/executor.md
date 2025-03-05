# Query executor

> ***Morsel-driven multi-core***. Modern hardware will often have tens of cores and a single heavy analytical query should benefit from near-linear scaling on these. This means that graph query languages should parallelize well and the state-of-the art here is flexible morsel-driven scheduling where a fixed number of threads pinned to the cores steal morsels of work (typically 10-100K data items) from a queue, and exploit the scheduling flexibility provided by shared data structures (e.g., hash tables), for good load balancing.
>
> (_Efficient Property Graph Queries in an analytical RDBMS_, 2023)


***Must reads***

* [1] For parallelism: _"Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age"_ (2014)
* [2] For partitioning: _"The State of the Art in Distributed Query Processing"_ (2000)

***On this page***

* **Compute graph**. Push based representation of computation

## Compute graph

* **Directed acyclic graph** (DAG), with data:
  * **Roots** represent data retrieval from disk
  * Graph has a single **terminal node** (represent return of query answer)
  * Internal nodes represent operations:
    * **joins** combine data from difference sources
    * **transformations** represent operations on data in place (could include, e.g., hash table construction etc.)
    * a node with multiple outputs "sends its data to multiple places"
  * Edges may be annotated with "**production information**"; i.e. the data variables that need to be fully produced at that point (important e.g. for "deferred" joins, see [planner spec](planner.md))
* A **subquery pipeline call / function call** pushes data in the compute graph of a function. 
  * Inlineable case:
    * For **inlined** functions, _no thought needs to be given to this_
    * For **inlineable recursive** functions, model a function `f` recursively calling itself (`f`) by letting `f_n` call `f_{n-1}` instead (the first "outermost" call is `f_m`, for some integer max depth `m`, and any recursively calls in `f_0` return no more results): then expand the compute graph accordingly, see example below.
  * Non-inlineable case:
    * Record as fully **separate computation graph**
    * We still "push" data from parent query to subquery, but need to run computation to termination
    * Affects: non-inlineable functions (due to aggregates, sorts) + negations

_Example_ An approximation of the expansion

![](images/inlined_recursive_graph.svg)

## Recursion and termination

* **Cycle breaking**. When `f_n` pushes a data row `r` to `f_k`, `k < n`, and `r` was also pushed to `f_n`, then we do not evaluate `f_k` on `r` but instead set a "**continuation point**": any new data that is push through `f_n(r)` will be also push through `f_k(r)`
* **Termination**. When no more data flows through the graph the computation is terminated.

## Batch-driven parallelism

_Note_: Terminology-wise "batch" = "morsel".

* *Basic idea*: **elastically** partition dataflow per edge across worker pool, by letting workers "steal work" when they can
* *Challenge*: if we do not fully complete an edge, we need a scheduler to prioritize work across different edges
* *Noteworthy*: any operations that requires **sortedness of `$x` per each `$y`** cannot easily be paralellized on `$x`, but can be parallelized on `$y`.


