# Ideas

## Calling functions directly from driver

Allow calling functions directly from driver with supplied input arguments.

## Templated write pipelines

Allow schema-defined function to contain write stages. Allow calling this functions directly from driver/console, but not from within patterns.

## ORM for structs

Language-native struct codegen for TypeDB-native structs, and easy mapping in driver between the two. (In particular as function inputs)

## Fetch for different types

Allow fetch to return different types, including native structs.

## Partitioning

* **Type-driven partitioning**: each server holds full records of certain types
* If types need to be partitioned, split them: i.e. partition `child sub person` into `child_s1 sub person` and `child_s2 sub person`

## Modern architecture

**General design ideas (by P. Boncz)**

* Consider hybrid columnar storage + lightweight compression
  * Compact storage, Fast (SIMD-friendly) scans
* Fast Query Executor
  * JIT (Umbra) or vectorized execution (DuckDB)
* Buffer Manager
  * data >> RAM (e.g. LeanStore = execute directly on SSD)
* Control over memory
  * C++, C or Rust
* Bottom-up Dynamic Programming Query Optimizer
  * Samples and hyperloglog as statistics
* Morsel-driven Parallellism
  * Atomics in shared hash tables, low-overhead queues

**Reference**:
> DuckPGQ: Efficient Property Graph Queries in an analytical RDBMS
