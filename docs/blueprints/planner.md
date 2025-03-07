# Planner concepts

> ***State-of-the-art query optimization***. To get efficient query plans, one needs **dynamic-programming-based** query optimization, informed by **good statistics**: typically a combination of table samples (that allow to detect correlated predicates within a table) and hyperloglogs on most data to estimate distinct counts
> 
> (_Efficient Property Graph Queries in an analytical RDBMS_, 2023)

Must-reads:
* [1] _"Adaptive Optimization of Very Large Join Queries"_ (2018), Sec. 4 in particular
* [2] _"Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"_ (2000)
* [3] _"A Survey on Advancing the DBMS Query Optimizer: Cardinality Estimation, Cost Model, and Plan Enumeration"_ (2021)

***One this page***

1. Overview
2. Preprocessing + Data structures
3. Algorithm
4. Partitioned Data
5. Parallelism

## Overview

### Notation

* **production rule**: `($x, $y) -> $z`

  > A **pipeline** (incl. function), **pattern**, or **constraint** produces `$z` from `($x, $y)`
* **limit rule**: `$x:($y, $z) = n`

  > A **planner graph** restricts outputs of `$x` to n for each `($y, $z)`
* **sort rule**:  `$x|($y, $z)`

  > A **(partial) plan** produces `$x` sorted for each `($y, $z)`

### Problem 1: efficient bushi-ness

***Problem***: Our current planner produces linear, non-bushy plans... (side note: IKKBZ would do a better job at that already).

***Solution***: Use dynamic programming (DP), but work in three regimes for efficiency:

* **small**: for < 150k subgraph (up to ~16 fully connected or ~128 linear constraints in graph)
  * run _Vanilla DP_
* **medium**: <= 128 vars
  * run _IKKBZ_ for linear (non-bushy query) plan
  * then run _Vanilla DP_ on segments of the linear plan to produce a bushy plan
* **large**: > 128 vars
  * run _iterative DP_:
    * _greedy_ selection of 128 vars
    * then run _medium_ algorithm
    * repeat

_Remark_: first mover advantage? (:

### Problem 2: Joining on unsorted intermediate results

***Problem***: The planner cannot join on unsorted results.

***Solution***: Allow the planner to plan to join two subplans by performing:
* nested-loop joins (loop over tuples in one plan, look up results in the other)
* sort-merge joins (merge sorted results from both plans, potentially looping over tuples which the sorting is grouped by)
* hash joins (loop over tuples in one plan, perform hash probes of tuples in the other)
* a combination thereof...

### Problem 3: Planning across stages and (recursive) functions

***Problem***: The planner cannot share information across stage and function boundaries.

***Solution***: Inline stages and functions, when possible:

1. ***Inlineable***: inlineable functions are dealt with as part of the same query plan.
    * any pipeline/function comprising (any number of) `match`, `select`, `limit`, `offset`, `distinct`, non-aggregate `return`, is inlineable.
2. ***Not inlineable***: Non-inlineable pipelines/functions get their own plan. Non-inlineable pipelines/functions are those containing
    * **termination-required** read stages `reduce`, `sort`, aggregate `return`. Need their own plan as they need to run to termination.
    * **state-sensitive** write stages: `insert`, `delete`, `update`, `put`. Mixing these into the plan could be _possible_, but seems difficult because writes could affect ongoing reads (transaction-level version control? :P)
3. ***Recursive*** functions. Recursion requires tabling which requires a well-defined plan for the function, so inlining becomes questionable except for shallow recursions.

In the inlineable case for pipelines and functions, distinguish:
1. Non-recursive case:
    * **inline full pattern**, technicalities of constraint construction are discussed below
    * _Note_: we need to **avoid name clashes** for de-selected variables
2. Recursive case:
    * ?? For depth-restricted recursions, consider feasibility of **inlining**
    * Even without inlining, **partial application** of recursive functions is straight-forward, so all possible production rules should be given to the planner. Example:
      ```
      with fun path($x: vertex, $y: vertex) -> bool: 
        match { edge($x, $y); } or { edge($x, $z); path($z, $y); }  # sugared
        return check;                                               # sugared
      match
        $x isa vertex, has id "1";
        path($x, $y);
      ```
      here, the planner should see the production rule `$x -> $y` for the call to `path($x, $y)`, and should generate a plan _with that production rule for the function itself_
    * cost estimation for recursive functions should be **sample based**
    * **maximal recursion depth** would be a useful thing to have to notify the user of potential cycles

_Note_. Inlining automatically solved the issue of "partial applications". Unfortunately, in the recursive case, this does not apply, so a separate partial application mechanism needs to be built.

### Problem 4: planning across disjunctions

***Problem***: Plans may vary greatly between disjunction, especially when these are chained (i.e. for `{A} or {B}; {C} or {D};` the `A;C` plan may be completely different from the `B;D` plan.)

***Solution***: Adaptively plan per branch in DNF, but share work in the planner.

Per-branch planning can be inefficient; therefore, use adaptive planning as follows.

* **medium DNF**: <= 32 branches (~5 binary, ~3 ternary `or`s) branches in DNF:
  * plan each branch separately, but let planner share the same DP table to avoid redoing work for the same subpatterns.
  * from the final planning **tree plans per branch**, produce a **DAG** plan, by identifying shared subtrees.
* **large DNF**: > 32 branches:
  * **freeze** some _homogeneous_ disjunctions, i.e. do not expand them in DNF.
    * A disjunction is **homogeneous** if all branches share at least one common production rule up to removing the locally scoped variables (indeed, this production rule can then be used for the entire disjunction).
     
    _Note_. A _"locally scoped variable in a disjunction"_ means _"the variable appears exclusively in branches of that disjunction"_
  * (branch size may still be > 32, but that's life)
  * then run small DNF with "frozen disjunctions" edges, see graph construction below

## Preprocessing

### The "Planner hyper-graph"

* Vertices == **Variables**: are "typed vertices" of the graph, categorize by:
  * **selected**, variables that are outputted by query
  * **unselected**, variables that are not outputted (automatically includes "internal" variables)
* Hyperedges == **Constraints**: the "typed edges" of the graph, _see types detailed below_
* **Production rules** per constraint: a set of rule `$a -> ($b, $c)` of which variables in the constraint can be produced from which other variables (empty set means all vars in constraint required)
* **Limit rules**: rules `$a:($b,$c)=size` restricting the size of outputs for variable tuples relative to other variable tuple. Limit rules arise from
  * internal vars in `not` subqueries
  * `return` in inlined functions
  * `select`, `limit`, `distinct`
* **Offset rules**: rules `$a:($b,$c)=offset` restricting the size of outputs for variable tuples relative to other variable tuple.

### Constructing constraints

* **Statement constraints**, are "unary" edges of the graph
  * Hyperedge relating its variables
  * May both produce and require its variable
  * Examples:
    * `$x == 10` (prod rules: `() -> $x; $x -> ();`)
    * `$x isa person` (prod rules: `() -> $x; $x -> ();`)
    * `$x * $x + 1 == 28` (prod rules: empty)
    * `$x == $y` (prod rules: `$x -> $y; $y -> $x; ($x, $y) -> ();``)
    * `$x * $x == $y` (prod rules `$x -> $y; ($x, $y) -> ();`)
    * `$x has name $y` (prod rules `$x -> $y; $y -> $x; () -> ($x, $y); ($x, $y) -> ();`)
    * `$x links $y` (prod rules `$x -> $y; $y -> $x; () -> ($x, $y); ($x, $y) -> ();`)
* **Inlined function call**:
  * Add all contained constraints in the function body individually
  * Account for other stages (`limit`, `select`) by appropriate limit/offset rules
  * Examples:
    * `let $x in f($y)` where `f($y): match P($y, $z, $x2); limit 5; return $x2;`
      * add _all_ constraints in `P` recursively, identifying `$x2` and `$x`
      * add limit rules: `$x:$y = 5`, `$z:($y,$x) = 1`
    * `let $x in f($y)` where `f($y): match P($u, $y, $z); select $u; match Q($u, $x2); return $x2;`
      * add _all_ constraints in `P` **and** `Q` recursively, identifying `$x2` and `$x`
      * add limit rules: `$u:($y,$x) = 1`, `$z:($y,$u) = 1`
    * `let $x = f($y)` where `f($y): match P($y, $z, $x2); return first $x2;`
      * add _all_ constraints in `P` recursively, identifying `$x2` and `$x`
      * add limit rules: `$x:$y = 1`, `$z:($y,$x) = 1`
* **Recursive function call**:
  * Add single hyperedge relating all input and output variables of the function call
  * Annotate constraint with all possible production rules (based on body of function)
  * Example: 
    * Consider recursive function
      ```
      fun path($x : vertex, $y : vertex) -> bool:
      match { edge($x, $y); } or { edge($x, $z); path($z, $y); };
      return check;
      ``` 
      * production rules: `$x, $y -> ()`, `$x -> $y`, `$y -> $x`, `() -> $x, $y`
      * In query `$x isa vertex, has ID 1; true == path($x, $y);` the planner may pick production rule `$x -> $y`, since `$y` isn't otherwise produced.
  * Remark: **costing recursive functions** is difficult, but important for the choice of production rule.
    * sample based approach: run a small number of (time-limited) sample calls to estimate cost
* **Non-inlined function call**, 
  * hyperedge relating function _input_ + _output_ vars. 
  * prod rules: `input -> output`
  * Examples:
    * `let $x in f($y, $z)` (prod rules: `($y, $z) -> $x`) 
* **Nested negation constraint**
  * hyperedge relates _output_ and _internal_ variables (see [IO categorization](read.md)) 
  * prod rules: `output -> internal` (note: if a not succeeds, internal variables will be empty)
  * limit rule: `internal:output = 1`
  * Example:
    * `not { <Inst $y>, <Check $z> }` (prod rules: `$y -> $z`)
* **Frozen disjunction constraints**
  * Add a single constraint relating all its variables
  * Production rules are the intersection set of production rule per branch disregarding "locally-scoped" variables (i.e. those occuring _only_ in one or more branches of that disjunction, but not elsewhere in the ambient pattern)
  * **Costing** (for a chosen production rule) requires planning branches individually and summing their cost

### Constructing rules

* `select` determines when variables are marked as selected/unselected (used by the planner to compute costs by reducing intermediate sizes)
* function assignments affect limit rules ... (used by the planner to compute costs by reducing intermediate sizes)
* `limit` affects limit rules ... (used by the planner to compute costs by reducing intermediate sizes) 
* `distinct` doesn't affect anything ...
* `offset` affects offset rules ... (used by the planner to compute costs by reducing intermediate sizes)

## Algorithm

### (Partial) Plan data

#### Single-branch plan

* **rooted tree of constraints**
  * leafs are data retrievals
  * non-branching nodes are operations that operate on data in place
    * e.g.: predicate checks
  * branching nodes are operations than combine constraints (potentially on **tuples** of vars)
    * e.g.: joins (as listed above)
* **sort-by data** `$x|($y,$z)` says "the plan produces `$x` in sorted order for each tuple `($y, $z)`". (This data is used for decisions on merge-sort)

#### Multi-branch plan

* **DAG** version of the above

### Vanilla DP

As usual. But a few notes:
1. In the tree representation of the plan, **later nodes may _interact_ with earlier nodes**
2. For WCOJ merge-sort need to consider **join on more than one variable** at a time
3. **Costs do not (always) compose** uniformly as before (they do for IKKBZ as usual), because of limit and offset rules

_Example_:
Triangle join `R($x,$y); S($y,$z); T($z; $x)`.

* Planner **Step 1**: Cost singleton constraint sets
  * say, plan `P0`, access `{R}` with `$x|$y` (`$x` sorted for each `$y`)
  * say, plan `P1`, access `{R}` with `$y|$x` (`$y` sorted for each `$x`)
* Planner **Step 2**: Cost binary constraint combinations, 
  * say, plan `P2`, combine `{R}` and `{S}` by **nested-loop join** of `{S}` on `($x,$y)` in `P1`, which means `$z|$y`, `$y|$x` (and so `$z|($y,$x)`) is sorted
  * say, plan `P3`, combine `{R}` and `{S}` by **sort-merge join** on `$y` in `P0`, which means `$x|$y` and `$z|$y` is sorted
* Planner **Step 3**: Cost ternary constraint combinations
  * say, plan `P4`, combine `{R,S}` via `P1` and `{T}` by **sort-merge join** on `$z` looped over `($x,$y)` in `{R,S}`.
    * (could also have variant `P4'` which sort merges `{T}` on `$z` for each `$x` and loops over `$x`)
  * say, plan `P5`, combine `{R,S}` via `P2` and `{T}` by **hash join** on tuples `($x, $z)` in `{R,S}`.

In the resulting tree for plan `P4`
```
   / \
  T  / \ 
    S   R
```
the operation of merging `{T}` into `{R,S}` **does not loop over** `$z`, so `$z`'s production can be **deferred until the actual merge** in the upper node... in this sense, the upper node "interacts" with the lower on.

In contrast, in `P5` we do loop over `($x, $z)` so these do need to be produced.

### IKKBZ

A very cool algorithm adapted from [operations research](https://web.archive.org/web/20190725183103/https://www.cockroachlabs.com/blog/join-ordering-ii-the-ikkbz-algorithm/).

## Partitioned data

The plans in DP can include information about **sites** where the data resides, see [2].

## Parallelism

The completed plan can be executed using **morsel-driven** parallelism, see [executor spec](executor.md).