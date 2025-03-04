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

***Problem***: Our current planner produces linear, non-bushy plans... (side note: IKKBZ would do a better job at that already). We need 

***Solution***: Use DP, but work in three regimes for efficiency:

* **small**: for < 100k subgraph (up to ~14 fully connected or ~100 linearly vars)
  * run _Vanilla DP_
* **medium**: <= 100 vars
  * run _IKKBZ_ for linear (non-bushy query) plan
  * then run _Vanilla DP_ on segments of the linear plan to produce a bushy plan
* **large**: > 100 vars
  * run _iterative DP_:
    * _greedy_ selection of 100 vars
    * then run _medium_ algorithm
    * repeat

`!! we need the capability to plan big queries for inlining functions !!`

## Preprocessing

### Expanding to disjunctive normal form (DNF)

**Problem**. Plans may vary greatly between disjunction, especially when these are chained (i.e. for `{A} or {B}; {C} or {D};` the `A;C` plan may be completely different from the `B;D` plan.)
**Solution**. Plan per branch in DNF

* **small DNF**: <= 64 branches in DNF
  * plan each branch separately, but let planner share the same DP table to avoid redoing work
  * unify subplans across branches
* **large DNF**: > 64 branches in DNF:
  * "freeze" the most homogenous `or` clauses by some measure of homogeneity, i.e. do not expand them in DNF
  * then run small DNF with "frozen disjunction" edges (see below)

### Inlining functions

### Planner "graph" construction

* **Variables**, both are "typed vertices" of the graph (types as categorized in [read spec](read.md))
* **Constraints** are the "typed edges" of the graph, _see types detailed below_
* **Validity dependencies**, mapping constraints to their "required variables"

#### Constraint construction

* **Access constraints**, are "unary" edges of the graph
  * Example:
    * `$x == 10`
    * `$x isa person`
* **Relation constraints**:
  *  
  * Example:
    * `$x has $T $y`
    * `$x links $y`;
* **Non-inlined function constraints**, 
  * Example:
    * `let $x = f($y) // match $z ... return $x;` where `f` is _not_ inlineable
* **Nested negation constraint**
  * Example:
    * `not { <Inst $y>, <Check $z> }` (see [varcategories](read.md))
* **Frozen disjunction constraints**, the n-ary edges of the graph, n > 1
 

### Validity dependency

### Size dependency

### "Currying"

***Planning boundaries***

Planner should work across `match`, `select`, `limit`, `offset`,

This means functions using only these stages are **inlineable**

"Planner boundaries": `not` and `reduce` and `sort`

## Algorithm

### Input

*

### DP Subplans

* A subplan is
  * a set of constraint of "sort-produced vars" (sorted = true, false)
  * (a site location, see "Partitioned data" below)

### Vanilla DP


### IKKBZ 





## Partitioned data

The plans in DP can include information about **sites** where the data resides, see [2].

## Parallelism

The completed plan can be executed using **morsel-driven** parallelism, see [executor spec](executor.md).