---
title: Compute Queries
keywords: graql, compute, match
tags: [graql]
summary: "Graql Compute Queries"
sidebar: documentation_sidebar
permalink: /docs/distributed-analytics/compute-queries
folder: docs
---

A compute query executes a [Pregel algorithm](https://www.quora.com/What-are-the-main-concepts-behind-Googles-Pregel)
to determine information about the knowledge graph in parallel.
Called within the Graql shell or dashboard, the general syntax is:

```graql-skip-test
compute goal [in subgraph], (using [strategy] where [modifiers];)
```

* `goal` can be any of the available [statistics](#available-statistics-methods)

or [graph queries](#available-graph-queries).
* `subgraph` (optional) is a comma separated list of types to be visited by the Pregel algorithm.

Additionally, for graph queries

* `strategy` (optional) is actual algorithm used
* `modifiers` (optional) are different depending on the specific query and algorithm.

The simplest query `count` can be executed using the following:

```graql-skip-test
compute count;
```
The following query compute the clusters, in the subgraph containing only person and marriage,
using connected component, and return the members of each cluster.

```graql-test-ignore
compute cluster in [person, marriage], using connected-component, where members=true;
```

## Subgraph

The subgraph syntax is provided to control the types that a chosen algorithm operates upon.
By default, the compute methods include instances of every type in the calculation.
Using the `in` keyword followed by a comma separated list of types will restrict the calculations to instances of those types only.

For example,

```graql-skip-test
compute count in person;
```

will return just the number of instances of the concept type person.
Subgraphs can be applied to all compute queries and therefore are different to `strategy` and `modifiers`.

The specific compute queries fall into two main categories and more information is given in the sections below.

## Available Statistics Methods

The following methods are available to perform simple statistics computations.
A summary of the statistics algorithms is given in the table below.

| Algorithm | Description                                   |
| ----------- | --------------------------------------------- |
|`count`    | Count the number of instances.                        |
| `max`     | Compute the maximum value of an attribute. |
| `min`     | Compute the minimum value of an attribute. |
| `mean`    | Compute the mean value of an attribute.                           |
| `median`  | Compute the median value of an attribute.                           |
| `std`     | Compute the standard deviation of an attribute. |
| `sum`     | Compute the sum of an attribute. |

For further information see the [`statistics queries`](./compute-statistics).

## Available Graph Queries

The following algorithms all compute values based on the structure of the graph.
A summary of the graph algorithms is given in the table below.

| Algorithm | Description                                   |
| ----------- | --------------------------------------------- |
| [`cluster`](./compute-cluster)     | Find the clusters of instances.                        |
| [`centrality`](./compute-centrality)    | Compute the centrality of each instance in the graph. |
| [`path`](./compute-shortest-path)    | Find the shortest path(s) between two instances.                           |


For further information see the individual sections.
