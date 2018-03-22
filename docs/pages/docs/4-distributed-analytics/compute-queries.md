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
compute [goal] [subgraph]; (using [strategy] where [modifiers];)
```

* `goal` can be any of the available [statistics](#available-statistics-methods) 
or [graph](#available-graph-queries) queries.
* `subgraph` is a comma separated list of types to be visited by the Pregel algorithm.

Additionally, for graph queries

* `strategy` is actual algorithm used
* `modifiers` are different depending on the specific query and algorithm.

The specific compute queries fall into two main categories and more information is given in the sections below.
The simplest query `count` can be executed using the following:

```graql-skip-test
compute count;
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

## Available Statistics Methods

The following methods are available to perform simple statistics compuations, and we aim to add to these as demand dictates. Please get
in touch on our [discussion](https://discuss.grakn.ai/) page to request any features that are of particular interest
to you. A summary of the statistics algorithms is given in the table below.

| Algorithm | Description                                   |
| ----------- | --------------------------------------------- |
| [`count`](#count)     | Count the number of instances.                        |
| [`max`](#maximum)    | Compute the maximum value of an attribute. |
| [`min`](#minimum)    | Compute the minimum value of an attribute. |
| [`mean`](#mean)    | Compute the mean value of an attribute.                           |
| [`median`](#mean)    | Compute the median value of an attribute.                           |
| [`std`](#standard-deviation)    | Compute the standard deviation of an attribute. |
| [`sum`](#sum)    | Compute the sum of an attribute. |

For further information see the individual sections below.

### Count

The default behaviour of count is to return a single value that gives the number of instances present in the graph. It
is possible to also count subsets of the instances in the graph using the [subgraph](#subgraph) syntax, as described above.

```graql-skip-test
compute count in person;
```

### Mean

Computes the mean value of a given attribute. This algorithm requires the [subgraph](#subgraph) syntax to be used.
For example,

```graql-skip-test
compute mean of age in person;
```

would compute the mean value of `age` across all instances of the type `person`.
It is also possible to provide a set of attributes.

```graql-skip-test
compute mean of attribute-a, attribute-b in person;
```

which would compute the mean of the union of the instances of the two attributes, 
given the two attribute types have the same data type.

### Median

Computes the median value of a given attribute, similar to [mean](#mean).

```graql-skip-test
compute median of age in person;
```

would compute the median of the value persisted in instances of the attribute `age`.

### Minimum

Computes the minimum value of a given attribute, similar to [mean](#mean).

```graql-skip-test
compute min of age in person;
```

### Maximum

Computes the maximum value of a given attribute, similar to [mean](#mean).

```graql-skip-test
compute max of age in person;
```

### Standard Deviation

Computes the standard deviation of a given attribute, similar to [mean](#mean).


```graql-skip-test
compute std of age in person;
```

### Sum

Computes the sum of a given attribute, similar to [mean](#mean).

```graql-skip-test
compute sum of age in person;
```

{% include warning.html content="When an instance has two attributes of the same type attached, or two attributes specified as arguments to the algorithm, statistics will include this by assuming there were two instances each with a single attribute." %}

## When to Use `aggregate` and When to Use `compute`

[Aggregate queries](../querying-data/aggregate-queries) are computationally light and run single-threaded on a single machine, but are more flexible than the equivalent compute queries described above.

For example, you can use an aggregate query to filter results by attribute. The following  aggregate query, allows you to match the number of people of a particular name:

```graql-skip-test
match $x has name 'Bob'; aggregate count;
```

Compute queries are computationally intensive and run in parallel on a cluster (so are good for big data).

```graql-skip-test
compute count of person;
```

Compute queries can be used to calculate the number of people in the graph very fast, but you can't filter the results to determine the number of people with a certain name.

## Available Graph Queries

The following algorithms all compute values based on the structure of the graph.
A summary of the graph algorithms is given in the table below.

| Algorithm | Description                                   |
| ----------- | --------------------------------------------- |
| [`cluster`](./compute-cluster)     | Find the clusters of instances.                        |
| [`centrality`](./compute-centrality)    | Compute the centrality of each instance in the graph. |
| [`path`](./compute-shortest-path)    | Find the shortest path(s) between two instances.                           |

<!--
For further information see the individual sections below.

### Cluster

### Degrees

### Path
-->
