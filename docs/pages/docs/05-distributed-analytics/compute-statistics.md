---
title: Statistics
tags: [analytics]
summary: "This page introduces the statistics functionality of analytics."
sidebar: documentation_sidebar
permalink: /docs/distributed-analytics/compute-statistics
folder: docs
KB: genealogy-plus
---

Computing simple statistics, such as the mean and standard deviations of datasets, is an easy task when considering a few
isolated instances. What happens when your knowledge graph becomes so large that it is distributed across many machines? What
if the values you are calculating correspond to many different types of things?

Graql analytics can perform the necessary statistics computations.
For example, the following query executes a distributed computation to determine the mean age of all of the people in the knowledge graph.

```graql
compute mean of age, in person;
```

The [Statistics Queries](./compute-statistics) documentation covers the usage of statistics in more detail.

## Available Statistics Methods

The following methods are available to perform simple statistics computations,
and we aim to add to these as demand dictates. Please get
in touch on our [discussion](https://discuss.grakn.ai/) page to request any features that are of particular interest
to you. A summary of the statistics algorithms is given in the table below.

| Algorithm                    | Description                                     |
| ---------------------------- | ----------------------------------------------- |
| [`count`](#count)            | Count the number of instances.                  |
| [`max`](#maximum)            | Compute the maximum value of an attribute.      |
| [`min`](#minimum)            | Compute the minimum value of an attribute.      |
| [`mean`](#mean)              | Compute the mean value of an attribute.         |
| [`median`](#median)          | Compute the median value of an attribute.       |
| [`std`](#standard-deviation) | Compute the standard deviation of an attribute. |
| [`sum`](#sum)                | Compute the sum of an attribute.                |

For further information see the individual sections below.

### Count

The default behaviour of count is to return a single value that gives the number of instances present in the graph. It
is possible to also count subsets of the instances in the graph using the [subgraph](#subgraph) syntax, as described below.

```graql-skip-test
compute count in person;
```

### Mean

Computes the mean value of a given attribute. This algorithm requires the [subgraph](#subgraph) syntax to be used.
For example,

```graql-skip-test
compute mean of age, in person;
```

would compute the mean value of `age` across all instances of the type `person`.
It is also possible to provide a set of attributes.

```graql-skip-test
compute mean of [attribute-a, attribute-b], in person;
```

which would compute the mean of the union of the instances of the two attributes,
given the two attribute types have the same data type.

### Median

Computes the median value of a given attribute, similar to [mean](#mean).

```graql-skip-test
compute median of age, in person;
```

would compute the median of the value persisted in instances of the attribute `age`.

### Minimum

Computes the minimum value of a given attribute, similar to [mean](#mean).

```graql-skip-test
compute min of age, in person;
```

### Maximum

Computes the maximum value of a given attribute, similar to [mean](#mean).

```graql-skip-test
compute max of age, in person;
```

### Standard Deviation

Computes the standard deviation of a given attribute, similar to [mean](#mean).

```graql-skip-test
compute std of age, in person;
```

### Sum

Computes the sum of a given attribute, similar to [mean](#mean).

```graql-skip-test
compute sum of age, in person;
```

{% include warning.html content="When an instance has two attributes of the same type attached, or two attributes specified as arguments to the algorithm, statistics will include this by assuming there were two instances each with a single attribute." %}

## When to Use `aggregate` and When to Use `compute`

[Aggregate queries](../querying-data/aggregate-queries) are computationally light and run single-threaded on a single machine, but are more flexible than the equivalent compute queries described above.

For example, you can use an aggregate query to filter results by attribute. The following aggregate query, allows you to match the number of people of a particular name:

```graql-skip-test
match $x has name 'Bob'; aggregate count;
```

Compute queries are computationally intensive and run in parallel on a cluster (so are good for big data).

```graql-skip-test
compute count of person;
```

Compute queries can be used to calculate the number of people in the graph very fast, but you can't filter the results to determine the number of people with a certain name.
