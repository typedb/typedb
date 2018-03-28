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

Graql analytics can perform the necessary computations through the `compute` query.  The available algorithms
are:

*  `count`
*  `max`
*  `mean`
*  `median`
*  `min`
*  `std (standard deviation)`
*  `sum`

For example, the following query executes a distributed computation to determine the mean age of all of the people in the knowledge graph.

```graql
compute mean of age in person;
```

The [Compute Queries](./compute-queries) documentation covers the usage of `compute` in more detail.
