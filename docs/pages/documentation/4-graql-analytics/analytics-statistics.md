---
title: Statistics
last_updated: February 2017
tags: [analytics]
summary: "This page introduces the statistics functionality of analytics."
sidebar: documentation_sidebar
permalink: /documentation/graql-analytics/analytics-statistics.html
folder: documentation
KB: genealogy-plus
---

Computing simple statistics, such as the mean and standard deviations of datasets, is an easy task when considering a few
isolated instances. What happens when your knowledge base becomes so large that it is distributed across many machines? What
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

For example, the following query executes a distributed computation to determine the mean age of all of the people in the knowledge base.

```graql
compute mean of age in person;
```

The [Compute Queries](../graql/compute-queries.html) documentation covers the usage of `compute` in more detail.


{% include links.html %}
