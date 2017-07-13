---
title: Statistics
last_updated: February 2017
tags: [analytics]
summary: "This page introduces the statistics functionality of analytics."
sidebar: documentation_sidebar
permalink: /documentation/graql-analytics/analytics-statistics.html
folder: documentation
comment_issue_id: 71
---

Computing simple statistics, such as the mean and standard deviations of datasets, is an easy task when considering a few
isolated instances. What happens when your graph becomes so large that it is distributed across many machines? What
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

For example, the following query executes a distributed computation to determine the mean age of all of the people in the graph.

```graql
compute mean of age in person;
```

The [Compute Queries](../graql/compute-queries.html) documentation covers the usage of `compute` in more detail.


{% include links.html %}

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/71" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of this page.
