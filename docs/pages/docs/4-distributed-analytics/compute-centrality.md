---
title: Degrees
tags: [analytics]
summary: "This page introduces the computation of degrees in a graph."
sidebar: documentation_sidebar
permalink: /docs/distributed-analytics/compute-centrality
folder: docs
---

What is the most important instance in a graph? Given no more context than this, `degrees` can provide an answer of
sorts. The degree of an instance gives the number of other instances directly connected to the instance. For an entity, such as
a `person`, this means the number of connections, for example instances of a `marriage` relationship.
The `person` with the highest degree, or the greatest number of marriages, is arguably the most "interesting".

### Degree

The `degree` algorithm computes how many arrows (edges) there are attached to instances in the graph. A map is returned
that displays an instance ID and its degree. If we call:

```graql
compute centrality; using degree;
```

on the graph below we expect the degrees of the people to be:

* **Barbara Herchelroth**: 3 - two marriages and a child
* **Jacob Young**: 2 - a marriage and a child
* **Mary Young**: 2 - two parents
* **John Newman**: 1 - a marriage

We do not count the edges labelled isa, because they are connected to the types and are not considered by analytics.
Don't forget that the relationships themselves will also have degrees.

![A simple social network.](/images/analytics_degree_full.png)

### Centrality Within a Subgraph

Consider that in this graph, people with more marriages are more interesting.
We can use the subgraph functionality to restrict the graph to only see people and who they are married to.
Once the graph has been restricted we can determine the number of marriages by computing the degree:

```graql
compute centrality in person, marriage; using degree;
```

The result will now be:

* **Barbara Herchelroth**: 2
* **Jacob Young**: 1
* **Mary Young**: 0
* **John Newman**: 1
