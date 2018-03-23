---
title: Centrality
tags: [analytics]
summary: "This page introduces the computation of centrality in a graph."
sidebar: documentation_sidebar
permalink: /docs/distributed-analytics/compute-centrality
folder: docs
---

What is the most important instance in a graph? 
More importantly, how do we measure importance in a graph?

Indicators of centrality identify the most important vertices within a graph. 
[Wikipedia](https://en.wikipedia.org/wiki/Centrality)

Currently, Graql supports two algorithms for computing centrality: 
- Degree 
- K-Core

## Degree

The degree of an instance gives the number of other instances directly connected to the instance. 
For an entity, such asa `person`, this means the number of connections, 
for example instances of a `marriage` relationship.
The `person` with the highest degree, or the greatest number of marriages, is arguably the most "interesting".

In analyics, the `degree` algorithm computes how many arrows (edges) there are attached to instances in the graph. 
A map is returned containing degree and instances with the degree (map<key=degree, value=set of instance>). If we call:

```graql
compute centrality; using degree;
```

On the graph below we expect the degrees of the people to be:

* **Barbara Herchelroth**: 3 - two marriages and a child
* **Jacob Young**: 2 - a marriage and a child
* **Mary Young**: 2 - two parents
* **John Newman**: 1 - a marriage

We do not count the edges labelled isa, because they are connected to the types and are not considered by analytics.
Don't forget that the relationships themselves will also have degrees.

{% include note.html content="Don't forget that the relationships themselves will also have degrees." %}

![A simple social network.](/images/analytics_degree_full.png)

## K-Core (Coreness)

Coreness is a measure that can help identify tightly interlinked groups within a network.
An instance have coreness k if the instance belongs to a 
[k-core](https://en.wikipedia.org/wiki/Degeneracy_(graph_theory)#k-Cores) but not to any
(k+1)-core.

We can compute the coreness centrality using the following:

```graql
compute centrality; using k-core;
```

Similar to degree, a map is returned.

Additionally, if we only care about the entities that have higher coreness, we can set the minimum value of k,
using the following command:

```graql
compute centrality; using degree where min-k = 10;
```

So the result map will only include entities with coreness greater than or equal to 10. 

{% include note.html content="Relationship instances will NOT have coreness." %}

## Centrality within a subgraph

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

The subgraph command can also be used when computing k-core centrality.

## Centrality of a given type

Consider the subgraph example again: because relationships also have degrees, 
the result map would also contain centrality of `marriage`.
However, we only want to compute the centrality of `person`.
We cannot exclude instances of `marriage` from the subgraph, otherwise every entity will have its degree = 0.
In a case like this, we can use the following:

```graql
compute centrality of person in person, marriage; using degree;
```

We can list all the types we are interested in, separated by comma, after the keyword `of`, 
so the result map would only contain these types. 

Another example:

```graql
compute centrality of cat, dog in man, cat, dog, mans-best-friend; using k-core;
```

where `mans-best-friend` is the relationship type containing two roles: human and pet.
The result map will only contain coreness of cat and dog.
