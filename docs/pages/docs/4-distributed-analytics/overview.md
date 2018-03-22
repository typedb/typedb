---
title: Analytics Overview
tags: [analytics]
summary: "This page provides an overview of the Graql analytics capabilities."
sidebar: documentation_sidebar
permalink: /docs/distributed-analytics/overview
folder: docs
KB: genealogy-plus
---

The distributed Grakn knowledge graph presents two different ways to obtain insight on a large dataset:

 *   It intelligently aggregates large amounts of information. Graql allows you to specify what you want, and analytics allows you to do it at scale. For example, finding out the mean number and standard deviation of vehicles owned by companies (not individuals), no matter how big the dataset.
 *  The structure of the knowledge graph contains valuable information about the importance of entities and also the communities they form. This is achieved by computing the number of relationships that certain entities take part in, and using this as a measure of how popular they are. An example of this can be seen on the [Moogi website](https://moogi.co), which uses only the structure of the graph to rank the results.

{% include note.html content="Under the hood we use implementations of the [Pregel algorithm](https://www.quora.com/What-are-the-main-concepts-behind-Googles-Pregel) distributed graph computing
framework and/or [map reduce](https://en.wikipedia.org/wiki/MapReduce) when we need to aggregate the result. This way we can implement algorithms that will scale horizontally." %}

## What Can I do With Analytics?

The functionality breaks down into two main tasks:

*  Statistics Query: computing statistics related to numeric resources
*  Graph Query: interrogating the structure of the knowledge graph.

### Statistics Query

Currently you can compute the `min`, `max`, `mean`, `median`, `std` (standard deviation) and `sum` of resources attached to entities. This
can also be achieved on a subgraph, which is a subset of the types in your dataset. For example, you can specify queries to find the mean age of people in a knowledge graph:

```graql
compute mean of age in person;
```

We cover this topic more in our documentation page on [statistics](./compute-statistics).

### Graph Query

At the moment we support queries for determining

* [centrality: degree, k-core (coreness)](./compute-centrality)
* [clusters or communities: connected component, k-core](./compute-cluster).
* [paths between nodes (shortest path)](./compute-shortest-path)

## The Knowledge Graph According to Analytics

Graql analytics functionality is accessed via the `compute` query in the Graql language. In order to fully understand the
syntax, an in-depth understanding of the knowledge graph is needed, so we will dive into some details here.

Analytics only "sees" the instances of types, but is aware of the schema. Therefore, if your knowledge graph has a type `person`
then the instances of this: `Jacob Young`, `Hermione Newman` and `Barbara Herchelroth` can be counted using analytics.
Often you are not interested in the whole knowledge graph when performing calculations, and it is possible to specify a subgraph (a subset of your data to work on) to Graql.
For example, a knowledge graph may contain groups, people and the relationships between them, but these can be excluded by specifying a subgraph using the `in` keyword.
To count just people:

```graql
compute count in person;
```

Consider the simple knowledge graph below that includes types and instances (some are entities and some are relationships).
Analytics will consider every instance in the knowledge graph, and therefore, will not consider the type nodes `person` and `marriage`, (coloured in pink).
To compute the count on this knowledge graph without specifying any subgraph, we call the following, which would return the number 4:

```graql
compute count;
```

Analytics has counted all of the instances of the types, which are specific people and the nodes representing
the marriage relationship.

![A simple knowledge graph.](/images/analytics_sub_Graph.png)

A subgraph is defined in analytics by using the types. For example, we could specify a subgraph that includes only
`person` like this:

```graql
compute count in person;
```

and this would return the number 3.
The analytics will now operate on can be seen below.

![A simple knowledge graph.](/images/analytics_another_sub_Graph.png)


We may also specify a subgraph for efficiency (so we do not have to count the things we are not interested in).
The algorithm for computing the degree is one example.
If we execute the following query, the number of arrows (edges) attached to each node is returned:

```graql
compute centrality in person, marriage; using degree;
```

In the example below this would be 1 for Jacob Young, 2 for Barbara Herchelroth, 1 for John Newman and 0 for the rest because we do not count the arrows indicating type, only arrows labelled with `spouse` roles.
This knowledge graph also happens to include the parentship relationship, but we have ignored this and only found out the number of marriages a person has taken part in for any size of graph.

![A simple knowledge graph.](/images/analytics_degree_sub_Graph.png)

{% include note.html content="The degree is the simplest measure of the importance (centrality) of a node in a graph.
Graql is very flexible and allows us to define the subgraph in which we want to compute the degree, and therefore determine
importance according to various structures in the graph." %}
