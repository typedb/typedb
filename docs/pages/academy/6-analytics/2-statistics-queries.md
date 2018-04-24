---
title: Statistics queries
keywords: getting started
last_updated: October 2017
summary: In this lesson you will learn the details of analytics statistics query
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/statistics-queries.html
folder: academy
toc: false
KB: academy
---

In the last lesson you have learned about the differences between OLTP and OLAP queries and you have been introduced to the Grakn Analytics component. In this lesson you will get deeper into the details of the Analytics component, starting with the statistics queries.

All the analytics query in Graql begin with the keyword `compute` and, for this reason, are called "compute queries" as well. Statistics compute queries have all the following structure:


```graql-skip-test
compute <STATISTIC> in <TYPES>;
```

The `in <TYPES>;` part of the query (`<TYPES>` just stands for a list of comma separated types) is optional and is a list of types representing the part of the knowledge graph on which you want to execute the query.

An example will clear up things. Let’s us do some counting.

## Count queries
If it’s not already running, launch the Grakn instance in your VM and open the dashboard in the "training" keyspace. Let’s say we want to count the number of oil platforms in our knowledge graph.

Launch this query:
```graql-skip-test
compute count in oil-platform;
```

After a while you will see the result in your dashboard:

 ![Count Query](/images/academy/6-analytics/count-query.png)

As you can see, it’s far from difficult.

Try it yourself now: write and launch a compute query to count how many companies AND bonds are in the knowledge graph.

## Statistics query
If you want to find out something a bit more complicated than the number of instances of a few types, you need proper statistics. The query, in this case looks like this:

```graql-skip-test
compute <STATISTIC> of <ATTRIBUTE> in <TYPES>;
```
Statistics like mean, standard deviation, median etc. can only be computed on numerical attributes, so that is what the `of <ATTRIBUTE>` part of the query is for. If the attribute is shared by several types, then you might want to restrict to the computation to only some of those with the `in <TYPES>` part. Imagine you have a knowledge graph of people, cats and dogs, for example, and both types have an "age" attribute. If you wanted to compute the median age of the dogs in the knowledge graph, you would do:

```graql-skip-test
compute median of age, in dog;
```

On the other hand, if you wanted to find the median age of the pets you would need

```graql-skip-test
compute median of age, in [dog, cat];
```

Finally, if you wanted to find the median age tout-court, you would simply do

```graql-skip-test
compute median of age;
```

All the statistics queries behave in the same way. The possible statistics are:

|Statistic |  Keyword |
|----------|----------|
|Minimum   | _min_ |
|Maximum   | _max_ |
|Average   | _mean_ |
|Median    | _median_ |
|Standard deviation  | _std_ |
|Sum     | _sum_ |

Can you write and execute a query to find the standard deviation of the risk of bonds in your knowledge graph?

## Compute queries vs aggregates
If you have paid close attention to the lessons of the academy and maybe have looked a bit into the documentation, you will have noticed that for each of the statistics compute query there exists an analogous aggregate query.

It is true that both versions of the query actually will give you the same results and compute the same things, but they do so in very different ways.

Aggregate queries actually perform an OLTP query and then do their computation on the results of the query. In general if you are expecting the number of results to be relatively small, they are much much more efficient.

If you want to compute your statistics over a large distributed knowledge graph, on the other hand, aggregate queries quickly become unusable (imagine, for example, that you want to count the number of people in a knowledge graph that contain several million of them: executing the query and, most importantly, returning the results would be a very inefficient way of doing that). In those cases, you will use the compute queries, that use much more advanced algorithms (it is the reason you have to wait a while when you launch one of those queries: the algorithm has to go through several setup steps) and can act on the whole knowledge graph at the same time.

In other words, the compute queries scale much better.

### What have you learned?
In this lesson you have learned about the basic structure of compute queries, you have learned what are the statistics query you can perform with Grakn analytics and what is the difference between compute and aggregate queries and which one to pick when.

## What next
In the next lesson we will conclude our overview of the Grakn Analytics component by looking at something that simple aggregate queries cannot do: perform [graph analytics](./graph-analytics.html). More details about the compute query syntax can be found in the [docs](../index.html). If you want to learn more about the distributed computations that compute queries perform you will have to look around for an introduction to Pregel algorithms. Warning, though: heavy maths ahead.
