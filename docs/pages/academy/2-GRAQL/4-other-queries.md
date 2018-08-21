---
title: Query variations
keywords: setup, getting started
last_updated: April 2018
summary: In this lesson you will get an introduction to aggregate and compute queries.
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/other-queries.html
folder: overview
toc: false
KB: academy
---

You have learned to query and manipulate data with Graql and that will probably constitute the bulk of the language you need for a while. Before we go on to other topics, you might want to know that there are also other kind of queries than `get`, `insert` and `delete`.

In this lesson we will very briefly review them, so that you have a more complete view of the language.

## Aggregates
Sometime you do not want to just look at the result set of a get query. Sometimes you actually want do things with it and manipulate it in some way.

Meet aggregate queries.

If you look into it, you will see that the bonds in our knowledge graph have a "risk" score attached. They are just randomly generated numbers, so do not try and look for any pattern, but let us see what we can do with them.

Imagine that you want to look up the minimum risk of a bond issued by the company "ENI". If you just wanted to find all risk scores for all bonds you would run the following query:

```graql
match
$x isa company has name "ENI";
($x, $y) isa issues;
$y isa bond has risk $r;
get;
```

If you want to find the minimum risk value you would just minimally adapt the last query and substitute `get` with  `aggregate min $r;`. This is how it looks like (notice that you cannot run aggregate queries in the graph visualiser as the returned response is not a graph).

  ![Aggregate example](/images/academy/2-graql/aggregate-query.png)

With aggregate queries you can calculate means, standard deviations, counts and many other useful things. You can find the complete list in the [documentation](..//docs/querying-data/aggregate-queries).

How would you find the standard deviation of the risk values of all bonds?


#### Count things
Answer me quickly: how many companies are named "AwesomePuppies" in our knowledge graph?

Let’s ask the dashboard. Literally. Run this query in the _Dashboard Console_:

```graql
match $x isa company has name "AwesomePuppies";
aggregate count;
```

To count the occurrence of something in the knowledge graph, you can use `count` aggregate. If the `get` query returns no result, then the corresponding `count` aggregate will return `0`. Otherwise it will return the total count. Nothing fancy, but very useful. Especially if you are building an app on top of Grakn.


## Compute queries
The aggregates effectively do their computations on the results of a match query. This means that before doing your calculations, Grakn first has to execute the query and return the results. If you want to do your statistics on large result graphs you would use a lot of resources. To stay efficient you need something more powerful, namely Grakn analytics.

To use Analytics, you run `compute` queries, that have a slightly different syntax: a `compute` query has the form `compute <method> <conditions>?;` where `<name>` the type of analytics functions you want to compute, `<conditions>` are the set of conditions you would like to provide to control the computation. The expected conditions for each compute name will vary depending on the name (you can find out more through the [Graql Analytics documentation](../docs/distributed-analytics/overview)), for example:

```graql
compute min of risk, in bond;
```

Apart from the basic statistics which can be performed with aggregate queries as well, you will be able to use `compute` queries to run powerful graph analytics on your whole knowledge graph. The topic definitely deserves more attention than this brief example, so we will dedicate a whole module to it later in the series.


### What have you learned?
You now know all the query types that Graql supports. You are ready to step up your Grakn level.


## What next?
First of all, try and do the [Graql review](./graql-review.html) before proceeding to [module 3](./schema-elements.html). If you want to know more about the queries introduced in this lesson you can, as always, have a look at the [docs](../index.html).
