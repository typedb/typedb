---
title: Query variations
keywords: setup, getting started
last_updated: September 2017
summary: TODO
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/other-queries.html
folder: overview
toc: false
KB: academy
---

You have learned to query and manipulate data with GRAQL, and that will probably constitute the bulk of the language you need for a while, but before we go on to other topics, you might want to know that there are also other kind of queries than get, insert and delete queries.

In this lesson we will very briefly review them, so that you have a more complete view of the language.

## Aggregates
Sometime you do not want to just look at the response of a get query. Sometimes you actually want do things with it and manipulate it in some way.

Meet aggregate queries.

If you look into it, you will see that the bonds in our knowledge graph have a "risk" score attached. They are just randomly generated numbers, so do not try and look for any pattern, but let us see what we can do with them.

Imagine that you want to check what is the minimum risk of a bond issued by the company "ENI". If you just wanted to do find those risks you would run the following query:

```graql
match
$x isa company has name "ENI";
($x, $y) isa issues;
$y isa bond has risk $r;
get;
```

If you want to find the minimum value of the risk you would just substitute `get` with  `aggregate min $r;` into the query. This is how it looks like (notice that you cannot run aggregate queries in the graph visualiser as the returned response is not a graph).

  ![Aggregate example](/images/academy/2-graql/aggregate-query.png)

With aggregate queries you can find means, standard deviations, counts and many other useful things. You can find the complete list in the [documentation](..//docs/querying-data/aggregate-queries).

How would you find the standard deviation of risk of all bonds?


#### Asking things
Answer me quick: do we have a company named "AwesomePuppies" in our knowledge graph?

Let’s ask the dashboard. Literally. Run this query in the _Dashboard Console_:

```graql
match $x isa company has name "AwesomePuppies";
aggregate ask;
```

To find whether something is in the knowledge graph, you just need to use the `ask` aggregate. If the `get` query returns no result, then the corresponding `ask` aggregate will return `False`. Otherwise it will return `True`. Nothing fancy, but very useful. Especially if you are building an app on top of GRAKN.


## Compute queries
The aggregates effectively do their computations on the results of a match query. This means that before doing your calculations, GRAKN first has to execute the query and return the results. If you want to do your statistics on the whole database this soon gets not possible. You need something more powerful, namely GRAKN analytics.

To use Analytics, you run `compute` queries, that have a slightly different syntax: a `compute` query has the form `compute SOMETHING of ATTRIBUTE in SUBTYPES;` where `SOMETHING` indicates what you want to compute, `ATTRIBUTE` is the numeric variable on which you want to perform and `SUBTYPES` is just a list of entities and relationships that have that attribute. For example

```graql
compute min of risk in bond;
```

Apart from the basic statistics which can be performed with aggregate queries as well, you will be able to use Analytics to run powerful graph analytics on your whole knowledge graph. The topic deserves definitely more attention that this brief example, so we will dedicate a whole module to it later in the series.


### What have you learned?
You now know all the query types that GRAQL supports. You are ready to step up your GRAKN level.


## What next?
First of all, try and do the [GRAQL review](./graql-review.html) before proceeding to [module 3](./schema-elements.html). If you want to know more about the queries introduced in this lesson you can, as always, have a look at the [docs](../index.html).
