---
title: Analytics - From Knowledge to Wisdom
keywords: getting started
last_updated: April 2018
summary: In this lesson you will learn about the different types of Graql Analytics queries and when to use them
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/analytics-intro.html
folder: academy
toc: false
KB: academy
---

In this module you will learn about the details of the Grakn distributed analytics capabilities, that allow to perform distributed computation at scale.

Before you begin this module, a disclaimer is in place.

### WARNING! EXPERIMENTAL!
Graql Analytics, although powerful, is still an experimental feature of Grakn, so things might change and sometimes you might get funny results. You have been warned.

## Distributed analytics
The queries you have seen so far to get information about your knowledge graph are commonly called OLTP queries (On Line Transaction Processing). With these you learn about specific concepts in your knowledge graph - whatever happens somewhere else is irrelevant. For example: in the query about the Italian referendum, if the rest of the knowledge graph (which you did not query as it wasn't important to answer this question) did not exist you would have obtained exactly the same responses.

Sometimes though, you want to ask questions that are related to the whole knowledge graph, or at least a large chunk of it. If you wanted to know the average risk value of a bond, changing some of the risk values somewhere in the graph would have changed the answer. These kind of queries are called OLAP queries (On Line Analytical Processing) and when you have a very large dataset, possibly distributed over several nodes in a large cluster, they can be very difficult to execute.

Luckily, Grakn takes care of them for you with its Analytics component in a distributed fashion that works at scale.
Of course, since OLAP queries are very different from OLTP ones, you will have to use a slightly different syntax.

Don’t worry, though, it will be very easy.

## Analytics query types
There are two types of Analytics queries: Statistics and Graph analytics.

**Statistics queries** are used to compute things like means, maximums, standard deviations and the like. You could achieve the same result with an aggregate query (more on the subject in the next lesson), but that would be very inefficient if your knowledge graph is very large.

**Graph queries** exploit the fact that as a high level knowledge graph, Grakn is not married to a specific data representation, so it can take the best of all worlds. Since knowledge is often structured like a graph (or better a [hypergraph](https://en.wikipedia.org/wiki/Hypergraph)), Grakn can be used to exploit this fact and efficiently compute things like the degree of a certain concept (i.e. the number of relationships in which it takes part) or the length of the shortest path between two concepts.
The latter kind of queries is particularly useful when you are dealing with analyses of communities and machine learning prediction problems (a very interesting topic but out of the scope of the Academy).

### What you have learned?
In this lesson you have learned the difference between OLTP and OLAP queries. You also learned about the two kind of analytics queries.

## What next
In the [next lesson](./statistics-queries.html) you will learn about the syntax and the types of the various analytics queries. If you want to learn more about analytics you can read this [blog post](https://blog.grakn.ai/distributed-big-data-analytics-with-graql-fc71500822d1) or have a look at the [docs](../index.html).
