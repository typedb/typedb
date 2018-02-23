---
title: Analytics - From Knowledge to Wisdom
keywords: getting started
last_updated: October 2017
summary: In this lesson you will learn about the different types of Graql Analytics query and when to use them
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/analytics-intro.html
folder: academy
toc: false
KB: academy
---

In this module you will learn the details of the Grakn distributed analytics capabilities, that allow to perform distributed computation at scale.

Before you begin this module, a disclaimer is in place.

### WARNING! EXPERIMENTAL!
Graql Analytics, although powerful, is still an experimental feature of Grakn, so things might change and sometimes you might get funny results. You have been warned.

## Distributed analytics
So far, in the Academy courses, you have learned about what are commonly called OLTP queries (On Line Transaction Processing), which are the queries in which you want to learn about specific concepts in your knowledge base and whatever happens somewhere else is irrelevant. For example, in the query about the Italian referendum you have examined, if the rest of the knowledge base did not exist, you would have obtained exactly the same responses.

Sometimes though, you want to ask questions that are related to the whole knowledge base, or at least a large chunk of it. If you wanted to know the average risk value of a bond, for example, changing some of the risk values of the bonds would change the answer. These kind of queries are called OLAP queries (On Line Analytical Processing) and when you have a very large dataset, possibly distributed over several nodes in a large cluster, they can be very difficult to execute, if not impossible.

Luckily, Grakn takes care of them for you with the Analytics component, that saves you all the pain of implementing them. In a distributed fashion that works at a scale.
Of course, since OLAP queries are very different from OLTP ones, you will have to use a slightly different syntax.

Don’t worry, though, it will be very easy.

## Analytics query types
There are two types of Analytics queries: Statistics and Graph analytics ones.

**Statistics queries** are used to compute things like mean, maximums, standard deviation and the like. You could achieve the same result with an aggregate query (more on the subject in the next lesson), but that would be very inefficient if your knowledge base is very large.

**Graph queries** exploit the fact that as a high level hyper-relational database, Grakn is not married to a specific data representation, so it can take the best of all worlds. Since knowledge is often structured like a graph (or better a [hypergraph](https://en.wikipedia.org/wiki/Hypergraph)), Grakn can be used to exploit this fact and efficiently compute things like the degree of a certain concept (i.e. the number of relationships into which it appears) or the length of the shortest path between two concepts.
The latter kind of queries are particularly useful when you are dealing with things the analysis of communities and machine learning predictions, but this is a bit out of the scope of the Academy.

### What you have learned?
In this lesson you have learned the difference between OLTP and OLAP queries and learned about the two kind of analytics queries.

## What next
In the [next lesson](./statistics-queries.html) you will learn about the syntax and the types of the carious analytics queries with some necessary low level caveats. If you want to learn more about analytics you can read this [blog post](https://blog.grakn.ai/distributed-big-data-analytics-with-graql-fc71500822d1) or have a look at the [docs](../index.html).
