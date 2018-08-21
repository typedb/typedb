---
title: Logic inference - The power of knowledge
keywords: setup, getting started
last_updated: April 2018
summary: In this lesson you will be introduced to logic inference and the the Grakn reasoning engine.
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/reasoner-intro.html
folder: academy
toc: false
KB: academy
---

If you have followed the Academy lessons up to this point, you should have loaded all the example dataset into a new knowledge graph. It is now in exactly the same state as the initial academy keyspace.

First of all: well done!

But you are not done yet: we can now start to use this knowledge graph, add a pinch of Grakn magic and turn what is essentially a powerful database into a true knowledge graph ready for the development of cognitive systems.

It is time for some logic inference.

## Back to the query
Far back in [module 2](./graql-intro.html) you learned the questions on which we built the knowledge graph. Further on we translated them into Graql

```graql
match
$article isa article has subject "Italian Referendum";
$platform isa oil-platform has distance-from-coast <= 18;
(location: $country, located: $platform) isa located-in;
$country isa country has name "Italy";
(owner: $company, owned: $platform) isa owns;
(issuer: $company, issued: $bond) isa issues;
limit 3; get $bond, $article;
```

If you run it in the graph visualiser you will notice that you get the results you expected (i.e. the articles about the Italian referendum and the bonds that might be affected by them), but they are not linked together.

In this module, we will use the Grakn inference engine to make sure that Grakn fixes this. All without actually modifying your data.

## Introduction to logic inference
Let us start this section with a tiny puzzle:

On a small, almost deserted island, there is a small hut on the beach. Alice is currently in the hut, while Bob is somewhere else on the island. There is nobody else.

Who is on the island? Notice that this is not a trick question.

The simple answer is, of course, Alice and Bob.

The answer comes to you easily because you are a human being equipped with a human brain.

Let us try to break the question in smaller pieces:

  1. There are two people, Alice and Bob

  1. Alice is currently in a hut

  1. Bob is on the island

  1. The hut is on the island

This is all the information I gave you. What your brain did in answering the question is adding the information that if a person is in a place that is on the island, then that person is on the island as well.

A computer would not know that, and if you asked the same question to a regular database, you would only get Bob as a response (because we know that Bob is on the island).

Unless you ask explicitly for all the people that are on the island OR all the people that are in a place that is on the island you will get an incomplete answer. Just imagine that Charlie is currently in the kitchen of the hut. That is a place that is in a place which is on the island. Writing this kind of query for a relational databes would be tedious (and error prone).

Fortunately some systems - like Grakn - have a dedicated reasoner engine for this kind of problem.

In the course of this module, you will learn how to augment your knowledge graph with reasoning rules that help you to get more useful and smart answers to your questions (like the one above).

You will learn to make your data more intelligent and knowledgable. Grakn will make this very easy for you, which is one of the reason why we say that Grakn is the database for knowledge engineering.

### What have you learned?
This was a very brief introductory lesson, which should have showed you what logic inference is about.

## What next
In the [next lesson](./inference-rules.html), we will learn how to add Graql inference rules to your knowledge graph; as you will see, it is a very simple yet powerful process. If you are impatient and what to know everything immediately, you can learn more about the Grakn Reasoner from the [docs](../index.html).
