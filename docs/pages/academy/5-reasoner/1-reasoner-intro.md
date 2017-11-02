---
title: Logic inference - The power of knowledge
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you will be introduced to logic inference and the uses of GRAKN reasoning engine.
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/reasoner-intro.html
folder: academy
toc: false
KB: academy
---

If you have followed the Academy lessons up to this point, you should have loaded all the example dataset into a new knowledge base, which is now exactly the same as the one that came preloaded with the GRAKN distribution in the VM.

First of all: well done!

But you are not done yet: we now need to start using this knowledge base and add a pinch of GRAKN magic and turn what is essentially a powerful database into a true knowledge base ready for development of cognitive systems.

It is time for some logic inference.

## Back to the query
Since from the beginning of [module 2](./graql-intro.html), you should have learned that the questions you have built your knowledge base around translate into GRAQL as

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

And if you run it into the graph visualiser you will notice that you do get the results expected (i.e. the articles about the Italian referendum and the bonds that might be affected by them), but they are not linked together.

In this module, we will use GRAKN inference engine to make so that GRAKN fixes this. All without actually modifying your data.

## Introduction to logic inference
Let us start this section with a small puzzle:

In a small almost deserted island, there is one small hut on the beach. Alice is currently in the hut, while Bob is somewhere else on the island. There is nobody else.

Who is on the island? Notice that this is not a trick question.

The simple answer is, of course, Alice and Bob.

The problem is that the answer is simple only if you are a human.

Let us try to break the question in smaller steps:

  1. There are two people, Alice and Bob

  1. Alice is currently in a hut

  1. Bob is on the island

  1. The hut is on the island

This is all the information I gave you. What your brain did in answering the question is adding the information that if a person is somewhere that is on the island, then that person is on the island as well.

A computer would not know that, and if you asked the same question to a regular database, you would only get Bob as a response (because we know that Bob is on the island).

Unless you ask explicitly for all the people that are on the island OR somewhere which is on the island (but what happens if Charlie is in the kitchen of the hut then?).

Or unless you have a reasoner engine, like GRAKN has.

In the course of this module, you will learn how to augment your knowledge base with reasoning rules that help you get more useful and smart answer to your questions, like the one above.

That is, you will learn to make your data more intelligent. Which is one of the reason why say that GRAKN is the database for the AI.

### What have you learned?
This was a very brief introductory lesson, but you should have at this point understood what logic inference is about.

## What next
In the [next lesson](./inference-rules.html), we will learn how to add GRAQL inference rules to your knowledge base; as you will see, it is a very simple yet powerful process. If you are impatient and what to know everything immediately, you can learn more about GRAKN Reasoner from the [docs](../index.html).
