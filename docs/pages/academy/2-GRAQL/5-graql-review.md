---
title: Module Review
keywords: setup, getting started
last_updated: September 2017
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/graql-review.html
folder: overview
toc: false
KB: academy
---

Wow! This was intense! We have gone through all the basic of the Graql. Let us review them with a few extra exercises.

### Exercise 1: Graql review
Do you remember the big query that I showed you at the beginning of this module? It looked like this:

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
Maybe it looked scary at the time, but by now you should be able to understand it completely. Try going over each line of the query and check whether you understand what is happening.

After doing so, run the query in the graph visualiser. Can you guess why the results appears as disconnected nodes?

### Exercise 2: Get queries
Run in the dashboard a query that retrieves all bonds issued by a company that owns an oil platform in the USA.

Don’t worry too much if the results don’t make too much sense: they have been generated randomly, so you are likely to see weird stuff like offshore platforms in landlocked countries and similar stuff.

### Exercise 3: Insert queries
Add to the knowledge graph a new bond named "MyAwesomeBond" issued by at least 3 companies.

Check in the graph visualiser that the bond has been correctly added and double click on it to show which companies it is connected to.

### Exercise 4: Delete queries
Delete from the knowledge graph the bond you have inserted in Exercise 3.

### Exercise 5: Ask
Write a query that tells you whether a country named "Burundi" is in the knowledge graph.

### Exercise 6: Statistics
Write a query to find the maximum risk value of a bond.
Write both a `compute` query and an `aggregate` for the task.

## Congratulations!
You are now a Graql expert! Or at least you already master Graql enough to take full advantage of the power of Grakn. There is still a lot of things that can be done and will be covered in the next modules, but you are now well equipped to start using Grakn. Remember: Graql is the language that is used for all the Grakn tasks, so you are now in a very good position.

## What next
The [next Academy module](./schema-elements.html) will teach you all there is to know about Grakn schemas. What they are and how to build one. If you want to review the Graql material, go back to [lesson 2.1](./graql-intro.html). Otherwise, if you want to have a deeper look, head to the [docs](../index.html).
