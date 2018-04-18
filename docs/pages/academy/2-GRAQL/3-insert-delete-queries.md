---
title: Insert and Delete Queries
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you will learn the Graql syntax to insert and delete things from your knowledge graph
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/insert-delete-queries.html
folder: overview
toc: false
KB: academy
---

Let us kick off this lesson with an exercise: open the dashboard and write a query to get a company named "Grakn".

What? There is no Grakn in your Grakn? We have to fix this!
We need to learn how to insert data into your knowledge graph.


## Insert queries
Try and run in the dashboard
```graql
insert $x isa company has name "Grakn";
```
As you might have noticed, the syntax for insert queries is the same as the syntax for get queries, so there is nothing new to learn here. In fact, once again, the query above is exactly the same as


```graql
insert $x isa company; $x has name "Grakn";
```

Try this query and you will see that it will work as well (notice that you cannot split the query with a new line in the Graql shell).

Let’s check if we can query the company we just created.
```graql
match $x isa company has name "Grakn"; get;
```
When the query gets executed you will see the new companies inserted. Notice that there are two of them, as we run two insert queries. For Graql when you say
```graql
insert $x isa company has name "Grakn";
```
you mean "Create a new company entity and assign to it the name ‘Grakn’". We have never said that there could not be more than one company with the same name, so a new one is added to the knowledge graph each time we run the query or an equivalent one.

  ![Two Grakn](/images/academy/2-graql/two-grakn.png)


## Adding relationships
So we have added Grakn to our knowledge graph. Actually we have added two copies of it, but we will take care of it later. Our "Grakn" companies are quite alone in the graph. We do not know much about them. How can we tell the knowledge graph, that Grakn is located in the UK? The company is there, the country is there, we know that the relation type is called `located-in` because we have used it before, but how can establish the connection?

Well that is what the `match` part of the query is for: to apply our insert action to something that is already in the knowledge graph. It looks like this:


```graql
match
$c isa country has name "UK";
$g isa company has name "Grakn";
insert
(location: $c, located: $g) isa located-in;
```

Before executing this query: are you able to guess what will happen?

Let us break the query into smaller pieces: the first part is a normal match, like the ones you have encountered with get queries; the second part of the query is a normal `insert` action that gets executed _once for each result of the first part_. This means that in our case, since the first part will return two results, two relations will be inserted, once for each instance of "Grakn". So both Grakns will be located in the UK.

Try it now.

If you want to see the newly created relations, run
```graql
match $x isa company has name "Grakn"; get;
```
and double click on one of the two copies of the company that appears. This is telling the visualiser to fetch all the relations connected to Grakn, so we will see that Grakn is in the UK.

  ![Grakn in the UK](/images/academy/2-graql/grakn-uk.png)

## Deleting things
How do we delete things that we don’t want in the knowledge graph? With a delete query of course.

A delete query has the same form as a get query that uses `delete` as the action keyword. The major difference is that you have to explicitly tell Graql which elements to delete - in this case that would be `$x`. Try this, for example:

```graql
match $r ($x) isa located-in; $x isa company has name "Grakn"; delete $r;
```
This would delete the relationship between the companies named "Grakn" and the country named "UK".

Retrieving relations is something that we haven’t seen so far, but it’s nothing complicated. Basically, if you want to assign a relation to a variable, just put a variable name before the parenthesis. It’s quite easy (notice how we use the variables in the delete action as we used them in the get action):

If you run the query above and commit, the relation will be gone and Grakn won’t be in the UK anymore. You can check it in the graph visualiser.

Did you notice that we did not had to specify the "UK" in the above query? When you run a query, Graql will do its best to try and understand what you mean. In this case, there are no other `located-in` relations involving "Grakn" and the company plays only that role, so that is all we need to specify.

At this point you are free to run
```graql
match $x isa company has name "Grakn"; delete;
```
Then commit, and everything will be returned like it was at the beginning of this lesson.

As a parting thought: if you omit the variables in the delete or get actions, it is considered to be just a shorthand to indicate that you want the action applied to all the variables defined in the match part of the query.


### What have you learned?
In this tutorial we have learned the basics of inserting and deleting data from a Grakn knowledge graph. We also have learned how to match relations. By now you should have a solid foundation of Graql and you could already go on and start building your first ontology. But there are a few types of queries that I want to introduce to you first.


## What next?
In the [next lesson](./other-queries.html) you will be introduced to other, less common kind of queries. Once again a more complete description of the Graql syntax can be found in the [docs](../index.html).
