---
title: Get Queries
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you will learn about the get queries, the fundamental queries used to explore Grakn Knowledge bases
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/get-queries.html
folder: overview
toc: false
KB: academy
---

Graql, the language used to query and manipulate data in a Grakn knowledge graph (and much more, as you will discover if you follow the whole Grakn Academy), has been built to be readable, easy to learn and use. In this lesson you will learn the basics of the language and at the end of it you will be able to query data in your knowledge graph.


## Basic get queries

Let us start with a very simple example: what do you think the following query will do?

 `match $x isa company; get;`

If you answered "return all companies in the knowledge graph" then congratulations! You are correct! Give yourself a pat on the back and let’s go onto something very slightly more complex. What do you think the following query does?

`match $x isa company has name "ENI"; get;`

If you answered "return a company named ‘ENI’", you were almost right: the correct answer is "return all the companies named ‘ENI’". If there happens to be only one, all the better, but it is good to keep in mind the subtle distinction.

One thing that is maybe useful to keep in mind is that the query above is exactly the same (and in fact under the hood Graql is doing) as the query


```graql
match
$x isa company;
$x has name "ENI";
get $x;
```

Since, as you have learned in the [last lesson](/academy/graql-intro.html), the order of patterns in a Graql query does not matter, the query above is no different from

```graql
match
$x has name "ENI";
$x isa company;
get;
```

Go ahead and try these queries in your running distribution of Grakn (need a refresher? [Here’s the link](/academy/setup.html) to the lesson in which you learned how to do that).


### Exercise: a query with an attribute
You should by now be able to write very basic Graql queries with or without resources. Try to think of how to query for a country named "UK".


## Querying for relationships
Even more fun! What do you think the following query will do? Take your time…

```graql
match
$x isa country has name "USA";
($x, $y) isa located-in; get;
```

Did you answer, by any chance, "return everything that is in a(ny) country named ‘USA’"? If so, you are almost right.

Try it now in the dashboard

  ![Relationship query](/images/academy/2-graql/relationship-query.png)

You see "North America" right there? Can you guess what is happening?

Let’s break the query into small pieces

`$x isa country has name "USA";`, as you should know by now, retrieves all the countries with the name "USA" and store them (actually there happens to be only one) in a variable accessed with the name `$x`.

The bit `($x, $y)` tells Graql to create a new variable `$y` and store into it anything that is attached to `$x` via a relationship (that is what the parenthesis are for).

Finally the `isa located-in` says that the relation that links `$x` and `$y` must be of type "located-in". The `get` part just says to return all the variables created.

Notice that we never specified that a thing should be located in a country, only that they need to have this kind of relationship. The order, as I said before, does not matter in Graql. In fact you will obtain the same results if you run the following query (flipping the position of `$x` and `$y`):

```graql
match
$x isa country has name "USA";
($y, $x) isa located-in; get;
```

Go ahead, try it.


## Introduction to Roles
So what if you want to specify that we want to retrieve all the things that are actually contained in the USA? That is what roles are for. In Graql every component of a relation plays a role in it, which specifies the function of that role player in that specific relation. The syntax for it looks like this:

```graql
match
$x isa country has name "USA";
(location: $x, located: $y) isa located-in; get;
```

Try it in the dashboard.

The name of the roles (in this case `location` and `located`) are user specified, I have assigned the label `location` to the container in the located-in relation and `located` for the thing which is contained. It is just a matter of adding the role names followed by `:`.

### Exercise
What is the query that finds all the things that _contain_ the country "USA"?

Can you write a query that finds all the bonds issued by a company named "ENI"? The names of the roles in this case are "issued" and "issuer", the relation is called "issues".

Try the same query without specifying the roles. Does the result change? Why do you think is that?


## Modifiers
After a get query you can use a number of optional modifiers that will change the results returned by the query. The most common one that you will need are `limit` and `select`.

The `limit` modifier, followed by an integer, restricts the number of results that are returned by Graql and you have already met it as it is automatically added to every query run in the graph visualiser to avoid too much confusion on the screen.


## The _get_ action
The `get` action, followed by one or more variables, limits the results to the variables specified. If no variables are specified, each result will contain an instance for each of the variables used in the match part of the query.

See it for yourself! Run this query and notice the different results with the same query without the `$y` variable at the end.

```graql
match
$x isa country has name "USA";
(location: $x, located: $y) isa located-in;
get $y;
```


### What have you learned?
In this lesson you have learned all the basics of match queries, which is probably the type of query you will be using the most. You know how to query for basic entities, for resources (the names in the examples) and for relation. You also know what roles are and why they are useful, but we will go back into more deeper details during the course of the Academy.


## What next?
The [next step](./insert-delete-queries.html) is to learn how to insert data into your knowledge graph. If you want to know more about the Graql syntax and the possible modifiers you can look at the [docs](../index.html). If you want to explore more about the roles and the other parts of our data model, you could skip to the [third module](./schema-elements.html) of the Academy, although it is not recommended.
