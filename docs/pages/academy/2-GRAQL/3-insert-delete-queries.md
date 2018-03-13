---
title: Insert and Delete Queries
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you will learn the GRAQL syntax to insert and delete things from your knowledge graph
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/insert-delete-queries.html
folder: overview
toc: false
KB: academy
---

Let us kick off this lesson with an exercise: open the dashboard and write a query to get a company named "GRAKN".

What? There is no GRAKN in your GRAKN? We have to fix this!
We need to learn how to insert data into your knowledge graph.


## Insert queries
Try and run in the dashboard
```graql
insert $x isa company has name "GRAKN";
```
You should see a big red box warning you that you cannot run insert queries into the dashboard. The reason for this is that the dashboard is an application independent of the main GRAKN engine and that you could expose as a web interface if you so desired. And you definitely would not want to have anybody visiting your website to be able to insert stuff into your graph, would you?

To insert new data into your knowledge graph you need the GRAQL shell. As you should recall, to start it in the appropriate keyspace, you have to go into the directory `grakn` from the VM terminal and then run `./graql console -k academy`.

Once you have done that, you can try again the query
```graql
insert $x isa company has name "GRAKN";
```
And this time it will work.

As you might have noticed, the syntax for insert queries is the same as the syntax for get queries, so there is nothing new to learn here. In fact, once again, the query above is exactly the same as


```graql
insert $x isa company; $x has name "GRAKN";
```

Try this query (notice that you cannot split the query with a new line in the GRAQL shell) as well and you will see that it will work as well.

Let’s open the dashboard and see what has changed: try running in the opening visualiser (**DO NOT CLOSE THE GRAQL SHELL!**)
```graql
match $x isa company has name "GRAKN"; get;
```

  ![No GRAKN](/images/academy/2-graql/no-grakn.png)

What? There is nothing? What happened?

What happened is that whenever we make a change to the knowledge graph from the GRAQL shell, the change is temporary until it gets stored in the graph.

To actually confirm  the change, go back into the GRAQL shell and type `commit`. When the query gets executed you will see the new companies inserted. Notice that there are two of them, as we run two insert queries. For GRAQL when you say
```graql
insert $x isa company has name "GRAKN";
```
you mean "Create a new company entity and assign to it the name ‘GRAKN’". We have never said that there could not be more than one company with the same name, so a new one is added to the knowledge graph each time we run the query or an equivalent one.

  ![Two GRAKN](/images/academy/2-graql/two-grakn.png)


## Adding relationships
So we have added GRAKN to our knowledge graph. Actually we have added two copies of it, but we will take care of it later. Our "GRAKN" companies, though are quite alone in the graph, so we do not know much about them. How can we add into the knowledge graph, for example the information that GRAKN is in the UK? The company is there, the country is there, we know that the relation type is call `located-in` because we have used it before, but how can we connect the two?

Well that is what the `match` part of the query is for: to apply our insert action to something that is already in the knowledge graph. It looks like this:


```graql
match
$c isa country has name "UK";
$g isa company has name "GRAKN";
insert
(location: $c, located: $g) isa located-in;
```

Before trying this query into the GRAQL shell (and committing) are you able to guess what will happen?

Let us split into smaller steps: the first part is a normal match, like the ones you have encountered with get queries; the second part of the query is a normal `insert` action that gets executed _once for each result of the first part_. This means that in our case, since the first part will return two results, two relations will be inserted, once for each instance of "GRAKN". So both GRAKNs will be located in the UK.

Try it now.

If you want to see the newly created relations, run
```graql
match $x isa company has name "GRAKN"; get;
```
into the dashboard and double click on one of the two copies of the company that appears. This is telling the visualiser to fetch all the relations connected to GRAKN, so we will see that GRAKN is in the UK.

  ![GRAKN in the UK](/images/academy/2-graql/grakn-uk.png)

## Deleting things
How do we delete things that we don’t want in the knowledge graph? But with a delete query of course…

A delete query is nothing more than a get query that uses `delete` as the action keyword there is no difference in syntax. Try this, for example:

```graql
match $x isa company has name "GRAKN"; delete;
```

Did it work? No? Of course not.
The reason for this is that both our companies "GRAKN" are in a relation with the "UK".
And if we delete the companies we will get two `located-in` relationships with only the "UK" participating in it.
And this is not allowed. So we have to delete the relationships first.

Retrieving relations is something that we haven’t seen so far, but it’s nothing complicated. Basically, if you want to assign a relation to a variable, just put a variable name before the parenthesis. It’s quite easy (notice how we use the variables in the delete action as we used them in the get action):

```graql
match $r ($x) isa located-in; $x isa company has name "GRAKN"; delete $r;
```

If you run the query above and commit, the relation will be gone and GRAKN won’t be in the UK anymore. You can check it in the graph visualiser.

Did you notice that we did not had to specify the "UK" in the above query? When you run a query, GRAQL will do its best to try and understand what you mean. In this case, there are no other `located-in` relations involving "GRAKN" and the company plays only that role, so that is all we need to specify.

At this point you are free to run
```graql
match $x isa company has name "GRAKN"; delete;
```
Then commit, and everything will be returned like it was at the beginning of this lesson.

As a parting thought: if you omit the variables in the delete or get actions, it is considered to be just a shorthand to indicate that you want the action applied to all the variables defined in the match part of the query.


### What have you learned?
In this tutorial we have learned the basics of inserting and deleting data from a GRAKN knowledge graph. We also have learned how to match relations. By now you should have a solid foundation of GRAQL and you could already go on and start building your first ontology. But there are a few type of queries that I want to introduce to you.


## What next?
In the [next lesson](./other-queries.html) you will be introduced to other, less common kind of queries. Once again a more complete description of the GRAQL syntax can be found in the [docs](../index.html).
