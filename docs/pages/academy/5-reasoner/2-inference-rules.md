---
title: Inference rules
keywords: setup, getting started
last_updated: April 2018
summary: In this lesson you will learn how to build inference rules to turn your data into knowledge
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/inference-rules.html
folder: academy
toc: false
KB: academy
---

As anticipated in the [last lesson](./reasoner-intro.html), in this module you are going to learn how to use the Grakn Reasoner to make your data more intelligent.

Among others, the two most common uses for Reasoner are:
Knowledge discovery: as in the deserted island example you have read about in the last lesson, we can use logic inference to extract more knowledge out of our data without modifying it.
Query shortening: sometimes you use the same long query over and over again. It is convenient in those cases to use reasoner to simply create new "virtual" relationship that will make running those same queries in a more succint way.
In this and the following lesson, we will build an example that will demonstrate both these points.

## Structure of inference rules
A rule in Grakn is, roughly speaking, an insert query that does not actually insert data. It rather modies the results of the get queries.

Since rules add to the knowledge model of your knowledge graph, they are actually part of the schema, and, as such, are added using the keyword `define`, like all the schema components.

From a syntax point of view, to define a new rule, you will need something like this added to your schema:

```
define
RULE_LABEL

  when {
        PRECONDITIONS
      }

  then {
        CONSEQUENCES
      }
```

The rule label is just a unique shorthand that you use to refer to the specific concept in the schema, like the names of the types and roles that you have used during [module 3](./schema-elements.html).

The first block of the rule, the WHEN part or block, is just a list of patterns and works exactly like the `match` part of a normal query; the THEN part is a bit more restrictive: you can only use variables that have been defined in the when part and you can only have at most one single `isa` pattern and one single `has` pattern. This form is called an _atomic pattern_.

There are quite deep theoretical reasons for these limitations but they are out of the scope of the academy, we will briefly come back to this topic in the next lesson.

If you define a rule, each time you run a query, Grakn

  * checks whether you are accessing part of the Knowledge base described by the _WHEN_ block of the rule,
  * and it responds as if the _THEN_ block were satisfied as well

As I said before: the WHEN and THEN blocks are roughly analogous to the match and insert parts of an insert query, except for the fact that no new data is stored in your knowledge graph.


### ASIDE: Common terminology.

There are several names for the when and then blocks of an inference rule. If you are coming from a programming background you might think of a rule as an If … Then statement. If you are familiar with logic and Horn clauses, you might want to call the blocks body and head respectively or left-hand-side and right-hand-side if you are more into mathematics. It doesn’t really matter how you call them as long as you know what you are referring to. During the course of the lessons, I will keep referring to them as when and then blocks for coherence, but feel free to use your favourite terminology.


## Your first rule
Enough talking, it is time to write some rules. Our objective is to build a rule that links the articles about the Italian Referendum to the bonds issued by companies that own affected oil platforms (review the [topic](./graql-intro.html#dataset-background) if you forgot how we came to this question).

We already know that we can query the knowledge graph with the following graql statement:

```graql
match
$article isa article has subject "Italian Referendum";
$platform isa oil-platform has distance-from-coast <= 18;
(location: $country, located: $platform) isa located-in;
$country isa country has name "Italy";
(owner: $company, owned: $platform) isa owns;
(issuer: $company, issued: $bond) isa issues;
get $bond, $article;
```

But the resulting articles and bonds are disconnected.

If you look carefully at the schema of the knowledge graph (little reminder: you can review your current schema by pressing the "All Types" button in the graph visualiser), you will notice that there is an "affects" relationship type that is not used anywhere. The reason that relationship has been added to the schema is to allow you to connect the articles and the bonds.

If you wanted to permanently add the relationships, you would use a match insert with the same match as the get query above and insert an `affects` relationship between `$article` and `$bond`.

Using the information above, try and build a rule that does just that without storing the relationship back in the knowledge graph. Remember that you don’t have to use the `match` and `insert` keywords in the when and then blocks.

Save your result in a file called, for example. `rules.gql` and store it for future reference. You will find the solution to this exercise in the next lesson.

### What have you learned?
You now should be able to write Grakn inference rules and know how a rule is structured. You are almost done building the example knowledge graph!

## What next?
In the [next lesson](./advanced-rules.html) you will discover how to improve the rule you just wrote, make it more useful, and will learn more about how  the reasoner works. If you want to know more about atomic patterns and what you can actually use in a then block, head over to the [docs](../index.html).
