---
title: GRAQL - the language of knowledge
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you will learn about the dataset you will be dealing with in the Academy and will get a first taste of the GRAQL language
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/graql-intro.html
folder: overview
toc: false
KB: academy
---

During the course of the Academy lessons, you will learn step by step how to reproduce the academy dataset that comes preloaded into the GRAKN distribution in the VM that you should have installe by now (if you have not, head back to [last lesson](./setup.html) and make sure you do so.)

In order to understand the dataset, let's first set up the scene.


## Dataset Background

Imagine you are company that gathers information about financial products issued by oil companies. You have your knowledge graph stored with GRAKN and you have employees who browse the internet for interesting articles and add them to the knowledge graph.

In 2016, a referendum in Italy has been held about the renovation of concessions on oil platforms within 18 kilometers from the coast. Had the referendum succeeded, this would have potentially affected the companies owning those platforms, and, indirectly, the financial products issued by those companies. So your employees start adding articles related to the referendum into the knowledge graph.


## The Big Question(s)

If you have some experience in Data Science you know that when you are trying to solve a problem (in this case building the dataset for the above scenario) it is always a good idea to start with one or more focused questions that will guide you through the process. Given the background above, the questions (that will return several times in this series of articles) are:

> What bond might be affected by the Italian referendum?

Or, more explicitly:

> What are the bonds issued by companies that own oil platforms located in Italy less than 18 kilometers from the coast? Those bonds should be connected to articles related to the Italian referendum.

Just to whet your appetite, here is how that question looks like in GRAQL. Don’t worry about getting it right now, there is no rush. You will be able to very soon.

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

### Try it yourself

Start GRAKN (if it’s not already running), then open the dashboard. Set the default limit to something small (I suggest 2 or 3), then try running that query in the dashboard to see how the answer looks like.

If you click and hold one of the nodes that appear you will see a box appear on your screen. You can there select the colours and displayed information for that type of nodes.

  ![GRAQL query result](/images/academy/2-graql/Big-Query.png)

Clear the graph with Shift+click on the (x) as you learned in last lesson and you are able to go on.


## The structure of a GRAQL query

Let’s explore how a GRAQL query looks like in general.

With some exception, GRAQL queries are made of three parts:

  1. A _match portion_, which reads the parts of the knowledge graph you are interested into. It is indicated by the _keyword_ match followed by a list of _patterns_ separated by semicolons (highlighted in the picture below), that actually specify what part of the knowledge graph to isolate. Notice that differently from what normally happens with query languages, **the order of patterns in a GRAQL query does not matter**: the system will take care of putting it in the correct order and execute the query in the most efficient way it can.

  1. One or more optional _modifiers._ This specify things about the result, like the order in which they should be displayed, the number of results you want  etc.

  1. A mandatory _action_ that actually specifies what the query does. With some exceptions, this take the form of a keyword followed by variables. The action defines the type of the query as well (`get`, `insert`, `delete`...)


This is how this structure looks like in our example query:

  ![Big Query structure](/images/academy/2-graql/query-structure.png)


### What have you learned?

By now, you should be familiar the background of the demo dataset we will be working with and the question we will be trying to answer. You should have had a first feel at how a GRAQL query looks like, and you should know how a GRAQL query is structured.


## What next?

In the [next lesson](./get-queries.html) we will dive more deeply into the syntax for get and insert queries. If you are curious about all the possible GRAQL query and you want to go faster, you can have a look at the GRAQL syntax [documentation](/index.html), but it might be a bit too advanced at this point in time.
