---
title: Quickstart Tutorial
keywords: setup, getting started
last_updated: February 2017
tags: [getting-started, graql]
summary: "This document will work through a simple example using the Graql shell to show how to get started with GRAKN.AI."
sidebar: documentation_sidebar
permalink: /documentation/get-started/quickstart-tutorial.html
folder: documentation
comment_issue_id: 17
---

## Summary

This example takes a simple genealogy dataset and briefly reviews its ontology, then illustrates how to query, extend and visualise the graph, before demonstrating reasoning and analytics with Graql.  

## Introduction 

If you have not yet set up GRAKN.AI, please see the [Setup guide](../get-started/setup-guide.html). In this tutorial, we will load a simple ontology and some data from a file, *basic-genealogy.gql* and use the Graql shell and Grakn visualiser to illustrate some key features of GRAKN.AI.
 
## The Graql Shell

The first few steps mirror those in the [Setup Guide](./setup-guide.html), and you can skip to [The Ontology](#the-ontology) if you have already run through that example. Start Grakn and load the example graph:

```bash
./bin/grakn.sh start
./bin/graql.sh -f ./examples/basic-genealogy.gql
```

{% include note.html content="Above, we are invoking the Graql shell and passing the -f flag to indicate the file to load into a graph. This starts the Graql shell in non-interactive mode, loading the specified file and exiting after the load is complete.
If you are interested, please see our documentation about other [flags supported by the Graql shell](https://grakn.ai/pages/documentation/graql/graql-shell.html)." %}

Then start the Graql shell in its interactive (REPL) mode:

```bash
./bin/graql.sh
```

You will see a `>>>` prompt. Type in a query to check that everything is working: 

```graql   
match $x isa person, has identifier $n;
```

You should see a printout of a number of lines of text, each of which includes a name, such as "William Sanford Titus" or "Elizabeth Niesz".

### The Ontology

You can find out much more about the Grakn ontology in our documentation about the [Grakn knowledge model](../the-fundamentals/grakn-knowledge-model.html), which states that

> "The ontology is a formal specification of all the relevant concepts and their meaningful associations in a given application domain. It allows objects and relationships to be categorised into distinct types, and for generic properties about those types to be expressed". 

For the purposes of this guide, you can think of the ontology as a schema that describes items of data and defines how they relate to one another. You need to have a basic understanding of the ontology to be able to make useful queries on the data, so let's review the chunks of it that are important for our initial demonstration:

```graql
insert

# Entities

person sub entity
  plays parent
  plays child
  plays spouse1
  plays spouse2

  has identifier
  has firstname
  has surname
  has middlename
  has picture
  has age
  has birth-date
  has death-date
  has gender;

# Resources

identifier sub resource datatype string;
name sub resource datatype string;
firstname sub name datatype string;
surname sub name datatype string;
middlename sub name datatype string;
picture sub resource datatype string;
age sub resource datatype long;
"date" sub resource datatype string;
birth-date sub "date" datatype string;
death-date sub "date" datatype string;
gender sub resource datatype string;

# Roles and Relations

marriage sub relation
  relates spouse1
  relates spouse2
  has picture;

spouse1 sub role;
spouse2 sub role;

parentship sub relation
  relates parent
  relates child;

parent sub role;
child sub role;
```

There are a number of things we can say about ontology shown above:

* there is one entity, `person`, which represents a person in the family whose genealogy data we are studying. 
* the `person` entity has a number of resources to describe aspects of them, such as their name, age, dates of birth and death, gender and a URL to a picture of them (if one exists). Those resources are all expressed as strings, except for the age, which is of datatype long.
* there are two relations that a `person` can participate in: `marriage` and `parentship`
* the person can play different roles in those relations, as a spouse (`spouse1` or `spouse2` - we aren't assigning them by gender to be husband or wife) and as a `parent` or `child` (again, we are not assigning a gender such as mother or father).   
* the `marriage` relation has a resource, which is a URL to a wedding picture, if one exists. 

### The Data

The data is rather cumbersome, so we will not reproduce it all here. It is part of our [genealogy-graph](https://github.com/graknlabs/sample-datasets/tree/master/genealogy-graph) project, and you can find out much more about the Niesz family in our [CSV migration](../examples/CSV-migration.html) and [Graql reasoning](../examples/graql-reasoning.html) example documentation. Here is a snippet of some of the data that you added to the graph when you loaded the *basic-genealogy.gql* file:

```
$57472 isa person has firstname "Mary" has identifier "Mary Guthrie" has surname "Guthrie" has gender "female";
$86144 has surname "Dudley" isa person has identifier "Susan Josephine Dudley" has gender "female" has firstname "Susan" has middlename "Josephine";
$118912 has age 74 isa person has firstname "Margaret" has surname "Newman" has gender "female" has identifier "Margaret Newman";
...
$8304 (parent: $57472, child: $41324624) isa parentship;
$24816 (parent: $81976, child: $41096) isa parentship;
$37104 isa parentship (parent: $49344, child: $41127960);
...
$122884216 (spouse2: $57472, spouse1: $41406488) isa marriage;
$40972456 (spouse2: $40964120, spouse1: $8248) isa marriage;
$81940536 (spouse2: $233568, spouse1: $41361488) has picture "http:\/\/1.bp.blogspot.com\/-Ty9Ox8v7LUw\/VKoGzIlsMII\/AAAAAAAAAZw\/UtkUvrujvBQ\/s1600\/johnandmary.jpg" isa marriage;
```

Don't worry about the numbers such as `$57472`. These are variables in Graql, and happen to have randomly assigned numbers to make them unique. Each statement is adding either a `person`, a `parentship` or a `marriage` to the graph.  We will show how to add more data to the graph shortly in the [Extending The Graph](#extending-the-graph) section. First, however, it is time to query the graph in the Graql shell. 

## Querying the Graph

Having started Grakn engine and the Graql shell in its interactive mode, we are ready to make a number queries. First, we will make a couple of `match` queries.

Find all the people in the graph, and list their `identifier` resources (a string that represents their full name):

```graql
match $p isa person, has identifier $i;
```

{% include note.html content="In queries, Graql variables start with a `$`, which represent wildcards, and are returned as results in `match` queries. A variable name can contain alphanumeric characters, dashes and underscores." %}

Find all the people who are married:

```graql
match (spouse1: $x, spouse2: $y) isa marriage; $x has identifier $xi; $y has identifier $yi;  
```

List parent-child relations with the names of each person:

```graql
match (parent: $p, child: $c) isa parentship; $p has identifier $pi; $c has identifier $ci; 
```

Find all the people who are named 'Elizabeth':

```graql
match $x isa person, has identifier $y; $y val contains "Elizabeth";
```

Querying the graph is more fully described in the [Graql documentation](../graql/graql-overview.html).

## Extending the Graph

Besides making `match` queries, it is also possible to `insert` items [(see further documentation)](../graql/insert-queries.html) and `delete` items [(see further documentation)](../graql/delete-queries.html) through the Graql shell. To illustrate inserting a fictional person:

```graql
insert $g isa person has firstname "Titus" has identifier "Titus Groan" has surname "Groan" has gender "male";
commit
```

{% include note.html content="<b>Don't forget to `commit`!</b> <br /> Nothing you have entered into the Graql shell has yet been committed to the graph, nor has it been validated. To save any changes you make to a graph, you need to type `commit` in the shell. It is a good habit to get into regularly committing what you have entered." %}

To find your inserted `person`:

```graql
match $x isa person has identifier "Titus Groan"; 
```

To delete the `person` again:

```graql
match $x isa person has identifier "Titus Groan"; delete $x;
commit
```

Alternatively, we can use `match...insert` syntax, to insert additional data associated with something already in the graph. Adding some fictional information (middle name, birth date, death date and age at death) for one of our family, Mary Guthrie:

```graql
match $p has identifier "Mary Guthrie"; insert $p has middlename "Mathilda"; $p has birth-date "1902-01-01"; $p has death-date "1952-01-01"; $p has age 50;
commit
```

## Using the Grakn Visualiser

You can open the [Grakn visualiser](../grakn-dashboard/visualiser.html) by navigating to [localhost:4567](http://localhost:4567) in your web browser. The visualiser allows you to make queries or simply browse the knowledge ontology within the graph. The screenshot below shows a basic query (`match $x isa person; offset 0; limit 100;`) typed into the form at the top of the main pane, and visualised by pressing ">":

![Person query](/images/match-$x-isa-person.png)

You can zoom the display in and out, and move the nodes around for better visibility. Please see our [Grakn visualiser](../grakn-dashboard/visualiser.html) documentation for further details.


## Using Inference

We will move on to discuss the use of GRAKN.AI to infer new information about a dataset. In the ontology, so far, we have dealt only with a person, not a man or woman, and the parentship relations were simply between parent and child roles. We did not directly add information about the nature of the parent and child in each relation - they could be father and son, father and daughter, mother and son or mother and daughter.

However, the `person` entity does have a gender resource, and we can use Grakn to infer more information about each relationship by using that property. The ontology accommodates the more specific roles of mother, father, daughter and son:

```graql
insert

person 
  plays son
  plays daughter
  plays mother
  plays father;
	
parentship sub relation
  relates mother
  relates father
  relates son
  relates daughter;

mother sub parent;
father sub parent;
son sub child;
daughter sub child;
```

{% include note.html content="You don't need to reload the *basic-genealogy.gql* file into Grakn pick up these extra roles. We simply didn't show this part in our earlier discussion of the ontology, to keep things as simple as possible." %}

Included in *basic-genealogy.gql* are a set of Graql rules to instruct Grakn's reasoner on how to label each parentship relation:

```graql
insert

$genderizeParentships1 isa inference-rule
when
{(parent: $p, child: $c) isa parentship;
$p has gender "male";
$c has gender "male";
}
then
{(father: $p, son: $c) isa parentship;};

$genderizeParentships2 isa inference-rule
when
{(parent: $p, child: $c) isa parentship;
$p has gender "male";
$c has gender "female";
}
then
{(father: $p, daughter: $c) isa parentship;};

$genderizeParentships3 isa inference-rule
when
{(parent: $p, child: $c) isa parentship;
$p has gender "female";
$c has gender "male";
}
then
{(mother: $p, son: $c) isa parentship;};

$genderizeParentships4 isa inference-rule
when
{(parent: $p, child: $c) isa parentship;
$p has gender "female";
$c has gender "female";
}
then
{(mother: $p, daughter: $c) isa parentship;};
```

If you're unfamiliar with the syntax of rules, don't worry too much about it too much just now. It is sufficient to know that, for each `parentship` relation, Graql checks whether the pattern in the first block (when) can be verified and, if it can, infers the statement in the second block (then) to be true, so inserts a relation between gendered parents and children.

Let's test it out!

First, try making a match query to find `parentship` relations between fathers and sons in the Graql shell:

```graql
match (father: $p, son: $c) isa parentship; $p has identifier $n1; $c has identifier $n2;
```

Did you get any results? Probably not, because reasoning is not enabled by default at present, although as Grakn develops, we expect that to change. If you didn't see any results, you need to `exit` the Graql shell and restart it, passing `-n` and `-m` flags to switch on reasoning (see our documentation for more information about [flags supported by the Graql shell](https://grakn.ai/pages/documentation/graql/graql-shell.html)).

```bash
./bin/graql.sh -n -m
```

Try the query again:

```graql
match (father: $p, son: $c) isa parentship; $p has identifier $n1; $c has identifier $n2;
```

There may be a pause, and then you should see a stream of results as Grakn infers the `parentships` between male `parent` and `child` entities. It is, in effect, building new information about the family which was not explicit in the dataset.

You may want to take a look at the results of this query in the Grakn visualiser and, as for the shell, you will need to activate inference before you see any results. 

1.	Browse to the visualiser at [localhost:4567](http://localhost:4567). 
2. Open the Query settings under the cog button, which is on the far right hand side of the horizontal icon menu (at the top of the screen).
3. You will see the "Activate inference" checkbox. Ensure that it is checked.

Now try submitting the query above or a variation of it for mothers and sons, fathers and daughters etc. Or, you can even go one step further and find out fathers who have the same name as their sons:

```graql
match (father: $p, son: $c) isa parentship; $p has firstname $n; $c has firstname $n;
```

![Father-Son Shared Names query](/images/father-son-shared-names.png)

If you want to find out more about the Graql reasoner, we have a [detailed example](../examples/graql-reasoning.html). An additional discussion on the same topic can be found in our ["Family Matters" blog post](https://blog.grakn.ai/family-matters-1bb639396a24#.525ozq2zy).

## Using Analytics

Turning to [Graql analytics](../graql-analytics/analytics-overview.html), we can illustrate some basic queries in the Grakn visualiser.

### Statistics
The mean age at death can be calculated using `compute mean` as follows, entering it into the visualiser's query form:

```graql
compute mean of age in person; # returns 78.23 (rounded to 2 decimal places)
```

Other statistical values can be calculated similarly, e.g. values for `count`:

```graql
compute count in person; # 60
```

A full list of statistics that can be explored is documented in the [Compute Queries](../graql/compute-queries.html) documentation.

### Shortest Path

It is also possible to find the shortest path between two nodes in the graph. The documentation for the Grakn visualiser describes how to use the [query builder tool](../grakn-dashboard/visualiser.html#analytics-queries---shortest-path), and includes a video.

In brief, let's select two people from the genealogy dataset:

```graql
match $x has identifier "Barbara Shafner"; $y has identifier "Jacob J. Niesz";
```

and then search for relationships joining two of them using:

<!-- Ignoring because uses fake IDs -->
```graql-test-ignore
compute path from "id1" to "id2"; # Use the actual values of identifier for each person
# e.g. compute path from "114848" to "348264";
```

You can see below that the two people selected are married.

The path query uses a scalable shortest path algorithm to determine the smallest number of relations required to get from once concept to the other.

![Shortest path between people](/images/analytics_path_marriage.png)

To narrow the path to specific relations between specific entities:

<!-- Ignoring because uses fake IDs -->
```graql-test-ignore
compute path from "id1" to "id2" in person, parentship;
```

The above limits the path to blood relations (parent/child relations) thus excludes marriage. As a result, the shortest path between the two people is now longer: Barbara Shafner and Jacob J. Niesz are cousins (their mothers, Mary Young and Catherine Young, are sisters, with *their* father being Jacob Young).

![Shortest path between people](/images/analytics_path_parentship.png)

## Data Migration

In this example we loaded data from *basic-genealogy.gql* directly into a graph. However, data isn't often conveniently stored in .gql files and, indeed, the data that we used was originally in CSV format. Our [CSV migration example](../examples/CSV-migration.html) explains in detail the steps we took to migrate the CSV data into Grakn. 

Migrating data in formats such as CSV, SQL, OWL and JSON into Grakn is a key use case. More information about each of these can be found in the [migration documentation](../migration/migration-overview.html).

## Where Next?

This page was a very high-level overview of some of the key use cases for Grakn, and has hardly touched the surface or gone into detail. The rest of our developer documentation and examples are more in-depth and should answer any questions that you may have, but if you need extra information, please [get in touch](https://grakn.ai/community.html).

A good place to start is to explore our additional [example code](../examples/examples-overview.html) and the documentation for:
 
* The [Grakn knowledge model](../the-fundamentals/grakn-knowledge-model.html)
* [Graql](../graql/graql-overview.html), including reasoning
* [Migration](../migration/migration-overview.html)
* [Analytics](../graql-analytics/analytics-overview.html) 
* [Grakn's Java APIs](https://grakn.ai/javadocs.html). 

{% include links.html %}

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/17" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

