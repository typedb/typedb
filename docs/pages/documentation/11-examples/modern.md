---
title: Modern Example in Graql
keywords: graql, query
last_updated: September 19, 2016
tags: [graql, examples]
summary: "A short example to illustrate Graql queries"
sidebar: documentation_sidebar
permalink: /documentation/examples/modern.html
folder: documentation
comment_issue_id: 27
---

If you have not yet set up the Grakn environment, please see the [Setup guide](../get-started/setup-guide.html). For a comprehensive guide to all Graql keywords, please see the [Graql documentation](https://grakn.ai/pages/documentation/graql/graql-overview.html).

## Introduction
We have a few examples on how to work with Graql using different datasets. For example, see our [blog posts](https://medium.com/p/e1125e02dc85) and the [Quickstart Tutorial](../get-started/quickstart-tutorial.html) that introduces how to make Graql queries.

This example takes a very simple example from [TinkerPop3 Documentation](http://tinkerpop.apache.org/docs/3.0.1-incubating/), called TinkerPop Modern. Here it is, shown diagrammatically:

![](/images/example-tinkerpop-modern.png)

The image above is used from the documentation provided for TinkerPop3, and licensed by the [Apache Software Foundation](http://www.apache.org). 

We have chosen this example as it may already be familiar, and is simple enough to demonstrate some of the fundamentals of Graql. We walk through the entities ("things") and relationships between them and show how to represent them using Graql to define a schema. We then use Graql to add the data to the knowledge base.  The main purpose of this example, however, is to use it for practice at making sample queries on the graph. 

### Starting Graql

If it is not already running, start Grakn, then open a Graql shell:

```bash
cd [your Grakn install directory]
bin/grakn.sh start
bin/graql.sh
```

## Defining a Schema

In the example, we have a number of entities: 

* people (marko, vadas, josh and peter), each of which has an age.    
* software (lop and ripple), each of which has an associated programming language. 

The relationships between the entities are straightforward: 

* there are two connections between marko and other people who he "knows" (josh and vadas). 
* there are four connections between people and software, such that the people "created" the software. Lop was created by marko, peter and josh, which ripple was created by josh.

### Entities

Here, we add person and software entities, via the Graql shell:

```graql
insert person sub entity;
insert software sub entity;
```


### Resources

To assign resources to the entities, which you can think of as attributes, we use resources. First, we define what they are (age is a number, programming language is a string that represents the language's name), then we allocate them to the entity in question:

```graql
insert age sub attribute datatype long;
insert name sub attribute datatype string;
insert person has age, has name;

insert lang sub attribute datatype string;
insert software has lang has name;

insert weight sub attribute datatype double;
```

### Relations

Let's first define the relationship between people. The diagram shows that marko knows vadas, but we don't have any information about whether the inverse is true (though it seems likely that vadas probably also knows marko). Let's set up a relationship called `knows`, which has two roles - `knower` (for marko) and `known-about` (for vadas):

```graql
insert knower sub role;
insert known-about sub role;
insert person plays knower;
insert person plays known-about;
insert knows sub relation, relates knower, relates known-about, has weight;
```

Note that the  `knows` relation also has an attribute, in the form of an attribute called `weight` (though it's not clear from the TinkerPop example what this represents).

We can set up a similar relationship between software and the people that created it:

```graql
insert programmer sub role;
insert programmed sub role;

insert person plays programmer;
insert software plays programmed;

insert programming sub relation, relates programmer, relates programmed, has weight;
```

And that's it. At this point, we have defined the schema of the knowledge base.

## Adding the Data
Now we have a schema, we can move on to adding in the data, which is pretty much just a typing exercise:

```graql
insert $marko isa person, has name "marko", has age 29;
insert $vadas isa person, has name "vadas", has age 27;
insert $josh isa person, has name "josh", has age 32;
insert $peter isa person, has name "peter", has age 35;
match $marko has name "marko"; $josh has name "josh"; insert (knower: $marko, known-about: $josh) isa knows has weight 1.0;
match $marko has name "marko"; $vadas has name "vadas"; insert (knower: $marko, known-about: $vadas) isa knows has weight 0.5;
```


```graql
insert $lop isa software, has lang "java", has name "lop";
insert $ripple isa software, has lang "java", has name "ripple";

match $marko has name "marko"; $lop has name "lop"; insert (programmer: $marko, programmed: $lop) isa programming has weight 0.4;
match $peter has name "peter"; $lop has name "lop"; insert (programmer: $peter, programmed: $lop) isa programming has weight 0.2;
match $josh has name "josh"; $lop has name "lop"; insert (programmer: $josh, programmed: $lop) isa programming has weight 0.4;
match $josh has name "josh"; $ripple has name "ripple"; insert (programmer: $josh, programmed: $ripple) isa programming has weight 1.0;
```
   
   
## Querying

This example is designed to get you up close and personal with Graql queries. It will run through a few basic examples, then ask you a set of "Test Yourself" questions. 

OK, so if you've followed the above, you should now have a schema and some data in a knowledge base. How do you go about using the graph to answer your queries? That's where the `match` statement comes in. 

As with any query language, you use a variable to receive the results of the match query, which you must prefix with a `$`. So, to make the query "List every person in the knowledge base", you would use the following in Graql:

```graql
>>> match $x isa person, has name $n; select $n;

$n val "vadas" isa name;
$n val "marko" isa name;
$n val "josh" isa name;
$n val "peter" isa name;
```
 
In Graql, a match is formed of three parts: the `match` statement, an optional `select` statement, and any other optional [modifiers](../graql/match-queries.html#modifiers) that you choose to apply to the listing of results. Only the first part of a match query is needed: the modifier parts are optional.   

In the `match $x isa person` query we are not using any select or delimiters, so let's add some now.  We can add a `select` statement to ask Graql to list out every person and to include their id (which is their name) and age. We use `order by` to modify how the results are listed out - in this case, we order them by ascending age, so the youngest person is shown first.

```graql
>>> match $x isa person, has name $n, has age $a; select $n, $a; order by $a asc;

$x id "vadas" has age "27"; 
$x id "marko" has age "29"; 
$x id "josh" has age "32"; 
$x id "peter" has age "35";
```

## Complete Example
Here is the complete example - the code to define the schema and insert the data into a knowledge base. You can load this directly into Graql, if you don't want to type it out for yourself. Cut and paste the Graql below and start Graql:

```bash
bin/graql.sh
```

Then type edit, which will open up the systems default text editor where you can paste your chunk of text. Upon exiting the editor, the Graql will execute.

```graql 
insert 
age sub attribute datatype long;
name sub attribute datatype string;
person sub entity;
person has age, has name;

$marko isa person;
$vadas isa person;
$josh isa person;
$peter isa person;
$marko has age 29, has name "marko";
$josh has age 32, has name "josh";
$vadas has age 27, has name "vadas";
$peter has age 35, has name "peter";

weight sub attribute datatype double;

knower sub role;
known-about sub role;

person plays knower;
person plays known-about;

knows sub relation
	relates knower
	relates known-about
	has weight;

(knower: $marko, known-about: $josh) isa knows has weight 1.0;
(knower: $marko, known-about: $vadas) isa knows has weight 0.5;

lang sub attribute datatype string;
software sub entity;
software has lang, has name;

$lop isa software;
$ripple isa software;

$lop has lang "java", has name "lop";
$ripple has lang "java", has name "ripple";

programmer sub role;
programmed sub role;

person plays programmer;
software plays programmed;

programming sub relation
	relates programmer
	relates programmed
	has weight;


(programmer: $marko, programmed: $lop) isa programming has weight 0.4;
(programmer: $peter, programmed: $lop) isa programming has weight 0.2;
(programmer: $josh, programmed: $lop) isa programming has weight 0.4;
(programmer: $josh, programmed: $ripple) isa programming has weight 1.0;
```   


## Test Yourself

1. List every person with their name and age in ascending age order


2. List every person who has an age over 30


3. List every person who knows someone else, and every person who is known about


4. List every person that Marko knows


5. List every item of software and the language associated with it


6. List everything that Josh has programmed


7. List everyone who has programmed Lop


8. List everything you know about Marko



{% include links.html %}


## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/27" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

