---
title: Graql Overview
keywords: graql, overview
last_updated: March 2017
tags: [graql]
summary: "An introduction to Graql"
sidebar: documentation_sidebar
permalink: /documentation/graql/graql-overview.html
folder: documentation
---

Graql enables users to write queries against a Grakn knowledge base leveraging the inherent semantics of the data. Concepts can be retrieved by specifying the patterns of types and relationships that identify them. Graql is declarative and therefore it handles the optimisation of the graph traversals needed to retrieve information.

You can execute Graql in the [Graql Shell](graql-shell.html) or using [Java
Graql](../developing-with-java/java-graql.html).

## Query types

There are five types of queries, which are begun with the following keywords:  
- [match](match-queries.html) - for matching patterns in the knowledge base  
- [ask](ask-queries.html) - for querying if certain patterns exist in the knowledge base  
- [insert](insert-queries.html) - for inserting types and instances  
- [delete](delete-queries.html) - for deleting types and instances    
- [compute](compute-queries.html) - for computing useful information about your knowledge base

## Reserved keywords

The following list Graql's reserved keywords:

#### Querying and query modifiers

```graql-test-ignore
aggregate, asc, ask
by
compute, contains
delete, desc, distinct
from
id, in, insert
label, limit
match
offset, order
regex
select
to
val
```

#### Datatypes

```graql-test-ignore
datatype
boolean, double, long, string, date
true, false
```

#### Ontology definition

```graql-test-ignore
has, has-scope, 
is-abstract, isa, 
key,
plays,
relates
```

#### Rules definition

```graql-test-ignore
when, then
```

#### Statistics 
Used with `compute` and `aggregate`:

```graql-test-ignore
count
group
max, mean, median, min
std, sum
```

#### Graql templates

```graql-template
and
concat
do
else, elseif
for
if, in
noescp, not, null
true, false
```

## Cheatsheet reference
If you are already familiar with Graql, you may find our [cheatsheet reference](graql-cheatsheet.html) a helpful page to bookmark or print out!

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/42" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.


{% include links.html %}

