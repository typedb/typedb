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

Graql enables users to write queries against a Grakn knowledge base leveraging the inherent semantics of the data.
Concepts can be retrieved by specifying the patterns of types and relationships that identify them. Graql is declarative
and therefore it handles the optimisation of the knowledge base queries needed to retrieve information.

You can execute Graql in the [Graql Shell](graql-shell.html) or using [Java
Graql](../developing-with-java/java-graql.html).

## Query types

There are seven types of queries, which are begun with the following keywords:
- [get](dml.html#get-query) - for getting concepts from the knowledge base
- [aggregate](dml.html#aggregate-query) - for transforming data in the knowledge base
- [define](ddl.html#define-query) - for defining schema concepts
- [undefine](ddl.html#undefine-query) - for removing schema concepts
- [insert](dml.html#insert-query) - for inserting data
- [delete](dml.html#delete-query) - for deleting data
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

#### Schema definition

```graql-test-ignore
has,
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

{% include links.html %}

