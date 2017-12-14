---
title: Graql Overview
keywords: graql, overview
tags: [graql]
summary: "An introduction to Graql"
sidebar: documentation_sidebar
permalink: /docs/querying-data/overview
folder: docs
---

Graql enables users to write queries against a Grakn knowledge base leveraging the inherent semantics of the data.
Concepts can be retrieved by specifying the patterns of types and relationships that identify them. Graql is declarative
and therefore it handles the optimisation of the knowledge base queries needed to retrieve information.

You can execute Graql in the [Graql Console](../get-started/graql-console) or using [Java
Graql](../java-library/graql-api).


# Query

[Queries](#query) are requests made to the knowledge base. They may modify the schema, such as
[define](../building-schema/defining-schema), or the data, such as [insert](./insert-queries) or be read-only, such as
[get](./get-queries).

A [query](#query) is comprised of an action (such as `get` or `delete`) with arguments, and preceded by a
[match](./match-clause). Examples of queries are [define](../building-schema/defining-schema),
[undefine](../building-schema/defining-schema), [get](./get-queries), [insert](./insert-queries) and
[delete](./delete-queries).

# Answer

Several [queries](#query) return [answers](#answer). An [answer](#answer) is a map from [variables](#variable) to
concepts. In general, [answers](#answer) will contain all [variables](#variable) mentioned in the
[variable patterns](#variable-pattern) of the [query](#query).


# Variable

A [variable](#variable) is an identifier prefixed with a dollar `$`. Valid identifiers must comprise of one or
more alphanumeric characters, dashes and underscores.


# Variable pattern

[`<variable>`](#variable) [`<property>, ...`](#property) `;`

A [variable pattern](#variable-pattern) is a [variable](#variable) (the _subject_ of the pattern) followed by zero or
more [properties](#property) (optionally separated by commas `,`) and ending in a semicolon `;`.

## Property

A [property](#property) is part of a [variable pattern](#variable-pattern). The [property](#property) describes
something about the _subject_ of the [variable pattern](#variable-pattern).

As well as the _subject_, [properties](#property) sometimes take other arguments, which can include things such as
[variables](#variable), [values](#value) and labels depending on the kind of [property](#property).

When a [property](#property) takes a [variable](#variable) argument representing a schema concept, it is possible to
substitute a label. For example,
```graql
match $x isa $A; $A label person; get;
```
should be written more succinctly as
```graql
match $x isa person; get;
```

There are several different [properties](#property). Which ones are supported and what they do depends on the context.
For example, [match](./match-clause) supports most [properties](#property), whereas [insert](./insert-queries) only supports a small subset.

## Value

An attribute's [value](#value) is constrained by the datatype of its type:

- `long` - a 64-bit signed integer
- `double` - a double-precision floating point number, including a decimal point.
- `string` - enclosed in double `" "` or single `' '` quotes
- `boolean` - `true` or `false`
- `date` - a date or a date-time, in ISO 8601 format

## Query types

There are seven types of queries, which are begun with the following keywords:
- [get](./get-queries) - for getting concepts from the knowledge base
- [aggregate](./aggregate-queries) - for transforming data in the knowledge base
- [define](../building-schema/defining-schema) - for defining schema concepts
- [undefine](../building-schema/defining-schema) - for removing schema concepts
- [insert](./insert-queries) - for inserting data
- [delete](./delete-queries) - for deleting data
- [compute](../distributed-analytics/compute-queries) - for computing useful information about your knowledge base

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
If you are already familiar with Graql, you may find our [cheatsheet reference](../api-references/graql-cheatsheet) a helpful page to bookmark or print out!
