---
title: Graql Queries
keywords: graql, query
last_updated: October 2017
tags: [graql]
summary: "Graql Queries"
sidebar: documentation_sidebar
permalink: /documentation/graql/query.html
folder: graql
---

# Query

[Queries](#query) are requests made to the knowledge base. They may modify the schema, such as
[define](./ddl.html#define-query), or the data, such as [insert](./dml.html#insert-query) or be read-only, such as
[get](./dml.html#get-query).

A [query](#query) is comprised of an action (such as `get` or `delete`) with arguments, in some cases preceded by a
[match](./dml.html#match). Examples of [queries](#query) are [define](./ddl.html#define-query),
[undefine](./ddl.html#undefine-query), [get](./dml.html#get-query), [insert](./dml.html#insert-query) and
[delete](./dml.html#delete-query).

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
For example, [match](./dml.html#match) supports most [properties](#property), whereas [insert](./dml.html#insert-query)
only supports a small subset.

## Value

An attribute's [value](#value) is constrained by the datatype of its type:

- `long` - a 64-bit signed integer
- `double` - a double-precision floating point number, including a decimal point.
- `string` - enclosed in double `" "` or single `' '` quotes
- `boolean` - `true` or `false`
- `date` - a date or a date-time, in ISO 8601 format
