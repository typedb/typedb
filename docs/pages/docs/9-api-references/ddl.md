---
title: Data Definition Language
keywords: graql, query, data definition language
tags: [graql]
summary: "Data Definition Language"
sidebar: documentation_sidebar
permalink: /docs/api-references/ddl
folder: docs
---

# Define Query

`define` [`<variable patterns>`](../querying-data/overview#variable-pattern)

The [define query](#define-query) will add the given [variable patterns](../querying-data/overview#variable-pattern) to the schema.

After execution, it will return a single [answer](../querying-data/overview#answer) containing bindings for all
[variables](../querying-data/overview#variable) mentioned in the [variable patterns](../querying-data/overview#variable-pattern).

# Undefine Query

`undefine` [`<variable patterns>`](../querying-data/overview#variable-pattern)

The [undefine query](#undefine-query) will remove the given [variable patterns](../querying-data/overview#variable-pattern) from the
schema.

In order to remove a schema concept entirely, it is sufficient to undefine its [direct super-concept](#sub):

<!-- Ignored so we don't delete the person type for real -->
```graql-test-ignore
undefine person sub entity;
```

# Supported Properties

Within the [variable patterns](../querying-data/overview#variable-pattern) of both [define](#define-query) and
[undefine](#undefine-query) queries, the following [properties](../querying-data/overview#property) are supported:

## id

`$x id <identifier>` will assign `$x` to an existing _concept_ with the given ID. It is an error if no such concept
exists.

## label

`$A label <identifier>` will assign `$A` to a _schema concept_ with the given label. If no such schema concept exists,
one will be created.

## sub

`$A sub $B` defines the _schema concept_ `$A` as the direct sub-concept of the _schema concept_ `$B`.

## relates

`$A relates $B` define the _relationship type_ `$A` to directly relate the _role_ `$B`.

In the case where `$B` does not have a defined `sub`, this will also implicit define `$B sub role`.

## plays

`$A plays $B` defines the _type_ `$A` to directly play the _role_ `$B`.

## has (type)

`$A has <identifier>` defines instances of the _type_ `$A` to have attributes of the _attribute type_ `<identifier>`.

> `has` is syntactic sugar for a particular kind of relationship.
>
> This is done using the following relationship structure:
> ```graql-test-ignore
> has-<identifier>-owner sub has-<sup>-owner;
> has-<identifier>-value sub has-<sup>-value;
> has-<identifier> sub has-<sup>, relates has-<identifier>-owner, relates has-<identifier>-value;
>
> $A plays has-<identifier>-owner;
> <identifier> plays has-<identifier>-value;
> ```
> Where `<sup>` is the direct super-concept of `<identifier>`.

## key (type)

`$A key <identifier>` defines instances of the _type_ `$A` to have a key of _attribute type_ `<identifier>`.

> `key` is syntactic sugar for a particular kind of relationship.
>
> This is done using the following relationship structure:
> ```graql-test-ignore
> key-<identifier>-owner sub key-<sup>-owner;
> key-<identifier>-value sub key-<sup>-value;
> key-<identifier> sub key-<sup>, relates key-<identifier>-owner, relates key-<identifier>-value;
>
> $A plays<<required>> has-<identifier>-owner;
> <identifier> plays has-<identifier>-value;
> ```
> Where `<sup>` is the direct super-concept of `<identifier>`.
> <!-- TODO: This is pretty bad -->
> (note that `plays<<required>>` is not valid syntax, but indicates that instances of the type _must_ play the required
> role exactly once).

## datatype

`$A datatype <datatype>` defines the _attribute type_ `$A` to have the specified
[datatype](../querying-data/overview#value).

## regex

`$A regex <regex>` defines the _attribute type_ `$A` to have the specified regex constraint.

## is-abstract

`$A is-abstract` defines the _type_ `$A` to be abstract.

## when

`$A when <pattern>` defines the _rule_ `$A` to have the specified `when` [pattern](./dml.html#pattern).

## then

`$A then <pattern>` defines the _rule_ `$A` to have the specified `then` [pattern](./dml.html#pattern).
