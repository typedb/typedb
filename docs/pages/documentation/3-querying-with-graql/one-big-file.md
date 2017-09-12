---
title: One Big File (Obviously change this)
keywords: graql, query
last_updated: April 2017
tags: [graql]
summary: "TODO"
sidebar: documentation_sidebar
permalink: /documentation/graql/one-big-file.html
folder: documentation
---

# Query

A [query](#query) is an action, in some cases preceded by a [match](#match). Examples of actions are
[define](#define-query), [undefine](#undefine-query), [get](#get-query), [insert](#insert-query) and
[delete](#delete-query).

# Variable pattern

A [variable pattern](#variable-pattern) is a [variable](#variable) (the _subject_ of the pattern) followed by zero or
more [properties](#property) (optionally separated by commas `,`) and ending in a semicolon `;`.

## Variable

A [variable](#variable) is an identifier prefixed with a dollar `$`. Valid identifiers must comprise of one or
more alphanumeric characters, dashes and underscores.

## Property

There are several different [properties](#property). Which ones are supported and what they do depends on the part of
the query. For example, [match](#match) supports most properties, whereas [insert](#insert) only supports a small
subset.

When a [property](#property) takes a [variable](#variable) representing a schema concept, it is possible to substitute
a label. For example,
```
match $x isa $A; $A label movie; get;
```
should be written more succinctly as
```
match $x isa movie; get;
```

# Data Definition Language

## Define Query

A [define query](#define-query) is the keyword `define` followed by one or more [variable patterns](#variable-pattern).

## Undefine Query

An [undefine query](#undefine-query) is the keyword `undefine` followed by one or more
[variable patterns](#variable-pattern).

## Supported Properties

Within the [variable patterns](#variable-pattern) of both [define](#define-query) and [undefine](#undefine-queries)
queries, the following [properties](#property) are supported:

<!-- TODO -->
- `$A sub $B`
- `$A relates $B`
- `$A plays $B`
- `$A id <identifier>`
- `$A label <identifier>`
- `$A has <identifier>`
- `$A key <identifier>`
- `($A: $x, ... )`
- `$A is-abstract`
- `$A regex <regex>`
- `$A when <pattern>`
- `$A then <pattern>`

# Data Manipulation Language

## Match

A [match](#match) is the keyword `match` followed by one or more [patterns](#pattern) and zero or more
[modifiers](#modifier).

A [match](#match) will find all _answers_ in the knowledge base that _satisfy_ all of the given [patterns](#patterns).
These _answers_ can be used with either [get](#get-query), [insert](#insert-query) or [delete](#delete-query).
<!-- TODO aggregate -->

### Pattern

A [pattern](#pattern) is either a [variable pattern](#variable-pattern), a conjunction or a disjunction:

- A conjunction is one or more [patterns](#pattern) surrounded by curly braces `{ }` and ending in a semicolon `;`. An
  _answer_ _satisfies_ a conjunction if it _satisfies_ all [patterns](#pattern) within the conjunction.

- A disjunction is two [patterns](#pattern) joined with the keyword `or` and ending in a semicolon `;`. An _answer_
  _satisfies_ a disjunction if it _satisfies_ either [pattern](#pattern) within the disjunction.

- An _answer_ _satisfies_ a [variable pattern](#variable-pattern) if the concept bound to the subject of the
  [variable pattern](#variable-pattern) _satisfies_ all [properties](#property) in the
  [variable pattern](#variable-pattern).

Within the [patterns](#pattern), the following [properties](#property) are supported:

#### isa

`$x isa $A` is _satisfied_ if `$x` is indirectly an instance of `$A`.

#### relationship

`$r ($A1: $x1, ..., $An: $xn)` is _satisfied_ if `$r` is a relation where for each `$Ai: $xi`, `$xi` is a role-player
in `$r` indirectly playing the role `$Ai`.

Additionally, for all pairs `$Ai: $xi` and `$Aj: $xj` where `i != j`, `$xi` and `$xj` must be different, or directly
playing different roles in the relationship `$r`.

The [variable](#variable) representing the role can optionally be omitted. If it is omitted, then the first
[variable](#variable) is implicitly bound to the label `role`.

#### has (data)

<!-- TODO: Describe without referring to relationships? -->
```
$x has <identifier> $y as $r;
```
is equivalent to
```
$r ($x, $y);
$y isa <identifier>;
```

The `as $r` is optional. Additionally, a predicate can be used instead of a [variable](#variable):

```
$x has <identifier> <predicate>;
```
which is equivalent to:
```
$x has <identifier> $_;
$_ val <predicate>;
```

#### val

`$x val <predicate>` is _satisfied_ if `$x` is an attribute with a value _satisfying_ the `<predicate>`.
<!-- TODO: predicates -->

#### id

`$x id <identifier>` is _satisfied_ if `$x` has the ID `<identifier>`.

#### !=

`$x != $y` is _satisfied_ if `$x` is not the same concept as `$y`.

#### label

`$A label <identifier>` is _satisfied_ if `$A` is a schema concept with the label `<identifier>`.

#### sub

`$A sub $B` is _satisfied_ if `$A` is indirectly a sub-concept of `$B`.

#### relates

`$A relates $B` is _satisfied_ if `$A` is a relationship type that directly relates a role `$B`.

#### plays

`$A plays $B` is _satisfied_ if `$A` is a type that indirectly plays a role `$B`.

#### has (type)

<!-- TODO: Describe without referring to relationships? -->
```
$A has <identifier>
```
is equivalent to
```
$_R sub relationship, relates $_O, relates $_V;
$_O sub role;
$_V sub role;

$A plays $_O;
<identifier> plays $_V;

$_O != $_V;
```

#### key

<!-- TODO: Describe without referring to relationships? -->
<!-- TODO: handle required part properly -->
```
$A key <identifier>
```
is equivalent to
```
$_R sub relationship, relates $_O, relates $_V;
$_O sub role;
$_V sub role;

$A plays <required thingy I GUESS> $_O;
<identifier> plays $_V;

$_O != $_V;
```

#### datatype

`$A dataype <datatype>` is _satisfied_ if `$A` is an attribute type with the given datatype.

#### regex

`$A regex <regex>` is _satisfied_ if `$A` is an attribute type with the given regex constraint.

#### is-abstract

`$A is-abstract;` is _satisfied_ if `$A` is an abstract type.

### Modifier

<!-- TODO -->

## Get Query

A [get query](#get-query) is a [match](#match) followed by the keyword `get` and zero or more [variables](#variable),
ending in a semicolon `;`.

The [get query](#get-query) will project each _answer_ over the provided [variables](#variable). If no
[variables](#variable) are provided, then the _answers_ are projected over all [variables](#variable) mentioned in the
[pattern](#pattern).

## Insert Query

An [insert query](#insert-query) is an optional [match](#match) followed by the keyword `insert` and one or more
[variable patterns](#variable-pattern).

The [insert query](#insert-query) will insert the given [variable patterns](#variable-pattern) into the knowledge base
and return an _answer_ with variables bound to concepts mentioned in the [variable patterns](#variable pattern).

If a [match](#match) is provided, then the [insert query](#insert-query) will operate for every _answer_ of the
[match](#match) and return one _answer_ for each [match](#match).

Within the [variable patterns](#variable-pattern), the following [properties](#property) are supported:

<!-- TODO -->
- `$x isa $A`
- `$x id <identifier>`
- `$x label <identifier>`
- `$x val <predicate>`
- `$x has <identifier> <predicate or variable> (as $y)`
- `($A: $x, ... )`

## Delete Query

A [delete query](#delete-query) is a [match](#match) followed by the keyword `delete` and one or more
[variables](#variable).

<!-- TODO  Aggregate + compute -->
