---
title: Data Manipulation Language
keywords: graql, query, data manipulation language
last_updated: April 2017
tags: [graql]
summary: "Data Manipulation Language"
sidebar: documentation_sidebar
permalink: /documentation/graql/dml.html
folder: graql
---

# Match

`match` [`<pattern> ...`](#pattern) [`(<modifier> ...)`](#modifier)

A [match](#match) will find all [answers](./query.html#answer) in the knowledge base that _satisfy_ all of the given
[patterns](#pattern). Any optional [modifiers](#modifier) are applied to the [answers](./query.html#answer) to the
[match](#match).

The [answers](./query.html#answer) to a [match](#match) can be used with either a [get](#get-query),
[insert](#insert-query), [delete](#delete-query) or [aggregate](#aggregate-query).

## Pattern

A [pattern](#pattern) describes a set of constraints that an [answer](./query.html#answer) must _satisfy_.

It is either a [variable pattern](./query.html#variable-pattern), a conjunction or a disjunction:

- An [answer](./query.html#answer) _satisfies_ a [variable pattern](./query.html#variable-pattern) if the concept bound
  to the subject of the [variable pattern](./query.html#variable-pattern) _satisfies_ all
  [properties](./query.html#property) in the [variable pattern](./query.html#variable-pattern).
  e.g. `$x isa person` _satisfies_ an [answer](./query.html#answer) `$x=c1, $y=c2` if the concept `c1` satisfies the
  [property](./query.html#property) `isa person`.

- Conjunction: `{ <pattern> ... };` - An [answer](./query.html#answer) _satisfies_ a conjunction if it _satisfies_
  **all** [patterns](#pattern) within the conjunction.
  e.g. `{ ($x, $y); $x isa person; }` _satisfies_ an answer `$x=c1, $y=c2` if the answer _satisfies_ both `($x, $y)`
  _and_ `$x isa person`.

- Disjunction: `<pattern> or <pattern>;` - An [answer](./query.html#answer) _satisfies_ a disjunction if it _satisfies_
  **any** [pattern](#pattern) within the disjunction.
  e.g. `{ ($x, $y); } or { $x isa person; }` _satisfies_ an answer `$x=c1, $y=c2` if the answer _satisfies_ either
  `($x, $y)` _or_ `$x isa person`.

Within the [patterns](#pattern), the following [properties](./query.html#property) are supported:

### isa

`$x isa $A` is _satisfied_ if `$x` is indirectly an _instance_ of the _type_ `$A`.

### relationship

`$r ($A1: $x1, ..., $An: $xn)` is _satisfied_ if `$r` is a _relation_ where for each `$Ai: $xi`, `$xi` is a role-player
in `$r` indirectly playing the _role_ `$Ai`.

Additionally, for all pairs `$Ai: $xi` and `$Aj: $xj` where `i != j`, `$xi` and `$xj` must be different, or directly
playing different roles in the relationship `$r`.

The [variable](./query.html#variable) representing the role can optionally be omitted. If it is omitted, then the first
[variable](./query.html#variable) is implicitly bound to the label `role`.

### has (data)

`$x has <identifier> $y via $r` is _satisfied_ if `$x` is a _thing_ related to an _attribute_ `$y` of _attribute type_
`<identifier>`, where `$r` represents the _relationship_ between them.

The `via $r` is optional. Additionally, a [predicate](#predicate) can be used instead of a
[variable](./query.html#variable):

```graql-test-ignore
$x has <identifier> <predicate>;
```
which is equivalent to:
```graql-test-ignore
$x has <identifier> $_;
$_ val <predicate>;
```

> `has` is syntactic sugar for a particular kind of relationship.
>
> It is equivalent to the following:
> ```graql-test-ignore
> $r ($x, $y);
> $y isa <identifier>;
> ```

### val

`$x val <predicate>` is _satisfied_ if `$x` is an _attribute_ with a [value](./query.html#value) _satisfying_ the
[`<predicate>`](#predicate).

### id

`$x id <identifier>` is _satisfied_ if `$x` has the ID `<identifier>`.

### !=

`$x != $y` is _satisfied_ if `$x` is not the same concept as `$y`.

### label

`$A label <identifier>` is _satisfied_ if `$A` is a _schema concept_ with the label `<identifier>`.

### sub

`$A sub $B` is _satisfied_ if `$A` is a _schema concept_ that is indirectly a sub-concept of a _schema concept_ `$B`.

### relates

`$A relates $B` is _satisfied_ if `$A` is a _relationship type_ that directly relates a _role_ `$B`.

### plays

`$A plays $B` is _satisfied_ if `$A` is a _type_ that indirectly plays a _role_ `$B`.

### has (type)

`$A has <identifier>` is _satisfied_ if `$A` is a _type_ whose instances can have instances of _attribute type_
`<identifier>`.

> `has` is syntactic sugar for a particular kind of relationship.
>
> It is equivalent to the following:
> ```graql-test-ignore
> $_R sub relationship, relates $_O, relates $_V;
> $_O sub role;
> $_V sub role;
>
> $A plays $_O;
> <identifier> plays $_V;
>
> $_O != $_V;
> ```

### key

`$A has <identifier>` is _satisfied_ if `$A` is a _type_ whose instances must have keys of _attribute type_
`<identifier>`.

> `key` is syntactic sugar for a particular kind of relationship.
>
> It is equivalent to the following:
> ```graql-test-ignore
> $_R sub relationship, relates $_O, relates $_V;
> $_O sub role;
> $_V sub role;
>
> $A plays<<required>> $_O;
> <identifier> plays $_V;
>
> $_O != $_V;
> ```
> <!-- TODO: This is pretty bad -->
> (note that `plays<<required>>` is not valid syntax, but indicates that instances of the type _must_ play the required
> role exactly once).

### datatype

`$A dataype <datatype>` is _satisfied_ if `$A` is an _attribute type_ with the given
[datatype](./query.html#value).

### regex

`$A regex <regex>` is _satisfied_ if `$A` is an _attribute type_ with the given regex constraint.

### is-abstract

`$A is-abstract;` is _satisfied_ if `$A` is an abstract _type_.

## Predicate

The following predicates can be applied to attributes:

- `=`, `!=`, `>`, `>=`, `<`, `<=` - _satisfied_ if the attribute's [values](./query.html#value) satisfy the comparison.
  These operators can also take a literal [value](./query.html#value).
- `contains <string>` - _satisfied_ if `<string>` is a substring of the attribute's [value](./query.html#value).
- `/<regex>/` - _satisfied_ if the attribute's [value](./query.html#value) matches the `<regex>`.

## Modifier

There are several modifiers to change the [answers](./query.html#answer) of the [match](#match):

- `limit <n>` - limit to only the first `<n>` [answers](./query.html#answer).
- `offset <n>` - skip the first `<n>` [answers](./query.html#answer).
- `order by <variable> (asc | desc)` - order the [answers](./query.html#answer) by the values of the concepts bound to
  the [variable](./query.html#variable) in ascending or descending order (ascending by default).

Modifiers are applied in order from left to right. For example, this means that `order by $n; limit 100;` will find the
100 [answers](./query.html#answer) with globally minimal `$n`, whereas `limit 100; order by $n;` will locally order the
first 100 [answers](./query.html#answer) to the query.

# Get Query

[`<match>`](#match) `get` [`(<variable>, ...)`](./query.html#variable) `;`

The [get query](#get-query) will project each [answer](./query.html#answer) over the provided
[variables](./query.html#variable). If no [variables](./query.html#variable) are provided, then the
[answers](./query.html#answer) are projected over all [variables](./query.html#variable) mentioned in the
[pattern](#pattern).

# Insert Query

[`(<match>)`](#match) `insert` [`<variable pattern> ...`](./query.html#variable-pattern)

The [insert query](#insert-query) will insert the given [variable patterns](./query.html#variable-pattern) into the
knowledge base and return an [answer](./query.html#answer) with variables bound to concepts mentioned in the
[variable patterns](./query.html#variable-pattern).

If a [match](#match) is provided, then the [insert query](#insert-query) will operate for every
[answer](./query.html#answer) of the [match](#match) and return one [answer](./query.html#answer) for each
[match](#match) [answer](./query.html#answer).

Within the [variable patterns](./query.html#variable-pattern), the following [properties](./query.html#property) are
supported:

## isa

`$x isa $A` creates a new direct instance of the _type_ `$A` and binds it to `$x`.

## relationship

`$r ($A1: $x1, ..., $An, $xn)` will, for each `$Ai: $xi`, add a new role-player `$xi` to the _relationship_ `$r`
directly playing the _role_ `$Ai`.

## has (data)

`$x has <identifier> $y via $r` will relate the _attribute_ `$y` of _attribute type_ `<identifier>` to `$x`, binding the
_relationship_ to `$r`.

If `<identifier>` is a key for the direct type of `$x`, then the resulting relationship will be a key relationship.

The `via $r` is optional. Additionally, a literal [value](./query.html#value) can be used instead of a
[variable](./query.html#variable):

```graql-test-ignore
$x has <identifier> <value>;
```

which is equivalent to:

```graql-test-ignore
$x has <identifier> $_;
$_ val <value>;
```

<!-- TODO: Describe without referring to relationships? -->
> `has` is syntactic sugar for a particular kind of relationship.
>
> If `<identifier>` is not a key for the direct type of `$x`, it is equivalent to:
>
> ```graql-test-ignore
> $r (has-<identifier>-owner: $x, has-<identifier>-value: $y) isa has-<identifier>;
> $y isa <identifier>;
> ```
>
> Otherwise, it is equivalent to:
>
> ```graql-test-ignore
> $r (key-<identifier>-owner: $x, key-<identifier>-value: $y) isa key-<identifier>;
> $y isa <identifier>;
> ```

## val

`$x val <value>;` specifies that the _attribute_ `$x` should have [value](./query.html#value) `<value>`.

## id

`$x id <identifier>` will assign `$x` to an existing _concept_ with the given ID. It is an error if no such concept
exists.

## label

`$A label <identifier>` will assign `$A` to an existing _schema concept_ with the given label. It is an error if no such
schema concept exists.

# Delete Query

[`<match>`](#match) `delete` [`(<variable>, ...)`](./query.html#variable) `;`

For every [answer](./query.html#answer) from the [match](#match), the [delete query](#delete-query) will delete the
concept bound to every [variable](./query.html#variable) listed. If no [variables](./query.html#variable) are provided,
then every variable mentioned in the [match](#match) is deleted.

# Aggregate Query

[`<match>`](#match) `aggregate` [`<aggregate>`](#aggregate) `;`

An [aggregate query](#aggregate-query) applies the given [aggregate](#aggregate) to the [answers](./query.html#answer)
of the [match](#match).

## Aggregate

`<identifier>` `(<` [`variable`](./query.html#variable) `or` [`aggregate`](#aggregate) `> ...)`

An [aggregate query](#aggregate-query) is a [match](#match) followed by the keyword `aggregate` and an
[aggregate](#aggregate).

An aggregate begins with an aggregate name followed by zero or more arguments. An argument may be either a
[variable](./query.html#variable) or another [aggregate](#aggregate).

Examples of [aggregates](#aggregate) are:

- `ask`
  - Return whether there are any [answers](./query.html#answer).
- `count`
- `group <variable> (<aggregate>)`
  - Group the [answers](./query.html#answer) by a [variable](./query.html#variable) and optionally apply an
    [aggregate](#aggregate) to each group.
- `max <variable>`
- `median <variable>`
- `mean <variable>`
- `min <variable>`
- `std <variable>`
- `sum <variable>`
- `(<aggregate> as <identifier> , ...)`
  - The "product" [aggregate](#aggregate) has a special syntax: a comma-separated sequence of named
    aggregates surrounded by brackets `( )`. This [aggregate](#aggregate) will execute all comprising
    [aggregates](#aggregate) and put the results in a map, keyed by the given identifiers.

