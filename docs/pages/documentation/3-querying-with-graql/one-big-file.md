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

## Answer

Several [queries](#query) return [answers](#answer). An [answer](#answer) is a map from [variables](#variable) to
concepts. In general, [answers](#answer) will contain all [variables](#variable) mentioned in the [patterns](#pattern)
of the [query](#query).


# Variable pattern

A [variable pattern](#variable-pattern) is a [variable](#variable) (the _subject_ of the pattern) followed by zero or
more [properties](#property) (optionally separated by commas `,`) and ending in a semicolon `;`.

## Variable

A [variable](#variable) is an identifier prefixed with a dollar `$`. Valid identifiers must comprise of one or
more alphanumeric characters, dashes and underscores.

## Property

There are several different [properties](#property). Which ones are supported and what they do depends on the part of
the [query](#query). For example, [match](#match) supports most [properties](#property), whereas [insert](#insert) only
supports a small subset.

When a [property](#property) takes a [variable](#variable) representing a schema concept, it is possible to substitute
a label. For example,
```graql
match $x isa $A; $A label movie; get;
```
should be written more succinctly as
```graql
match $x isa movie; get;
```

# Data Definition Language

## Define Query

A [define query](#define-query) is the keyword `define` followed by one or more [variable patterns](#variable-pattern).

The [define query](#define-query) will add the given [variable patterns](#variable-pattern) to the schema.

After execution, it will return a single [answer](#answer) containing bindings for all [variables](#variable) mentioned
in the [variable patterns](#variable-pattern).

## Undefine Query

An [undefine query](#undefine-query) is the keyword `undefine` followed by one or more
[variable patterns](#variable-pattern).

The [define query](#define-query) will remove the given [variable patterns](#variable-patterns) from the schema.

In order to remove a schema concept entirely, it is sufficient to undefine its [direct super-concept](#sub):

```graql
undefine person sub entity;
```

## Supported Properties

Within the [variable patterns](#variable-pattern) of both [define](#define-query) and [undefine](#undefine-queries)
queries, the following [properties](#property) are supported:

<!-- TODO -->

### id

`$x id <identifier>` will assign `$x` to an existing concept with the given ID. It is an error if no such concept
exists.

### label

`$A label <identifier>` will assign `$A` to a schema concept with the given label. If no such schema concept exists,
one will be created.

### sub

`$A sub $B` defines `$A` as the direct sub-concept of `$B`.

### relates

`$A relates $B` define the relationship type `$A` to directly relate the role `$B`.

### plays

`$A plays $B` defines the type `$A` to directly play the role `$B`.

### has (type)

`$A has <identifier>` defines the type `$A` to have attributes of type `<identifier>`.

This is done using the following relationship structure:
```graql
has-<identifier>-owner sub has-<sup>-owner;
has-<identifier>-value sub has-<sup>-value;
has-<identifier> sub has-<sup>, relates has-<identifier>-owner, relates has-<identifier>-value;

$A plays has-<identifier>-owner;
<identifier> plays has-<identifier>-value;
```
Where `<sup>` is the direct super-concept of `<identifier>`.

### key (type)

`$A key <identifier>` defines the type `$A` to have a key of attribute type `<identifier>`.

This is done using the following relationship structure:
```graql
key-<identifier>-owner sub key-<sup>-owner;
key-<identifier>-value sub key-<sup>-value;
key-<identifier> sub key-<sup>, relates key-<identifier>-owner, relates key-<identifier>-value;

$A plays<<required>> has-<identifier>-owner;
<identifier> plays has-<identifier>-value;
```
Where `<sup>` is the direct super-concept of `<identifier>`.
<!-- TODO: This is pretty bad -->
(note that `plays<<required>>` is not valid syntax, but indicates that instances of the type _must_ play the required
role exactly once).

### datatype

`$A datatype <datatype>` defines the attribute type `$A` to have the specified datatype.

### regex

`$A regex <regex>` defines the attribute type `$A` to have the specified regex constraint.

### is-abstract

`$A is-abstract` defines the type `$A` to be abstract.

### when

`$A when <pattern>` defines the rule `$A` to have the specified `when` [pattern](#pattern).

### then

`$A then <pattern>` defines the rule `$A` to have the specified `then` [pattern](#pattern).

# Data Manipulation Language

## Match

A [match](#match) is the keyword `match` followed by one or more [patterns](#pattern) and zero or more
[modifiers](#modifier).

A [match](#match) will find all [answers](#answer) in the knowledge base that _satisfy_ all of the given
[patterns](#patterns). These [answers](#answer) can be used with either [get](#get-query), [insert](#insert-query) or
[delete](#delete-query).
<!-- TODO aggregate -->

### Pattern

A [pattern](#pattern) is either a [variable pattern](#variable-pattern), a conjunction or a disjunction:

- A conjunction is one or more [patterns](#pattern) surrounded by curly braces `{ }` and ending in a semicolon `;`. An
  [answer](#answer) _satisfies_ a conjunction if it _satisfies_ all [patterns](#pattern) within the conjunction.

- A disjunction is two [patterns](#pattern) joined with the keyword `or` and ending in a semicolon `;`. An
  [answer](#answer) _satisfies_ a disjunction if it _satisfies_ either [pattern](#pattern) within the disjunction.

- An [answer](#answer) _satisfies_ a [variable pattern](#variable-pattern) if the concept bound to the subject of the
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

`$x has <identifier> $y as $r` is _satisfied_ if `$x` is related to an attribute `$y` of type `<identifier>`, where
`$r` represents the relationship between them.

This is equivalent to the following:
<!-- TODO: Describe without referring to relationships? -->
```graql
$r ($x, $y);
$y isa <identifier>;
```

The `as $r` is optional. Additionally, a predicate can be used instead of a [variable](#variable):

```graql
$x has <identifier> <predicate>;
```
which is equivalent to:
```graql
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

`$A has <identifier>` is _satisfied_ if `$A` is a type who have attribute type `<identifier>`.

<!-- TODO: Describe without referring to relationships? -->
This is equivalent to the following:
```graql
$_R sub relationship, relates $_O, relates $_V;
$_O sub role;
$_V sub role;

$A plays $_O;
<identifier> plays $_V;

$_O != $_V;
```

#### key

`$A has <identifier>` is _satisfied_ if `$A` is a type who have attribute type `<identifier>` as a key.

<!-- TODO: Describe without referring to relationships? -->
<!-- TODO: handle required part properly -->
This is equivalent to the following:
```graql
$_R sub relationship, relates $_O, relates $_V;
$_O sub role;
$_V sub role;

$A plays<<required>> $_O;
<identifier> plays $_V;

$_O != $_V;
```
<!-- TODO: This is pretty bad -->
(note that `plays<<required>>` is not valid syntax, but indicates that instances of the type _must_ play the required
role exactly once).

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

The [get query](#get-query) will project each [answer](#answer) over the provided [variables](#variable). If no
[variables](#variable) are provided, then the [answers](#answer) are projected over all [variables](#variable) mentioned
in the [pattern](#pattern).

## Insert Query

An [insert query](#insert-query) is an optional [match](#match) followed by the keyword `insert` and one or more
[variable patterns](#variable-pattern).

The [insert query](#insert-query) will insert the given [variable patterns](#variable-pattern) into the knowledge base
and return an [answer](#answer) with variables bound to concepts mentioned in the [variable patterns](#variable pattern).

If a [match](#match) is provided, then the [insert query](#insert-query) will operate for every [answer](#answer) of the
[match](#match) and return one [answer](#answer) for each [match](#match) [answer](#answer).

Within the [variable patterns](#variable-pattern), the following [properties](#property) are supported:

<!-- TODO -->
### isa

`$x isa $A` creates a new direct instance of `$A` and binds it to `$x`.

### relationship

`$r ($A1: $x1, ..., $An, $xn)` will, for each `$Ai: $xi`, add a new role-player `$xi` to `$r` directly playing the role
`$Ai`.

### has (data)

`$x has <identifier> $y as $r` will relate the attribute `$y` of type `<identifier>` to `$x`, binding the relationship
to `$r`.

If `<identifier>` is a key for the direct type of `$x`, then the resulting relationship will be a key relationship.

<!-- TODO: Describe without referring to relationships? -->
If `<identifier>` is not a key for the direct type of `$x`, this is equivalent to:

```graql
$r (has-<identifier>-owner: $x, has-<identifier>-value: $y) isa has-<identifier>;
$y isa <identifier>;
```

Otherwise, it is equivalent to:

```graql
$r (key-<identifier>-owner: $x, key-<identifier>-value: $y) isa key-<identifier>;
$y isa <identifier>;
```

The `as $r` is optional. Additionally, a literal value can be used instead of a [variable](#variable):

```graql
$x has <identifier> <value>;
```

which is equivalent to:

```graql
$x has <identifier> $_;
$_ val <value>;
```

### val

`$x val <value>;` specifies that the attribute `$x` should have value `<value>`.

### id

`$x id <identifier>` will assign `$x` to an existing concept with the given ID. It is an error if no such concept
exists.

### label

`$A label <identifier>` will assign `$A` to an existing schema concept with the given label. It is an error if no such
schema concept exists.

## Delete Query

A [delete query](#delete-query) is a [match](#match) followed by the keyword `delete` and zero or more
[variables](#variable).

For every [answer](#answer) from the [match](#match), the [delete query](#delete-query) will delete the concept bound
to every [variable](#variable) listed. If no [variables](#variable) are provided, then every variable mentioned in
the [match](#match) is deleted.

<!-- TODO  Aggregate + compute -->

<!-- TODO datatype, values, predicates -->
