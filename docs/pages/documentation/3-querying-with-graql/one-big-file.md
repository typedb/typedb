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

## Query

A [query](#query) is an action, in some cases preceded by a [match](#match). Examples of actions are
[define](#define-query), [undefine](#undefine-query), [get](#get-query), [insert](#insert-query) and
[delete](#delete-query).

## Pattern

A [pattern](#pattern) is either a [variable pattern](#variable-pattern), a [conjunction](#conjunction) or a
[disjunction](#disjunction).

### Variable pattern

A [variable pattern](#variable-pattern) is a [variable](#variable) followed by zero or more [properties](#property)
(optionally separated by commas `,`) and ending in a semicolon `;`.

### Conjunction

A [conjunction](#conjunction) is one or more [patterns](#pattern) surrounded by curly braces `{ }` and ending in a
semicolon `;`.

### Disjunction

A [disjunction](#disjunction) is two [patterns](#pattern) joined with the keyword `or` and ending in a semicolon `;`.

### Variable

A [variable](#variable) is an identifier prefixed with a dollar `$`. Valid identifiers must comprise of one or
more alphanumeric characters, dashes and underscores.

### Property

There are several different [properties](#property). Which ones are supported and what they do depends on the part of
the query. For example, [match](#match) supports most properties, whereas [insert](#insert) only supports a small
subset.

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

# Data Manipulation Language

## Match

A [match](#match) is the keyword `match` followed by one or more [patterns](#pattern) and zero or more
[modifiers](#modifier).

Within the [patterns](#pattern), the following [properties](#property) are supported:

<!-- TODO -->

### Modifier

<!-- TODO -->

## Get Query

A [get query](#get-query) is a [match](#match) followed by the keyword `get` and zero or more [variables](#variable),
ending in a semicolon `;`.

## Insert Query

An [insert query](#insert-query) is an optional [match](#match) followed by the keyword `insert` and one or more
[variable patterns](#variable-pattern).

Within the [variable patterns](#variable-pattern), the following [properties](#property) are supported:

<!-- TODO -->

## Delete Query

A [delete query](#delete-query) is a [match](#match) followed by the keyword `delete` and one or more
[variables](#variable).

<!-- TODO  Aggregate + compute -->
