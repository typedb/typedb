---
title: Java Graql
keywords: graql, java
tags: [graql, java]
summary: "How to construct and execute Graql queries programmatically in Java."
sidebar: documentation_sidebar
permalink: /docs/java-library/graql-api
folder: docs
---

As well as the Graql shell, users can also construct and execute Graql queries programmatically in Java. The Java Graql API expresses the concepts and functionality of the Graql language in the syntax of Java. It is useful if you want to make queries using Java, without having to construct a string containing the appropriate Graql expression.

To use the API, add the following to your imports:

```java-test-ignore
import ai.grakn.graql.QueryBuilder;
import static ai.grakn.graql.Graql.*;
```

## QueryBuilder

A `QueryBuilder` is constructed from a `GraknTx`:

```java-test-ignore
Grakn.Session session = Grakn.session(new SimpleURI("localhost:48555"), Keyspace.of("grakn"));
Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)

QueryBuilder qb = tx.graql();
```

The user can also choose to not provide a knowledge graph with `Graql.withoutTx()`.
This can be useful if you need to provide the knowledge graph later (using `withTx`),
or you only want to construct queries without executing them.

The `QueryBuilder` class provides methods for building `match`es and `insert`
queries. Additionally, it is possible to build `aggregate`, `match..insert` and `delete` queries from `match`
queries.

## Match

Matches are constructed using the `match` method. This will produce a `Match` instance, which includes additional
methods that apply modifiers such as `limit` and `distinct`:

```java
Match match = qb.match(var("x").isa("person").has("firstname", "Bob")).limit(50);
```

If you're only interested in one variable name, it also includes a `get` method
for requesting a single variable:

```
match.get("x").forEach(x -> System.out.println(x.asResource().getValue()));
```

## Get Queries

Get queries are constructed using the `get` method on a `match`.

```java
GetQuery query = qb.match(var("x").isa("person").has("firstname", "Bob")).limit(50).get();
```

`GetQuery` is `Iterable` and has a `stream` method. Each result is a `Map<Var, Concept>`, where the keys are the
variables in the query.

A `GetQuery` will only execute when it is iterated over.

```java
for (Map<String, Concept> result : query) {
  System.out.println(result.get("x").getId());
}
```

## Aggregate Queries

```java
if (qb.match(var().isa("person").has("firstname", "Bob")).stream().findAny().isPresent()) {
  System.out.println("There is someone called Bob!");
}
```

## Insert Queries

```java
InsertQuery addAlice = qb.insert(var().isa("person").has("firstname", "Alice"));

addAlice.execute();

// Marry Alice to everyone!
qb.match(
  var("someone").isa("person"),
  var("alice").has("firstname", "Alice")
).insert(
  var().isa("marriage")
    .rel("spouse", "someone")
    .rel("spouse", "alice")
).execute();
```

## Delete Queries

```java
qb.match(var("x").has("firstname", "Alice")).delete("x").execute();
```

## Query Parser

The `QueryBuilder` also allows the user to parse Graql query strings into Java Graql
objects:

```java
for (ConceptMap a : qb.<GetQuery>parse("match $x isa person; get;").execute()) {
    System.out.println(a);
}

qb.parse("insert isa person, has firstname 'Alice';").execute();

qb.parse("match $x isa person; delete $x;").execute();
```
