---
title: Java Graql
keywords: graql, java
last_updated: February 2017
tags: [graql, java]
summary: "How to construct and execute Graql queries programmatically in Java."
sidebar: documentation_sidebar
permalink: /documentation/developing-with-java/java-graql.html
folder: documentation
---

As well as the Graql shell, users can also construct and execute Graql queries programmatically in Java. The Java Graql API expresses the concepts and functionality of the Graql language in the syntax of Java. It is useful if you want to make queries using Java, without having to construct a string containing the appropriate Graql expression.

To use the API, add the following to your `pom.xml`:

```xml
<dependency>
  <groupId>ai.grakn</groupId>
  <artifactId>grakn-graql</artifactId>
  <version>${project.version}</version>
</dependency>
```

and add the following to your imports.

```java-test-ignore
import ai.grakn.graql.QueryBuilder;
```

## QueryBuilder

`Graql` contains several useful static methods such as `var` and `eq`, so it's recommended that you use a static import:

```java-test-ignore
import static ai.grakn.graql.Graql.*;
```

A `QueryBuilder` is constructed from a `GraknTx`:

```java-test-ignore
GraknTx tx = Grakn.session(Grakn.IN_MEMORY, "MyGraph").open(GraknTxType.WRITE);
QueryBuilder qb = tx.graql();
```

The user can also choose to not provide a knowledge base with `Graql.withoutTx()`.
This can be useful if you need to provide the knowledge base later (using `withTx`),
or you only want to construct queries without executing them.

The `QueryBuilder` class provides methods for building `match` and `insert`
queries. Additionally, it is possible to build `aggregate`, `match..insert` and `delete` queries from `match`
queries.

## Match Queries

Match queries are constructed using the `match` method. This will produce a
`MatchQuery` instance, which includes additional methods that apply modifiers
such as `limit` and `distinct`:

```java
MatchQuery query = qb.match(var("x").isa("person").has("firstname", "Bob")).limit(50);
```

`MatchQuery` is `Iterable` and has a `stream` method. Each result is a
`Map<String, Concept>`, where the keys are the variable names in the query.

A `MatchQuery` will only execute when it is iterated over.

```java
for (Map<String, Concept> result : query) {
  System.out.println(result.get("x").getId());
}
```

If you're only interested in one variable name, it also includes a `get` method
for requesting a single variable:

```
query.get("x").forEach(x -> System.out.println(x.asResource().getValue()));
```

## Aggregate Queries

```java
if (qb.match(var().isa("person").has("firstname", "Bob")).aggregate(ask()).execute()) {
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
    .rel("spouse1", "someone")
    .rel("spouse2", "alice")
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
for (Concept x : qb.<MatchQuery>parse("match $x isa person;").get("x")) {
    System.out.println(x);
}

if (qb.<AggregateQuery<Boolean>parse("match has name 'Bob' isa person; aggregate ask;").execute()) {
  System.out.println("There is someone called Bob!");
}

qb.parse("insert isa person, has firstname 'Alice';").execute();

qb.parse("match $x isa person; delete $x;").execute();
```

## Reasoning

Reasoning can be configured using `QueryBuilder` objects in the following way:

### Switching reasoning on

```java
//tx is a GraknTx instance
qb = tx.graql().infer(true);
```

### Switching materialisation on

```java
//tx is a GraknTx instance
qb = tx.graql().infer(true).materialise(true);
```

Once the `QueryBuilder` has been defined, the constructed queries will obey the specified reasoning variants.
    
The table below summarises the available reasoning configuration options together with their defaults.

| Option       | Description | Default
| -------------------- |:--|:--|
| `QueryBuilder::infer(boolean)` | controls whether reasoning should be turned on | False=Off |
| `QueryBuilder::materialise(boolean)`       | controls whether inferred knowledge should be persisted to knowledge base | False=Off |


## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/23" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

{% include links.html %}
