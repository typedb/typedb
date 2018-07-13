---
title: Java Development Setup
keywords: java
tags: [java]
summary: "Overview and setup guide for Java developers."
sidebar: documentation_sidebar
permalink: /docs/java-library/setup
folder: docs
---

## Basic Setup

This section will discuss how to develop an application with Grakn, using the Java API.
All applications which use **Grakn 1.3.0** will require the `client-java` dependency to be declared on the `pom.xml` of your application.

```xml
<repositories>
  <repository>
    <id>releases</id>
    <url>https://oss.sonatype.org/content/repositories/releases</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>client-java</artifactId>
    <version>1.3.0</version>
  </dependency>
</dependencies>
```

Alternatively, applications which are still using **Grakn 1.2.0** will instead require the `grakn-client` dependency.
```xml
<repositories>
  <repository>
    <id>snapshots</id>
    <url>http://maven.grakn.ai/nexus/content/repositories/snapshots/</url>
  </repository>
  <repository>
    <id>releases</id>
    <url>https://oss.sonatype.org/content/repositories/releases</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>grakn-client</artifactId>
    <version>1.2.0</version>
  </dependency>
</dependencies>
```

Please be noted that most of the materials in the documentation will use the syntax of Grakn 1.3.0.

## Connecting to Grakn

{% include note.html content="Before proceeding, make sure that the Grakn database has already been started. Otherwise, refer to the [Setup guide](./docs/get-started/setup-guide#install-graknai) on how to install and start Grakn properly." %}

First, make sure to import the following classes:
```java-test-ignore
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
```


Now, connect to Grakn with:

```java-test-ignore
GraknSession session = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("grakn"));
try (GraknTx tx = session.open(GraknTxType.READ)) {
  // ...
}

```

A "Keyspace" uniquely identifies the knowledge graph and allows you to create different knowledge graphs.

Please note that keyspaces are **not** case sensitive, so the following two keyspaces are actually the same:

```java-test-ignore
    GraknTx tx1 = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("grakn")).open(GraknTxType.WRITE);
    GraknTx tx2 = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("grakn")).open(GraknTxType.WRITE);
```

All knowledge graphs are also singletons specific to their keyspaces so be aware that in the following case:

```java-test-ignore
   tx1 = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("grakn")).open(GraknTxType.WRITE);
   tx2 = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("grakn")).open(GraknTxType.WRITE);
   tx3 = RemoteGrakn.session(new SimpleURI("localhost:48555"), Keyspace.of("grakn")).open(GraknTxType.WRITE);
```

any changes to `tx1`, `tx2`, or `tx3` will all be persisted to the same knowledge graph.

## Controlling The Behaviour of Knowledge Graph Transactions

When initialising a transaction on a knowledge graph it is possible to define the type of transaction with `GraknTxType`.
We currently support three types of transactions:

* `GraknTxType.WRITE` - A transaction that allows mutations to be performed on the knowledge graph
* `GraknTxType.READ` - Prohibits any mutations to be performed to the knowledge graph
* `GraknTxType.BATCH` - Allows faster mutations to be performed to the knowledge graph at the cost of switching off some internal consistency checks. This option should only be used if you are certain that you are loading a clean dataset.

## Where Next?

The pages in this section of the documentation cover some of the public APIs available to Java developers:

* [Java API](./core-api)
* [Java Graql](./graql-api)
* [Migration API](./migration-api)
* [Loader API](./loader-api)

There is also a page (in progress) that discusses advanced topics in Java development, such as transactions and multi-threading.

There is an example described in our [blog](https://blog.grakn.ai/working-with-grakn-ai-using-java-5f13f24f1269#.8df3991rw) that discusses how to get set up to develop using Java, and how to work with the Java API.
