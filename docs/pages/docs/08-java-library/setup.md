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

This section will discuss how to start developing with Grakn using the Java API.
All Grakn applications require the following Maven dependency:

```xml
<properties>
  <grakn.version>1.2.0</grakn.version>
</properties>

<dependencies>
  <dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>client-java</artifactId>
    <version>1.2.0</version>
  </dependency>
</dependencies>
```

This dependency will give you access to the Core API as well as an in-memory knowledge graph, which serves as a toy knowledge graph, should you wish to use the stack without having to have an instance of the Grakn server running.

## Server Dependent Setup

If you require persistence and would like to access the entirety of the Grakn stack, then it is vital to have an instance of engine running.  
Please see the [Setup Guide](../get-started/setup-guide) on more details on how to set up a Grakn server.

Here are some links to guides for adding external jars using different IDEs:

- [IntelliJ](https://www.jetbrains.com/help/idea/2016.1/configuring-module-dependencies-and-libraries.html)
- [Eclipse](http://www.tutorialspoint.com/eclipse/eclipse_java_build_path.htm)
- [Netbeans](http://oopbook.com/java-classpath-2/classpath-in-netbeans/)


## Connecting to Grakn

First, make sure that Grakn is running. Otherwise, boot it up with `grakn server start`. Now, connect to Grakn with:

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
