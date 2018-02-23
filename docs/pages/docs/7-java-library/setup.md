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
  <grakn.version>1.1.0</grakn.version>
</properties>

<dependencies>
  <dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>grakn-kb</artifactId>
    <version>${grakn.version}</version>
  </dependency>
</dependencies>
```

This dependency will give you access to the Core API as well as an in-memory knowledge base, which serves as a toy knowledge base, should you wish to use the stack without having to have an instance of the Grakn server running.

## Server Dependent Setup

If you require persistence and would like to access the entirety of the Grakn stack, then it is vital to have an instance of engine running.  
Please see the [Setup Guide](../get-started/setup-guide) on more details on how to set up a Grakn server.

Depending on the configuration of the Grakn server, your Java application will require the following dependency:
```xml   
<dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>grakn-factory</artifactId>
    <version>${grakn.version}</version>
</dependency>
```    

The JAR files you will need to develop with Grakn can be found inside the `lib` directory of the distribution zip file. All the JARs are provided with no dependencies, so using these requires you to use Maven to acquire dependencies.

Alternatively, you may include a reference to the snapshot repository, which contains the third-party dependencies. Add the following to your `pom.xml`:

```xml
<repositories>
  <!--Snapshot repository for 3rd party libraries -->
  <repository>
    <id>grakn-development-snapshots</id>
    <url>https://maven.grakn.ai/content/repositories/snapshots/</url>
    <releases>
      <enabled>false</enabled>
    </releases>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>
```

Here are some links to guides for adding external jars using different IDEs:

- [IntelliJ](https://www.jetbrains.com/help/idea/2016.1/configuring-module-dependencies-and-libraries.html)
- [Eclipse](http://www.tutorialspoint.com/eclipse/eclipse_java_build_path.htm)
- [Netbeans](http://oopbook.com/java-classpath-2/classpath-in-netbeans/)


## Initialising a Transaction on The knowledge base

You can initialise an in memory knowledge base without having the Grakn server running with:  

<!-- These are ignored in tests because they connect to non-existent servers -->
```java
GraknTx tx = Grakn.session(Grakn.IN_MEMORY, "keyspace").open(GraknTxType.WRITE);
```    

If you are running the Grakn server locally then you can initialise a knowledge base with:

```java-test-ignore
tx = Grakn.session(Grakn.DEFAULT_URI, "keyspace").open(GraknTxType.WRITE);
```

If you are running the Grakn server remotely you must initialise the knowledge base by providing the IP address of your server:

```java-test-ignore
tx = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
```

The string "keyspace" uniquely identifies the knowledge base and allows you to create different knowledge bases.  

Please note that knowledge base keyspaces are **not** case sensitive so the following two knowledge bases are actually the same:

```java-test-ignore
    GraknTx tx1 = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
    GraknTx tx2 = Grakn.session("127.6.21.2", "KeYsPaCe").open(GraknTxType.WRITE);
```

All knowledge bases are also singletons specific to their keyspaces so be aware that in the following case:

```java-test-ignore
   tx1 = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
   tx2 = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
   tx3 = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
```

any changes to `tx1`, `tx2`, or `tx3` will all be persisted to the same knowledge base.

## Controlling The Behaviour of Knowledge Base Transactions

When initialising a transaction on a knowledge base it is possible to define the type of transaction with `GraknTxType`.      
We currently support three types of transactions:

* `GraknTxType.WRITE` - A transaction that allows mutations to be performed on the knowledge base
* `GraknTxType.READ` - Prohibits any mutations to be performed to the knowledge base
* `GraknTxType.BATCH` - Allows faster mutations to be performed to the knowledge base at the cost of switching off some internal consistency checks. This option should only be used if you are certain that you are loading a clean dataset.

## Where Next?

The pages in this section of the documentation cover some of the public APIs available to Java developers:

* [Java API](./core-api)
* [Java Graql](./graql-api)
* [Migration API](./migration-api)
* [Loader API](./loader-api)

There is also a page (in progress) that discusses advanced topics in Java development, such as transactions and multi-threading.

There is an example described in our [blog](https://blog.grakn.ai/working-with-grakn-ai-using-java-5f13f24f1269#.8df3991rw) that discusses how to get set up to develop using Java, and how to work with the Java API.
