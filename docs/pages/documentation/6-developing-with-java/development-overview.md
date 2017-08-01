---
title: Java Development Overview
keywords: java
last_updated: March 2017
tags: [java]
summary: "Overview guide for Java developers."
sidebar: documentation_sidebar
permalink: /documentation/developing-with-java/java-setup.html
folder: documentation
---

## Basic Setup

This section will discuss how to start developing with Grakn using the Java API. 
All Grakn applications require the following Maven dependency: 

```xml
<properties>
  <grakn.version>0.12.0</grakn.version>
</properties>

<dependencies>
  <dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>grakn-graph</artifactId>
    <version>${grakn.version}</version>
  </dependency>
</dependencies>
```
    
This dependency will give you access to the Core API as well as an in-memory graph, which serves as a toy graph, should you wish to use the stack without having to have an instance of the Grakn server running.

## Server Dependent Setup

If you require persistence and would like to access the entirety of the Grakn stack, then it is vital to have an instance of engine running.  
Please see the [Setup Guide](../get-started/setup-guide.html) on more details on how to set up a Grakn server.

Depending on the configuration of the Grakn server, your Java application will require one of the following dependencies. When your server is running against a Janus backend: 

```xml   
<dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>janus-factory</artifactId>
    <version>${grakn.version}</version>
</dependency>
```    

{% include note.html content="The distribution package comes with a Janus backend configured out of the box." %}


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


## Initialising a Transaction on The Graph

You can initialise an in memory graph without having the Grakn server running with:  

<!-- These are ignored in tests because they connect to non-existent servers -->
```java
GraknGraph graph = Grakn.session(Grakn.IN_MEMORY, "keyspace").open(GraknTxType.WRITE);
```    
    
If you are running the Grakn server locally then you can initialise a graph with:

```java-test-ignore
graph = Grakn.session(Grakn.DEFAULT_URI, "keyspace").open(GraknTxType.WRITE);
```
    
If you are running the Grakn server remotely you must initialise the graph by providing the IP address of your server:

```java-test-ignore
graph = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
```
    
The string "keyspace" uniquely identifies the graph and allows you to create different graphs.  

Please note that graph keyspaces are **not** case sensitive so the following two graphs are actually the same graph:

```java-test-ignore
    GraknGraph graph1 = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
    GraknGraph graph2 = Grakn.session("127.6.21.2", "KeYsPaCe").open(GraknTxType.WRITE);
```
   
All graphs are also singletons specific to their keyspaces so be aware that in the following case:

```java-test-ignore
   graph1 = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
   graph2 = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
   graph3 = Grakn.session("127.6.21.2", "keyspace").open(GraknTxType.WRITE);
```
  
any changes to `graph1`, `graph2`, or `graph3` will all be persisted to the same graph.

## Controlling The Behaviour of Graph Transactions
  
When initialising a transaction on a graph it is possible to define the type of transaction with `GraknTxType`.      
We currently support three types of transactions:

* `GraknTxType.WRITE` - A transaction that allows mutations to be performed on the graph
* `GraknTxType.READ` - Prohibits any mutations to be performed to the graph 
* `GraknTxType.BATCH` - Allows faster mutations to be performed to the graph at the cost of switching off some internal consistency checks. This option should only be used if you are certain that you are loading a clean dataset. 

## Where Next?

The pages in this section of the documentation cover some of the public APIs available to Java developers:

* [Graph API](./graph-api.html)
* [Java Graql](./java-graql.html)
* [Migration API](./migration-api.html)
* [Loader API](./loader-api.html)

There is also a page (in progress) that discusses advanced topics in Java development, such as transactions and multi-threading.

There is an example described in our [blog](https://blog.grakn.ai/working-with-grakn-ai-using-java-5f13f24f1269#.8df3991rw) that discusses how to get set up to develop using Java, and how to work with the Graph API and Java Graql.

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/23" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.


{% include links.html %}

