---
title: Loader Client API
keywords: java
tags: [java]
summary: "The Loader Client API"
sidebar: documentation_sidebar
permalink: /docs/java-library/loader-api
folder: docs
---


The Grakn loader is a Java client for loading large quantities of data into a Grakn knowledge graph using multithreaded batch loading. The loader client operates by sending requests to the Grakn REST Tasks endpoint and polling for the status of submitted tasks, so the user no longer needs to implement these REST transactions. The loader client additionally provides a number of useful features, including batching insert queries, blocking, and callback on batch execution status. Configuration options allow the user to finely-tune batch loading settings.

It is possible for batches of insert queries to fail upon insertion. By default, the client will not log the status of the batch execution. The user can specify a callback function to operate on the result of the batch operation and print and accumulate status information.

If you are using the [Graql shell](../get-started/graql-console), batch loading is available using the `-b` option.

To use the loader client API, add the following to your pom.xml:

```xml
<dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>grakn-client</artifactId>
    <version>${grakn.version}</version>
</dependency>
```
 and add the following to your imports:

```
import ai.grakn.client.BatchExecutorClient;
```

# Basic Usage

The loader client can be instantiated by giving the engine URI.

```java
BatchExecutorClient loader = BatchExecutorClient.newBuilderforURI(uri).build();
```

The loader client can be thought of as an empty bucket in which to dump insert queries that will be batch-loaded into the specified knowledge graph. Batching, blocking and callbacks are all executed based on how the user has configured the client, which simplifies usage. The following code will load 100 insert queries into the knowledge graph.

```java
InsertQuery insert = insert(var().isa("person"));

for(int i = 0; i < 100; i++){
    loader.add(insert, keyspace).subscribe({System.out.println(it)});
}
```

Note that  the output is  a Java RX Observable that needs subscription or blocking.

## Close

The loader should be closed as follows

```java
loader.close();
```
