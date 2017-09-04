---
title: Loader Client API
keywords: java
last_updated: February 2017
tags: [java]
summary: "The Loader Client API"
sidebar: documentation_sidebar
permalink: /documentation/developing-with-java/loader-api.html
folder: documentation
---


The Grakn loader is a Java client for loading large quantities of data into a Grakn knowledge base using multithreaded batch loading. The loader client operates by sending requests to the Grakn REST Tasks endpoint and polling for the status of submitted tasks, so the user no longer needs to implement these REST transactions. The loader client additionally provides a number of useful features, including batching insert queries, blocking, and callback on batch execution status. Configuration options allow the user to finely-tune batch loading settings. 

It is possible for batches of insert queries to fail upon insertion. By default, the client will not log the status of the batch execution. The user can specify a callback function to operate on the result of the batch operation and print and accumulate status information.

If you are using the [Graql shell](../graql/graql-shell.html), batch loading is available using the `-b` option. 

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
import ai.grakn.client.BatchMutatorClient;
```

# Basic Usage

The loader client provides two constructors. 
The first accepts only the keyspace to load data into, the URI endpoint where Grakn Engine Server is running and a flag to enable debug mode.

```java
BatchMutatorClient loader = new BatchMutatorClient(keyspace, uri, false);
```

The second constructor additionally allows the user to specify a callback function that executes on completion of tasks. 

```java
loader = new BatchMutatorClient(keyspace, uri, callback, false);
```

The loader client can be thought of as an empty bucket in which to dump insert queries that will be batch-loaded into the specified knowledge base. Batching, blocking and callbacks are all executed based on how the user has configured the client, which simplifies usage. The following code will load 100 insert queries into the knowledge base.

```java
InsertQuery insert = insert(var().isa("person"));

for(int i = 0; i < 100; i++){
	loader.add(insert);
}

loader.waitToFinish();
```

The user should call `waitToFinish()`, which will flush the last batch of queries and block until all batches have executed. 

# Configuring the client

## Configuring Batch Size 

The batch size represents the number of tasks that will be executed in a single transaction when batch loading.

The default batch size is **25**.

```java
// Setting the batch size to one
loader.setBatchSize(1);

// Each of these insert queries will be executed in their own transaction
loader.add(insert);
loader.add(insert);
loader.add(insert);

loader.waitToFinish();

// Set the batch size to five
loader.setBatchSize(5);

// All of these insert queries will be executed in one single transaction
loader.add(insert);
loader.add(insert);
loader.add(insert);

loader.waitToFinish();
```

Flushing in the middle of adding queries will force-send batches to the server, overriding the set batch size. The following code will execute in two transactions, the first containing one insert and the second transaction containing two, even though the overall batch size is set to five. 

```java
loader.setBatchSize(5);

// First transaction
loader.add(insert);

loader.flush();

// Second transaction
loader.add(insert);
loader.add(insert);

loader.waitToFinish();
```

This batch size property can directly effect loading times. If you find that your data is loading too slowly, try increasing the size of the batch. 

## Configuring Active Tasks
The number of active tasks represents the capcity beyond which no additional inserts can be added without blocking. 

The default number of active tasks is **25**.


```java
loader.setBatchSize(1);
loader.setNumberActiveTasks(1);

// First transaction
loader.add(insert);

// Second transaction. Block here until the first transaction completes. 
loader.add(insert);

loader.waitToFinish();
```

In the above scenario, the loader will block the calling thread when adding the second transaction. Only when the first transaction completes execution will the second transaction be sent to the server. 

## Task Completion Callback

Specify a callback function that will execute over the server response after batch completion. 

The server response is describes more in deatils in the REST api documentation. It will look something like this:

```json
{
	"creator":"ai.grakn.client.BatchMutatorClient",
	"runAt":"2017-02-28T10:38:07.585Z",
	"recurring":false,
	"className":"ai.grakn.engine.loader.LoaderTask",
	"interval":0,
	"id":"9118075f-afd7-48dd-b594-b49d9e2cc8d7",
	"status":"COMPLETED",
	"engineID":"67128a90-1112-400a-a38e-0f4a35f2c8f6"
}
```

Migration uses this callback function to track status information about the number of batches that have completed for the running migration.

<!-- Ignored because it contains a Java lambda, which Groovy doesn't support -->
```java-test-ignore
AtomicInteger numberBatchesCompleted = new AtomicInteger(0);

loader.setTaskCompletionConsumer((Json json) -> {
	Integer completed = numberBatchesCompleted.incrementAndGet();
	String status = json.at("status").asString();

	LOG.info("Batches completed: {}\nStatus of last batch: {}", completed, status);
});
```

This callback is executed whenever a terminal response from the server is received, even if an exception was thrown. In the case of an execption, the Json argument will be either empty or contain the errored response from the server including the server-side exception. 

## Retry policy
Allows the user to specify whether the client should retry sending requests if the server cannot be reached. 

The default setting is for the retry policy to be **false**. 


```java
// If the server is unavailable, the client will attempt to re-send the queries
loader.setRetryPolicy(true);

// The client will not attempt to resend the batch of queries if the server becomes unavailable
loader.setRetryPolicy(false);
```


## Close

The loader can be closed as follows

```java
loader.close();
```

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/23" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.


{% include links.html %}

