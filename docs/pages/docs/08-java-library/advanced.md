---
title: Advanced Java Development
keywords: java
tags: [java]
summary: "Transaction Processing & Multithreading in Java"
sidebar: documentation_sidebar
permalink: /docs/java-library/advanced
folder: docs
---

{% include warning.html content="Please note that this page is in progress and subject to revision." %}

In this section we focus on using the Java API in a multi-threaded environment, and show how to create multiple transactions, which can affect the knowledge graph concurrently.

## Creating Concurrent Transactions

Transactions in GRAKN.AI are thread bound, which means that for a specific keyspace and thread, only one transaction can be open at any time.
The following would result in an exception because the first transaction `tx1` was never closed:

<!-- Ignored because this is designed to crash! -->
```java-test-ignore
Grakn.Session session = Grakn.session(new SimpleURI("localhost:48555"), Keyspace.of("grakn"));
Grakn.Transaction tx1 = session.transaction(GraknTxType.WRITE)
Grakn.Transaction tx2 = session.transaction(GraknTxType.WRITE)
```

If you require multiple transactions open at the same time then you must do this on different threads. This is best illustrated with an example. Let's say that you wish to create 100 entities of a specific type concurrently.  The following will achieve that:

<!-- Ignored because it contains a Java lambda, which Groovy doesn't support -->
```java-test-ignore
Grakn.Session session = Grakn.session(new SimpleURI("localhost:48555"), Keyspace.of("grakn"));
Set<Future> futures = new HashSet<>();
ExecutorService pool = Executors.newFixedThreadPool(10);

//Create sample schema
Grakn.Transaction tx = session.transaction(GraknTxType.WRITE);
EntityType entityType = tx.putEntityType("Some Entity Type");
tx.commit();

//Load the data concurrently
for(int i = 0; i < 100; i ++){
    futures.add(pool.submit(() -> {
        Grakn.Transaction innerTx = session.transaction(GraknTxType.WRITE);
        entityType.addEntity();
        innerTx.commit();
    }));
}

for(Future f: futures){
    f.get();
}
```

As you can see each thread opened its own transaction to work with. We were able to safely pass `entityType` into different threads but this was only possible because:

* We committed `entityType` before passing it around
* We opened the transaction in each thread before trying to access `entityType`.

## Issues With Concurrent Mutations

### Locking Exceptions

When mutating the knowledge graph concurrently and attempting to load the same data simultaneously, it is possible to encounter a `GraknLockingException`.  When this exception is thrown on `commit()` it means that two or more transactions are attempting to mutate the same thing. If this occurs it is recommended that you retry the transaction.

### Validation Exceptions

Validation exceptions may also occur when mutating the knowledge graph concurrently. For example, two transactions may be trying to create the exact same relationship and one of them may fail. When this occurs it is recommended retrying the transaction. If the same exception occurs again then it is likely that the transaction contains a validation error that would have still occurred even in a single threaded environment.
