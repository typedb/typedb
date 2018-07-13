---
title: Java Development Setup
keywords: java
tags: [java]
summary: "This section will discuss how to develop an application with Grakn, using the Java API."
sidebar: documentation_sidebar
permalink: /docs/java-library/setup
folder: docs
---

## Declaring The Dependency In Maven
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

## Opening A Session And Transaction

{% include note.html content="Before proceeding, make sure that the Grakn knowledge graph has already been started. Otherwise, refer to the [Setup guide](./docs/get-started/setup-guide#install-graknai) on how to install and start Grakn properly." %}

A **session** object is responsible for maintaining a connection to a specific keyspace in the knowledge graph. Opening a session is performed by invoking the `Grakn.session` method.
Once the session is open, you can proceed by creating a **transaction** in order to manipulate the data in the keyspace.

The following snippet shows how to open a Grakn session and transaction:

```java-test-ignore
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.util.SimpleURI;

public class App {
  public static void main(String[] args) {
    SimpleURI localGrakn = new SimpleURI("localhost", 48555);
    Keyspace keyspace = Keyspace.of("grakn");
    try (Grakn.Session session = Grakn.session(localGrakn, keyspace)) {
      try (Grakn.Transaction transaction = session.transaction(GraknTxType.WRITE)) {
        // ...
        transaction.commit();
      }
    }
  }
}
```

## Keyspace Uniqueness
A "Keyspace" uniquely identifies the knowledge graph and allows you to create different knowledge graphs.

Please note that keyspaces are **not** case sensitive. This means that these `grakn`, `Grakn`, and `GrAkn` names refer to the same keyspace.

## Transaction Types
We currently support three transaction types:

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
