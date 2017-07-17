---
title: Migration API
keywords: java
last_updated: February 2017
tags: [java, migration]
summary: "The Java Migration API"
sidebar: documentation_sidebar
permalink: /documentation/developing-with-java/migration-api.html
folder: documentation
---


All Grakn migrators extend the `AbstractMigrator` class or implement the `Migrator` interface. The migrators are a wrapper around the `LoaderClient` and as such expose many of the same configuration options. 

## Construction

Each migrator may have a slightly different constructor (for example, `CSV` and `Json` migration constructors accept the data file whereas `SQL` migration accepts the SQL JDBC connection). They should all accept a template and a data accessor. 

```java
CSVMigrator migrator = new CSVMigrator(String template, File dataFile);
```

## Migration

To migrate the provided data into a graph, call the `load` function, providing the keyspace and location where Grakn Engine is running. 

```java
migrator.load(String uri, String keyspace);
```

Alternatively, the user can call a `load` method with more configuration options. The configuration options are described in more detail on the [loader client API page](./loader-api.html).

```java
migrator.load(String uri, String keyspace, int batchSize, int numberActiveTasks, boolean retry)
```

## Termination

Some migrators have a `close` method that should be called after loading has completed. As the `load` method blocks the calling thread, this is easily done. A quick example of JSON migration performed from java:

```java
CSVMigrator migrator = new CSVMigrator("insert $x isa person has name <name>", new File("people.csv"));
migrator.load(Grakn.DEFAULT_URI, "genealogy", 10, 10, false);
migrator.close();
```

## Examples

### JSON Migration

There is an example of using the [Java Migration API for JSON data](https://github.com/graknlabs/sample-projects/tree/master/example-json-migration-giphy) on the sample projects repository on Github. 

### SQL Migration

There is an example of [SQL migration using the Java API](../examples/SQL-migration.html) discussed on this portal. 

### OWL Migration

There is an example of [OWL migration using the Java API](./OWL-migration.html) on this portal.

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/23" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.


{% include links.html %}

