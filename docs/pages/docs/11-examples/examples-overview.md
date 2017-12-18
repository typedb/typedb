---
title: Examples Overview
keywords: examples
tags: [getting-started, examples]
summary: "Landing page for Grakn examples."
sidebar: documentation_sidebar
permalink: /docs/examples/examples-overview
folder: docs
---


## Introduction

This page lists the examples of Grakn that we suggest you study to learn how to work with our stack.  We plan to continue to expand our set, and we also encourage you to let us know if you have example code to share, so we can link to it (for example, to your repo or a blog post).

If you would like to request a particular example, please get in touch with us by leaving a comment on this page or posting a question on the discussion boards.  Our [Community page](https://grakn.ai/community) lists other ways you can talk to us.

### Genealogy Dataset

The genealogy dataset is widely used across our documentation about GRAKN.AI, because it allows for a simple, yet powerful, illustration of many key features. As described below, it is used to illustrate CSV migration and the Grakn Reasoner.

It is available on the [sample-datasets repo on Github](https://github.com/graknlabs/sample-datasets/tree/master/genealogy-graph), and is also discussed in the ["Family Matters" blog post](https://blog.grakn.ai/family-matters-1bb639396a24#.4gnoaq2hr).

## Use Cases

### CSV Migration

There are several examples available:

* We use the genealogy dataset to show [how to migrate genealogy data into Grakn from CSV](../examples/CSV-migration).

* Our sample-projects repository on Github also contains [an example that takes a simple CSV data file of pets](https://github.com/graknlabs/sample-projects/tree/master/example-csv-migration-pets). Please see the [readme file](https://github.com/graknlabs/sample-projects/blob/master/example-csv-migration-pets/README.md) to get started.

* CSV migration is also covered in a [blog post](https://blog.grakn.ai/twenty-years-of-games-in-grakn-14faa974b16e#.cuox3cew2).

### JSON Migration

There is an example of using the Java Migration API for JSON migration on the [sample-projects repository](https://github.com/graknlabs/sample-projects/tree/master/example-json-migration-giphy) on Github.

### SQL Migration

There are several examples available:

* A common use-case is to migrate existing SQL data to a knowledge base in Grakn. We walk through a simple example of using the migration script as part of the documentation about [SQL migration](../migrating-data/migrating-sql)

* There is a an additional example of [SQL migration using the Java API](../examples/SQL-migration).

* We also cover SQL migration in a [blog post](https://blog.grakn.ai/populating-mindmapsdb-with-the-world-5b2445aee60c#).


### Reasoning with Graql

We use the genealogy dataset to illustrate how to write rules to infer new information from a dataset. You can find the example [here](./graql-reasoning).

### Learn Graql

* The [Modern example](./modern) is a simple one, designed to test your knowledge of Graql.
* We have a [simple Pokemon example](./pokemon) to illustrate how to form a range of different Graql queries.
* The [philosophers.gql](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/philosophers.gql) file, also distributed in the Grakn release zip, contains a simple schema and data, for use as an example.

### Analytics

We have two examples to illustrate how to use Graql analytics:

* [Statistical Analysis](./analytics) describes using the `compute` and `aggregate` methods on a familar R dataset (MTCars) to compare them and illustrate them.
* [Analytics using Java APIs](./java-analytics) uses the Java APIs to show how to calculate clusters and degrees using the familar genealogy example set.

## Languages

### Haskell, R and Python Bindings
It is possible to extract data from Grakn and use it as a data science tool for analysis. You can take the results of a Graql query and store the results in a dataframe or similar structure, for use with Haskell, R or Python.

* Haskell: This [blog post](https://blog.grakn.ai/grakn-ai-and-haskell-c166c7cc1d23#.9jc7xu79l) is the first in a series of posts about combining GRAKN.AI and Haskell.
* R and Python: This [blog post](https://blog.grakn.ai/there-r-pandas-in-my-graph-b8b5f40a2f99#) explains and gives a simple example.
* Python: A further [blog post](https://blog.grakn.ai/grakn-pandas-celebrities-5854ad688a4f#.k5zucfp6f) uses the Python driver to examine our example movie dataset.

### Java Examples

* Use of the core APIs for creating a schema, adding data and making basic queries is covered in a [blog post](https://blog.grakn.ai/working-with-grakn-ai-using-java-5f13f24f1269#.giljgrjb3), and also as a [basic example](./java-api-example).
* JSON migration: There is an example of using the [Java Migration API](../java-library/migration-api) for JSON migration on the [sample-projects repository](https://github.com/graknlabs/sample-projects/tree/master/example-json-migration-giphy) on Github.
* SQL migration: We have documented an example of [SQL migration using the Java API](../examples/SQL-migration).
* Pokemon: The [sample-projects](https://github.com/graknlabs/sample-projects/tree/master/example-pokemon) repo on Github contains a Java project that uses the Java API on Pokemon data and a schema.
* Philosophers: The [sample-projects](https://github.com/graknlabs/sample-projects/tree/master/example-philosophers) repo on Github contains a Java project that uses the Java API on a Philosophers dataset and schema.
* [Analytics using Java APIs](./java-analytics) uses the Java APIs to show how to calculate clusters and degrees using the familar genealogy example set.

### Moogi Movie Database

[Moogi](https://moogi.co) is a large database of information about movies. We have provided a subset of its data to try out in our [sample-datasets repository](https://github.com/graknlabs/sample-datasets/tree/master/movies) on Github.

## Where Next?

If you are interested in writing an example on Grakn, maybe as a way of trying it out, please take a look at the [Example Projects](./projects) page, which lists some ideas that we have for potential examples or research projects.
