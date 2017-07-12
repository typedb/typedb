---
title: GRAKN.AI Architecture Overview
keywords: setup, getting started, basics
last_updated: February 2017
tags: [getting-started]
summary: "Introducing the GRAKN.AI Architecture"
sidebar: documentation_sidebar
permalink: /documentation/the-fundamentals/grakn-architecture.html
folder: documentation
comment_issue_id: 17
---

GRAKN.AI is an AI knowledge management engine. It is a database for managing very large knowledge graphs and takes the form of a development platform, offering a tool suite for:

* modelling and reasoning over data
* computing large-scale analytics over graph-oriented, structured data.

There are two parts to GRAKN.AI: Grakn (the storage), and Graql (a  declarative, knowledge-oriented, graph query language).

## Grakn

Grakn builds upon several major industry-standard technologies such as Apache Tinkerpop, Apache Cassandra, Apache Spark and others. At its core lies a graph database that implements the [Apache Tinkerpop](https://tinkerpop.apache.org) APIs, making Grakn agnostic about graph data storage. Grakn’s own algorithms are scalable and capable of handling big data problems. Scalability of the system as a whole is conditioned upon the performance characteristics of the underlying database selected. 

Knowledge management in the GRAKN.AI follows the [knowledge representation model](../the-fundamentals/grakn-knowledge-model.html). The model is logically formalized to guarantee consistency of the data as well as soundness and completeness of the reasoning procedure. The model is defined in Java as a set of interfaces implemented on top of the Tinkerpop API. The knowledge model serves as a foundation for the knowledge-oriented language Graql, which incorporates querying, reasoning, data migration and graph analytics.

## Graql

The Graql query language makes extensive use of Gremlin. In fact, Graql queries are translated into Gremlin queries before submission to Tinkerpop for execution. Graql comes with its own syntax and can be used stand-alone via a shell. But it also has a Java API that mimics the native syntax closely. The Java API is convenient for avoiding the messiness of constructing query strings, typical for many other development stacks.

The reasoning engine works directly with Graql constructs and performs backwards chaining via an iterative deepening algorithm. Unification as well as recursive rules are supported.

The distributed analytics component employees massively parallel algorithms based on the Tinkerpop BSP/OLAP processing model, which in turn makes use of technologies such as Apache Hadoop and Apache Spark. 

## Grakn Engine

The Grakn engine is the main server component running the platform. A client always needs an engine running before it can talk to the knowledge graph, even when the client is accessing the graph database in embedded mode. A Grakn engine process hosts several distinct services:

* REST API
* web-based GUI for interacting with and visualizing the graph
* websockets endpoint for Graql queries
* distributed background task processing peer.

An engine server makes use of industry standard infrastructure packages such as an embedded HTTP server, Kafka, Zookeeper, Spark and Hadoop. Engines can be deployed in a horizontally scalable cluster. They are stateless and co-operate in a peer-to-peer fashion. They can be load-balanced and support failover, providing guarantees that any submitted task will be eventually completed. See the [Deployment Guide](../deploy-grakn/grakn-deployment-guide.html) for more information on how to configure a GRAKN Engine or a cluster thereof.

The following diagram shows how the various components stack together:

![Architecture](/images/grakn-architecture.png)

As a developer, you can interact with the platform in one of several ways:

* Load the GRAKN.AI components in a JVM process and use the Java APIs to manipulate the knowledge graph, perform queries, kick off graph analytics jobs etc.
* Visualize information through the Grakn visualiser, which is a GUI Shell.
* Start a command line shell and run Graql queries in the terminal, including loading ontologies, rules and data.
* Call the REST API to load data or navigate the graph following [standard HAL practices](http://stateless.co/hal_specification.html).


## Where Next?

We recommend that you read more about the [Grakn knowledge model](../the-fundamentals/grakn-knowledge-model.html) then get started by [setting up Grakn](..get-started/setup-guide.html) and running through our [quickstart tutorial](../get-started/quickstart-tutorial.html).

{% include links.html %}

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/17" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.
