![GRAKN.AI](https://grakn.ai/assets/img/grakn-logo-name.png)

---
[![CircleCI](https://circleci.com/gh/graknlabs/grakn/tree/master.svg?style=shield)](https://circleci.com/gh/graknlabs/grakn/tree/master)
[![GitHub release](https://img.shields.io/github/release/graknlabs/grakn.svg)](https://github.com/graknlabs/grakn/releases/latest)
[![Slack Status](http://grakn-slackin.herokuapp.com/badge.svg)](https://grakn.ai/slack)
[![Discussion Forum](https://img.shields.io/discourse/https/discuss.grakn.ai/topics.svg)](https://discuss.grakn.ai)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-grakn-796de3.svg)](https://stackoverflow.com/questions/tagged/grakn)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-graql-3dce8c.svg)](https://stackoverflow.com/questions/tagged/graql)

Building intelligent systems starts at the database. Grakn is an intelligent database: a knowledge graph engine to organise complex networks of data and make it queryable.

| Get Started | Documentation | Discussion |
|:------------|:--------------|:-----------|
| Whether you are new to coding or an experienced developer, it’s easy to learn and use Grakn. Get set up quickly with [quickstart tutorial](https://dev.grakn.ai/docs/general/quickstart). | Documentation for Grakn’s development library and Graql language API, along with tutorials and guides, are available online. Visit our [documentation portal](https://dev.grakn.ai/). | When you’re stuck on a problem, collaborating helps. Ask your question on [StackOverflow](https://stackoverflow.com/questions/tagged/graql+or+grakn) or discuss it on our [Discussion Forum](https://discuss.grakn.ai/). |

# Meet Grakn and Graql

Grakn is an intelligent database: a knowledge graph engine to organise complex networks of data and making it queryable, by performing [knowledge engineering](https://en.wikipedia.org/wiki/Knowledge_engineering). Rooted in [Knowledge Representation and Automated Reasoning](https://en.wikipedia.org/wiki/Knowledge_representation_and_reasoning), Grakn provides the [knowledge foundation](https://en.wikipedia.org/wiki/Knowledge_base) for cognitive and intelligent (e.g. AI) systems, by providing an intelligent language for modelling, transactions and analytics. Being a distributed database, Grakn is designed to scale over a network of computers through partitioning and replication.

Under the hood, Grakn has built an expressive knowledge representation system based on [hypergraph theory](https://en.wikipedia.org/wiki/Hypergraph) (a subfield in mathematics that generalises an edge to be a set of vertices) with a transactional query interface, Graql. Graql is Grakn’s reasoning (through OLTP) and analytics (through OLAP) declarative query language. 

## Knowledge Schema

Grakn provides an enhanced [entity-relationship](https://en.wikipedia.org/wiki/Entity–relationship_model) schema to model complex datasets. The schema allows users to model type hierarchies, hyper-entities, hyper-relationships and rules. The schema can be updated and extended at any time in the database lifecycle. Hyper-entities are entities with multiple instances of a given attribute, and hyper-relationships are nested relationships, cardinality-restricted relationships, or relationships between any number of entities. This enables the creation of complex knowledge models that can evolve flexibly.

## Logical Inference

Grakn’s query language performs logical inference through [deductive reasoning](https://en.wikipedia.org/wiki/Deductive_reasoning) of entity types and relationships, to infer implicit facts, associations and conclusions in real-time, during runtime of OLTP queries. The inference is performed through entity and relationship type reasoning, as well as rule-based reasoning. This allows the discovery of facts that would otherwise be too hard to find, the abstraction of complex relationships into its simpler conclusion, as well as translation of higher level queries into the lower level and more complex data representation.

## Distributed Analytics

Grakn’s query language performs distributed [Pregel](https://kowshik.github.io/JPregel/pregel_paper.pdf) and [MapReduce](https://en.wikipedia.org/wiki/MapReduce) ([BSP](https://en.wikipedia.org/wiki/Bulk_synchronous_parallel)) algorithms abstracted as OLAP queries. These types of queries usually require custom development of distributed algorithms for every use case. However, Grakn creates an abstraction of these distributed algorithms and incorporates them as part of the language API. This enables large scale computation of BSP algorithms through a declarative language without the need of implementing the algorithms.

## Higher-Level Language

With the expressivity of the schema, inference through OLTP and distributed algorithms through OLAP, Grakn provides strong abstraction over low-level data constructs and complicated relationships through its query language. The language provides a higher-level schema, OLTP, and OLAP query language, that makes working with complex data a lot easier. When developers can achieve more by writing less code, productivity rate increases by orders of magnitude.

## Download and Running Grakn Core

To run Grakn Core (which you can download from the [Download Centre](https://grakn.ai/download) or [GitHub Releases](https://github.com/graknlabs/grakn/releases)), you need to have Java 8 (OpenJDK or Oracle Java) installed.

You can visit the [Setup Guide](https://dev.grakn.ai/docs/running-grakn/install-and-run) to help your installation.

## Compiling Grakn Core from Source

> Note: You don't need to compile Grakn Core from source if you just want to use Grakn. See the _"Download and Running Grakn Core"_ section above.

1. Make sure you have the following dependencies installed on your machine:
    - Java 8
    - Python >= 2.7 and Pip >= 18.1
    - [Bazel](http://docs.bazel.build/install.html)
1. Depending on your Operating System, you can build Grakn with either one of the following commands: 
```
$ bazel build //:assemble-linux-targz
```
Outputs to: `bazel-bin/grakn-core-all-linux.tar.gz`
```
$ bazel build //:assemble-mac-zip
```
Outputs to: `bazel-bin/grakn-core-all-mac.zip`
```
$ bazel build //:assemble-windows-zip
```
Outputs to: `bazel-bin/grakn-core-all-windows.zip`

## Contributions

Grakn Core is built using various state-of-the-art open-source Graph and Distributed Computing frameworks: [ANTLR](http://www.antlr.org), [Apache Cassandra](http://cassandra.apache.org), [Apache Hadoop](https://hadoop.apache.org), [Apache Spark](http://spark.apache.org), [Apache TinkerPop](http://tinkerpop.apache.org), [Bazel](https://bazel.build), [GRPC](https://grpc.io) and [JanusGraph](http://janusgraph.org). Thank you!

## Licensing

This product includes software developed by [Grakn Labs Ltd](https://grakn.ai/).  It's released under the GNU Affero GENERAL PUBLIC LICENSE, Version 3, 29 June 2007. For license information, please see [LICENSE](https://github.com/graknlabs/grakn/blob/master/LICENSE). Grakn Labs Ltd also provides a commercial license for Grakn Enterprise KGMS - get in touch with our team at enterprise@grakn.ai.

Copyright (C) 2020 Grakn Labs
