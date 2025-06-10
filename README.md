[![TypeDB](./docs/banner.png)](https://typedb.com/introduction)

[![Factory](https://factory.vaticle.com/api/status/typedb/typedb/badge.svg)](https://factory.vaticle.com/typedb/typedb)
[![CircleCI](https://circleci.com/gh/typedb/typedb/tree/master.svg?style=shield)](https://circleci.com/gh/typedb/typedb/tree/master)
[![GitHub release](https://img.shields.io/github/release/typedb/typedb.svg)](https://github.com/typedb/typedb/releases/latest)
[![Discord](https://img.shields.io/discord/665254494820368395?color=7389D8&label=discord&logo=discord&logoColor=ffffff)](https://typedb.com/discord)
[![Discussion Forum](https://img.shields.io/badge/discourse-forum-blue.svg)](https://forum.typedb.com)
[![Hosted By: Cloudsmith](https://img.shields.io/badge/OSS%20hosting%20by-cloudsmith-blue?logo=cloudsmith&style=flat)](https://cloudsmith.com)

# Introducing TypeDB

**TypeDB** is a next-gen database with a modern programming paradigm that lets you build data applications faster, safer, and more elegantly. Its intuitive and powerful data model unifies the strengths of relational, document and graph databases without their shortcomings. **TypeQL**, its groundbreaking query language, is declarative, functional, and strongly-typed, drastically simplifying data handling and logic. So now, even the most nested and interconnected datasets can be managed with ease. With TypeDB, we’ve reinvented the database for the modern programming era.

## Getting started

- [Deploy TypeDB](https://cloud.typedb.com) in the Cloud. Or, [download and install](https://typedb.com/docs/manual/install/CE) TypeDB Community Edition.
- Explore the basics of TypeDB in our [Quickstart](https://typedb.com/docs/home/quickstart) and [Crash Course](https://typedb.com/docs/home/crash-course).
- Master TypeDB with [TypeDB Academy](https://typedb.com/docs/academy/).
- Discover more of TypeDB’s unique [Features](https://typedb.com/features).
- Find further articles and lectures in our [Learning Center](https://typedb.com/learn).
- Stay updated with the latest TypeDB news by [subscribing to the TypeDB newsletter](https://typedb.com/?dialog=newsletter).
- Join our vibrant developer community over on our [Discord](https://typedb.com/discord) chat server.

> **IMPORTANT NOTE:** As of version 3.0, TypeDB & TypeQL are now written in [Rust](https://www.rust-lang.org)! The first Rust release went live in December 2024. TypeDB is currently in a phase of rapid iteration, with new features and patches being launched regularly. You can browse the [roadmap blog post](https://typedb.com/blog/typedb-3-roadmap). TypeDB 3.0 comes with a new storage data structure and architecture that significantly boosts performance when compared against version 2.x. We’re aiming to release preliminary benchmarks of TypeDB 3.0 in 2025.

##  Why TypeDB?

* TypeDB was crafted to natively express and combine diverse data features, allowing users to build advanced data models from a set of simple and intuitive building blocks.
* TypeDB's type system provides safety and flexibility at the same time, which makes both prototyping and building performant, production-ready data applications fast, elegant, and _enjoyable_.
* With TypeDB, and its query language TypeQL, we envision databases catching up with modern typed programming languages, allowing users to write clear, intuitive, and easy to maintain code.
* TypeDB comes with a mature ecosystem including language drivers and a graphical user interface: **TypeDB Studio!**

## Database Fundamentals

### The schema

TypeDB schemas are based on a modern type system that natively supports inheritance and interfaces, and follows a [conceptual data modeling](https://typedb.com/features#conceptual-modeling) approach, in which user-defined types subtype (based on their function) three root types: [entities](https://typedb.com/features#conceptual-modeling), [relations](https://typedb.com/features#expressive-relations), and [attributes](https://typedb.com/features#intuitive-attributes).

- *Entities* are independent objects,
- *Relations* depend on their *role* interfaces played by either entities or relations,
- *Attributes* are properties with a value that can be *owned* by entities or relations.

Interface and inheritance for these types can be combined in many ways, resulting in highly expressive ways of modeling data.

```typeql
define

attribute full-name value string;
attribute id value string;
attribute email sub id;
attribute employee-id sub id;

entity user,
    owns full-name,
    owns email @unique,
    plays mentorship:trainee;
entity employee,
    owns employee-id @key,
    plays mentorship:mentor;

relation mentorship,
    relates mentor,
    relates trainee;
```

### The query language

The query language of TypeDB is [TypeQL](https://typedb.com/docs/typeql/overview). The syntax of TypeQL is fully variablizable and provides native support for polymorphic queries. The language is based on [fully declarative and composable](https://typedb.com/features#modern-language) patterns, mirroring the structure of natural language.

```typeql
match $user isa user,
    has full-name $name,
    has email $email;
# This returns all users of any type

match $user isa employee,
    has full-name $name,
    has email $email,
    has employee-id $id;
# This returns only users who are employees

match $user-type sub user;
$user isa $user-type,
    has full-name $name,
    has email $email;
# This returns all users and their type
```

### Functions

Functions, a new concept in TypeDB 3.0 and a cornerstone of TypeQL's query model, are like subqueries you can re-use and invoke whenever you want. You can learn more about them from the [TypeQL Functions Documentation](https://typedb.com/docs/typeql/functions/).

## Effective database engineering

TypeDB breaks down the patchwork of existing database paradigms into three fundamental ingredients: [types](https://typedb.com/features#strong-type-system), [inheritance](https://typedb.com/features#conceptual-modeling), and [interfaces](https://typedb.com/features#polymorphic-queries). This provides a unified way of working with data across all database applications, that directly impacts development:

- Make use of full [object model parity](https://typedb.com/#solve-object-relational-mismatch-entirely-within-the-database) when working with OOP
- Ensure [continuous extensibility](https://typedb.com/features#conceptual-modeling) of your data model
- Work with high-level [logical abstractions](https://typedb.com/features#conceptual-modeling) eliminating the need for physical data modeling
- Let TypeDB's inference engine guarantee [data-consistency](https://typedb.com/#avoid-data-redundancy-and-ensure-data-consistency-in-real-time) at all times
- Write high-clarity code with TypeQL's [near-natural](https://typedb.com/features#modern-language) queries even for the most complex databases
- Unleash the power of [fully declarative and composable](https://typedb.com/features#modern-language) patterns onto your data

## Installation and editions

### TypeDB editions

* [TypeDB Cloud](https://cloud.typedb.com) — multi-cloud DBaaS
* [TypeDB Enterprise](mailto://enterprise@typedb.com) — allows you to deploy TypeDB Cloud in your own environment
* **TypeDB Community Edition (CE)** — Open-source edition of TypeDB ← _This repository_

For a comparison of all three editions, see the [Deploy](https://typedb.com/deploy) page on our website.

### Download and run TypeDB CE

You can download TypeDB from the [GitHub Releases](https://github.com/typedb/typedb/releases). 

Check our [Installation guide](https://typedb.com/docs/typedb/2.x/installation) to get started.

### Compiling TypeDB CE from source using Bazel

> Note: You DO NOT NEED to compile TypeDB from the source if you just want to use TypeDB. See the _"Download and Run 
> TypeDB CE"_ section above.

1. Make sure you have the following dependencies installed on your machine:
   - Java JDK 11 or higher
   - [Bazel 6.2.0 or higher](https://bazel.build/install).

2. You can build TypeDB server with TypeDB Console included with this command:

   ```sh
   $ bazel build //:assemble-typedb-all
   ```

   or either one of the following commands, depending on the targeted architecture and operating system:
   
      ```sh
      $ bazel build //:assemble-all-mac-x86_64-zip
      $ bazel build //:assemble-all-mac-arm64-zip
      $ bazel build //:assemble-all-linux-x86_64-targz
      $ bazel build //:assemble-all-linux-arm64-targz
      $ bazel build //:assemble-all-windows-x86_64-zip
      ```
   
   To build only TypeDB server, use:
   
      ```sh
      $ bazel build //:assemble-server-mac-x86_64-zip
      $ bazel build //:assemble-server-mac-arm64-zip
      $ bazel build //:assemble-server-linux-x86_64-targz
      $ bazel build //:assemble-server-linux-arm64-targz
      $ bazel build //:assemble-server-windows-x86_64-zip
      ```
   
   The commands above output to: `bazel-bin/`.

3. If you're on a Mac and would like to run any `bazel test` commands, you will need to install:
   - snappy: `brew install snappy`
   - jemalloc: `brew install jemalloc`

### Compiling TypeDB CE from source using Cargo

**For macs:**

Install prerequisites:
1. Rustup
2. `brew install protoc`

## Resources

### Developer resources

- Documentation: https://typedb.com/docs
- Discussion Forum: https://forum.typedb.com/
- Discord Chat Server: https://typedb.com/discord
- Community Projects: https://github.com/typedb-osi

### Useful links

If you want to begin your journey with TypeDB, you can explore the following resources:

* More on TypeDB's [features](https://typedb.com/features)
* In-depth dive into TypeDB's [philosophy](https://typedb.com/philosophy)
* [TypeDB Quickstart](https://typedb.com/docs/home/quickstart) and [Crash Course](https://typedb.com/docs/home/crash-course)
* [TypeDB Academy](https://typedb.com/docs/academy)
* **[TypeQL](https://github.com/typedb/typeql)**
* **[TypeDB Studio](https://github.com/typedb/typedb-studio)**

## Contributions

TypeDB and TypeQL are built using various open-source frameworks and technologies throughout its evolution. 
Today TypeDB and TypeQL use
[RocksDB](https://rocksdb.org),
[Rust](https://www.rust-lang.org/),
[pest](https://pest.rs/),
[Bazel](https://bazel.build),
[gRPC](https://grpc.io),
and [ZeroMQ](https://zeromq.org).

Thank you!

In the past, TypeDB was enabled by various open-source products and communities that we are hugely thankful to:
[Speedb](https://www.speedb.io/),
[ANTLR](https://www.antlr.org),
[Apache Cassandra](http://cassandra.apache.org), 
[Apache Hadoop](https://hadoop.apache.org), 
[Apache Spark](http://spark.apache.org), 
[Apache TinkerPop](http://tinkerpop.apache.org),
[Caffeine](https://github.com/ben-manes/caffeine),
[JanusGraph](http://janusgraph.org),
and [SCIP](https://www.scipopt.org). 

### Package hosting
Package repository hosting is graciously provided by [Cloudsmith](https://cloudsmith.com).
Cloudsmith is the only fully hosted, cloud-native, universal package management solution, that
enables your organization to create, store and share packages in any format, to any place, with total
confidence.

## Licensing

It's released under the Mozilla Public License 2.0 (MPL 2.0).
For license information, please see [LICENSE](https://github.com/typedb/typedb/blob/master/LICENSE). 
