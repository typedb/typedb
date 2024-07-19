[![TypeDB](./docs/banner.png)](https://typedb.com/introduction)

[![Factory](https://factory.vaticle.com/api/status/vaticle/typedb/badge.svg)](https://factory.vaticle.com/vaticle/typedb)
[![CircleCI](https://circleci.com/gh/vaticle/typedb/tree/master.svg?style=shield)](https://circleci.com/gh/vaticle/typedb/tree/master)
[![GitHub release](https://img.shields.io/github/release/vaticle/typedb.svg)](https://github.com/vaticle/typedb/releases/latest)
[![Discord](https://img.shields.io/discord/665254494820368395?color=7389D8&label=chat&logo=discord&logoColor=ffffff)](https://typedb.com/discord)
[![Discussion Forum](https://img.shields.io/badge/discourse-forum-blue.svg)](https://forum.typedb.com)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typedb-796de3.svg)](https://stackoverflow.com/questions/tagged/typedb)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typeql-3dce8c.svg)](https://stackoverflow.com/questions/tagged/typeql)
[![Hosted By: Cloudsmith](https://img.shields.io/badge/OSS%20hosting%20by-cloudsmith-blue?logo=cloudsmith&style=flat)](https://cloudsmith.com)

# Introducing TypeDB

**TypeDB** is a next-gen database with a modern programming paradigm that lets you build data applications faster, safer, and more elegantly. Its intuitive and powerful data model unifies the strengths of relational, document and graph databases without their shortcomings. **TypeQL**, its groundbreaking query language, is declarative, functional, and strongly-typed, drastically simplifying data handling and logic. So now, even the most nested and interconnected datasets can be managed with ease. With TypeDB, we’ve reinvented the database for the modern programming era.

## Getting started

- Get started by [installing TypeDB](https://typedb.com/docs/home/install/overview).
- Explore the basics of TypeDB in our [Quickstart](https://typedb.com/docs/home/quickstart) and [Crash Course](https://typedb.com/docs/home/crash-course).
- Master TypeDB with [TypeDB Academy](https://typedb.com/docs/academy).
- Discover more of TypeDB’s unique [Features](https://typedb.com/features).
- Find further articles and lectures in our [Learning Center](https://typedb.com/learn).

> **IMPORTANT NOTE:** TypeDB & TypeQL are in the process of being rewritten in [Rust](https://www.rust-lang.org). There will be significant refinement to the language, and minor breaks in backwards compatibility. Learn about the changes on our [roadmap blog post](https://typedb.com/blog/typedb-3-roadmap). The biggest change to TypeDB 3.0 will be our storage data structure and architecture that significantly boosts performance. We’re aiming to release 3.0 in the summer this year, along with preliminary benchmarks of TypeDB.

##  Why TypeDB?

* TypeDB was crafted to natively express and combine diverse data features, allowing users to build advanced data models from a set of simple and intuitive building blocks.
* TypeDB's type system provides safety and flexibility at the same time, which makes both prototyping and building performant, production-ready data applications fast, elegant, and _enjoyable_.
* With TypeDB, and its query language TypeQL, we envision databases catching up with modern typed programming languages, allowing users to write clear, intuitive, and easy to maintain code.
* TypeDB comes with a mature ecosystem include language drivers and a graphical user interface: **TypeDB Studio!**


## Database Fundamentals

### The schema

TypeDB schemas are based on a modern type system that natively supports inheritance and interfaces, and follows a [conceptual data modeling](https://typedb.com/features#conceptual-modeling) approach, in which user-defined types subtype (based on their function) three root types: [entities](https://typedb.com/features#conceptual-modeling), [relations](https://typedb.com/features#expressive-relations), and [attributes](https://typedb.com/features#intuitive-attributes).

- *Entities* are independent objects,
- *Relations* depend on their *role* interfaces played by either entities or relations,
- *Attributes* are properties with a value that can be *owned* by entities or relations.

Interface and inheritance for these types can be combined in many ways, resulting in highly expressive ways of modeling data.

```php
define

full-name sub attribute, value string;
id sub attribute, value string;
email sub id;
employee-id sub id;

user sub entity,
    owns full-name,
    owns email @unique,
    plays mentorship:trainee;
employee sub user,
    owns employee-id @key,
    plays mentorship:mentor;

mentorship sub relation,
    relates mentor,
    relates trainee;
```


### The query language

The query language of TypeDB is [TypeQL](https://typedb.com/docs/typeql/overview). The syntax of TypeQL is fully variablizable and provides native support for polymorphic queries. The language is based on [fully declarative and composable](https://typedb.com/features#modern-language) patterns, mirroring the structure of natural language.

```php
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


### The inference engine

Any query in TypeDB is [semantically validated](https://typedb.com/features#strong-type-system) by TypeDB’s inference engine for consistency with the database schema. This prevents invalid schema updates and data inserts before they can affect the integrity of the database.

TypeDB can also work with data that is not physically stored in the database, but instead logically inferred based on user-specified [rules](https://typedb.com/features#symbolic-reasoning). This enables developers to cleanly separate their source data from their application logic, often allowing for complex systems to be described by combinations of simple rules.

```php
define
rule transitive-team-membership:
    when {
        (team: $team-1, member: $team-2) isa team-membership;
        (team: $team-2, member: $member) isa team-membership;
    } then {
        (team: $team-1, member: $member) isa team-membership;
    };

insert
$john isa user, has email "john@vaticle.com";
$eng isa team, has name "Engineering ";
$cloud isa team, has name "Cloud";
(team: $eng, member: $cloud) isa team-membership;
(team: $cloud, member: $john) isa team-membership;

match
$john isa user, has email "john@vaticle.com";
(team: $team, member: $john) isa team-membership;
# This will return both Cloud and Engineering for $team due to the defined rule
```
 

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
* [TypeDB Cloud self-hosted](mailto://sales@vaticle.com) — allows you to deploy TypeDB Cloud in your own environment
* **TypeDB Core** — Open-source edition of TypeDB ← _This repository_

For a comparison of all three editions, see the [Deploy](https://typedb.com/deploy) page on our website.


### Download and run TypeDB Core

You can download TypeDB from the [GitHub Releases](https://github.com/vaticle/typedb/releases). 

Check our [Installation guide](https://typedb.com/docs/typedb/2.x/installation) to get started.


### Compiling TypeDB Core from source

> Note: You DO NOT NEED to compile TypeDB from the source if you just want to use TypeDB. See the _"Download and Run 
> TypeDB Core"_ section above.

1. Make sure you have the following dependencies installed on your machine:
   - Java JDK 11 or higher
   - [Bazel 6 or higher](https://bazel.build/install).

2. You can build TypeDB with either one of the following commands, depending on the targeted architecture and 
   Operation system: 

   ```sh
   $ bazel build //:assemble-linux-x86_64-targz
   $ bazel build //:assemble-linux-arm64-targz
   $ bazel build //:assemble-mac-x86_64-zip
   $ bazel build //:assemble-mac-arm64-zip
   $ bazel build //:assemble-windows-x86_64-zip
   ```

   Outputs to: `bazel-bin/`.

3. If you're on a Mac and would like to run any `bazel test` commands, you will need to install:
   - snappy: `brew install snappy`
   - jemalloc: `brew install jemalloc`


## Resources

### Developer resources

- Documentation: https://typedb.com/docs
- Discussion Forum: https://forum.typedb.com/
- Discord Chat Server: https://typedb.com/discord
- Community Projects: https://github.com/typedb-osi


### Useful links

If you want to begin your journey with TypeDB, you can explore the following resources:

* More on TypeDB's [features](https://typedb.com/features)
* [TypeDB Quickstart](https://typedb.com/docs/home/quickstart) and [Crash Course](https://typedb.com/docs/home/crash-course)
* [TypeDB Academy](https://typedb.com/docs/academy)
* **[TypeQL](https://github.com/vaticle/typeql)**
* **[TypeDB Studio](https://github.com/vaticle/typedb-studio)**


## Contributions

TypeDB and TypeQL are built using various open-source frameworks and technologies throughout its evolution. 
Today TypeDB and TypeQL use
[Speedb](https://www.speedb.io/),
[pest](https://pest.rs/),
[SCIP](https://www.scipopt.org),
[Bazel](https://bazel.build),
[gRPC](https://grpc.io),
[ZeroMQ](https://zeromq.org), 
and [Caffeine](https://github.com/ben-manes/caffeine). 

Thank you!

In the past, TypeDB was enabled by various open-source products and communities that we are hugely thankful to:
[RocksDB](https://rocksdb.org),
[ANTLR](https://www.antlr.org),
[Apache Cassandra](http://cassandra.apache.org), 
[Apache Hadoop](https://hadoop.apache.org), 
[Apache Spark](http://spark.apache.org), 
[Apache TinkerPop](http://tinkerpop.apache.org), 
and [JanusGraph](http://janusgraph.org). 

### Package hosting
Package repository hosting is graciously provided by  [Cloudsmith](https://cloudsmith.com).
Cloudsmith is the only fully hosted, cloud-native, universal package management solution, that
enables your organization to create, store and share packages in any format, to any place, with total
confidence.

## Licensing

This software is developed by [Vaticle](https://vaticle.com/).  
It's released under the Mozilla Public License 2.0 (MPL 2.0).
For license information, please see [LICENSE](https://github.com/vaticle/typedb/blob/master/LICENSE). 

Vaticle also provides a commercial license for TypeDB Cloud self-hosted - get in touch with our team at 
[commercial@vaticle.com](emailto://sales@vaticle.com).

Copyright (C) 2023 Vaticle.
