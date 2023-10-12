[![TypeDB Studio](./docs/banner.png)](https://typedb.com/introduction)

[![Factory](https://factory.vaticle.com/api/status/vaticle/typedb/badge.svg)](https://factory.vaticle.com/vaticle/typedb)
[![CircleCI](https://circleci.com/gh/vaticle/typedb/tree/master.svg?style=shield)](https://circleci.com/gh/vaticle/typedb/tree/master)
[![GitHub release](https://img.shields.io/github/release/vaticle/typedb.svg)](https://github.com/vaticle/typedb/releases/latest)
[![Discord](https://img.shields.io/discord/665254494820368395?color=7389D8&label=chat&logo=discord&logoColor=ffffff)](https://vaticle.com/discord)
[![Discussion Forum](https://img.shields.io/discourse/https/forum.vaticle.com/topics.svg)](https://forum.vaticle.com)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typedb-796de3.svg)](https://stackoverflow.com/questions/tagged/typedb)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typeql-3dce8c.svg)](https://stackoverflow.com/questions/tagged/typeql)

TypeDB is a [polymorphic](https://typedb.com/features#polymorphic-queries) database with 
a [conceptual data model](https://typedb.com/features#conceptual-modeling),
a [strong subtyping system](https://typedb.com/features#strong-type-system),
a [symbolic reasoning engine](https://typedb.com/features#symbolic-reasoning),
and a beautiful and elegant [type-theoretic language TypeQL](https://typedb.com/features#modern-language).

- [Core philosophy of TypeDB](#core-philosophy-of-typedb)
- [Usage examples](#usage-examples)
- [Installation and editions](#installation-and-editions)
- [Resources](#resources)
- [Contributions](#contributions)
- [Licensing](#licensing)


## Core philosophy of TypeDB

### Thinking in polymorphic databases

The data model of TypeDB unifies various schools of thought on databases,
breaking down the clutter of existing database paradigms into three fundamental ideas:
[types](https://typedb.com/features#strong-type-system),
[inheritance](https://typedb.com/features#conceptual-modeling),
and [interfaces](https://typedb.com/features#polymorphic-queries).
This enables TypeDB to represent data structures polymorphically:
data is organized into logical type hierarchies by inheritances, and made interdependent via interfaces.
The polymorphic database paradigm is highly generalizable and adaptable,
and so alleviates many common headaches that we found with existing database systems.

### Concept-centric type system

The type system of TypeDB follows a [conceptual](https://typedb.com/features#conceptual-modeling) data modeling approach,
which organizes types (based on their function) into three root categories: entities,
[relations](https://typedb.com/features#expressive-relations),
and [attributes](https://typedb.com/features#intuitive-attributes).
Entities are independent concepts, relations depend on role interfaces played by either entities or relations,
and attributes are properties with a value that can interface with (namely, be owned by) entities or relations.
Interface and inheritance for these types can be combined in various ways,
leading to a high level of schema expressivity.
For example, the roles of a relation can also be overwritten by subtypes! 

### Intuitive queries and all-expressive rules

The conceptual data model and type system of TypeDB are complemented by the query language [TypeQL](https://github.com/vaticle/typeql),
which features a [fully declarative](https://typedb.com/features#modern-language) and highly composable syntax
closely mirroring the structure of natural language,
thereby providing a completely new and intuitive experience to querying even the most complex databases.
TypeDB can even query for data that is not physically stored in the database,
but instead logically inferred based on user-specified [rules](https://typedb.com/features#symbolic-reasoning).
This enables teams to cleanly separate their source data from their application logic,
often allowing for complex systems to be described by combinations of simple rules
and enabling high-level insights into these systems.

## Usage examples

### A polymorphic schema

The schema provides a structural blueprint for data organization, ensuring referential integrity in production.
Extend your data model seamlessly in TypeDB,
maintaining integrity during model updates and avoiding any query rewrites or code refactors.

```typeql
define

full-name sub attribute, value string;
id sub attribute, value string;
email sub id;
employee-id sub id;

user sub entity,
    owns full-name,
    owns email @unique;
employee sub user,
    owns employee-id @key;
```

### Polymorphic querying

Use subtyping to write polymorphic queries that return data of multiple types by querying a common supertype.
The schema is used to automatically resolve queries to retrieve all matching data.
Variablize queries to return types, roles, and data.
New types added to the schema are included in the results of pre-existing queries against their supertype,
so no refactoring is necessary.

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

### Logical abstractions

Define rules in your schema using first-order logic to derive new facts from existing data.
Reasoning can produce complex behavior from simple rules,
and reasoned facts are generated at query time using the latest data, minimizing disk usage.
TypeDB's Explanations feature functions on deductive reasoning,
so inferred data can always be traced back to its root cause.

```typeql
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

## Installation and editions

### TypeDB editions

* [TypeDB Cloud](https://cloud.typedb.com) -- multi-cloud DBaaS
* [TypeDB Enterprise](mailto://sales@vaticle.com) -- Enterprise edition of TypeDB
* **TypeDB Core** -- Open-source edition of TypeDB <--- _This repository_

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
* In-depth dive into TypeDB's [philosophy](https://typedb.com/philosophy)
* Our [TypeDB quickstart](https://typedb.com/docs/typedb/2.x/quickstart-guide)

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

## Licensing

This software is developed by [Vaticle](https://vaticle.com/).  
It's released under the GNU Affero GENERAL PUBLIC LICENSE version 3 (AGPL v.3.0).
For license information, please see [LICENSE](https://github.com/vaticle/typedb/blob/master/LICENSE). 

Vaticle also provides a commercial license for TypeDB Enterprise - get in touch with our team at 
[commercial@vaticle.com](emailto://sales@vaticle.com).

Copyright (C) 2023 Vaticle.
