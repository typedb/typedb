[![Factory](https://factory.vaticle.com/api/status/vaticle/typedb/badge.svg)](https://factory.vaticle.com/vaticle/typedb)
[![CircleCI](https://circleci.com/gh/vaticle/typedb/tree/master.svg?style=shield)](https://circleci.com/gh/vaticle/typedb/tree/master)
[![GitHub release](https://img.shields.io/github/release/vaticle/typedb.svg)](https://github.com/vaticle/typedb/releases/latest)
[![Discord](https://img.shields.io/discord/665254494820368395?color=7389D8&label=chat&logo=discord&logoColor=ffffff)](https://vaticle.com/discord)
[![Discussion Forum](https://img.shields.io/discourse/https/forum.vaticle.com/topics.svg)](https://forum.vaticle.com)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typedb-796de3.svg)](https://stackoverflow.com/questions/tagged/typedb)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typeql-3dce8c.svg)](https://stackoverflow.com/questions/tagged/typeql)

# Meet TypeDB (and [TypeQL](https://github.com/vaticle/typeql))

TypeDB is a strongly typed database with an intelligent reasoning engine to infer new data and a 
declarative query language based on composable patterns to find it easily.

TypeDB looks beyond relational and NoSQL databases by introducing a strong type system and extending it with inference 
and pattern matching for simple yet powerful querying.

## The benefits of strong typing

TypeDB brings the benefits of strong typing, found in modern programming languages, to the database. It allows developers to use 
abstraction, inheritance, and polymorphism when modeling and querying data. 
TypeDB entirely removes the infamous "[impedance mismatch](https://en.wikipedia.org/wiki/Object%E2%80%93relational_impedance_mismatch)" problem.

### Logical data model

Model data naturally – as the entities, relations, and attributes defined in an entity-relationship diagram. 
There's no need for separate logical and physical data models due to database limitations and operational optimizations 
(e.g., normalization). In TypeDB, the logical data model is the physical data model.

```typeql
define

# entities
employee sub entity, 
   owns full-name, 
   owns email,
   plays group-membership:member;

business-unit sub entity, 
   owns name, 
   plays group-membership:group;

# relations
group-membership sub relation,
   relates group, 
   relates member;

# attributes
full-name sub attribute, value string;
name sub attribute, value string;
email sub attribute, value string;
```

### Inheritance

Take advantage of inheritance in the database the same way developers do in OO languages, and remove one of the most 
common and painful mismatches between the logical data model and its physical data model. As an added benefit, data 
can be queried using the same vocabulary which defines it.

```typeql
define

# can be user or group
subject sub entity, abstract, owns credential;

# can be employee or contractor
user sub subject, abstract, owns full-name;

# full-name is inherited
employee sub user, owns salary;
contractor sub user, owns rate;

# can be users in business unit or role
user-group sub subject, abstract, owns name;

# name is inherited
business-unit sub user-group;
user-role sub user-group;
```

### Relations

Contextualize relations, which are first-class citizens in TypeDB. Relations define one or more roles played by entities, 
relations and attributes, each with its own name, and optionally have attributes too – just like entities, and well 
beyond a simple foreign key constraint.

```typeql
define

subject sub entity, abstract,
   plays sod-violation:of;

object sub entity, abstract, 
   plays sod-violation:on;

action sub entity, abstract,
   plays sod-policy:prohibits;

sod-policy sub relation, 
   owns name,
   relates prohibits,
   plays sod-violation:against;

sod-violation sub relation,
   relates of,
   relates on,
   relates against;
```

### Multi-valued attributes

Use multi-valued attributes rather than creating join tables or parse strings. For example, if a person has multiple hobbies, 
create multiple hobby attributes. And since attributes exist independently of entities and relations, and are shared, 
there’s no need to create lookup tables. Simply query the database to find all attributes of a specific type 
(e.g., hobby).

```typeql
define

employee sub entity, 
   owns full-name,
   owns primary-email,
   owns email-alias;

full-name sub attribute, value string;

email sub attribute, abstract, value string;

primary-email sub email, value string;
email-alias sub email, value string;

insert $e isa employee,
   has full-name "John Doe", 
   has primary-email "john.doe@vaticle.com",
   has email-alias "jdoe@vaticle.com",
   has email-alias "john@vaticle.com";
```

## The ease of pattern matching

TypeQL is a fully declarative query language based on pattern matching. There is no need for developers to tell TypeDB 
what to do (e.g., joins). Rather, they use patterns to describe what they're looking for. And because patterns are 
composable, they can be reused and combined to create new queries.

### Truly declarative

Just describe what you are looking for, and TypeDB will find it. You never have to influence how queries are executed, 
let alone explicitly tell the database what to do. TypeDB’s query language, TypeQL, is designed specifically for 
expressing what data looks like, not how to get it. There are no joins, no unions and no need for ordered query logic.

```typeql
# no need to union employee + contractor
# or join user to employee|contractor
match $u isa user;

# no need to recursively traverse a hierarchy of groups
match
   $u isa user;
   $ug isa user-group;
   $gm (group: $ug, member: $u) isa group-membership;

# no need to specify how permissions are granted
match
   $u isa user;
   $p (subject: $u) isa permission;
```

### Composable patterns

Combine discrete patterns to describe all of the data which should be included in the results of a query. 
Rather than rewriting or trying to edit a large, complex statement a la SQL, developers can change the results of 
TypeQL query by simply adding, removing or substituting patterns – making it so much easier to iterate and troubleshoot 
queries on the fly.

```typeql
match
   $u isa user, has email "john.doe@vaticle.com";
   $p (subject: $u, access: $ac) isa permission;

# add file and access patterns to narrow the results to permissions on files
match
   $u isa user, has email "john.doe@vaticle.com";
   $p (subject: $u, access: $ac) isa permission;
   $f isa file;                                         # narrow to files
   $ac (object: $f, action: $a) isa access;             # any action on files 

# add an attribute pattern to further narrow the results to WRITE permissions on files
match
   $u isa user, has email "john.doe@vaticle.com";
   $p (subject: $u, access: $ac) isa permission;   
   $f isa file;      
   $ac (object: $f, action: $a) isa access;                               
   $a has name "WRITE";                                 # narrow to writes
```

## The power of inference

TypeDB's built-in reasoning engine, a technology previously found only in knowledge-based systems, enables it to infer 
data on the fly by applying logical rules to existing data – and in turn, allows developers to query highly 
interconnected data without having to specify where or how to make the connections.

### Polymorphic queries

Feel free to query data based on abstract supertypes. For example, a query on cars (a supertype) will return all sedans, 
coupes and SUVs (subtypes). And as new subtypes are added (e.g., crossover), they will automatically be included in the 
results of queries on their supertype – no code changes necessary. There’s no need to be explicit, and query every 
subtype.

```typeql
match $e isa employee, has full-name $fn, has salary $sa;

match $c isa contractor, has full-name $fn, has rate $r;

match $u isa user, has full-name $fn;

match $bu isa business-unit, has name $n;

match $ur isa user-role, has name $n;

match $ug isa user-group, has name $n;

match $s isa subject, has credential $cr;
```

### Implicit data

Don’t worry about modeling data which can be inferred. For example, if a person plays a role in a marriage with no end 
date, we can infer their relationship status is “Married”. However, there is no need for a relationship status attribute 
on the person entity. If it doesn't exist, TypeDB’s reasoning engine will temporarily materialize it when queried.

```typeql
define

# infer implicit READ permission from explicit MODIFY permission
rule add-read-permission:
    when {
        $write isa action, has name "WRITE";
        $read isa action, has name "READ";
        $ac_write (object: $obj, action: $write) isa access;
        $ac_read (object: $obj, action: $read) isa access;
        (subject: $subj, access: $ac_write) isa permission;
    } then {
        (subject: $subj, access: $ac_read) isa permission;
    };
```

### Transitive relations

Find data based on the existence of other data, even when you don’t know what that other data is or how its related. 
It’s often impossible to describe all of the ways in which things are indirectly connected. Thankfully, TypeDB’s 
reasoning engine can discover and traverse relations on its own, allowing it to infer data on your behalf.

```typeql
define

# members of a user group are members of other user groups
# for which their user group is a member
rule transitive-group-membership:
   when {
      (group: $g1, member: $g2) isa group-membership;
      (group: $g2, member: $s) isa group-membership;
   } then {
      (group: $g1, member: $s) isa group-membership;
   };

# members in a user group inherit its permissions
rule subject-permission-inheritance:
   when {
      $s isa subject;
      (group: $g, member: $s) isa group-membership;
      (subject: $g, access: $ac) isa permission;
   } then {
      (subject: $s, access: $ac) isa permission;
   };
```

### Simple & stateful API

TypeDB Driver provide stateful objects, Sessions and Transactions, to interact with the database programmatically. 
The transactions provide [ACID guarantees](https://typedb.com/docs/typedb/2.x/development/connect#_acid_guarantees), 
up to snapshot isolation.

TypeDB's Driver API is provided through a gRPC driver, providing bi-directional streaming, compression, and strong 
message typing, that REST APIs could not provide. 

TypeDB Drivers are delivered as libraries in dedicated languages 
that provide stateful objects, Session and Transactions, for you to interact with the database programmatically.

- [Java](https://typedb.com/docs/drivers/2.x/java/java-overview)
- [Python](https://typedb.com/docs/drivers/2.x/python/python-overview)
- [Node.js](https://typedb.com/docs/drivers/2.x/node-js/node-js-overview)
- [Community drivers](https://typedb.com/docs/drivers/2.x/other-languages)

## TypeDB editions

* [TypeDB Cloud](htttps://cloud.typedb.com) -- DBaaS
* [TypeDB Enterprise](mailto://sales@vaticle.com) -- Enterprise edition of TypeDB
* TypeDB Core -- Open-source edition of TypeDB

For a comparison of all three editions, see the [Deploy](https://typedb.com/deploy) page on our website.

## Download and run TypeDB Core

You can download TypeDB from the [GitHub Releases](https://github.com/vaticle/typedb/releases). 

Check our [Installation guide](https://typedb.com/docs/typedb/2.x/installation) to get started.

## Developer resources

- Documentation: https://typedb.com/docs
- Discussion Forum: https://forum.typedb.com/
- Discord Chat Server: https://typedb.com/discord
- Community Projects: https://github.com/typedb-osi

## Compiling TypeDB Core from source

> Note: You DO NOT NEED to compile TypeDB from the source if you just want to use TypeDB. See the _"Download and Run 
> TypeDB Core"_ section above.

1. Make sure you have the following dependencies installed on your machine:
   - Java JDK 11 or higher
   - Python 3 and Pip 18.1 or higher
   - [Bazel 5 or higher](http://bazel.build/). We use [Bazelisk](https://github.com/bazelbuild/bazelisk) to manage 
     multiple Bazel versions transparently. Bazelisk runs the appropriate Bazel version for any `bazel` command as 
     specified in [`.bazelversion`](https://github.com/vaticle/typedb/blob/master/.bazelversion) file. In order to 
     install it, follow the platform-specific guide:
     - MacOS: `brew install bazelisk`
     - Linux: `wget https://github.com/bazelbuild/bazelisk/releases/download/v1.4.0/bazelisk-linux-amd64 -O /usr/local/bin/bazel`

2. You can build TypeDB with either one of the following commands, depending on the targeted Operation system: 
   ```sh
   $ bazel build //:assemble-linux-targz
   ```
   Outputs to: `bazel-bin/typedb-all-linux.tar.gz`
   ```sh
   $ bazel build //:assemble-mac-zip
   ```
   Outputs to: `bazel-bin/typedb-all-mac.zip`
   ```sh
   $ bazel build //:assemble-windows-zip
   ```
   Outputs to: `bazel-bin/typedb-all-windows.zip`

3. If you're on a Mac and would like to run any `bazel test` commands, you will need to install:
   - snappy: `brew install snappy`
   - jemalloc: `brew install jemalloc`

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

Copyright (C) 2023 Vaticle
