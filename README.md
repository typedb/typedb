[![Grabl](https://grabl.io/api/status/vaticle/typedb/badge.svg)](https://grabl.io/vaticle/typedb)
[![CircleCI](https://circleci.com/gh/vaticle/typedb/tree/master.svg?style=shield)](https://circleci.com/gh/vaticle/typedb/tree/master)
[![GitHub release](https://img.shields.io/github/release/vaticle/typedb.svg)](https://github.com/vaticle/typedb/releases/latest)
[![Discord](https://img.shields.io/discord/665254494820368395?color=7389D8&label=chat&logo=discord&logoColor=ffffff)](https://vaticle.com/discord)
[![Discussion Forum](https://img.shields.io/discourse/https/forum.vaticle.com/topics.svg)](https://forum.vaticle.com)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typedb-796de3.svg)](https://stackoverflow.com/questions/tagged/typedb)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typeql-3dce8c.svg)](https://stackoverflow.com/questions/tagged/typeql)

# Meet TypeDB (and [TypeQL](https://github.com/vaticle/typeql))

TypeDB is a strongly-typed database with a rich and logical type system. TypeDB empowers you to tackle complex problems, and TypeQL is its query language.

## A higher level of expressivity

TypeDB allows you to model your domain based on logical and object-oriented principles. Composed of entity, relationship, and attribute types, as well as type hierarchies, roles, and rules, TypeDB allows you to think higher-level as opposed to join-tables, columns, documents, vertices, edges, and properties.

### Entity-Relationship Model

TypeDB allows you to model your domain using the well-known Entity-Relationship model. It is composed of entity types, relation types, and attribute types, with the introduction of role types. TypeDB allows you to leverage the full expressivity of the ER model, and describe your schema through first normal form.

```typeql
define

person sub entity,
  owns name,
  plays employment:employee;
company sub entity,
  owns name,
  plays employment:employer;
employment sub relation,
  relates employee,
  relates employer;
name sub attribute,
  value string;
```

### Type Hierarchies

TypeDB allows you to easily model type inheritance into your domain model. Following logical and object-oriented principle, TypeDB allows data types to inherit the behaviours and properties of their supertypes. Complex data structures become reusable, and data interpretation becomes richer through polymorphism.

```typeql
define

person sub entity,
  owns first-name,
  owns last-name;

student sub person;
undergrad sub student;
postgrad sub student;

teacher sub person;
supervisor sub teacher;
professor sub teacher;
```


### N-ary Relations

In the real world, relations aren't just binary connections between two things. In rich systems, we often need to capture three or more things related with each other at once. Representing them as separate binary relationships would lose information. TypeDB can naturally represent arbitrary number of things as one relation.

```typeql
match
 
$person isa person, has name "Leonardo";
$character isa character, has name "Jack";
$movie isa movie;
(actor: $person, character: $character, movie: $movie) isa cast;
get $movie;
 
answers>>
 
$movie isa movie, has name "Titanic";
```


### Nested Relations

Relations are concepts we use to describe the association between two or more things. Sometimes, those things can be relations themselves. TypeDB can represent these structures naturally, as it enables relations to be nested in another relation, allowing you to express the model of your system in the most natural form.

```typeql
match
 
$alice isa person, has name "Alice";
$bob isa person, has name "Bob";
$mar ($alice, $bob) isa marriage;
$city isa city;
($mar, $city) isa located;
 
answers>>
 
$city isa city, has name "London";
```


## A higher degree of safety

Types provide a way to describe the logical structures of your data, allowing TypeDB to validate that your code inserts and queries data correctly. Query validation goes beyond static type checking, and includes logical validations of meaningless queries. With strict type-checking errors, you have a dataset that you can trust.

### Logical Data Validation

Inserted data gets validated beyond static type checking of attribute value types. Entities are validated to only have the correct attributes, and relations are validated to only relate things that are logically allowed. TypeDB performs richer validation of inserted entities and relations by evaluating the polymorphic types of the things involved.

```typeql
insert

$charlie isa person, has name "Charlie";
$dataCo isa company, has name "DataCo";
(husband: $charlie, wife: $dataCo) isa marriage; # invalid relation

commit>>

ERROR: invalid data detected during type validation
```


### Logical Query Validation

Read queries executed on TypeDB go through a type resolution process. This process not only optimises the query's execution, but also acts as a static type checker to reject meaningless and unsatisfiable queries, as they are likely a user error.

```typeql
match

$alice isa person, has name "Alice";
$bob isa person, has name "Bob";
($alice, $bob) isa marriage;
$dataCo isa company, has name "DataCo";
($bob, $dataCo) isa marriage; # invalid relation

answers>>

ERROR: unsatisfiable query detected during type resolution
```

## Evolved with logical inference

TypeDB encodes your data for logical interpretation by a reasoning engine. It enables type-inference and rule-inference that creates logical abstractions of data. This allows the discovery of facts and patterns that would otherwise be too hard to find; and complex queries become much simpler.

### Rules

TypeDB allows you to define rules in your schema. This extends the expressivity of your model as it enables the system to derive new conclusions when a certain logical form in your dataset is satisfied. Like functions in programming, rules can chain onto one another, creating abstractions of behaviour at the data level.

```typeql
define

rule transitive-location:
when {
  (located: $x, locating: $y);
  (located: $y, locating: $z);
} then {
  (located: $x, locating: $z);
};
```

### Inference

TypeDB's inference facility translates one query into all of its possible interpretations. This happens through two mechanisms: type-based and rule-based inference. Not only does this derive new conclusions and uncovers relationships that would otherwise be hidden, but it also enables the abstraction of complex patterns into simple queries.

```typeql
match

$person isa person;
$uk isa country, has name "UK";
($person, $uk) isa location;
get $person;

answers>>

$person isa teacher, has name "Alice";
$person isa postgrad, has name "Bob";
```

## A robust, programmatic API

TypeDB's API is provided through a gRPC client, built with robust functionalities that REST cannot provide. TypeDB Clients provide stateful objects, Sessions and Transactions, to interact with the database programmatically. The transactions provide ACID guarantees, up to snapshot isolation.

### Simple & Stateful API

TypeDB's API is provided through a gRPC client, providing bi-directional streaming, compression, and strong message typing, that REST APIs could not provide. TypeDB Clients are delivered as libraries in dedicated languages that provide stateful objects, Session and Transactions, for you to interact with the database programmatically.

#### Java

```java
try (TypeDBClient client = TypeDB.coreClient("localhost:1729")) {
    try (TypeDBSession session = client.session("my-typedb", DATA)) {
        try (TypeDBTransaction tx = session.transaction(WRITE)) {
            tx.query().insert(TypeQL.insert(var().isa("person")));
            tx.commit();
        }
        try (TypeDBTransaction tx = session.transaction(READ)) {
            Stream<ConceptMap> answers = tx.query().match(TypeQL.match(var("x").isa("person")));
        }
    }
}
```

#### Python

```python
with TypeDB.core_client("localhost:1729") as client:
    with client.session("my-typedb", SessionType.DATA) as session:
        with session.transaction(TransactionType.WRITE) as tx:
            tx.query().insert("insert $_ isa person;")
            tx.commit()
        
        with session.transaction(TransactionType.READ) as tx:
            answers: Iterator[ConceptMap] = tx.query().match("match $x isa person")
```

#### Node.js

```js
let client, session, tx;
try {
    client = TypeDB.coreClient("localhost:1729");
    session = await client.session("my-typedb");
    tx = session.transaction(TransactionType.WRITE);
    tx.query().insert("insert $_ isa person");
    tx.commit()
    tx = session.transaction(TransactionType.READ);
    const answer = tx.query().match("match $x isa person");
} finally {
    if (tx) tx.close(); if (session) session.close(); if (client) client.close();
}
```

#### Other languages

TypeDB Clients developed by the community:
- Julia (completed): https://github.com/Humans-of-Julia/TypeDBClient.jl
- Haskell (under development): https://github.com/typedb-osi/typedb-client-haskell
- Go (under development): https://github.com/taliesins/typedb-client-go

### ACID Transactions

TypeDB provides ACID guarantees, up to Snapshot Isolation, through of schema validation and consistent transactions. With lightweight optimistic transactions, TypeDB allows a high number of concurrent read and write transactions. With atomic all-or-nothing commits, transactional semantics become easy to reason over.

```
$ ./typedb console
>
> transaction my-typedb data write
my-typedb::data::write> insert $x isa person;
my-typedb::data::write> rollback
my-typedb::data::write> insert $x isa person;
my-typedb::data::write> commit
>
> transaction my-typedb data read
my-typedb::data::read> match $x isa person;
...
```

## Download and Run TypeDB

You can download TypeDB from the [Download Centre](https://vaticle.com/download) or [GitHub Releases](https://github.com/vaticle/typedb/releases). Make sure have Java 11 or higher (OpenJDK or Oracle Java) installed. Visit the [installation documentation](https://docs.vaticle.com/docs/running-typedb/install-and-run) to get started.

## Developer Resources

- Documentation: https://docs.vaticle.com
- Discussion Forum: https://forum.vaticle.com
- Discord Chat Server: https://vaticle.com/discord
- Community Projects: https://github.com/typedb-osi

## Compiling TypeDB from Source

> Note: You DO NOT NEED to compile TypeDB from the source if you just want to use TypeDB. See the _"Download and Run TypeDB"_ section above.

1. Make sure you have the following dependencies installed on your machine:
    - Java JDK 11 or higher
    - Python 3 and Pip 18.1 or higher
    - [Bazel 4 or higher](http://bazel.build/). We use [Bazelisk](https://github.com/bazelbuild/bazelisk) to manage Bazel versions which runs the build with the Bazel version specified in [`.bazelversion`](https://github.com/vaticle/typedb/blob/master/.bazelversion). In order to install it, follow the platform-specific guide:
        - macOS (Darwin): `brew install bazelbuild/tap/bazelisk`
        - Linux: `wget https://github.com/bazelbuild/bazelisk/releases/download/v1.4.0/bazelisk-linux-amd64 -O /usr/local/bin/bazel`

2. Depending on your Operating System, you can build TypeDB with either one of the following commands: 
   ```
   $ bazel build //:assemble-linux-targz
   ```
   Outputs to: `bazel-bin/typedb-all-linux.tar.gz`
   ```
   $ bazel build //:assemble-mac-zip
   ```
   Outputs to: `bazel-bin/typedb-all-mac.zip`
   ```
   $ bazel build //:assemble-windows-zip
   ```
   Outputs to: `bazel-bin/typedb-all-windows.zip`

3. If you're on a mac and would like to run any `bazel test` commands, you will need to instal:
   - snappy: `brew install snappy`
   - jemalloc: `brew install jemalloc`

## Contributions

TypeDB & TypeQL has been built using various open-source frameworks throughout its evolution. Today TypeDB & TypeQL is built using [RocksDB](https://rocksdb.org), [ANTLR](http://www.antlr.org), [SCIP](https://www.scipopt.org), [Bazel](https://bazel.build), [GRPC](https://grpc.io), and [ZeroMQ](https://zeromq.org), and [Caffeine](https://github.com/ben-manes/caffeine). In the past, TypeDB was enabled by various open-source technologies and communities that we are hugely thankful to: [Apache Cassandra](http://cassandra.apache.org), [Apache Hadoop](https://hadoop.apache.org), [Apache Spark](http://spark.apache.org), [Apache TinkerPop](http://tinkerpop.apache.org), and [JanusGraph](http://janusgraph.org). Thank you!

## Licensing

This software is developed by [Vaticle](https://vaticle.com/).  It's released under the GNU Affero GENERAL PUBLIC LICENSE, Version 3, 19 November 2007. For license information, please see [LICENSE](https://github.com/vaticle/typedb/blob/master/LICENSE). Vaticle also provides a commercial license for TypeDB Cluster - get in touch with our team at commercial@vaticle.com.

Copyright (C) 2022 Vaticle
