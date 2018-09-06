---
title: Migrating SQL data to Grakn
keywords: setup, getting started
tags: [migration]
summary: "This document will teach you how to migrate SQL data into Grakn."
sidebar: documentation_sidebar
permalink: /docs/migrating-data/migrating-sql
folder: docs
---

## Introduction

This tutorial shows you how to populate a knowledge graph in Grakn with SQL data, by walking through a simple example. If you wish to follow along and have not yet set up the Grakn environment, please see the [setup guide](../get-started/setup-guide).

## Migration Shell Script for SQL

The migration shell script can be found in _/bin_ directory of your Grakn environment. We will illustrate its usage in an example below:

```bash
usage: graql migrate sql -template <arg> -driver <arg> -user <arg> -pass <arg> -location <arg> -keyspace <arg> [-help] [-no] [-batch <arg>] [-uri <arg>] [-retry <arg>] [-v]

 -a,--active <arg>     Number of tasks (batches) running on the server at
                       any one time. Default 25.
 -b,--batch <arg>      Number of rows to execute in one Grakn transaction.
                       Default 25.
 -c,--config <arg>     Configuration file.
 -driver <arg>         JDBC driver
 -h,--help             Print usage message.
 -k,--keyspace <arg>   Grakn knowledge graph. Required.
 -location <arg>       JDBC url (location of DB)
 -n,--no               Write to standard out.
 -pass <arg>           JDBC password
 -q,--query <arg>      SQL Query
 -r,--retry <arg>      Retry sending tasks if engine is not available
 -t,--template <arg>   Graql template to apply to the data.
 -u,--uri <arg>        Location of Grakn Engine.
 -user <arg>           JDBC username
 -v,--verbose          Print counts of migrated data.
 -d,--debug            Migration immediatly stops if any transaction fails
```

Grakn relies on the JDBC API to connect to any RDBMS that uses the SQL language. The example that follows is written in MySQL, but SQL to Grakn migration will work with any database it can connect to using a JDBC driver. This has been tested on MySQL, Oracle and PostgresQL.

### SQL Migration Basics

The steps to migrate the SQL to GRAKN.AI are:

- define a schema for the data to derive the full benefit of a knowledge graph
- create templated Graql to map the data to the schema by instructing the migrator on how the results of a SQL query can be mapped to your schema. The SQL migrator will apply the template to each row of data in the table, replacing the indicated sections in the template with provided data. In this migrator, the column header is the key, while the content of each row at that column is the value.
- invoke the Grakn migrator through the shell script or Java API.

{% include note.html content="SQL Migration makes heavy use of the Graql templating language. You will need a foundation in Graql templating before continuing, so please read through our [migration langauge documentatino](../migrating-data/migration-language) to find out more." %}

### SQL Schema Migration

Let's say you have an SQL table with the following schema for pets, with two tables. One has details of a pet (name, species etc) and the other table records events occurring to the pet, such as having a litter:

```sql
CREATE TABLE pet
(
  name    VARCHAR(20),
  owner   VARCHAR(20),
  species VARCHAR(20),
  sex     CHAR(1),
  birth   DATE,
  death   DATE
);

DROP TABLE IF EXISTS event;

CREATE TABLE event
(
  name        VARCHAR(20),
  date        DATE,
  eventtype   VARCHAR(15),
  remark      VARCHAR(255)
);

ALTER TABLE event ADD FOREIGN KEY ( name ) REFERENCES pet ( name );
```

We can define a schema that corresponds to the SQL tables as follows:

```graql-test-ignore
insert
pet sub entity
  has name
  has owner
  has sex
  has birth
  has death
  is-abstract;

cat sub pet;
dog sub pet;
snake sub pet;
hamster sub pet;
bird sub pet;

event sub entity,
  has name,
  has date,
  has description;

name sub attribute datatype string;
owner sub attribute datatype string;
sex sub attribute datatype string;
birth sub attribute datatype string;
death sub attribute datatype string;
count sub attribute datatype long;
date sub attribute datatype date;
description sub attribute datatype string;
```

The schema is not complete at this point, as we have not included any relationship between pets and their events. In SQL, a `foreign key` is a column that references another column, as seen in the SQL schema line `ALTER TABLE event ADD FOREIGN KEY ( name ) REFERENCES pet ( name );`.

For Grakn, we can use the following:

```graql-test-ignore
insert
occurs sub relationship
  relates event-occurred
  relates pet-in-event;

pet plays pet-in-event;
event plays event-occurred;
```

To load the schema into Grakn, we create a single file that contains both sections shown above, named _schema.gql_. From the Grakn installation folder, invoke the Graql shell, passing the -f flag to indicate the schema file to load into a knowledge graph. This call starts the Graql shell in non-interactive mode, loading the specified file and exiting after the load is complete:

```
./graql console -f ./schema.gql
```

### SQL Data Migration

Lets imagine that the data in the SQL database is as follows:

**pet**

```
---------------------------------------------------------------
| name     | owner  | species | sex | birth      | death      |
---------------------------------------------------------------
| Bowser   | Diane  | dog     | m   | 1979-08-31 | 1995-07-29 |
---------------------------------------------------------------
| Fluffy   | Harold | cat     | f   | 1993-02-04 | NULL       |
---------------------------------------------------------------
| Claws    | Gwen   | cat     | m   | 1994-03-17 | NULL       |
---------------------------------------------------------------
| Buffy    | Harold | dog     | f   | 1989-05-13 | NULL       |
---------------------------------------------------------------
| Fang     | Benny  | dog     | m   | 1990-08-27 | NULL       |
---------------------------------------------------------------
| Puffball | Diane  | hamster | f   | 1999-03-30 | NULL       |
---------------------------------------------------------------
```

**event**

```
-----------------------------------------------------------------------
| name   | date       | eventtype| remark                             |
-----------------------------------------------------------------------
| Bowser | 1991-10-12 | kennel   | NULL                               |
-----------------------------------------------------------------------
| Fang   | 1991-10-12 | kennel   | NULL                               |
-----------------------------------------------------------------------
| Fluffy | 1995-05-15 | litter   | 4 kittens, 3 female, 1 male        |
-----------------------------------------------------------------------
| Buffy  | 1993-06-23 | litter   | 5 puppies, 2 female, 3 male        |
-----------------------------------------------------------------------
| Buffy  | 1994-06-19 | litter   | 3 puppies, 3 female                |
-----------------------------------------------------------------------
| Fang   | 1998-08-28 | birthday | Gave him a new chew toy            |
-----------------------------------------------------------------------
| Claws  | 1998-03-17 | birthday | Gave him a new flea collar         |
-----------------------------------------------------------------------
```

In order to migrate the pets table from the SQL database, we prepare a SQL query to extract the data:

```sql
SELECT * FROM pet;
```

We also prepare a Graql template, _pet-template.gql_ which creates instances for data according to the defined schema. The template will create an entity of the appropriate pet subtype (`cat`, `dog`, `snake`, `hamster` or `bird`) for each row returned by the query. It will attach name, owner and sex resources to each of these entities, and if the birth and death dates are present in the data, attaches those too.

```graql-skip-test
insert

$x isa <SPECIES>
  has name <NAME>
  has owner <OWNER>
  has sex <SEX>
  if(<BIRTH> != null) do { has birth <BIRTH> }
  if(<DEATH> != null) do { has death <DEATH> };
```

To apply the template above to the SQL query and populate the knowledge graph with the `pet` entities, we use Grakn migration script:

```
./graql migrate sql -q "SELECT * FROM pet;" -location jdbc:mysql://localhost:3306/world -user root -pass root -t ./pet-template.gql -k grakn
```

Similarly, to migrate the events from the table, we prepare a SQL query to extract the data:

```sql
SELECT event.name AS name,
       event.date AS date,
       event.eventtype AS description
FROM event;
```

We prepare a Graql template _event-template.gql_:

```graql-skip-test
match $pet has name <name>
insert $event isa event
  has "date" <date>
  has description <description>;
  (event-occurred: $event, pet-in-event: $pet) isa occurs;
```

To populate the knowledge graph with the `event` entities, we then use the Grakn migration script:

```
./graql migrate sql -q "SELECT event.name AS name, event.date AS date, event.eventtype AS description FROM event;" -location jdbc:mysql://localhost:3306/world -user root -pass root -t ./pet-template.gql -k grakn
```

Note: The SQL query is entered into the command line in quotes, although in future releases of Grakn, we plan to allow queries to be saved in a file, which can be specified with an appropriate flag.

At this point, the SQL data has been added to a knowledge graph in Grakn, and can be queried. For example:

```graql-skip-test
match $x isa cat; # Get all cats
match ($x, $y) isa occurs; $x isa cat; $y isa event has description "litter"; get $x; # Get all cats that have had litters of kittens
```

### In Java

While the migration seems rather lengthy when written out in Graql, you only need a few lines of code to accomplish this migration in Grakn:

```java-test-ignore
String jdbcDBUrl = "";
String jdbcUser = "root";
String jdbcPass = "root"
String KEYSPACE = "pets-example";

String sqlQuery = "SELECT * FROM pet;"
File template = "./pet-template.gql"

// get the JDBC connection
try(Connection connection = DriverManager.getConnection(jdbcDBUrl, jdbcUser, jdbcPass)) {

    Migrator migrator = Migrator.to(Grakn.DEFAULT_URI, tx.getKeyspace());

    // create migrator
    SQLMigrator sqlMigrator = new SQLMigrator(query, connection);

    // perform migration
    migrator.load(template, sqlMigrator.convert());
}
```

You can find an example of how to use the Java API for SQL migration [here](../examples/SQL-migration).

## Where Next?

You can find further documentation about migration in our API reference documentation (which is in the _docs_ directory of the distribution zip file, and also online [here](http://javadoc.io/doc/ai.grakn/grakn).
