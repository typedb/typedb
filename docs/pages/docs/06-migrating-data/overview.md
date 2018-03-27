---
title: Migration Overview
keywords: setup, getting started
tags: [getting-started, graql, migration]
summary: "Overview of migrating data into Grakn from various data sources"
sidebar: documentation_sidebar
permalink: /docs/migrating-data/overview
folder: docs
---

## Introduction
This page introduces the concept of data migration into a Grakn knowledge graph. We currently support migration of CSV, JSON, XML and SQL data. For each type of data, the steps to migrate to GRAKN.AI are:

- define a schema for the data in Graql
- create templated Graql to map the data to the schema
- invoke the Grakn migrator through the shell script or Java API.

If you have not yet set up the Grakn environment, please see the [setup guide](../get-started/setup-guide).

> **Note:** During migration reasoning is switched off. This is to ensure that data will always load consistently regardless of load order   

## Migration Shell Script
The migration shell script can be found in *grakn-dist/bin* after it has been unzipped. Usage is specific to the type of migration being performed:

+ [CSV migration documentation](./migrating-csv)
+ [JSON migration documentation](./migrating-json)
+ [SQL migration documentation](./migrating-sql)
+ [XML migration documentation](./migrating-xml)

## Using Java APIs

  Check the [Developing With Java](../java-library/migration-api) section for more details on how to migrate data using java.

## Exporting Data from Grakn

It is also possible to export data from Grakn using the migration shell script. Usage is as follows:

```bash
usage: graql migrate export -data -schema [-help] [-no] [-batch <arg>] [-uri <arg>] [-keyspace <arg>]
 -data                 export data
 -schema             export schema
 -h,--help             print usage message
 -k,--keyspace <arg>   keyspace to use
 -n,--no               dry run- write to standard out
 -u,--uri <arg>        uri to engine endpoint
 -r, --retry           Number of times to retry sending tasks if engine is not available
 -d,--debug            Migration immediatly stops if any transaction fails
```

Exporting data or the schema from Grakn, into Graql, will always redirect to standard out.

## Where Next?
You can find further documentation about the [migration language](./migration-language).
