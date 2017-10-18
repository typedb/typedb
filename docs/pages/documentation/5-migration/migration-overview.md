---
title: Migration Overview
keywords: setup, getting started
last_updated: February 2017
tags: [getting-started, graql, migration]
summary: "Landing page for documentation about loading data in different formats to populate a knowledge base in Grakn."
sidebar: documentation_sidebar
permalink: /documentation/migration/migration-overview.html
folder: documentation
---

## Introduction
This page introduces the concept of data migration into a Grakn knowledge base. We currently support migration of CSV, JSON, XML and SQL data. For each type of data, the steps to migrate to GRAKN.AI are:

- define a schema for the data in Graql
- create templated Graql to map the data to the schema
- invoke the Grakn migrator through the shell script or Java API.

If you have not yet set up the Grakn environment, please see the [setup guide](../get-started/setup-guide.html).

## Migration Shell Script
The migration shell script can be found in *grakn-dist/bin* after it has been unzipped. Usage is specific to the type of migration being performed:

+ [CSV migration documentation](./CSV-migration.html)
+ [JSON migration documentation](./JSON-migration.html)
+ [SQL migration documentation](./SQL-migration.html)
+ [XML migration documentation](./XML-migration.html)

## Using Java APIs

Check the [Developing With Java](../developing-with-java/migration-api.html) section for more details on how to migrate data using java.

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
You can find further documentation about migration in our API reference documentation (which is in the */docs* directory of the distribution zip file, and also online [here](https://grakn.ai/javadocs.html).

{% include links.html %}
