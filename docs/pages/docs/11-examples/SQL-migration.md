---
title: An Example of Migrating SQL to Grakn
keywords: migration
tags: [migration, examples, java]
summary: "A short example to illustrate migration of SQL to Grakn"
sidebar: documentation_sidebar
permalink: /docs/examples/SQL-migration
folder: docs
---

If you have not yet set up the Grakn environment, please see the [setup guide](../get-started/setup-guide). For a comprehensive guide to migration, please see both our [Migration Tutorial](../migrating-data/overview) and our additional documentation specific to [SQL Migration](../migrating-data/migrating-sql).

## Migrating the World

In this example, we will show you how to import SQL data, from a relational database, to Grakn. The code for this example can be found in our [github repo](https://github.com/graknlabs/sample-projects/tree/master/example-sql-migration). We have a [blog post](https://blog.grakn.ai/populating-mindmapsdb-with-the-world-5b2445aee60c#) that also discusses SQL migration, using the migration shell script.

### Prerequisites

#### MySQL

To run this example, you should have set up [MySQL](http://dev.mysql.com/doc/mysql-getting-started/en/) and the [MySQL world](http://dev.mysql.com/doc/world-setup/en/world-setup-installation.html) example.

Once you have the SQL database loaded, you need to allow the Grakn migration default user (username: `mindmaps`, password: `mindmaps`) access, which you can do with the following command from within the MySQL shell:

```sql
CREATE USER 'mindmaps'@'localhost' IDENTIFIED BY 'mindmaps';
GRANT ALL PRIVILEGES ON *.* TO 'mindmaps'@'localhost'
    WITH GRANT OPTION;
```

This will allow the migration example access to your new MySQL world database.

#### Grakn Engine

Grakn Engine needs to be running for this example to work. If you need help starting Engine, please see the [Setup guide](../get-started/setup-guide).

### Running the Example in Java

You can run this example by running the `Main` class. Look at the `SqlWorldMigrator` class for the bulk of the migration code.  

We run a few queries in the example to prove that the data has been migrated. After running the example, you should be able to answer the following questions (see the bottom of the page for answers - but please don't peek until you've tried it!):

+ What are the Types in the World knowledge base in Grakn?
+ How many countries are in the world?
+ How may cities are in the world?
+ What are the cities in Niger?

(We do not provide any guarantees for data integrity. Data is provided by MySQL.)


## Test Yourself Answers

You can see the code to answer these questions in the function `printInformationAboutWorld()`.



```
Migrated Types:
[24800] - Base Type [ENTITY_TYPE] - Id [24800]  - Label [protest] - Abstract [false]
[28896] - Base Type [ENTITY_TYPE] - Id [28896]  - Label [municipality] - Abstract [false]
[37104] - Base Type [ENTITY_TYPE] - Id [37104]  - Label [suburab] - Abstract [false]
[122888416] - Base Type [ENTITY_TYPE] - Id [122888416]  - Label [district] - Abstract [false]
[122900704] - Base Type [ENTITY_TYPE] - Id [122900704]  - Label [country] - Abstract [false]
[20600] - Base Type [ENTITY_TYPE] - Id [20600]  - Label [province] - Abstract [false]
[24600] - Base Type [ENTITY_TYPE] - Id [24600]  - Label [origin] - Abstract [false]
[28792] - Base Type [ENTITY_TYPE] - Id [28792]  - Label [city] - Abstract [false]
[32888] - Base Type [ENTITY_TYPE] - Id [32888]  - Label [police-station] - Abstract [false]
[81924200] - Base Type [ENTITY_TYPE] - Id [81924200]  - Label [region] - Abstract [false]
[122892488] - Base Type [ENTITY_TYPE] - Id [122892488]  - Label [continent] - Abstract [false]
[122933272] - Base Type [ENTITY_TYPE] - Id [122933272]  - Label [language] - Abstract [false]

239 countries in our world

4202 cities in our world

Cities in Niger:
Zinder
Niamey
Maradi
```


## Where Next?

After running this SQL migration, check out the [Graql documentation](../querying-data/overview) and the [Java API documentation](../java-library/core-api) for more instructions on how you can explore the world.
