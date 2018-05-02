---
title: Module review
keywords: setup, getting started
last_updated: April 2018
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/migration-review.html
folder: overview
toc: false
KB: academy
---

You are at the end of another module of the Academy. It is time to review what you have learned about loading data into Grakn and migrating CSV and XML files. We assume that you followed all the lessons of this module including all the exercises.

### Exercise 1: Loading files
Load the file `articles.gql` to your knowledge graph and check that the two articles about the Italian Referendum have been loaded.

Batch load the file `country-region.gql`. Have a look at the file and try to understand what it does.

Do you remember what the difference between loading a file and using the batch loader is? When should you use the normal loader?

### Exercise 2: Loading CSV files
There is no reason not to use multiple templates against the same data file to migrate different aspects of the data to your knowledge graph. Write a template file to be run against the oil platform csv file to establish relationships linking platforms to their respective owner company and relationships linking them to the countries they are located in.

Hint: you can use a single (match) insert query to migrate both relationships for each platform.

When you are done, compare it to the template you can find in your training template directory (`academy/short-training/templates`) and execute the migration against your knowledge graph.

Check that the migration was executed successfully using the graph visualiser.

### Exercise 3: Loading XML
Migrate the file `bonds.xml` using the schema `bonds.xsd` and the template file `bond-template.gql` into the knowledge graph.

Verify that the bonds have been migrated into the knowledge graph using the Graql shell.

## Test your knowledge graph!
Congratulations! You should have built your first Grakn knowledge graph at this point!

Retrieve the big Graql query from [module 2](./graql-intro.html) and test it on your knowledge graph. Did you get the same answer as you did within the "Academy" keypsace? Celebrate!

## What next?
Now that you have data in your knowledge graph, it’s time to make use of the powerful Grakn engine. It is time to look at [inference rules](./reasoner-intro.html).
