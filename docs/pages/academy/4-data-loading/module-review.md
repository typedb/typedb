---
title: Module review
keywords: setup, getting started
last_updated: September 2017
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/migration-review.html
folder: overview
toc: false
KB: academy
---

Another module of the Academy has gone, so it is time to review what you have learned about loading data into GRAKN and migrating CSV and XML files. Notice that the following assumes that you have followed the lessons of this module and done all the exercises.

### Exercise 1: Loading files
Load the file `articles.gql` to your knowledge dataset and check that the two articles about the Italian Referendum have been loaded.

Batch load the file `country-region.gql`. Have a look at the file and try to understand what it does.

Do you remember what is the difference between loading a file and using the batch loader? When should you use the normal loader?

### Exercise 2: Loading CSV files
There is no reason not to use multiple templates against the same data file to migrate different aspects of your knowledge base. Write a template file to be run against the oil platform csv file relationships linking platforms to their owner and relationships linking them to the countries they are located in.

Hint: you can use a single (match) insert query to migrate both relationships for each platform.

When you are done, compare it to the template you can find into the VM (`academy/short-training/templates`) and migrate it into your knowledge base.

Check that the migration has executed successfully using the graph visualiser.

### Exercise 3: Loading XML
Migrate the file `bonds.xml` using the schema `bonds.xsd` and the template file `bond-template.gql` into the knowledge base.

Verify that bonds have been migrated into the knowledge base using the GRAQL shell.

## Test your knowledge base!
Congratulations! You should have built your first GRAKN knowledge base at this point!

Retrieve the big GRAQL query from [module 2](./graql-intro.html) and test it on your knowledge base. Did you get the same answer as you did within the "Academy" keypsace? Celebrate!

## What next?
Now that you have data into your knowledge base, we have barely started the engine. It’s time to push the gas pedal a bit. It is time to look at [inference rules](./reasoner-intro.html).
