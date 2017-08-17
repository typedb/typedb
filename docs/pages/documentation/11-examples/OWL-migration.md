---
title: An Example of Migrating OWL to Grakn
keywords: migration
last_updated: September 19, 2016
tags: [migration, examples]
summary: "A short example to illustrate migration of OWL to Grakn"
sidebar: documentation_sidebar
permalink: /documentation/examples/OWL-migration.html
folder: documentation
comment_issue_id: 27
---

For a comprehensive guide to migration, please see both our [Migration Tutorial](../migration/migration-overview.html) and our additional documentation specific to [OWL Migration](../migration/OWL-migration.html).

## Introduction
This example addresses the topic of OWL interoperability through a migration of a family tree OWL schema. The code for this example can be found in our [github repository](https://github.com/graknlabs/sample-projects/tree/master/example-owl-migration).

The sole prerequisite of this example is having the Grakn environment installed and the Engine running. If you need help starting Grakn Engine, please see the [setup guide](../get-started/setup-guide.html).

### Running the example
You can run this example by running the [`Main`](https://github.com/graknlabs/sample-projects/blob/master/example-owl-migration/src/main/java/Main.java) class. Check out the `OWLResourceMigrator` class for the bulk of the migration code.  

We run a few queries in the example to prove that the data has been migrated. After running the example, you should be able to answer the following questions (see the bottom of the page for answers - but please don't peek until you've tried it!):

+ What are the Types in the Family Tree knowledge base in Grakn?
+ How many people are in the family tree?
+ How many descendants does Eleanor Pringle have?
+ Who are the great uncles of Ethel Archer?

## Test Yourself Answers

**What are the Types in the Family Tree knowledge base?**   
Answer:

```
tThing
tPerson
tMan
tWoman
```     	

**How many people are in the family tree?**   

Answer: `411`

**How many descendants does Eleanor Pringle have?**   

Answer: `55`

**Who are the great uncles of Ethel Archer?**   
Answer:   

```
William Whitfield
Harry Whitfield
George Whitfield
Walter Whitfield
James Whitfield
```

## Where next?

After running this OWL migration, check out the [Graql documentation](../graql/graql-overview.html) and the [Graph API documentation](../developing-with-java/graph-api.html) for more instructions on how you can explore the family tree.

{% include links.html %}


## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/27" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.
