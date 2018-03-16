---
title: Module Review
keywords: setup, getting started
last_updated: September 2017
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/schema-review.html
folder: overview
toc: false
KB: academy
---

As usual, the last lesson of the module is just a bunch of exercises to check whether you remember what you have learned. Understanding GRAKN data model and schema is particularly important, so be sure of going back to review the relevant lesson if you are struggling with some of the exercises.

Let us start with an open form question: can you describe the entity-relationship modeling process, i.e. the steps we have followed to build our conceptual model?

### Exercise 1: Extending the schema
Open the schema file you have built in this module. Imagine that you wanted to add to your knowledge graph people owning companies, how would you modify your schema? Obviously in the real world rarely a company, and especially an oil company is owned by a person, but for the sake of this example let’s pretend it is possible. Don’t forget that you do not need to add relationships and roles that are already there, make sure that a person can have a name.

How can you extend your schema so that companies can be located in countries?

Remember that our final objective is to be able to link articles to bonds via oil platforms. Add all the necessary concepts to the schema. Notice that this time you will have to introduce a new relationship and new roles.

### Exercise 2: The complete schema
It is quite rare that the first version of a schema is also the final one. Often times when you start migrating data you will find mistakes in your model or you will simply realise that you have missed something. You can find the complete schema file for the training dataset on [github](https://github.com/graknlabs/academy/blob/master/short-training/schema.gql) take your time to review it and understand what is going on and what has been added with respect to the file you have written.

### Exercise 3: Visualising the schema
Start GRAKN and open the graph visualiser (you do not remember how? Head back to the [relevant lesson](./setup.html) if you need to review the material). With the `training` keyspace selected, click on the "Type" button on the left of the query editor and then the "All types" to visualises the concepts that make up the schema of the dataset, with their hierarchy. If you ALT+Click on one of the roles (try it on the "located" node for example) you will also see the schema connections to the other concepts.

  ![Academy Schema](/images/academy/3-schema/academy-schema.png)


### Exercise 4: removing a type
Try and remove some (or all) of the types you have defined in Exercise 1. Verify with the dashboard (remember to clean the graph visualiser) that the types are no longer there.


## What you have learned so far and where to go next
In this module you have learned how to model and write, and modify a GRAKN schema. Give yourself a pat on the back, because you are now well equipped to design a GRAKN knowledge graph. [Next step](./loading-files.html) is to add some meat, i.e. learn to load the file you have composed and add data to the knowledge graph. If you want to know about GRAKN data model or GRAKN schemas, as always, head to the [docs](../index.html).
