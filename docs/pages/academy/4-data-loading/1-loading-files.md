---
title: Loading data - Building knowledge
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you will learn how to load schema and data GRAQL files into your GRAKN distribution.
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/loading-files.html
folder: overview
toc: false
KB: academy
---

Following the lessons of the Academy you should be by now at a point where you have a solid grasp on the basics of GRAQL, the GRAKN object model, you should be able to understand what a GRAKN schema is and how to build one.

The problem at this point is: how can you load the schema you have created? And how to add data to you knowledge graph when you have loaded the schema?

Of course, you could use the GRAQL shell, one `define` or `insert` query at a time ([review the topic](./insert-delete-queries.html) if you need a refresher), but that would not be efficient. We need a way of migrating data files into the knowledge graph.
And this is what we will be talking about in this module.

## Loading files
The first thing we need to build our knowledge graph is to load our schema file. To load a file in GRAKN we will use the command line.
In the VM, from the directory  `/grakn` you will  have to use the command

`./graql console -k KEYSPACE -f FILE_TO_LOAD`

You can pick whatever name for the KEYSPACE as long as it does not already exist. Try something like `exercise`, `mydataset` or whatever you prefer. The `-f` option just stands or "file".

The schema file for to be loaded can be found in the VM in the directory `academy/short-training/schema.gql`.

Go ahead and try and load the schema in your VM in a new keyspace.

The command is (to be launched from the home directory: just run `cd` in the VM to be sure you are in it)

```bash
grakn/graql console -k yourkeyspace -f academy/short-training/schema.gql
```

To check whether it has loaded, open the Graph Visualiser in the correct keyspace and click on the `All Types` button. It should look like the following:

  ![Academy schema](/images/academy/3-schema/academy-schema.png)

As an exercise load the file `companies.gql` (which is in the same location as the schema in the subfolder `data`) and check that it has loaded by running the query

```graql
match $x isa company; get;
```


## Batch loading

What the `-f` command you learned about does is, basically, execute all the queries that are in the file one at a time. This is not particularly efficient especially when you want to load a lot of data.

When the file you have to load contains queries that can be executed in parallel (i.e. when the order in which the query are executed does not matter), you want to "batch load the file". To do so, just use the option `-b` instead of `-f`. The rest of the command is the same and the file will be loaded much faster.

Two things to keep in mind:

  * You cannot batch load a schema file. For that you need the normal file loading.
  * Just to reiterate: when batch loading, you cannot guarantee the order in which the queries in the file will be executed, so if the order matters, you have to use `-f`

As an exercise, batch load the file `countries.gql` (you can see how it looks like [here](https://github.com/graknlabs/academy/blob/master/short-training/data/countries.gql) and you can find it as well in the VM in the directory `academy/short-training/data`). To check that it has loaded, run in the dashboard the query

```graql
match $x isa country; get;
```

### What have you learned?
You have learned how to load Graql files, both with the simple loader and as a batch load. As a consequence, you should have managed to create your first GRAKN knowledge graph and load some data into it.

Isn’t it exciting?

## What next?
The knowledge graph is far from complete yet. In the [next lesson](./csv-migration) you will start learning about the GRAQL [templating language](https://en.wikipedia.org/wiki/Template_processor) and how to load structured data files into GRAKN. It is a good idea at this point to have a look at how the various file that we are using look like you can either find them into the VM, if you are confident enough with the command line or you can download them from [github](https://github.com/graknlabs/academy.git) and open them with your favourite text editor.
