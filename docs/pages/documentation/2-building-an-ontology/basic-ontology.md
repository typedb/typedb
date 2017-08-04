---
title: Define a Basic Ontology
keywords: schema
last_updated: December, 2016
tags: [graph-api, java, advanced-grakn]
summary: "Demonstrates how to create a basic ontology"
sidebar: documentation_sidebar
permalink: /documentation/building-an-ontology/basic-ontology.html
folder: documentation
comment_issue_id: 22
---

{% include warning.html content="Please note that this page is in progress and subject to revision." %}

## Introduction

{% include links.html %}

In this section we are going to run through the construction of a basic ontology. We recommend that you refer to the [Knowledge Model](../the-fundamentals/grakn-knowledge-model.html) documentation before reading this page. The process we will follow is a general guideline as to how you may start designing an ontology.

The ontology we will be building will be used for a genealogy graph used for mapping out a family tree. You can find the complete ontology, the dataset and rules that accompany it, on Github in our [sample-datasets repository](https://github.com/graknlabs/sample-datasets/tree/master/genealogy-graph).


## Identifying Entity Types

The first step is about identifying the categories of things that will be in your graph.
For example if you are modelling a retail store, valid categories may be `product`, `electronics`, `books`, etc.  It is up to you to decide the granularity of your categories.

For our genealogy graph we know that it will mostly be filled with people. So we can create an entity type:

```graql
insert
  person sub entity;
```

Naturally, we could break this up into `man` and `woman` but for this example we are going to keep things simple.  

## Describing Entity Types

Grakn provides you with the ability to attach resources to entity types. For example a `car` could have an `engine`, a `licence number`, and a `transmission type` as resources that help to describe it.

So what helps describe a `person`?
Philosophical debates aside let us go with something simple. A `person` typically has a `firstname`, a `lastname`, and a `gender`. We can model this and other resources that identify a person with:

```graql
insert

person sub entity
  has identifier
  has firstname
  has surname    
  has middlename
  has picture
  has age
  has birth-date
  has death-date
  has gender;

  identifier sub resource datatype string;
  name sub resource datatype string;
  firstname sub name datatype string;
  surname sub name datatype string;
  middlename sub name datatype string;
  picture sub resource datatype string;
  age sub resource datatype long;
  "date" sub resource datatype string;
  birth-date sub "date" datatype string;
  death-date sub "date" datatype string;
  gender sub resource datatype string;   
```	    

## Supported Resource Types
The following resource types are supported: `string`, `boolean`, `long`, `double`, `date`.

## Identifying Relationships and Roles

The next step is to ask how your data is connected, that is, what are the relationships between your data?

This can be between different entity types, for example, a `person` **drives** a `car`, or even between the same entity types, for example, a `person` **marries** another `person`.

In a Grakn, N-ary relationships are also possible. For example, a `person` has a `child` with another `person`.

In our example, we will add `marriage` and `parentship` relationships. A `marriage` has two roles: `spouse1` and `spouse2`, while `parentship` has a `parent` role and a `child` role.

```graql
insert

marriage sub relation
  relates spouse1
  relates spouse2
  has picture;

spouse1 sub role;
spouse2 sub role;

parentship sub relation
  relates parent
  relates child;

parent sub role;
child sub role;
```

## Allowing Roles to be Played

The next step is to give our entity types permission to play specific roles.  We do this explicitly so that we don't accidentally relate data which should not be related. For example, this will prevent us from accidentally saying that a `dog` and a `person` can have a child.

For this current example we only have one entity type, which can play all our current roles, so we explicitly state that with:  

```graql
insert

person sub entity
  plays parent
  plays child
  plays spouse1
  plays spouse2;
```    

We have now completed our basic genealogy ontology.

## The Complete Ontology

The final ontology will now look something like this:

```graql
insert

 # Entities

  person sub entity
    has identifier
    has firstname
    has surname
    has middlename
    has picture
    has age
    has birth-date
    has death-date
    has gender
    plays parent
    plays child
    plays spouse1
    plays spouse2;

 # Resources

  identifier sub resource datatype string;
  name sub resource datatype string;
  firstname sub name datatype string;
  surname sub name datatype string;
  middlename sub name datatype string;
  picture sub resource datatype string;
  age sub resource datatype long;
  "date" sub resource datatype string;
  birth-date sub "date" datatype string;
  death-date sub "date" datatype string;
  gender sub resource datatype string;

 # Roles and Relations

  marriage sub relation
    relates spouse1
    relates spouse2
    has picture;

  spouse1 sub role;
  spouse2 sub role;

  parentship sub relation
    relates parent
    relates child;

  parent sub role;
  child sub role;

```

![Ontology](/images/basic-ontology1.png)

## Summary

In this tutorial we described our entity type `person` across separate steps. This was done to demonstrate the typical thought process when creating an ontology. It is typically good practice to group entity type definitions together as above.

{% include note.html content="It is worth noting that the ontology does not need to be completely finalised before loading data. The ontology of a Grakn graph can be expanded even after loading data." %}

## Where Next?

We will continue to explore the development of an ontology in the next section on defining a [hierarchical ontology](./hierarchical-ontology.html).


## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/22" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.
has
