---
title: Define a Basic Schema
keywords: schema
tags: [java-api, java, advanced-grakn]
summary: "Example of building a basic schema"
sidebar: documentation_sidebar
permalink: /docs/building-schema/basic-schema
folder: docs
---

{% include warning.html content="Please note that this page is in progress and subject to revision." %}

## Introduction



In this section we are going to run through the construction of a basic schema. We recommend that you refer to the [Knowledge Model](../knowledge-model/model) documentation before reading this page. The process we will follow is a general guideline as to how you may start designing a schema.

The schema we will be building will be used for a genealogy knowledge base used for mapping out a family tree. You can find the complete schema, the dataset and rules that accompany it, on Github in our [sample-datasets repository](https://github.com/graknlabs/sample-datasets/tree/master/genealogy-knowledge-base).


## Identifying Entity Types

The first step is about identifying the categories of things that will be in your knowledge base.
For example if you are modelling a retail store, valid categories may be `product`, `electronics`, `books`, etc.  It is up to you to decide the granularity of your categories.

For our genealogy knowledge base we know that it will mostly be filled with people. So we can create an entity type:

```graql
define
  person sub entity;
```

Naturally, we could break this up into `man` and `woman` but for this example we are going to keep things simple.  

## Describing Entity Types

Grakn provides you with the ability to attach resources to entity types. For example a `car` could have an `engine`, a `licence number`, and a `transmission type` as resources that help to describe it.

So what helps describe a `person`?
Philosophical debates aside let us go with something simple. A `person` typically has a `firstname`, a `lastname`, and a `gender`. We can model this and other resources that identify a person with:

```graql
define

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

  identifier sub attribute datatype string;
  name sub attribute datatype string;
  firstname sub name datatype string;
  surname sub name datatype string;
  middlename sub name datatype string;
  picture sub attribute datatype string;
  age sub attribute datatype long;
  "date" sub attribute datatype string;
  birth-date sub "date" datatype string;
  death-date sub "date" datatype string;
  gender sub attribute datatype string;   
```

## Supported Resource Types
The following attribute types are supported: `string`, `boolean`, `long`, `double`, `date`.

## Identifying Relationships and Roles

The next step is to ask how your data is connected, that is, what are the relationships between your data?

This can be between different entity types, for example, a `person` **drives** a `car`, or even between the same entity types, for example, a `person` **marries** another `person`.

In a Grakn, N-ary relationships are also possible. For example, a `person` has a `child` with another `person`.

In our example, we will add `marriage` and `parentship` relationships. A `marriage` has two roles: `spouse1` and `spouse2`, while `parentship` has a `parent` role and a `child` role.

```graql
define

marriage sub relationship
  relates spouse1
  relates spouse2
  has picture;

parentship sub relationship
  relates parent
  relates child;
```

## Allowing Roles to be Played

The next step is to give our entity types permission to play specific roles.  We do this explicitly so that we don't accidentally relate data which should not be related. For example, this will prevent us from accidentally saying that a `dog` and a `person` can have a child.

For this current example we only have one entity type, which can play all our current roles, so we explicitly state that with:  

```graql
define

person sub entity
  plays parent
  plays child
  plays spouse1
  plays spouse2;
```    

We have now completed our basic genealogy schema.

## The Complete Schema

The final schema will now look something like this:

```graql
define

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

  identifier sub attribute datatype string;
  name sub attribute datatype string;
  firstname sub name datatype string;
  surname sub name datatype string;
  middlename sub name datatype string;
  picture sub attribute datatype string;
  age sub attribute datatype long;
  "date" sub attribute datatype string;
  birth-date sub "date" datatype string;
  death-date sub "date" datatype string;
  gender sub attribute datatype string;

 # Roles and Relations

  marriage sub relationship
    relates spouse1
    relates spouse2
    has picture;

  parentship sub relationship
    relates parent
    relates child;
```

![Schema](/images/basic-schema1.png)

## Summary

In this tutorial we described our entity type `person` across separate steps. This was done to demonstrate the typical thought process when creating a schema. It is typically good practice to group entity type definitions together as above.

{% include note.html content="It is worth noting that the schema does not need to be completely finalised before loading data. The schema of a Grakn knowledge base can be expanded even after loading data." %}

## Where Next?

We will continue to explore the development of a schema in the next section on defining a [hierarchical schema](./hierarchical-schema).
