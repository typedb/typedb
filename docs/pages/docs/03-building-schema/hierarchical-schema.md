---
title: Define a Hierarchical Schema
keywords: overview
tags: [graql, java, java-api]
summary: "How to build a hierarchical schema"
sidebar: documentation_sidebar
permalink: /docs/building-schema/hierarchical-schema
folder: docs
---

{% include warning.html content="Please note that this page is in progress and subject to revision." %}

## Introduction

In this section we are going to expand the schema we defined in the [Basic Schema documentation](./basic-schema), which we recommend you read before starting here. You may also find it helpful to refer to the [Knowledge Model](../knowledge-model/model) documentation.
We are going to introduce the idea of making ontologies deeper and more meaningful by defining a hierarchy of types.

When we left off, our schema looked as follows:

![Schema](/images/basic-schema1.png)

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

This schema represents a genealogy knowledge graph which models a family tree.
This is a very simplistic schema with plenty of room for extension, so let's begin!

## Hierarchies of Entity Types

It is possible to define entity types more granularly. Think of sub-categories that enable additional details to be embedded in the schema.
For example, if we have a entity type called `vehicle`, we can break that down further by differentiating between `cars` and `motorbikes`. This can be done as follows:

```graql
define

vehicle sub entity;
car sub vehicle;
motorbikes sub vehicle;
```    

In the above example we are saying that a `car` is a subtype (a specialised type) of a `vehicle`. This means that when adding data to our knowledge graph, when we know we have a `vehicle`, we can also differentiate between a `car` and a `motorbike`.

So how can we use this technique to improve our existing genealogy schema?

We could specialise the `person` entity into `man` and `woman` for example. However, for the sake of making things more interesting, we are going to introduce a new entity to the knowledge graph. A family is made up not only of people but of events, like births, weddings, funerals, and many others, which link those people together and better define their lives.

We can model this as follows:

```graql-test-ignore
define

event sub entity
  is-abstract
  has degree
  has confidence
  has "date"
  has identifier
  has notes
  has conclusion
  has happening;

wedding sub event;

funeral sub event
  has death-date;

christening sub event
  has birth-date;

birth sub event
  has firstname
  has middlename
  has surname
  has gender
  has birth-date;

death sub event
  has death-date;

# Resources
  degree sub attribute datatype string;
  confidence sub attribute datatype string;
  "date" sub attribute datatype string;
  identifier sub attribute datatype string;
  notes sub attribute datatype string;
  conclusion sub attribute datatype string;
  happening sub attribute datatype string;

```

Notice that for the `event` entity type we added `is-abstract`, this is an optional additional restriction to ensure that we do not create any instances of `event`, but instead use the most granular definitions provided, i.e. `birth`, `death`, etc . . .  

## Hierarchies of Relationship Types and Roles

Grakn also allows you to design hierarchies of relationship types and role types, enabling the schema to be deeper and more expressive. For example, if we have a relationship type called `partnership` between two people we can expand on this by defining more detailed partnerships; `civil-partnership`, `marriage`, `unions`, etc.

Now lets take a look at expanding our genealogy schema. When modelling a domain there are many ways of doing so. For this example we are going to redo the `marriage` relationship type so that it can provide more meaning:

```graql
define

relatives sub relationship
  is-abstract;

marriage sub relatives
  relates spouse
  relates spouse1 as spouse
  relates spouse2 as spouse
  relates husband as spouse
  relates wife as spouse
  has "date";

```


We have defined a new super type called `relatives` which enables us to link generic relatives together, and we have said that marriage is a type of relative relationship. We have also expanded on the roles which make up a marriage, enabling us to be more expressive and detailed about the domain we are modelling.
From now on, we can be clear if a person is a `husband` or a `wife` or just a `spouse` in a marriage. Note that, when we query for people who play the role of a `spouse` we will get all the `husbands` and `wives` as well.


Lets expand this even further:

```graql
define

parentship sub relatives
  relates parent
  relates mother as parent
  relates father as parent
  relates child
  relates son as child
  relates daughter as child;

```

Now we have provided more detail about being a parent.
We have also said that being a parent is a `relatives` relationship.
This is quite useful because when we ask for all relatives we will be getting relatives via birth and via marriage.

## Wrapping up

We could go into far more detail regarding our genealogy knowledge graph but I will leave that to you.
For the moment here is our more complex schema to get you started on making your own deeper ontologies.

```graql-test-ignore

define

# Entities

  person sub entity
    has gender
    has birth-date
    has death-date
    has identifier
    has firstname
    has middlename
    has surname
    plays spouse
    plays parent
    plays child;

    gender sub attribute datatype string;
    birth-date sub "date";
    death-date sub "date";
    name sub attribute datatype string;
    firstname sub name;
    middlename sub name;
    surname sub name;
    identifier sub attribute datatype string;

  event sub entity
    is-abstract
    has degree
    has confidence
    has "date"
    has identifier
    has notes
    has conclusion
    has happening;

  wedding sub event;

  funeral sub event
    has death-date;

  christening sub event
    has birth-date;

  birth sub event
    has firstname
    has middlename
    has surname
    has gender
    has birth-date;

  death sub event
    has death-date;   	    

## Relations

  relatives sub relationship
    is-abstract;

  marriage sub relatives
    relates spouse
    relates spouse1 as spouse
    relates spouse2 as spouse
    relates husband as spouse
    relates wife as spouse
    has "date";

  parentship sub relatives
    relates parent
    relates mother as parent
    relates father as parent
    relates child
    relates son as child
    relates daughter as child;

## Attributes
  "date" sub attribute datatype string;
  notes sub attribute datatype string;
  happening sub attribute datatype string;
  degree sub attribute datatype string;
  conclusion sub attribute datatype string;
  confidence sub attribute datatype string;

```



## Where Next?

We will continue to explore the development of a schema in the next section on defining a [rule-driven schema](./rule-driven-schema).

You can find the complete schema for our genealogy knowledge graph project, the dataset and rules that accompany it, on Github in the [examples](https://github.com/graknlabs/grakn/tree/master/grakn-dist/src/examples) part of the distribution.
