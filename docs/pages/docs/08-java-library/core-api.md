---
title: Core API
keywords: java
tags: [java]
summary: "The Core API."
sidebar: documentation_sidebar
permalink: /docs/java-library/core-api
folder: docs
---

The Core API is the low level API that encapsulates the [Grakn knowledge model](../knowledge-model/model). The API provides Java object constructs for ontological elements (entity types, relationship types, etc.) and data instances (entities, relationships, etc.), allowing you to build a knowledge graph programmatically.

To get set up to use this API, please read through our [Setup Guide](../get-started/setup-guide) and guide to [starting Java development with GRAKN.AI](./setup).

## Core API

On this page we will focus primarily on the methods provided by the `GraknTx` interface which is used by all knowledge graph mutation operations executed by Graql statements. If you are primarily interested in mutating the knowledge graph, as well as doing simple concept lookups the `GraknTx` interface will be sufficient.

It is also possible to interact with the knowledge graph using a Core API to form Graql queries via `GraknTx.graql()`, which is discussed separately [here](./graql-api), and is best suited for advanced querying.

## Building a Schema with the Core API

In the [Basic Schema documentation](../building-schema/basic-schema) we introduced a simple schema built using Graql.
Let's see how we can build the same schema exclusively via the Core API.
First we need a knowledge graph. For this example we will just use an
[in-memory knowledge graph](./setup#initialising-a-transaction-on-the-knowledge-base):

```java
GraknTx tx = Grakn.session(Grakn.IN_MEMORY, "myknowlegdebase").open(GraknTxType.WRITE);
```

We need to define our constructs before we can use them. We will begin by defining our attribute types since they are used everywhere. In Graql, they were defined as follows:

```graql
define

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

These same attribute types can be built with the Core API as follows:

```java
AttributeType identifier = tx.putAttributeType("identifier", AttributeType.DataType.STRING);
AttributeType firstname = tx.putAttributeType("firstname", AttributeType.DataType.STRING);
AttributeType surname = tx.putAttributeType("surname", AttributeType.DataType.STRING);
AttributeType middlename = tx.putAttributeType("middlename", AttributeType.DataType.STRING);
AttributeType picture = tx.putAttributeType("picture", AttributeType.DataType.STRING);
AttributeType age = tx.putAttributeType("age", AttributeType.DataType.LONG);
AttributeType birthDate = tx.putAttributeType("birth-date", AttributeType.DataType.STRING);
AttributeType deathDate = tx.putAttributeType("death-date", AttributeType.DataType.STRING);
AttributeType gender = tx.putAttributeType("gender", AttributeType.DataType.STRING);
```

Now the role and relationship types. In Graql:

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

Using the Core API:

```java
Role spouse1 = tx.putRole("spouse1");
Role spouse2 = tx.putRole("spouse2");
RelationshipType marriage = tx.putRelationshipType("marriage")
                            .relates(spouse1)
                            .relates(spouse2);
marriage.attribute(picture);

Role parent = tx.putRole("parent");
Role child = tx.putRole("child");
RelationshipType parentship = tx.putRelationshipType("parentship")
                            .relates(parent)
                            .relates(child);
```

Now the entity types. First, in Graql:

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
  has gender
  plays parent
  plays child
  plays spouse1
  plays spouse2;
```

Using the Core API:

```java
EntityType person = tx.putEntityType("person")
                        .plays(parent)
                        .plays(child)
                        .plays(spouse1)
                        .plays(spouse2);

person.attribute(identifier);
person.attribute(firstname);
person.attribute(surname);
person.attribute(middlename);
person.attribute(picture);
person.attribute(age);
person.attribute(birthDate);
person.attribute(deathDate);
person.attribute(gender);
```

Now to commit the schema using the Core API:

```java
tx.commit();
```

If you do not wish to commit the schema you can revert your changes with:

```java
tx.abort();
```

{% include note.html content="When using the in-memory knowledge graph, mutations to the knowledge graph are performed directly." %}


## Loading Data

Now that we have created the schema, we can load in some data using the Core API. We can compare how a Graql statement maps to the Core API. First, the Graql:

```graql
insert $x isa person has firstname "John";
```

Now the equivalent Core API:    

```java
tx = Grakn.session(Grakn.IN_MEMORY, "myknowlegdebase").open(GraknTxType.WRITE);

Attribute johnName = firstname.putAttribute("John"); //Create the attribute
person.addEntity().attribute(johnName); //Link it to an entity
```   

What if we want to create a relationship between some entities?

In Graql we know we can do the following:

```graql
insert
    $x isa person has firstname "John";
    $y isa person has firstname "Mary";
    $z (spouse1: $x, spouse2: $y) isa marriage;
```

With the Core API this would be:

```java
//Create the attributes
johnName = firstname.putAttribute("John");
Attribute maryName = firstname.putAttribute("Mary");

//Create the entities
Entity john = person.addEntity();
Entity mary = person.addEntity();

//Create the actual relationships
Relationship theMarriage = marriage.addRelationship().addRolePlayer(spouse1, john).addRolePlayer(spouse2, mary);
```

Add a picture, first using Graql:

```graql
match
    $x isa person has firstname "John";
    $y isa person has firstname "Mary";
    $z (spouse1: $x, spouse2: $y) isa marriage;
insert
    $z has picture "www.LocationOfMyPicture.com";
```

Now the equivalent using the Core API:

```java
Attribute weddingPicture = picture.putAttribute("www.LocationOfMyPicture.com");
theMarriage.attribute(weddingPicture);
```


## Building A Hierarchical Schema  

In the [Hierarchical Schema documentation](../building-schema/hierarchical-schema), we discussed how it is possible to create more expressive ontologies by creating a type hierarchy.

How can we create a hierarchy using the Core API? Well, this graql statement:

```graql
define
    event sub entity;
    wedding sub event;
```

becomes the following with the Core API:

```java
EntityType event = tx.putEntityType("event");
EntityType wedding = tx.putEntityType("wedding").sup(event);
```

From there, all operations remain the same.

It is worth remembering that adding a type hierarchy allows you to create a more expressive database but you will need to follow more validation rules. Please check out the section on [validation](../knowledge-model/model#data-validation) for more details.

## Rule Core API

Rules can be added to the knowledge graph both through the Core API as well as through Graql. We will consider an example:

```graql
define

R1
when {
    (parent: $p, child: $c) isa Parent;
},
then {
    (ancestor: $p, descendant: $c) isa Ancestor;
};

R2
when {
    (parent: $p, child: $c) isa Parent;
    (ancestor: $c, descendant: $d) isa Ancestor;
},
then {
    (ancestor: $p, descendant: $d) isa Ancestor;
};
```

As there is more than one way to define Graql patterns through the API, there are several ways to construct rules. One options is through the Pattern factory:

```java
Pattern rule1when = var().rel("parent", "p").rel("child", "c").isa("Parent");
Pattern rule1then = var().rel("ancestor", "p").rel("descendant", "c").isa("Ancestor");

Pattern rule2when = and(
        var().rel("parent", "p").rel("child", "c").isa("Parent')"),
        var().rel("ancestor", "c").rel("descendant", "d").isa("Ancestor")
);
Pattern rule2then = var().rel("ancestor", "p").rel("descendant", "d").isa("Ancestor");
```

If we have a specific `GraknTx tx` already defined, we can use the Graql pattern parser:

```java
rule1when = and(tx.graql().parser().parsePatterns("(parent: $p, child: $c) isa Parent;"));
rule1then = and(tx.graql().parser().parsePatterns("(ancestor: $p, descendant: $c) isa Ancestor;"));

rule2when = and(tx.graql().parser().parsePatterns("(parent: $p, child: $c) isa Parent;(ancestor: $c, descendant: $d) isa Ancestor;"));
rule2then = and(tx.graql().parser().parsePatterns("(ancestor: $p, descendant: $d) isa Ancestor;"));
```

We conclude the rule creation with defining the rules from their constituent patterns:

```java
Rule rule1 = tx.putRule("R1", rule1when, rule1then);
Rule rule2 = tx.putRule("R2", rule2when, rule2then);
```
