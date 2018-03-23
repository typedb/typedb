---
title: Define a Rule-Driven Schema
keywords: schema
tags: [java, java-api]
summary: "Demonstrates how to create a rule-driven schema for Grakn."
sidebar: documentation_sidebar
permalink: /docs/building-schema/rule-driven-schema
folder: docs
---

{% include warning.html content="Please note that this page is in progress and subject to revision." %}

<!--
description of their syntax, what is allowed in their conclusion and what is not,
configuration options, whether to materialize or not
show configuration from both graql native syntax in shell, and graql java api,
best practices
supported constructs-->


## Introduction
In this section we are going to further push the schema we defined in the [Hierarchical Schema](./hierarchical-schema.html) section by complementing it with suitably defined rules.
Defining rules as a part of the knowledge graph gives rise to rule-based reasoning permitting automated capture and evolution of patterns within the knowledge graph.
By using rules, common patterns in the knowledge graph can be defined and associated with existing schema elements as well as their instances. This not only allows you to compress and simplify typical queries, but offers the ability to derive new, non-trivial information by combining defined patterns.

Graql rules assume the following general form:

```
when {rule-body} then {rule-head}
```
where `rule-body` and `rule-head` are Graql patterns. Rules are defined as a part of the schema by subbing the `rule` concept.

Intuitively, rules can be regarded as pattern matching constructs. By defining a rule we are telling the system that if we can find an instantiation of the body pattern (set of concepts satisfying the pattern) then the corresponding instantiation of head pattern is treated as if it existed in the knowledge graph.

In the following subsection we will show examples of how to augment the schema by using the rule facility.

## Defining relationships with hierarchical role structure

In the [Hierarchical Schema](./hierarchical-schema.html) section, we have defined the following parentship relationship together with its role hierarchy:

```graql-test-ignore
parentship sub relatives
  relates parent
  relates mother
  relates father
  relates child
  relates son
  relates daughter;

parent sub role;
mother sub parent;
father sub parent;

child sub role;
son sub child;
daughter sub child;
````

Now instead of specifying all relationship combinations for a given parentship pair. We can define a basic `(parent, child)` relationship and let rules do the genderisation for us. One way to accomplish that is to define the following rules:

```graql-test-ignore
define

genderizeParentships1 sub rule
when
{(parent: $p, child: $c) isa parentship;
$p has gender 'male';
$c has gender 'male';
}
then
{(father: $p, son: $c) isa parentship;};

genderizeParentships2 sub rule
when
{(parent: $p, child: $c) isa parentship;
$p has gender 'male';
$c has gender 'female';
}
then
{(father: $p, daughter: $c) isa parentship;};

genderizeParentships3 sub rule
when
{(parent: $p, child: $c) isa parentship;
$p has gender 'female';
$c has gender 'male';
}
then
{(mother: $p, son: $c) isa parentship;};

genderizeParentships4 sub rule
when
{(parent: $p, child: $c) isa parentship;
$p has gender 'female';
$c has gender 'female';
}
then
{(mother: $p, daughter: $c) isa parentship;};
```

The rules simply declare specialised `parentship` relationships based on the gender of the role players. As a result, once we insert a single base `parentship` relationship declaring Alice to be a child of Bob:

```graql
insert

$bob isa person, has firstname 'Bob', has gender 'male';
$alice isa person, has firstname 'Alice', has gender 'female';
(parent: $bob, child: $alice) isa parentship;
```

the knowledge graph will recognise the following `parentship` relationship instances:

```graql-test-ignore
(parent: $bob, child: $alice) isa parentship;
(parent: $bob, daughter: $alice) isa parentship;
(father: $bob, child: $alice) isa parentship;
(father: $bob, daughter: $alice) isa parentship;
```

where $x and $y are respectively Bob and Alice.

## Creating dependent relationships
Having defined the genderised `parentship` relation we can proceed with using rules to define complex relationships derived from the `parentship` relation. We will focus our examples on defining the `cousins` relation.
The relationship will be defined through a `siblings` relationship. We define both relationships as symmetric hence having a single role:

```graql
define

siblings sub relationship
    relates sibling;

cousins sub relationship
    relates cousin;
```


Having the schema definition of the relationships, we can subsequently tell the system to recognise the relationships based on what `parentship` relationships we have defined. This is done via rules in the following manner:

```graql-test-ignore
define

peopleWithSameParentsAreSiblings sub rule
when
{(mother: $m, $x) isa parentship;
(mother: $m, $y) isa parentship;
(father: $f, $x) isa parentship;
(father: $f, $y) isa parentship;
$x != $y;
}
then
{(sibling: $x, sibling: $y) isa siblings;};

peopleWithSiblingsParentsAreCousins sub rule
when
{
(parent: $p, child: $c1) isa parentship;
($p, $p2) isa siblings;
(parent: $p2, child: $c2) isa parentship;
}
then
{(cousin: $c1, cousin: $c2) isa cousins;};
```

As we can see, we simply define `siblings` to be people sharing common parents and then define `cousins` as people having parents that are siblings. Consequently, provided the following information has been inserted into
the knowledge graph:

```graql
insert

$bob isa person, has firstname 'Bob', has gender 'male';
$alice isa person, has firstname 'Alice', has gender 'female';
$charlie isa person, has firstname 'Charlie', has gender 'male';
$daisy isa person, has firstname 'Daisy', has gender 'female';
$eva isa person, has firstname 'Eva', has gender 'female';
(parent: $bob, child: $alice) isa parentship;
(parent: $bob, child: $charlie) isa parentship;
(parent: $alice, child: $daisy) isa parentship;
(parent: $charlie, child: $eva) isa parentship;

```

Alice and Charlie will be recognised as siblings and Daisy and Eva as cousins once we query for the `siblings` and `cousins` relationships respectively.

## Where Next?

Further information about the syntax of Graql rules can be found in the documentation on [defining rules](../building-schema/defining-rules).
