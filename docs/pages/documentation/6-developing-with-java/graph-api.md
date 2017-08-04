---
title: Graph API
keywords: java
last_updated: March 2017
tags: [java]
summary: "The Graph API."
sidebar: documentation_sidebar
permalink: /documentation/developing-with-java/graph-api.html
folder: documentation
---

The Java Graph API is the low level API that encapsulates the [Grakn knowledge model](../the-fundamentals/grakn-knowledge-model.html). The API provides Java object constructs for ontological elements (entity types, relation types, etc.) and data instances (entities, relations, etc.), allowing you to build a graph programmatically. 

To get set up to use this API, please read through our [Setup Guide](../get-started/setup-guide.html) and guide to [starting Java development with GRAKN.AI](./java-setup.html).

## Graph API vs Graql

On this page we will focus primarily on the methods provided by the `GraknGraph` interface which is used by all graph mutation operations executed by Graql statements. If you are primarily interested in mutating the graph, as well as doing simple concept lookups the `GraknGraph` interface will be sufficient. 

It is also possible to interact with the graph using a Java API to form Graql queries via `GraknGraph.graql()`, which is discussed separately [here](./java-graql.html), and is best suited for advanced querying.

## Building an Ontology with the Graph API

In the [Basic Ontology documentation](../building-an-ontology/basic-ontology.html) we introduced a simple ontology built using Graql.
Let's see how we can build the same ontology exclusively via the graph API.
First we need a graph. For this example we will just use an [in-memory graph](./java-setup.html#initialising-a-graph):

```java
GraknGraph graph = Grakn.session(Grakn.IN_MEMORY, "MyGraph").open(GraknTxType.WRITE);
```

We need to define our constructs before we can use them. We will begin by defining our resource types since they are used everywhere. In Graql, they were defined as follows:

```graql
insert

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

These same resource types can be built with the Graph API as follows:

```java
ResourceType identifier = graph.putResourceType("identifier", ResourceType.DataType.STRING);
ResourceType firstname = graph.putResourceType("firstname", ResourceType.DataType.STRING);
ResourceType surname = graph.putResourceType("surname", ResourceType.DataType.STRING);
ResourceType middlename = graph.putResourceType("middlename", ResourceType.DataType.STRING);
ResourceType picture = graph.putResourceType("picture", ResourceType.DataType.STRING);
ResourceType age = graph.putResourceType("age", ResourceType.DataType.LONG);
ResourceType birthDate = graph.putResourceType("birth-date", ResourceType.DataType.STRING);
ResourceType deathDate = graph.putResourceType("death-date", ResourceType.DataType.STRING);
ResourceType gender = graph.putResourceType("gender", ResourceType.DataType.STRING);
```

Now the role and relation types. In Graql:

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

Using the Graph API: 

```java
Role spouse1 = graph.putRole("spouse1");
Role spouse2 = graph.putRole("spouse2");
RelationType marriage = graph.putRelationType("marriage")
                            .relates(spouse1)
                            .relates(spouse2);
marriage.resource(picture);
                           
Role parent = graph.putRole("parent");
Role child = graph.putRole("child");
RelationType parentship = graph.putRelationType("parentship")
                            .relates(parent)
                            .relates(child);
```

Now the entity types. First, in Graql:

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
  has gender
  plays parent
  plays child
  plays spouse1
  plays spouse2;
```

Using the Graph API:

```java
EntityType person = graph.putEntityType("person")
                        .plays(parent)
                        .plays(child)
                        .plays(spouse1)
                        .plays(spouse2);
                        
person.resource(identifier);
person.resource(firstname);
person.resource(surname);
person.resource(middlename);
person.resource(picture);
person.resource(age);
person.resource(birthDate);
person.resource(deathDate);
person.resource(gender);
```

Now to commit the ontology using the Graph API:

```java
graph.commit();
```

If you do not wish to commit the ontology you can revert your changes with:

```java
graph.abort();
```

{% include note.html content="When using the in-memory graph, mutations to the graph are performed directly." %}


## Loading Data

Now that we have created the ontology, we can load in some data using the Graph API. We can compare how a Graql statement maps to the Graph API. First, the Graql:

```graql
insert $x isa person has firstname "John";
```
    
Now the equivalent Graph API:    

```java
graph = Grakn.session(Grakn.IN_MEMORY, "MyGraph").open(GraknTxType.WRITE);

Resource johnName = firstname.putResource("John"); //Create the resource
person.addEntity().resource(johnName); //Link it to an entity
```   

What if we want to create a relation between some entities? 

In Graql we know we can do the following:

```graql
insert
    $x isa person has firstname "John";
    $y isa person has firstname "Mary";
    $z (spouse1: $x, spouse2: $y) isa marriage;
```

With the Graph API this would be:

```java
//Create the resources
johnName = firstname.putResource("John");
Resource maryName = firstname.putResource("Mary");

//Create the entities
Entity john = person.addEntity();
Entity mary = person.addEntity();

//Create the actual relationships
Relation theMarriage = marriage.addRelation().addRolePlayer(spouse1, john).addRolePlayer(spouse2, mary);
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

Now the equivalent using the Graph API:

```java
Resource weddingPicture = picture.putResource("www.LocationOfMyPicture.com");
theMarriage.resource(weddingPicture);
```


## Building A Hierarchical Ontology  

In the [Hierarchical Ontology documentation](../building-an-ontology/hierarchical-ontology.html), we discussed how it is possible to create more expressive ontologies by creating a type hierarchy.

How can we create a hierarchy using the graph API? Well, this graql statement:

```graql
insert 
    event sub entity;
    wedding sub event;
```

becomes the following with the Graph API:

```java 
EntityType event = graph.putEntityType("event");
EntityType wedding = graph.putEntityType("wedding").sup(event);
```

From there, all operations remain the same. 

It is worth remembering that adding a type hierarchy allows you to create a more expressive database but you will need to follow more validation rules. Please check out the section on [validation](../the-fundamentals/grakn-knowledge-model.html#data-validation) for more details.

## Rule Java API

All rule instances are of type inference-rule which can be retrieved by:

```java
RuleType inferenceRule = graknGraph.getMetaRuleInference();
```

Rule instances can be added to the graph both through the Graph API as well as through Graql. We will consider an example:

```graql
insert

$R1 isa inference-rule,
when {
    (parent: $p, child: $c) isa Parent;
},
then {
    (ancestor: $p, descendant: $c) isa Ancestor;
};

$R2 isa inference-rule,
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

If we have a specific `GraknGraph graph` already defined, we can use the Graql pattern parser:

```java
rule1when = and(graph.graql().parsePatterns("(parent: $p, child: $c) isa Parent;"));
rule1then = and(graph.graql().parsePatterns("(ancestor: $p, descendant: $c) isa Ancestor;"));

rule2when = and(graph.graql().parsePatterns("(parent: $p, child: $c) isa Parent;(ancestor: $c, descendant: $d) isa Ancestor;"));
rule2then = and(graph.graql().parsePatterns("(ancestor: $p, descendant: $d) isa Ancestor;"));
```

We conclude the rule creation with defining the rules from their constituent patterns:

```java
Rule rule1 = inferenceRule.putRule(rule1when, rule1then);
Rule rule2 = inferenceRule.putRule(rule2when, rule2then);
```


## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/23" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.


{% include links.html %}