---
title: Building the schema
keywords: setup, getting started
last_updated: April 2018
summary: In this lesson you will learn the basics of Grakn data model and start turning your conceptual model into a Grakn schema
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/schema-building.html
folder: overview
toc: false
KB: academy
---
In the [last lesson](./conceptual-modeling-intro.html), we sketched up a conceptual model for our data, using an entity-relationship modelling process. In this lesson we will see how to translate that sketch into a working Grakn schema. As you will see, the whole thing will be quite straightforward.

## Concept hierarchies
The first thing you need to know is that every concept in a Grakn schema is part of a hierarchy; a very powerful concept, that is one of the things that separates Grakn from a [property graph](https://github.com/tinkerpop/gremlin/wiki/Defining-a-Property-Graph) or a traditional relational database (Object Relational Database Management Systems also have this feature, but it comes with a lot of added weight).

Grakn hierarchies are tree-like structures, this means that every concept you add to your schema must have a direct superconcept. It cannot have more than one.

For example, we could say that `animal` is the direct superconcept of `cat`.

As a general rule, the lower you get into the hierarchy concepts become more specialised. You can say that a concept in your schema is "a kind of" its direct superconcept. In the example above, `cat` is a kind of `animal`. This has several natural consequences, but for the moment let us focus on two of them:

  1. If something is a cat, then it is also an animal, so `match $x isa animal; get;` will return all the cats, as well as the other animals.

  1. If an animal is allowed to have an attribute (for example `name`), then all its subconcepts are also allowed to have that attribute (so you do not have to specify that cats can have a name).

Since every concept you add to the schema must be the direct subconcept of something, every new Grakn knowledge graph comes equipped with what we call a _metaschema_ that includes 3 basic types (**entity**, **relationship** and **attribute**, which are direct subconcepts of the type **thing**) and one special **role** concept, which is used to connect types. Inference **rules** will also be added to the schema, but we will talk about them later in the Academy.

  ![The metaschema](/images/academy/3-schema/meta-schema.png)

## Graql entities and relationships
Enough theory, let’s create our schema! In our last lesson, we created the following model sketch:

```
Entities:
Bond
Company
Oil Platform
Article
Country

Relationships:
Owns (between companies and oil platforms)
Issues (between companies and bonds)
Located in (between oil platforms and countries)

Attributes:
Name (of countries)
Distance from coast (of oil platforms)
Subject (of articles)
```

In order to add a type you need a `define` query where you define the direct superconcept of the type using the `sub` keyword. An example is worth more than a long explanation: if you wanted to add animals and cats to our schema, you would create a new file in our favourite text editor write something like this

```graql
define
"animal" sub entity;
"cat" sub animal;
```

And save it as `schema.gql` or something similar (the `.gql` file extension is completely optional, but it is the default extension for Graql files).

Notice that in the example above we are defining two types (`animal` and `cat`) but we have used the keyword `define` only once. It is in fact possible, and often useful, to actually define the whole schema in one single file, with one single query; this way GRAKN will take care of adding concepts to your knowledge graph in the correct order (for example it will add "animal" before "cat" as the latter is a subconcept of the former).

The only other thing you need to know is that when you add a concept and name it in some way, or more precisely you assign a **label** to it (like "animal" or "cat") you are allowed to use spaces, but it is in general not a good idea.

Take another look at your model. Start a new file in your text editor and start adding entities to your schema. It should be quite easy.

Relationships are added in pretty much the same way, except that they have to be made subconcepts of, you guessed it, **relationship**. Go ahead and try to do it.

If you have followed the lesson so far, your schema file at this point should look a bit like this:

```graql
define
"bond" sub entity;
"company" sub entity;
"oil-platform" sub entity;
"article" sub entity;
"country" sub entity;

"owns" sub relationship;
"issues" sub relationship;
"located-in" sub relationship;
```

If your definition statement looks like the given example, good job! Otherwise: don't panic! Just revisit your work in progress and try to understand what the differences would mean for your schema and why you'd most likely want to avoid that.


## Attributes
Adding attributes to your schema requires a bit more information. The basic process is still the same: you define your new concept as a `sub` of the meta-concept `attribute`. Each attribute just needs two more things:

  1. A `datatype` which indicates what kind of values the attribute can have (you can think of it as the domain of a SQL attribute if you are familiar with it). Typical data types are **long** (i.e. integers numbers), **double** (i.e. decimal numbers) and **string**. There are a few more but those three are the most common. To declare the data type of an attribute we use the keyword `datatype`. For example, to add the attribute "name" to your schema, you would add the line `"name" sub attribute datatype string;` to your schema file. 

  1. Some concept to be attached to. Every attribute must be attached to at least one other type (more than one is of course possible). For example: to say that a country is allowed to have a name, we modify the relevant line in the ontology file to look like `"country" sub entity has name;`. It is a lot easier in practice than in explanation.

Try adding the resources that you have listed in the conceptual model to your schema file. Use _long_ as the datatype for the distance from the coast and _string_ for the other attributes.


## What next?
In this lesson you should have learned how to turn your conceptual model into a working Grakn schema. Only one step remains: linking concepts together with roles, which will be done in the [next lesson](./schema-building-continued.html), where you will also find a draft of how your schema file should look like so far.
