---
title: Conceptual modeling
keywords: setup, getting started
last_updated: April 2018
summary: In this lesson you will learn how to build a conceptual Entity-Relationships model
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/conceptual-modeling-intro.html
folder: overview
toc: false
KB: academy
---

Just to reiterate on something we have seen a few lessons ago, when we have to deal with data modeling it is always a good idea to start with a question. In our context (if you want to review our problem context you will find it [here](./graql-intro.html)) the question we want to model our knowledge around is:

> What are the bonds issued by companies that own oil platforms located in Italy less than 18 kilometers from the coast? Those bonds should be connected to articles related to the Italian referendum.

## Finding the concepts
The first step into our conceptual modelling process is finding the concepts in our questions. That means, simply, finding the words that are somewhat related to our knowledge domain. Have a look at the question above and try and find all the words or group of words that are describing something specific relative to the question we are asking.

If you have written down a list of concepts, it could look something like this:

  * Bonds
  * Issued
  * Companies
  * Own
  * Oil platforms
  * Located in
  * Italy
  * 18 kilometres
  * from the coast
  * Articles
  * Italian referendum

## Identifying types (entities, relationships and attributes)
Once you have your basic list of concepts, it is time to start and assign them to the basic types offered by Grakn. It is not a hard and fast rule, but it is usually a good idea to begin with finding the entities. Those tend to be the common nouns in your list.

Before going on, try and find the main entities in the list above. Remember: the entities are usually the main actors in your knowledge graph, the things about which you want ask questions.

Done? Let’s review them. There are a few obvious ones:

  * Bond
  * Company
  * Oil Platform
  * Article

There is also another one. Can you spot it?

If you answered "Italy", you are _almost_ correct: as I said before, entities tend to be represented by common nouns; Italy, in this case is the name of a country, so our missing entity type is

  * Country

After entities, it is time to look for relationships. Relationships are connections between other concepts; when we write down our questions, relationships tend to be represented by verbs (I feel that here it is important to stress once again that this process is more art than science, always double check what you are doing).

Singling out relationships in our concepts list should be straightforward; I like to put my verbs into the third person, but it is a matter of personal taste:

  * Issues
  * Owns

Our last task for this lesson is to identify the attributes in our schema. To find them, go over each of the concept and ask yourself: "what is this?". If your answers sounds like "it is the X of a Y" you are likely to have found the attribute X of concept Y (and you should have Y already as an entity or relationship).

In our case we have:

  * "Italy" is the _name_ of a country
  * "18 kilometres" is the _distance from the coast_ of an oil platform
  * "Italian Referendum" is the _subject_ of an article

We already found all of the concepts to which our attributes are attached, so that is reassuring. And in fact we have assigned every item on the list, so we are done with our conceptual modelling. Good job!

### What have you learned?
If you followed all the steps in this lesson, you now know how to identify the most important concepts in your knowledge model and you are able to categorise them as entity, relationship and attribute types. You have learned the foundations of entity-relationship modelling. Congratulations!

Your conceptual model should look somewhat like this:
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


## What next?
We are now ready to turn our conceptual model into a valid Grakn schema in the [next lesson](./schema-building.html). If you want to know more about Entity-Relationship modelling you can [head to wikipedia](https://en.wikipedia.org/wiki/Entity–relationship_model). On the other hand, if you want to know more about Grakn object model (which will make it clearer why it is so easy to use after you have done your conceptual modelling), you can read about it in the [docs](../index.html).
