---
title: Schemas - The structure of knowledge
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you will learn what is a Grakn schema and what are its main elements
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/schema-elements.html
folder: overview
toc: false
KB: academy
---

You have learned the language, it is now time to put it to good use: it is time to learn about the Grakn conceptual model. As Grakn is, as they say, a "schema first" system, It is time to learn building Grakn schemas.

## What is a Grakn schema
A schema in Grakn is a part of the knowledge base that describes how the data is structured. The data model, in other words.

If you know a bit about other schema first database knowledge systems, you know that normally database design involves three schemas:

  1. _A high-level conceptual schema_, that models your problem and usually involves some variation of the entity-relation model

  1. _A mid-level logical schema_, that depends on the database type you are aiming at (for example if you are going relational, this would involve turning the conceptual model into tables and going over a series of normalisation steps of your schema)

  1. _A low level physical schema_, that requires you to optimise your schema according to how your physical resources are distributed

With Grakn, thanks to our high level knowledge model, your schema will closely resemble the conceptual schema of step one, essentially avoiding you the hassle of going through the other two modelling steps: your Grakn system will take care of those.

This greatly simplifies the design process, getting you what can be considered a highly normalised distributed schema without the need of going through the logical and physical modelling.

Let us go over the main components of a Grakn knowledge base and schema.

## Concepts
Everything that describes your domain  in a Grakn knowledge base is a concept. This includes the elements of the schema (namely types and roles, which we call schema concepts) and the actual data (which we simply call things; you can think of them as instances of types if you are the programmer kind of person).


## Types
Types are what constitutes the core of your schema. They come in three flavours: entities, relationships and attributes.

__Entities__ are the main actors in your domain. These are usually the type of things you want to know about.

__Relationships__ are things that connect other concepts. Each relationship can connect a number of things, as specified in your schema.

__Attributes__ are small pieces of data that get attached to other concept (think of numbers, strings, dates etc.).


## Roles
_Roles_ are what connect types together. They are not types themselves: you cannot have a thing which is an instance of a role, but you will be able to have things playing a role in a specific relationship. In your schema, we will need to specify what role relates to each relationship type and who can play those role. Thanks to roles, you will be able to guarantee the logical integrity of your data, avoiding to have a marriage between a cat and a building, for example, unless you specifically allow such a thing in the schema.

### What have you learned?
In this lesson you have learned what a Grakn schema is and the basic terminology of a schema. You are ready to get into the actual process of modelling a Grakn knowledge base.

As a review exercise, try to describe what are the three modeling steps in database design and what are the main elements of a Grakn schema.


## What next?
In our [next lesson](./conceptual-modeling-intro.html)  we will be start modeling the big question we have introduced several lessons ago. If you haven’t read it or you need a refresher, here [is the link](./graql-intro.html). If you want to know more about Grakn schemas, as always, head over to [the docs](../index.html).
