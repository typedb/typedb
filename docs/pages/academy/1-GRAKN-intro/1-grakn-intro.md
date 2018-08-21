---
title: Introduction to Grakn
keywords: getting started
last_updated: April 2018
summary: This is a brief introduction to what a knowledge graph system is, how Grakn fits into the picture and the broad topics that will be covered in later parts of the Grakn Academy.
tags: [getting-started]
sidebar: academy_sidebar
permalink: ./academy/grakn-intro.html
folder: overview
toc: false
KB: academy
---

If you are reading this, you probably want to learn how to use Grakn.

In this series of guides, we will cover most of the knowledge needed to exploit the power of Grakn to build intelligent applications and to get useful insight from your data.

In other words, these guides will teach you how to turn your data into Knowledge using the Grakn.

#### Welcome to the Academy.


Let us tart with the most basic question.

## What is Grakn?

In simple and cheesy terms, Grakn is a database on steroids that makes it easy to build intelligent applications.

More precisely, it is what we call a [Knowledge Graph System](https://en.wikipedia.org/wiki/Knowledge-based_systems): a piece of software that allows to model complex problems, made of several components:

  * __Data layer.__ A data storage component that allows to store and efficiently access your data. A database. In order to be able to store highly interconnected data at a scale, Grakn uses a graph database, but that is just because it happens to be the best tool for the job. As you will see, Grakn is a much more than a graph database. And much more than a relational database for all that matters. That is why we call Grakn a _knowledge graph_.

  * __Data model.__ A knowledge representation object model (our Schema model) that allows to describe your knowledge of the domain and store it together with the data. In simpler words, this helps giving structure to your data.

  * __Reasoning.__ An inference engine, that allows to gain new knowledge that is not explicitly in your data and obtain more intelligent answers to your questions.

  * __User interfaces.__ A set of user interfaces that allow to access and interact with your knowledge graph both graphically and programmatically and make it easy to extend and build applications on top of Grakn.

A knowledge graph management system like Grakn helps you turning data into information (thanks to its data layer and model components) and to turn information into knowledge thanks to its reasoning engine.

  ![DIKW Pyramid](/images/academy/1-welcome/DIKW.svg)

On top of that, Grakn adds

  * Powerful distributed graph analytics that allow to effortlessly perform graph data analysis at scale.

  * A migrator component that greatly simplify the ETL process to migrate data into your knowledge graph.

  * Graql, an easy to learn, easy to read querying and templating language used to control all the different components.

The fact that Grakn adds knowledge graph system capabilities on top of a graph database, all governed by a simple language, makes it the ideal tool both for effectively exploring large datasets and to build AI-based applications. With the added (huge) value that our schema-first approach guarantees the logical integrity of the data.

## What you will build

During the course of the Academy lessons, you will build step by step a fully functional Grakn knowledge graph, starting from the schema, loading data, adding inference rules and performing distributed analytics on it. You will learn the fundamentals of Grakn and Graql and you will learn more about our data model.

At the end of the Academy, you will be able to take full advantage of the Grakn software stack for your intelligent applications and you will be ready to start developing with it.

### What have you learned?
In this section you will find questions and exercises that you should be able to solve after reading each of the guides that make up the Grakn Academy. They are used to make sure that you have understood the topics covered. The more technical the guides become, the more you will find exercises sprinkled along the text.

For this lesson, the exercises are quite free-form, so feel free to answer them as you see fit:

  * What are the main components of a knowledge graph systems?

  * What differentiates a knowledge graph from a database?

  * Can you list 3 features of Grakn that make it good for AI applications?

Extra (these will require some additional external research):

  * What are the advantages of graph databases over relational databases?

  * What are the advantages of relational databases over graph databases?

  * What are the advantages of having a schema-first approach to data modeling?

## What next?
In the [next lesson](./setup.html) you will get ready for the Academy lessons, you will setup your computer and will be introduced to the main entrypoint of Grakn that you will use during the following lessons.
