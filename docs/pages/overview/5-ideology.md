---
title: GRAKN.AI Ideology - Simplicity and Maintainability
keywords: overview
tags: [overview, faq]
sidebar: overview_sidebar
permalink: /overview/ideology.html
folder: overview
---

*The following article is taken from a [blog post](https://blog.grakn.ai/the-grakn-ai-ontology-simplicity-and-maintainability-ab78340f5ff6) published in April 2017.    
If you have any questions about it, please do reach out to us via [Slack](https://grakn.ai/slack.html), [Twitter](https://twitter.com/graknlabs) or our [discussion forums](https://discuss.grakn.ai/).*


Technology is invented for solving difficult problems in this world. The shape or form in which it is delivered is almost irrelevant, and should not get in the way of its potential purpose. Ontology systems are an example; it is a technology that humans invented to solve difficult problems in the field of information science. At GRAKN.AI, we provide a schema language for you to model your knowledge graph.

However, the current schema languages and tools (RDF/OWL) have never made it practical for engineers to implement, and so they have mainly stayed in the research space for a long time. At GRAKN.AI, our core mission is to make knowledge graphs easy for engineers to use for the first time. We want engineers to be able to develop their knowledge graphs rapidly and update their data model frequently. This way, their data platform can continue to evolve as the business grows, and immediately reap the real benefit of a knowledge graph: to enable machines to reason and infer hidden knowledge that is too complex for human cognition to uncover.

As you can imagine, we are often asked how GRAKN.AI contrasts with traditional schema languages and tools in the RDF/OWL world. We have previously touched upon [why we implemented our own knowledge representation model](https://blog.grakn.ai/knowledge-graph-representation-grakn-ai-or-owl-506065bd3f24). We believe that RDF & OWL are for semantic web (not databases) and logicians (not software engineers). And GRAKN.AI, as you may expect, is for databases and software engineers.

This time we thought to dive in a little deeper and discuss the practicality of GRAKN.AI’s schema. We believe that the language we provide, Graql, is simple and maintainable for software engineers to use as their de facto data/development platform.

## Simplicity

### Simple and intuitive syntax
GRAKN.AI has an intuitive schema language, Graql, which is the same language used to query the knowledge graph. It has a simple syntax that is not burdened with URIs or complex serialisation of OWL axioms in RDF/XML, Turtle, or N3 formats.

### A smaller set of higher level modelling constructs
Compared to OWL, Graql has a smaller set of [schema modelling constructs](../docs/knowledge-model/model), but can express all the core use cases of complex domain modelling. Graql’s schema language contains higher level constructs that defines [N-ary] hyper-relationships and hyper-objects as basic concepts in the building blocks.    

This is contrasted with modelling each OWL (data/object) property separately and then combining them into N-ary relationship patterns using auxiliary class names and property restrictions. Although OWL offers potentially higher expressivity, it comes at the cost of increased complexity. Crucially, our goal here with GRAKN.AI is to model the real world in a straightforward way, and not to burden engineers with additional complexity. Additionally, OWL lacks essential facilities for meta-knowledge and higher-level modelling (nested relationships, information about relationships, etc.) which consequently makes it more complicated to work with.

### Familiar OOP principle
GRAKN.AI’s modelling principle is based on the familiar object-oriented software engineering principles, which are well known to software engineers, and integrated within the general software development process. This is a significant distinction to the modelling principle of OWL, which is deeply rooted in pure formal logic. In effect, GRAKN.AI provides developers necessary tools to build intelligent systems, without requiring familiarity with the minutiae of complex formalisms (as we abstract it deep in the underlying system).

## Maintainability
By virtue of its simplicity, a GRAKN.AI schema is easier to maintain than more complex ontologies written in RDF/OWL, but that’s not all we got to offer.

### Flexible and scalable schema
GRAKN.AI’s schema is flexible and can be updated at any point in the lifecycle of the database, even when you have terabytes of data. The knowledge representation model takes into account future changes as much as possible, in such a way that addition of new data types to the schema does not break existing schema definitions.   

For example, your data model may need to accommodate new entity, attribute and relationship types in varying orders and combinations after data was already loaded to the database, that can all be easily done in our schema without breaking your existing model. Thus, GRAKN.AI allows the data model to evolve with the business model, even when there is lots of data, building a lasting advantage as your business grows and learns.

### Abundant modelling expertise
As noted above, having a modelling principle based on OOP makes GRAKN.AI more developer-oriented and it can be easily adopted by any engineer. On the other hand, there is a substantial discrepancy between OWL and conventional OOP modelling principles; OWL requires a deep expertise in formal logic systems. Modelling in RDF/OWL often requires a PhD to simplify the schema as they come up with notions of schema patterns.

### Fully integrated environment
GRAKN.AI is a fully integrated knowledge-base environment, which has storage, querying, validation, reasoning, IDE (in progress), visualisation and discovery (in progress) all in one system where the user does not need to care about integrations. RDF/OWL platforms require complex layers of loosely coupled components from many different systems, such as ontology editors (e.g. Protege), storage (RDF triple stores), query engines (e.g. SPARQL), reasoners (e.g. Pellet, other OWL reasoners).

### Automated validation and reasoning
Given a schema, GRAKN.AI automatically takes care of validating input data in real-time, and reasoning/inference is a native behaviour of the Graql language. You immediately reap the validation and reasoning benefits of the schema. It is not as straightforward in the RDF/OWL world; validation does not happen in real-time and reasoners are not naturally integrated. Validation is thus a slow, off-line computation and reasoning requires integration of separate reasoner-tools with your SPARQL query language, which is not always seamless, often very buggy, and does not scale.

## Why are Schema-Constraints Necessary?
GRAKN.AI’s schema is basically an intelligent type system for your complex data.
A complex relationship model combined with no data schema-constraint means that there are an exponential number of possible mistakes. Thus, one of value propositions of GRAKN.AI’s schema is that it functions as a schema constraint to your data, which gets validated in real-time upon writing it to the database.

If a database does not have any form of schema-constraint, the application layer code is instead burdened with the responsibility of maintaining data consistency and logical integrity. The application layer then becomes bloated with complex business logic, to facilitate the process of query abstractions and interpretations.   

There is a debate in the programming world about the virtues of strongly-typed and loosely-typed languages. Loosely-typed languages, e.g. javascript, are easy to start coding and give no compiler errors. Similarly, it is easy to start with a database that has no-schema constraint, and ingest data quickly. But as the system grows, you end up needing type systems, so that you won’t always crash because of incorrect assumptions about data types. GRAKN.AI’s schema is basically a highly expressive and intelligent type system for your complex data, which reduces the complexity of your code by ensuring a higher degree of data integrity, consistency, and consequently, quality.

## Summary

In this article, we have presented the reasons why we believe GRAKN.AI is simple to use and easy to maintain when compared to traditional ontology languages and tools in the RDF/OWL world, which include:

* simple and intuitive schema and query language
* smaller set of higher level modelling constructs
* familiar Object-Oriented software engineering principles
* flexible and scalable data model
* fully integrated knowledge-base environment
* real-time validation and native reasoning

## Where can I find out more?
To find out more, take a look at our [documentation](https://grakn.ai/pages/index.html) — the [Knowledge Model documentation](../docs/knowledge-model/model) is a good place to start for more about the subjects touched upon above.

And if you have any questions, we are always happy to help. A good way to ask questions is via our Slack channel. We also have a [discussion forum](https://discuss.grakn.ai). For news, sign up for our [community newsletter](https://grakn.ai/community) and — if you’d like to meet us in person — we run regular [meetups](https://www.meetup.com/graphs/).
