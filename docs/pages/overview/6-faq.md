---
title: GRAKN.AI FAQs
keywords: overview
tags: [overview, faq]
sidebar: overview_sidebar
permalink: /overview/faqs.html
folder: overview
---

*The following FAQ is taken from a [blog post](https://blog.grakn.ai/grakn-ai-q-a-episode-1-33455f9549c8) published in April 2017.*    

*If you have other questions or want to clarify any of the following, please do reach out to us via [Slack](https://grakn.ai/slack.html), [Twitter](https://twitter.com/graknlabs) or our [discussion forums](https://discuss.grakn.ai/).*

### What is GRAKN.AI?

GRAKN.AI is a distributed [knowledge graph](https://en.wikipedia.org/wiki/Knowledge_base) (Grakn) with a reasoning query language (Graql) that enables you to query for explicitly stored data and implicitly derived information.

Grakn uses an intuitive [schema](https://blog.grakn.ai/what-is-an-ontology-c5baac4a2f6c) as a data model that allows you to define a set of types, properties, and relationship types. The schema allows you to model extremely complex datasets, and functions as a data schema constraint to guarantee data consistency, i.e. [logical integrity](https://en.wikipedia.org/wiki/Data_integrity). The schema modelling constructs include: type hierarchies, n-ary relationships, and higher-order modelling constructs. Grakn allows you to model the real world and all the hierarchies and hyper-relationships contained within it.   

Grakn is built using several graph computing and distributed computing platforms. It is designed to be sharded and replicated over a network of distributed machines. Internally, it stores data in a way that allows machines to understand the meaning of information in the complete context of their relationships. Consequently, Grakn allows computers to process complex information more intelligently, with less human intervention.   

Graql is a [declarative](https://en.wikipedia.org/wiki/Declarative_programming), knowledge-oriented query language that uses machine reasoning to retrieve explicitly stored and implicitly derived information from Grakn. On other database systems, queries have to define the data patterns they are looking for explicitly. Graql, on the other hand, will translate a query pattern into all its logical equivalents and execute them against the database. This includes the inference of types, relationships, context, and pattern combination.   

Graql allows you to derive implicit information that is hidden in your dataset, and makes finding new knowledge easy.
In combination, Grakn and Graql are what makes GRAKN.AI, the knowledge graph for working with complex data.

### Can you explain GRAKN.AI’s “schema-first” model?

In Grakn, the schema is the formal specification of all the relevant concepts and their meaningful associations in a given domain. It allows objects and relationships to be categorised into distinct types, and for generic properties of those types to be expressed. Specifying the schema enables [automated reasoning](https://en.wikipedia.org/wiki/Inference_engine) over the [represented knowledge](https://en.wikipedia.org/wiki/Knowledge_representation_and_reasoning), such as the extraction of implicit information from explicit data ([inference](../docs/knowledge-model/model#rule-and-sub-type-inference)) or discovery of inconsistencies in the data ([validation](../docs/knowledge-model/model#data-validation)). For this reason, the schema must be clearly defined before loading data into the knowledge graph.

### Won’t that cause extra complexity?

We don’t think so! To model the world accurately, you need to model type hierarchies, since without that level of representation, you cannot interpret data or knowledge accurately, and you cannot build a model that is easily extensible. On GRAKN.AI, since the type system exists in the schema rather than in the data, you have control over what goes into the model. Wild streams of input data cannot mess up the model, and type definitions can only go out of control if you explicitly mess up the schema. You face fewer hurdles when ingesting your data and you spend less time and effort on data cleanup and integration.   

If you didn’t model your data in a schema, you would have to do it in your system application layer. But modelling your data domain within code is difficult, hard to scale, maintain and extend. Our approach allows you to keep your data model and code separate.

### What are the advantages compared to relational schema?

GRAKN.AI’s schema modelling approach differs from relational schema because:

* It can model type hierarchies.
* It can model hyper-relationships, such as relationships in relationships, N-ary relationships, and virtual relationships.
* It can be updated easily even after you’ve added data.
* It provides more granular access control at a single type level.
* It is interpretable by a computer/reasoner, such that querying can infer relationships and can compress complex queries.
* If you model your relationships in a relational schema, it doesn’t mean that you can query long sequences of relationships since the sql-joins involved would severely impact performance.

In this we explain modelling with GRAKN.AI (via Data Day Texas 2017)

<iframe style="width: 100%; height: 400px" src="https://www.youtube.com/embed/OeFrudRlXAM?list=PLDaQNzoeb9L7UZDPq7z1Gd2Rc0m_oeSDQ" frameborder="0" allowfullscreen></iframe>

### Isn’t it limiting to have to model data before it is ingested? I’m sure my data will change.

If you have new data that requires a new model, which you have not considered before, then, yes, you will need to extend your schema. However, Grakn’s schema/data model:

* is as robust as relational schemas.
* is as easy to extend/update as adding in data. It gives you more control on new data models that goes into your system and maintains higher quality data.
* allows you to retain logical integrity of your data, which is one of the purposes of the schema.
* can be circumvented if you want to. You can still add data that doesn’t fit your schema, by creating a generic entity-relationship-resource model to ingest general information that doesn’t have any particular type. Imagine it to be “an abstract type”, but not really “abstract”. You will still get the benefit of having an intelligent and simple query language, but you won’t get the benefit of deep/advanced inference.
* allows you to ask previously unimagined questions about the data because the schema provides a reasoning model for the query language to interpret future questions in the most flexible and expressive manner. Without the schema, you would be limited in this respect.

### Is it practical to have to define a schema before I’ve worked with the data. Why can’t I just put it into a knowledge graph as a set of entities and relationships?

In the past, it hasn’t been practical with other technologies that use an ontology, but that is the main mission for us at GRAKN.AI: to make ontologies and knowledge representation practical for the very first time, by integrating seamlessly with a database. Our goal is to ensure that users don’t worry about perceived “baggage”, but simply get expressive modelling abilities without having to worry about how to implement data structure and constraints.

And yes, you could just use a graph database, but with GRAKN.AI, your data sits in a knowledge graph, which enables automation, pattern matching, inference and discovery with very little human intervention. You can uncover hidden patterns in the data that are too complex for human cognition.

### How can I visualise my data?

[GRAKN.AI’s UI](../docs/visualisation-dashboard/visualiser) is a relatively new addition to our platform, and has only been in development for several months. It allows you to view a portion of your dataset, by filtering it using Graql queries.  

We’re committed to extending our UI over the years to come. This year alone, we plan to collaborating with other technologies in the industry on building a WebGL-based graph visualiser, which uses the GPU to render tens of thousands of nodes on the screen.  

The visualiser is a way to show relationships structures of specific portions of data, but ultimately is not the final place for users to analyse data. For that kind of work, we’re building a Knowledge Discovery Terminal, allowing users to visualise and analyse their data through different views: tabular, charts, diagrams and custom combinations thereof. Similarly, we’re also developing a Knowledge Development Environment for users to visualise, edit, and develop advanced data models using visual aids.

### Can you support a high volume of data?

GRAKN.AI scales completely horizontally: we shard and replicate data easily, unlike some other DBs. We will publish some benchmark data soon!
