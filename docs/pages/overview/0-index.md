---
title: Introduction to GRAKN.AI
keywords: intro
last_updated: January 2017
summary: GRAKN.AI is the database for AI. It is a database in the form of a knowledge graph that uses machine reasoning to simplify data processing challenges for AI applications.
tags: [overview]
sidebar: overview_sidebar
permalink: ./overview/index.html
folder: overview
toc: false
---

For an application to be more intelligent, it needs to know more. To know more, an application needs to collect more information. When collecting and integrating more information, the resulting dataset becomes increasingly complex.

*What makes a dataset complex?*

Data models make datasets complex. Real world models are filled with hierarchies and hyper-relationships, but our current modelling techniques are all based on binary relations. Querying these legacy  datasets is challenging, because query languages are only able to retrieve explicitly stored data, and not implicitly derived information.

GRAKN.AI is the database solution for working with complex data that intelligent applications rely on. Grakn allows an organisation to grow its competitive advantage by uncovering hidden knowledge that is too complex for human cognition. The data model can evolve as a business “learns” all while reducing engineering time, cost and complexity.

# Meet Grakn and Graql
*GRAKN.AI is composed of two parts: 1. Grakn (the storage), and 2. Graql (the language).*


<!--![Grakn and Graql](/images/grakn_and_graql.png) -->

## Grakn

Grakn is a database in the form of a knowledge graph, that uses an intuitive ontology to model extremely complex datasets. It stores data in a way that allows machines to understand the meaning of information in the complete context of their relationships. Consequently, Grakn allows computers to process complex information more intelligently, with less human intervention.

> Grakn allows you to model the real world and all the hierarchies and hyper-relationships contained in it.

Grakn’s ontology modelling constructs include, but are not limited to, data type hierarchy, relation type hierarchy, bi-directional relationships, multi-type relationships, N-ary relationships, relationships in relationships, conditional relationships, virtual relationships, dynamic relationships, and so on. In other words, Grakn allows you to model the real world and all the hierarchies and hyper-relationships contained within it.

> Grakn’s ontology functions as a data schema constraint that guarantees information consistency.

Grakn’s ontology is flexible, allowing a data model to evolve. Later in the year, we will show how you can have a machine learning system adapt and grow your Grakn ontology. Our ontology is a key feature in maintaining data quality, as it functions as a data schema constraint that guarantees information consistency.

Grakn is built using several graph computing and distributed computing platforms, such as [Apache TinkerPop](https://tinkerpop.apache.org/) and [Apache Spark](http://spark.apache.org/). Grakn is designed to be sharded and replicated over a network of distributed machines.

## Graql

Graql is a [declarative](https://en.wikipedia.org/wiki/Declarative_programming), knowledge-oriented graph query language that uses machine reasoning for retrieving explicitly stored and implicitly derived knowledge from Grakn.

> Graql allows you to derive implicit information that is hidden in your dataset, as well as reduce the complexity of that information.

When using legacy systems, database queries have to define the data patterns they are looking for explicitly. Graql, on the other hand, will translate a query pattern into all its logical equivalents and evaluate them against the database. This includes but is not limited to the inference of types, relationships, context, and pattern combination. In other words, Graql allows you to derive implicit information that is hidden in your dataset, as well as reduce the complexity of expressing intelligent questions. In effect, Graql helps finding new knowledge easy with concise and intuitive statements.

Graql is also capable of performing distributed graph analytics as part of the language, which allows you to perform analytics over large graphs out of the box. These types of analytics are usually not possible without developing custom distributed graph algorithms that are unique to a use case.

One may consider Graql as an OLKP (OnLine Knowledge Processing) language, which combines both OLTP (OnLine Transaction Processing) and OLAP (OnLine Analytical Processing).

####  Here is our CEO and Founder, Haikal Pribadi, presenting GRAKN.AI at a recent event

<iframe style="width: 100%; height: 400px" src="https://www.youtube.com/embed/OeFrudRlXAM?list=PLDaQNzoeb9L7UZDPq7z1Gd2Rc0m_oeSDQ" frameborder="0" allowfullscreen></iframe>
