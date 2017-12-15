---
title: Connected Components
tags: [analytics]
summary: "This page introduces the connected components algorithm and explains how to use it."
sidebar: documentation_sidebar
permalink: /docs/distributed-analytics/compute-connected-components
folder: docs
---

The connected components algorithm can be used to find clusters of instances in the knowledge base that are connected.
The algorithm finds all instances (relationships, resources and entities) that are connected via relationships in the knowledge base and gives each set a unique label.
In the knowledge base below you can see three connected components that correspond to people who are related through marriage.
In this knowledge base three unique labels will be created one corresponding to each of the sets of connected instances.

 ![Three connected components representing groups of friends.](/images/analytics_conn_comp.png)

You can call the cluster algorithm to find the clusters above using:

```graql
compute cluster in person, marriage;
```

The results you get involve 3 clusters with sizes: 3, 5, 3.
If you want to see the actual members of the clusters you have to use the modifier `members`.

```graql
compute cluster in person, relatives; members;
```

Here, the [subgraph](./overview) functionality has been used to get more meaningful results, because executing the cluster algorithm without specifying a subgraph will not result in meaningful information.
