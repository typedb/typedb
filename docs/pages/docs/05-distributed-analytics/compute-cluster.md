---
title: Connected Components
tags: [analytics]
summary: "This page introduces the cluster queries. "
sidebar: documentation_sidebar
permalink: /docs/distributed-analytics/compute-cluster
folder: docs
---

Currently, Graql supports two algorithms for identifying clusters in the graph.

## Connected Component

The [connected components algorithm](https://en.wikipedia.org/wiki/Connected_component_(graph_theory))
can be used to find clusters of instances that are connected.
The algorithm finds all instances (relationships/resources/entities) 
that are connected via relationships in the knowledge graph and gives each cluster a unique label.
In the knowledge graph below you can see three connected components 
that correspond to people who are related through marriage.
In this knowledge graph three unique labels will be created one corresponding to each of the sets of connected instances.

 ![Three connected components representing groups of friends.](/images/analytics_conn_comp.png)

You can call the connected component algorithm to find the clusters above using:

```graql
compute cluster in person, marriage; using connected-component;
```

The results you get involve 3 clusters with sizes: 3, 5, 3.
If you want to see the actual members of the clusters you have to use the modifier `members`:

```graql
compute cluster in person, marriage; using connected-component where members = true;
```

Here, the [subgraph](./overview) functionality has been used to get more meaningful results. 

If you only wanna find out which cluster contains the given entity, you can use the modifier `source`.
 
```graql-test-ignore
compute cluster in person, marriage; using connected-component where source = "V123" members = true;
```
Here, assuming V123 is the id of John Newman in the example above, 
the cluster query will return the cluster in the middle, which contains John Newman.

## K-Core
[K-Core](https://en.wikipedia.org/wiki/Degeneracy_(graph_theory)#k-Cores) can also be used
to find clusters of instances that are tightly interlinked within a network.

Similar to Connected Component, we can compute the clusters using the following:

```graql
compute cluster in person, marriage; using k-core;
```

By default k = 2. Of course we can set the value of k (k > 2):

```graql
compute cluster in person, marriage; using k-core where k = 10;
```

{% include note.html content="Unlike Connected Component, cluster query using K-Core 
will always return the members of each cluster, instead of cluster size." %}

