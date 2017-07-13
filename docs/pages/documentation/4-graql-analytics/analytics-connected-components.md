---
title: Connected Components
last_updated: February 2017
tags: [analytics]
summary: "This page introduces the connected components algorithm and explains how to use it."
sidebar: documentation_sidebar
permalink: /documentation/graql-analytics/analytics-connected-components.html
folder: documentation
comment_issue_id: 71
---

The connected components algorithm can be used to find clusters of instances in the graph that are connected.
The algorithm finds all instances (relations, resources and entities) that are connected via relations in the graph and gives each set a unique label.
In the graph below you can see three connected components that correspond to people who are related through marriage.
In this graph three unique labels will be created one corresponding to each of the sets of connected instances.

 ![Three connected components representing groups of friends.](/images/analytics_conn_comp.png)

You can call the cluster algorithm to find the clusters above using:

```graql
compute cluster in person, marriage;
```

The results you get involve 3 clusters with sizes: 3, 5, 3.
If you want to see the actual members of the clusters you have to use the modifier `members`.

```graql
compute cluster in person, knows; members;
```

Here, the [subgraph](./analytics-overview.html) functionality has been used to get more meaningful results, because executing the cluster algorithm without specifying a subgraph will not result in meaningful information.


{% include links.html %}

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/71" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of this page.


