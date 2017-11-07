---
title: Graph analytics - The GRAph of KNowledge
keywords: getting started
last_updated: October 2017
summary: In this lesson you will learn about the graph analytics capabilities of GRAKN, together with the appropriate syntax
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/graph-analytics.html
folder: academy
toc: false
KB: academy
---

At this point in the academy it should be somewhat clear that GRAKN is not a graph database. So why a lesson about graph analytics?

Well the answer is that an entity-relationship conceptual model (thus a GRAKN knowledge base) can actually be represented in the form of a graph.

And if you have a graph, then it is often useful to be able to do perform graph analytics.


## A few words before going on
As you will see, it is quite straightforward to perform graph analytics with Grakn. There are two caveats though:

  1. GRAKN is not a graph database, so to forse a knowledge based with a underlying hypergraph structure into a graph in order to perform graph analytics requires some getting used to.
  2. Actually using the results of graph analytics queries requires some programming expertise (except for the shortest path). So this is probably the most advanced topic covered so far in the Academy.

To cast GRAKN hyper-relational knowledge bases as a graph, imagine that each concept (i.e. an entity, a relationship or an attribute) is a node in your graph and two nodes are connected via an edge if there is a relationship in which both concept play a role. This is easier to visualise in some cases and requires a bit more imagination in some other, but it is quite natural.

One last thing to keep in mind is that each Graph Analytics query type is different, so the results will look different for each query type. We will look at them one by one in the following sections.



## Shortest path

The shortest path, as the name suggests, finds the quickest way (if there is one) of getting from one concept to another, looking for a path that only passes through instances of particular types that you specify.

The basic syntax for the shortest path query is

```
compute path from CONCEPT_ID1 to CONCEPT_ID2 in TYPES;
```

In the query, `CONCEPT_ID1` and `CONCEPT_ID2` refer to, as you might guess, concept ids. These are identifiers that are assigned internally by the GRAKN system. When you query for a concept in the Dashboard or Graql Console, concept IDs will be returned in the response (they look like `V123456`). If you are querying through the Graph visualiser, click on any once concept node and you will see on the top right the id of the node.

  ![Finding IDs in the visualiser](/images/academy/6-analytics/finding-id.png)

As for the other analytics queries, the `in TYPES` part of the query is optional, if you do not specify it, Analytics will assume that all types are allowed, so the result will be the actual shortest path (or better: one of the paths of smallest length).

Let's make an example: let us find the shortest path that links the company "ENI" to the country "UK" in our dataset. With the GRAKN server running in your VM, run the following query in the dashboard

```
match $x isa company has name "ENI";
$y isa country has name "UK"; get;
```

This will bring up the two nodes of which you need to find the IDs. You could use one of the methods outlined above and paste the IDs somewhere, but there is a better way.

Click on an empty point of the background of the graph visualiser to make sure that no node is selected. Then CTRL+Click anywhere on the background and you will see a green rectangle extending from the mouse pointer. You can use this window to select multiple nodes at the same time: just drag it over both the nodes currently shown as shown below and click again on the background. This will select both ENI and the UK.

  ![Selecting multiple nodes](/images/academy/6-analytics/multi-selection.png)

After this, you can right click one of the nodes and you will be able to select "Shortest path" among the options. If you click on it, the `compute path` query will be shown in the GRAQL editor ready for you to be launched or modified.

  ![Shortest path GUI](/images/academy/6-analytics/path-gui.png)

Runnning a path query in the Graph visualiser actually lets you see the path between the two nodes, as shown in the following screenshot:

  ![Shortest path in the Visualiser](/images/academy/6-analytics/path-visualiser.png)

The result of the same query on the Dashboard console are a bit less interesting, but shows you how the result of the query looks like if you are interacting programmatically with the GRAKN server.

  ![Shortest path in the Console](/images/academy/6-analytics/path-console.png)

## Degrees

The last two type of analytics queries (Degrees and Clusters) are strictly linked to graph theory concepts. It does not make much sense to run those queries in the graph visualiser as the results have nothing to do with a graph, so you will do so in the Dashboard Console.

In a graph, the _degree_ of a node is the number of edges connected to that node. In Grakn this means that the degree of a concept is the number of relationships in which that concept appears.

Although it might seems a bit confusing at first, an example will make it clearer. As you should know by now, there is a relationship type in the academy dataset called `owns`, that links companies to the oil platforms they know. If you know the degree of a company with respect to the `owns` relationship, you effectively now how many oil platforms that company owns.

This is how the query looks like in GRAQL:

```
compute degrees of company in company, owns;
```

The `of company` part of the query (this might actually be a list of types) is telling that you are only interested in the degrees of companies: if you omit it, the response will also contains the degrees of instances of `owns` and they will all be 1, since every `owns` relationship is connected to a single company in this case. The `in company, owns` is specifying the types to consider when computing the degrees; if you omit this part, as usual, all the types will be considered.

This is how the query result looks like:

  ![Degrees query](/images/academy/6-analytics/degrees-query.png)

As you can see, the result is a list of degrees where each degree is followed by the set of IDs having that degree. For example, you can see that there are 3 companies having degree 11 (that is owning 11 different oil platforms).

A word of caution: depending on your kb the response of a compute degree query can be huge, so be careful.

## Clusters

In graph theory, a _cluster_ (or connected component) is formally a maximal set of nodes where every two nodes are connected by a path. To explain the definition, let us make an example: consider the query

```
compute cluster in oil-company, located-in, country, region;
```

If two concepts are in the same cluster according to this query, say having IDs `123` and `234`, it means that the query

```
compute path from 123 to 234 in oil-company, located-in, country, region;
```

will actually have a result.

Or, in other words, there is a path between the two concepts that passes only through instances of the types `oil-company`, `locate-in`, `country`, and `region`. If two concepts are in different clusters, on the other hand, there will be no path between the two.

So basically we are groupunig the oil companies geographically.

If you try and run the cluster query above in the Dashboard Console, the results will not be the actual clusters, but the cluster _sizes_:

  ![Cluster sizes](/images/academy/6-analytics/cluster-size.png)

This is for two reasons: first of all often you are more interested in the cluster distribution (i.e. how the sizes of the cluster varies) than the actual clusters; secondly, computing clusters can very easily to huge results, so it's better to check how large the results will be before hand.

If you want to know the actual clusters, just append `members;` to the query above. The results will be a list of cluster labels (those are just identifiers of one of the concepts in the cluster) followed by the set of concepts belonging to that cluster.

  ![Cluster query](/images/academy/6-analytics/cluster-members.png)

### What have you learned?

In this lesson, you have learned about the graph analytics capabilities of GRAKN; you have learned what how to compute shortest paths, cluster and degrees in a GRAKN knowledge base and what these terms mean.

## What next

You are almost done! You have learned about all the basics of GRAKN! The last thing left is doing the last [module review](./analytics-review.html)!
