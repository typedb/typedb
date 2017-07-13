---
title: Shortest Path
last_updated: February 2017
tags: [analytics]
summary: "This page introduces the shortest path functionality of analytics."
sidebar: documentation_sidebar
permalink: /documentation/graql-analytics/analytics-shortest-path.html
folder: documentation
comment_issue_id: 71
---

## How are two instances in the graph related?
When starting a task you don't always know in advance what you are looking for.
Finding the shortest path between two instances in a graph can be a great way to explore connections because you do not need to provide any guidance.
In the graph below I have displayed two specific people using the query:

```graql
match
$x has identifier "Barbara Shafner";
$y has identifier "Jacob J. Niesz";
```

and then searched for relationships joining two of them using:

```graql
compute path from "id1" to "id2";
```

You can see below that the two people selected are married.
The path query uses a scalable shortest path algorithm to determine the smallest number of relations required to get from once concept to the other.

![Shortest path between people](/images/analytics_path_marriage.png)

### Subgraph

If you are looking for more specific connections you can of course use the [subgraph](./analytics-overview.html) functionality.
In the following query only the blood relations (parent/child relations) are investigated and the resulting graph is shown below.
We have excluded marriage in this subgraph and as a result the shortest path is longer than before - it turns out the Barbara Shafner and Jacob J. Niesz are cousins (their mothers, Mary Young and Catherine Young, are sisters, their father being Jacob Young).

```graql
compute path from "id1" to "id2" in person, parentship;
```

![Shortest path between people](/images/analytics_path_parentship.png)

{% include links.html %}

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/71" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of this page.
