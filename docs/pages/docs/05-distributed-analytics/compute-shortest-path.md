---
title: Shortest Path
tags: [analytics]
summary: "This page introduces the shortest path functionality of analytics."
sidebar: documentation_sidebar
permalink: /docs/distributed-analytics/compute-shortest-path
folder: docs
---

## How are two instances in the knowledge graph related?
When starting a task you don't always know in advance what you are looking for.
Finding the shortest path between two instances in a knowledge graph can be a great way to explore connections because you do not need to provide any guidance.
In the knowledge graph below I have displayed two specific people using the query:

```graql
match
$x has identifier "Barbara Shafner";
$y has identifier "Jacob J. Niesz";
get;
```

and then searched for relationships joining two of them using:

<!-- Ignoring because uses made-up IDs -->
```graql-test-ignore
compute path from "id1" to "id2";
```

There is an easier way to do this in dashboard: simply selecting the two entities, right-click on either of them, select `Shortest path`, then hit `Enter`.

![Shortest path between people](/images/analytics_path_selecting_persons.png)

You can see below that the two people selected are married.
The path query uses a scalable shortest path algorithm to determine the smallest number of relationships required to get from one concept to the other.

![Shortest path between people](/images/analytics_path_marriage.png)

## Subgraph

If you are looking for more specific connections you can of course use the [subgraph](./overview) functionality.
In the following query only the blood relationships (parent/child relationships) are investigated and the resulting knowledge graph is shown below.
We have excluded marriage in this subgraph and as a result the shortest path is longer than before - it turns out the Barbara Shafner and Jacob J. Niesz are cousins (their mothers, Mary Young and Catherine Young, are sisters, their father being Jacob Young).

<!-- Ignoring because uses made-up IDs -->
```graql-test-ignore
compute path from "id1", to "id2", in [person, parentship];
```

![Shortest path between people](/images/analytics_path_parentship.png)

## Finding all paths

If you are looking for all the shortest paths between two entities, simply change `compute path` to `compute paths`.

<!-- Ignoring because uses made-up IDs -->
```graql-test-ignore
compute paths from "id1", to "id2", in [person, parentship];
```

You can see below that there are two paths.

![Shortest path between people](/images/analytics_path_parentships.png)
