---
title: Module review
keywords: setup, getting started
last_updated: April 2018
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/analytics-review.html
folder: academy
toc: false
KB: academy
---

You have almost completed the Grakn Academy. Only a bunch of exercises left to do!

### Exercise 1: OLAP and OLTP queries

What is the difference between an OLAP and an OLTP query? Can you come up with an example of each in Graql?

### Exercise 2: Count queries

Write a `compute count` query that tells you how many countries there are in the academy dataset and run it in the Dashboard.

### Exercise 3: Statistics queries

Use Graql analytics to find:

  1. The average risk of a bond
  2. The maximum distance from the coast of an oil platform

### Exercise 4: Compute queries and aggregates

What are the differences between compute queries and aggregate queries? When should you use which one?

### Exercise 5: Shortest paths

Find the shortest path between the region named "Africa" and the company named "BP". Use the graphical interfaces (i.e. the graph visualiser).

### Exercise 6: Centrality queries

Write a `compute centrality` query using the `degree` algorithm to find out how many countries are contained in each region. Copy the ID of the region with the most countries and verify in the graph visualiser that it is Africa by running the query

```
match $x id REGION_ID; get;
```

### Exercise 7: Cluster queries

Find clusters of companies, bonds and `issues` relationships and verify that they are more or less evenly distributed (that is, every company issues more or less the same number of bonds).


## Keep calm and release the Grakn!

**CONGRATULATIONS!** You have graduated from the Grakn Academy! The only thing left to do is to start developing your own projects. If you need more help or want to ask question you can delve into the [documentation](../index.html) and the [blog](https://blog.grakn.ai) to get more ideas on how to use Grakn. Make sure as well to check our [discussion forum](https://discuss.grakn.ai) and to [join our Slack community](https://grakn.ai/slack.html), where you will find a lot of people willing to help should you ever need assistance.

Be sure to mention, that you completed the Grakn Academy for extra kudos!
