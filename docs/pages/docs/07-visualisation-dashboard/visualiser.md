---
title: Visualising a Grakn knowledge graph
keywords: setup, getting started
tags: [getting-started, graql]
summary: "How to use the Grakn Visualiser."
sidebar: documentation_sidebar
permalink: /docs/visualisation-dashboard/visualiser
folder: docs
---

{% include note.html content="These instructions refer to the <b>[0.12.1](https://github.com/graknlabs/grakn/releases/tag/v0.12.1)</b> release of GRAKN.AI. Later versions of the product may include some changes to the user experience, and you may find that some instructions become out-dated. We will endeavour to update information on this page as soon after a release as is possible. The instructions here refer to use of the visualiser on macOS, and screen grabs were taken in Safari." %}

## Introduction

The Grakn visualiser provides a graphical tool to inspect and query your knowledge graph data. This article shows how to run it with a basic example and introduces the visualiser's key features.

## Loading an Example Knowledge Graph

If you have not yet set up the Grakn environment, please see the [Setup guide](../get-started/setup-guide).

You can find the _basic-genealogy.gql_ example that we will work with in the _examples_ directory of the Grakn distribution zip. You can also find this file on [Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

The first step is to load the schema and data into Grakn. You need to use your terminal to do this, as the visualiser is a read-only interface to a knowledge graph. From the terminal, start Grakn, and load the file as follows:

```bash
./grakn server start
./graql console -f ./examples/basic-genealogy.gql -k "family"
```

To illustrate the use of different keyspaces we will use a keyspace called `family` in this example. You can simply use the default (`grakn`) keyspace if you prefer, by omitting the -k argument.

You can test in the Graql shell that all has loaded correctly. For example:

```bash
./graql console -k family
>>>match $p isa person, has identifier $i; get;
```

If all is well, you can open the visualiser by browsing to [localhost:4567](http://localhost:4567).

There are a number of horizontal tabs on the left hand side of the screen, which open panes, described as follows.

## Graph Pane

This is the default section of the visualiser, and is the main pane that you will use to explore a knowledge graph with GRAKN.AI.

Go to the keyspace selector at the top right and select the appropriate keyspace, e.g. `family`.

![Visualiser UI](/images/visualiser-ui-0.12.png)

### Make a Query

The main pane of your knowledge graph will be empty at this point. You can submit queries by typing them into the form in the middle of the top menu bar. You will then need to click '>' (or _Enter_) to visualise the knowledge graph. For example:

```graql
match $x isa person, has firstname "John"; get;
```

![John query](/images/john-query.png)

You can zoom the display in and out, and move the nodes around for better visibility.

Alternatively, for simple visualisation, you can click the Types dropdown in the top menu to list out what is in the schema. For our example, go to the Entities dropdown and choose `person`. The query specific to your selection will be displayed in the form with a default offset and result limit, which is applied by the visualiser (`offset 0; limit 30`).

```graql
match $x isa person; offset 0; limit 30; get;
```

You can change the offset and limit on the number of results as [described below](#query-limit).

![Person query](/images/match-$x-isa-person.png)

If you click and hold on any of the entities, a pop-up will open to allow you to select the labels shown on each node in the knowledge graph. In the screenshot below, we have selected to show the identifiers of each person.

### Save a Query

If you make a query that you think you'll need to repeat regularly, and don't want to type it, or copy and paste it each time, you can save your query. The small plus sign in a circle on the right hand side of the form will bring up a summary of the query, allowing you to assign it a name and save it. Saved queries can then be retrieved using the star button on the left hand side of the horizontal icon set.

### Clear the Knowledge Graph

To clear the query from the form, press the "Clear" button (the circle with the cross through it).

To clear the entire knowledge graph area, press Shift + the "Clear" button.

### Investigate a Node

A single click on any node in the knowledge graph brings up a pane of information about the node at the top right hand side of the screen. The information displayed includes the ID of the node, its type and any resources associated with it, as shown in the figure below.

![Single click](/images/single-click-info-pane.png)

Holding shift and making a click on a node also brings up the resources associated with it, displaying them in the knowledge graph, as shown.

![Single click](/images/shift-click.png)

A double click on a node will bring up the relationships associated with the node, as shown below.

![Double click](/images/double-click.png)

### Change the Display

A single click and hold on a node in the knowledge graph brings up a pane on the lower left hand side of the screen. You can use this to show different information about a node on the graph and change the colour of the nodes.

<!--Add this back in when it works as expected in release 0.13 or beyond
### Explore Types

We have already shown an example of how to examine `person` entities using the entity selector. As another example, select "Types", followed by "Relations" and filter on `marriage` relationships. The query will be shown in the query section at the top of the main pane, as previously, and the visualiser displays all the `marriage` relationships in the knowledge graph.

![Marriages query](/images/marriages.png)

-->

### Query Settings

Query Settings can be accessed under the cog icon at the right hand of the horizontal icon set. The following settings are available

![Person query](/images/query-settings.png)

#### Lock Nodes Position

This option allows you to organise your nodes, lock them into position, and then use the visualiser to explore the knowledge graph, clicking to reveal connections and details about the nodes without them jumping out of place.

If you want to tidy your nodes by aligning them all horizontally or vertically, you can do this through the Query Builder menu, described below. You need to first unlock the nodes so they can be automatically aligned for you.

#### Inference

There is 1 inference setting that can be changed.

- Activate inference - activates inference, per query. It is off by default, but turn this on when you need to run queries that use inference.

#### Query Limit

You can change the offset and limit on the number of results returned by editing the value directly in the submission form, or by adjusting the Query Limit setting.

### Query Builder menu

Right-clicking the mouse brings up the Query Builder menu, which allows you to further explore the knowledge graph, as follows.

#### Shortest Path

{% include note.html content="Make sure to [activate inference](#inference) before running a shortest path query!" %}

<!-- The following video illustrates how to build a shortest path query:

NEEDS UPDATING

<iframe width="640" height="360" src="https://www.youtube.com/embed/OLuVwjPrhbc" frameborder="0" allowfullscreen></iframe>

<br /> -->

The first step is to clear the knowledge graph, then choose two people from the genealogy dataset, to determine the shortest path between them. For example, use the following query, and enter it into the form in the visualiser, to bring up two nodes:

```graql
match $x isa person has firstname "Susan" has surname "Dudley"; $y isa person has firstname "Barbara" has surname "Herchelroth"; get;
```

1. Submit the query by pressing '>' (or _Enter_) to visualise the knowledge graph. The two people in question (Susan Dudley and Barbara Herchelroth) should be shown.
2. Holding down the _control_ key, single click on each of the two nodes.
3. Right click the mouse to bring up the Query Builder menu, and select _Shortest path_ from the menu.
4. The submission form will now contain the shortest path query for those two nodes, for example:

<!-- Ignoring because uses made-up IDs -->

```graql-test-ignore
compute path from "102432", to "192584"; # (The ID values in the strings will be different for each knowledge graph)
```

Submit the query as usual by clicking '>' (or _Enter_) and the knowledge graph will display the relationships and nodes that connect the two by the shortest path. For Susan Dudley and Barbara Herchelroth, you should discover that Barbara is the great-grandmother of Susan’s husband.

![Person query](/images/shortest-path.png)

#### Explore Relations

{% include note.html content="Make sure to [activate inference](#inference) before exploring relationships!" %}

The Query Builder menu that is brought up from a right click of the mouse also has an "Explore Relations" option. This option allows you to determine the relationships between nodes. To illustrate that, clear and submit a query as follows:

```graql
match $x isa person has surname "Niesz"; offset 0; limit 100; get; # Find everyone with surname Niesz
```

1. Select any two people with surname Niesz (it doesn't matter who) by single left clicking on two nodes while holding down the _control_ key.
2. Open the Query Builder menu by right clicking the mouse.
3. The submission form will now contain a query for those two nodes, for example:

```graql
match $x id "651472"; $y id "889000"; $r ($x, $y); get;
```

Submit the query as usual by clicking '>' or _Enter_ and the display will show the relationships, and nodes, that connect the two. The visualiser will display the relationships between the two nodes you selected (e.g. siblings).

#### Align Nodes

If you want to tidy your display to align the nodes horizontally or vertically, you can do this from the Query Builder menu.

1. To select the nodes you want to tidy, you need to 'capture' them in a selection area. Bring up the selector by holding down _ctrl_ and clicking on the blank canvas (not on a node).
2. A green movable square will be illuminated: pull it over the nodes you wish to capture.
3. Right click the mouse to pull up the Query Builder menu.
4. Select Align nodes horizontally or vertically, as required.

## Console

You can use this console to make queries instead of running a Graql shell in your terminal. You can run `match` and `compute` queries, but because the visualiser is read-only, you cannot make insertions.

## Tasks

The tasks page is used to monitor the asynchronous tasks run by Grakn engine, and stop them if necessary. Tasks shown are those that perform loading or post processing, which is triggered after loading. It is possible to list, filter, sort and stop tasks.

## Config

This pane displays a view on the Grakn configuration file, showing configurable properties and their values.

## Documentation

This opens a separate tab in your browser and points it to the Grakn documentation portal. It may be how you ended up on this page!

## Where Next?

Now you have started getting to grips with Grakn, please explore our additional [example code](../examples/examples-overview) and documentation. We are always adding more and welcome ideas and improvement suggestions. Please [get in touch](https://grakn.ai/community)!
