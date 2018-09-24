---
title: Visualising a Grakn knowledge graph
keywords: setup, getting started, visualiser, workbase, visualizer
tags: [getting-started, graql]
summary: "How to use the Grakn Workbase Visualiser."
sidebar: documentation_sidebar
permalink: /docs/workbase/visualiser
folder: docs
---

{% include note.html content="These instructions refer to the <b>[1.3.0](https://github.com/graknlabs/grakn/releases/tag/v1.3.0)</b> release of GRAKN.AI. Later versions of the product may include some changes to the user experience, and you may find that some instructions become out-dated. We will endeavour to update information on this page as soon after a release as is possible. The instructions here refer to use of the visualiser on macOS, and screen grabs were taken in Safari." %}

![Visualiser UI](/images/workbase/visualiser.png)

## Introduction
The Grakn visualiser provides a graphical tool to inspect and query your knowledge graph data. This article shows how to run it with a basic example and introduces the visualiser's key features.

## Loading an Example Knowledge Graph
If you have not yet set up the Grakn environment, please see the [Setup guide](../get-started/setup-guide).

You can find the *basic-genealogy.gql* example that we will work with in the *examples* directory of the Grakn distribution zip. You can also find this file on [Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

The first step is to load the schema and data into Grakn. You need to use your terminal to do this, as the visualiser is a read-only interface to a knowledge graph. From the terminal, start Grakn, and load the file as follows:

```bash
./grakn server start
./graql console -f ./examples/basic-genealogy.gql -k "family"
```

To illustrate the use of different keyspaces we will use a keyspace called `family` in this example. You can simply use the default (`grakn`) keyspace if you prefer, by omitting the -k argument.

You can test in the Graql shell that all has loaded correctly. For example:

```bash
./graql console -k family
>>>match $p isa person, has identifier $i;
```

## Keyspace Handler

At the top right corner. there is the keyspace handler which allows you to select a keyspace to visualise.

![Visualiser UI](/images/workbase/keyspace_handler.png)

## Menu Bar

At the top center, there is the menu bar where you can access the main functionalities related to your Graql queries.

![Visualiser UI](/images/workbase/menu_bar.png)

From left to right:

* <b>[Starred Queries](#save-a-query)</b> button - List all the saved queries which you can use or delete

* <b>[Types Panel](#make-a-query)</b> button - Provides a list of concepts that are available in your keyspace, divided into "Entities", "Attributes", "Relationships".

* <b>[Graql Editor](#make-a-query)</b> - Input box for your graql queries
    - `enter` - run query
    - `shift + enter` - will add a new line to the editor (useful for long queries)
    - `shift + up/down` - navigate through previous graql queries

* <b>[Save Query](#save-a-query)</b> - Allows user to save the current query to starred queries

* <b>[Execute Query](#make-a-query)</b> button - executes the graql query.

* <b>Clear Graql Editor</b> button - clears the graql editor

* <b>[Query Settings](#query-settings)</b> button - provides the user with certain settings for queries

## Context Menu

* <b>[Node Settings](#change-the-display)</b> - Open the node settings panel to show different information about the node on the graph and change the colour or the nodes of the same type. 
  
* <b>Delete Node</b> - Delete a node or multiple nodes from the graph.
  
* <b>[Explain](#explain-an-inferred-concept)</b> - If the node is inferred, show the relationships and nodes which explain how it is inferred.
  
* <b>[Shortest Path](#shortest-path)</b> - Show shortest path between two nodes.
  
* <b>Clear Graph</b> - Clear everything on the graph.

## Manage Keyspace

Shows the list of keyspaces and allows you to delete an exisiting one or create a new one.

![Manage keyspaces](/images/workbase/manage_keyspaces.png)

## Interacting With The Graph

### Make a Query

The graph will be empty at this point. You can submit queries by typing them into the form in the middle of the top menu bar. You will then need to click `>` or hit `enter` to visualise the knowledge graph. For example:

```graql
match $x isa person, has firstname "John"; get;
```

![John query](/images/workbase/john_query.png)

You can zoom the display in and out, and move the nodes around for better visibility.

Alternatively, for simple visualisation, you can click the Types dropdown in the top menu to list out what is in the schema. For our example, go to the Entities dropdown and choose `person`. The query specific to your selection will be displayed in the form with a default offset and result limit, which is applied by the visualiser `offset 0; limit 30`.

```graql
match $x isa person; offset 0; limit 30; get;
```

You can change the offset and limit on the number of results as [described below](#limit-query).

![Visualiser UI](/images/workbase/types_panel.png)

### Save a Query

If you make a query that you think you'll need to repeat regularly, and don't want to type it, or copy and paste it each time, you can save your query. The small plus sign in a circle on the right hand side of the form will bring up a tool tip, allowing you to assign it a name and save it. Saved queries can then be retrieved using the star button on the left hand side of the menu bar.

![Visualiser UI](/images/workbase/save_query.png)  ![Visualiser UI](/images/workbase/starred_queries.png)

### Investigate a Node

A single click on any node in the knowledge graph brings up a tool tip of information about the node at the top right hand side of the screen. The information displayed includes the ID of the node, its type and any attributes associated with it, as shown in the figure below.

![Single click](/images/workbase/node_panel.png)

A double click on a node will bring up the relationships associated with the node, as shown below. The first doule click will load a single batch of relationships as per the [neighbours limit](#limit-neighbours). Any consequent double click will load the next batch of relationships.

![Double click](/images/workbase/double_click.png)

Holding shift and double clicking on a node also brings up the attributes associated with it, displaying them in the knowledge graph, as shown below. The first shift + doule click will load a single batch of attributes as per the [neighbours limit](#limit-neighbours). Any consequent shift + double click will load the next batch of attributes.

![shift double click](/images/workbase/shift_click.png)

You may also select multiple nodes by holding Cmd/Ctrl and clicking on nodes.

### Change the Display

Selecting `Node Settings` in the context menu opens the node settings panel on the lower left hand side of the screen. You can use this to show different information about a node on the graph and change the colour of the nodes.

![Single click](/images/workbase/node_settings_panel.png)

### Query Settings

Query Settings can be accessed under the cog icon at the right hand of the horizontal icon set. The following settings are available:

![Person query](/images/workbase/query_settings.png)

#### Limit Query 

You can change the offset and limit on the number of results returned by editing the value directly in the submission form, or by adjusting the Query Limit setting. (This limit will only be enforced only when no `offset` is specified by the user in the query)

#### Limit Neighbours

Allows the user to limit the nodes loaded when double clicking or shift + double clicking on a node.

#### Autoload Role Players

You can enable or disable loading role players with relationships. This is useful when you only want to visualise the relationships without their role players. You can see the differences between the same query when autoload roleplayers is enabled and disabled below.

enabled:
![Person query](/images/workbase/autoload_roleplayers_enabled.png)
disabled:
![Person query](/images/workbase/autoload_roleplayers_disabled.png)

### Explain an Inferred Concept

If a nodes is inferred, you can select the `Explain` option from the context menu and get the explainaion of how it is inferred. e.g if the cousin relationship is inferred, selecting explain would give us the following:

![explain](/images/workbase/explain_cousin2.png)

![explain](/images/workbase/explain_cousin1.png)

### Shortest Path

Select two nodes and select the `Shortest Path` option in the context menu to display the shortest path between the two nodes if it exists.

![explain](/images/workbase/shortest_path1.png)

![explain](/images/workbase/shortest_path.png)

### Graph Data

The tool tip on the bottom right of the graph will display the number of nodes and edges in the graph.

![canvas data](/images/workbase/canvas_data.png)

### Commands 

Selecting the information icon on the bottom left of the graph opens a modal containing all the ways to interact with the graql editor and graph.

![commands icon](/images/workbase/commands_icon.png)

## Manage Settings

### Engine Settings

Allows user to configure the host and port of Grakn.

![engine settings](/images/workbase/engine_settings.png)

### Preference Settings

Allows user to clear all saved preferences:
* Favourite queries
* Query limit
* Neighbours limit
* Autoload roleplayers
* Node labels
* Node Colours

![preference settings](/images/workbase/preference_settings.png)

## Where Next?

Now you have started getting to grips with Grakn, please explore our additional [example code](../examples/examples-overview) and documentation. We are always adding more and welcome ideas and improvement suggestions. Please [get in touch](https://grakn.ai/community)!
