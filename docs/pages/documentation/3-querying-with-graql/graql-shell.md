---
title: Graql Shell
keywords: graql, shell
last_updated: January 2017
tags: [graql]
summary: "
The Graql shell is used to execute Graql queries from the command line, or to let Graql be invoked from other applications."
sidebar: documentation_sidebar
permalink: /documentation/graql/graql-shell.html
folder: documentation
---

The Graql shell is contained in the `bin` folder. After starting the Grakn server, as shown below, you can start the Graql shell, without any parameters, to open a REPL (Read Evaluate Print Loop):

```bash
<relative-path-to-Grakn>/bin/grakn.sh start 
<relative-path-to-Grakn>/bin/graql.sh
```

## Arguments

You can optionally pass arguments when starting the Graql shell, as follows:


| Long Option   | Option   | Description                                      | Note |
| ------------- | -------- | ------------------------------------------------ | |
| `--batch <arg>`     | `-b`     | A path to a file containg a query to batch load. | The REPL does not open. <br/> The resulting graph is automatically committed. |
| `--execute <arg>`     | `-e`     | A query to execute.                              | The REPL does not open. <br/> The resulting graph is automatically committed. |
| `--file <arg>`      | `-f`     | A path to a file containg a query to execute.    | The REPL does not open. <br/> The resulting graph is automatically committed. |
| `--help`      | `-h`     | Print usage message.                             | |
| `--implicit`  | `-i`     | Show implicit types.                             | | 
| `--keyspace <arg>`  | `-k`     | The keyspace of the graph.                 | |
| `--materialise` | `-m`   | Materialise inferred results.                    | Materialisation is not enabled by default at present, although as Grakn develops, we expect that to change.|
| `--infer`     | `-n`     | Perform inference on results.                    | Reasoning is not enabled by default at present, although as Grakn develops, we expect that to change.|
| `--output <arg>`  | `-o` | Output format for results                        | | 
| `--pass <arg>`    | `-p`     | The password to sign in.                     | |
| `--uri <arg>`   | `-r`|  The URI to connect to engine.                            | |
| `--user <arg>`  | `-u`     | Username to sign in.                    | |
| `--version`     | `-v`     | Print version                                    | |


{% include tip.html content="You can see this list in the terminal by typing `graql.sh -h`" %}

For example, to load some data from a file into a graph:

```bash
bin/graql.sh -f ./examples/mammal-dataset.gql
```


To load data into a different graph, or keyspace, you can specify the graph name:

```bash
./graql.sh -k <graphname> -f ./examples/reptile-dataset.gql
``` 

## Queries

The following queries are supported by the shell. Examples and additional details can be found on the corresponding documentation pages.

| Query | Description                                   |
| ----------- | --------------------------------------------- |
| [`match`](./match-queries.html)     | Match a pattern in the graph. Defaults to return the first 100 results. |
| [`ask`](./ask-queries.html)       | Query for a specific pattern in the graph. Returns `true` or `false`. |
| [`insert`](./insert-queries.html)    | Inserts the specified concept into the graph. |
| [`delete`](./delete-queries.html)    | Deletes from the graph with no output. |
| [`compute`](./compute-queries.html)   | Computes analytics about the graph. Returns either a value or a map from concept to value. |

   
The interactive shell commits to the graph only when the user types `commit`.

## Special Commands

While working in the shell, the following special commands can be used:

| Query        | Description                                            |
| -----------  | ------------------------------------------------------ |
| `clear`      | Clears the console window. |
| `commit`     | Commits and validates the graph. If validation fails, the graph will not commit. |
| `edit`       | Opens the user's default text editor, specified by the `$EDITOR` environment variable. By default this is set to `vim`. When the editor exits, any query it contains is executed in the Graql shell. This is useful for executing a large chunk of Graql without typing it all in the terminal (e.g. to cut and paste from an example). |
| `exit`       | Exits the REPL |
| `license`    | Prints the license. |
| `load <filename>` | Executes the given file containing a Graql query. |
| `rollback`   | Rolls back the transaction, undoing everything that hasn't been committed. |

{% include tip.html content="Graql shell maintains a history of past commands with the 'up' and 'down' arrows. You can also autocomplete keywords, type and variable names using tab!" %}


## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/42" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

{% include links.html %}
