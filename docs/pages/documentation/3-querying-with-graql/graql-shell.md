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
./grakn server start 
./graql console
```

## Arguments

You can optionally pass arguments when starting the Graql shell, as follows:


| Long Option   | Option   | Description                                      | Note |
| ------------- | -------- | ------------------------------------------------ | |
| `--batch <arg>`     | `-b`     | A path to a file containg a query to batch load. | The REPL does not open. <br/> The resulting knowledge base is automatically committed. |
| `--execute <arg>`     | `-e`     | A query to execute.                              | The REPL does not open. <br/> The resulting knowledge base is automatically committed. |
| `--file <arg>`      | `-f`     | A path to a file containg a query to execute.    | The REPL does not open. <br/> The resulting knowledge base is automatically committed. |
| `--help`      | `-h`     | Print usage message.                             | |
| `--keyspace <arg>`  | `-k`     | The keyspace of the knowledge base.                 | |
| `--infer`     | `-n`     | Perform inference on results.                    | Reasoning is not enabled by default at present, although as Grakn develops, we expect that to change.|
| `--output <arg>`  | `-o` | Output format for results                        | | 
| `--uri <arg>`   | `-r`|  The URI to connect to engine.                            | |
| `--version`     | `-v`     | Print version                                    | |


{% include tip.html content="You can see this list in the terminal by typing `./graql console -h`" %}

For example, to load some data from a file into a knowledge base:

```bash
./graql console -f ./examples/philosophers.gql
```


To load data into a different knowledge base, or keyspace, you can specify the name:

```bash
./graql console -k <knowledge-base-name> -f ./examples/pokemon.gql
``` 

The interactive shell commits to the knowledge base only when the user types `commit`.

## Special Commands

While working in the shell, the following special commands can be used:

| Query        | Description                                            |
| -----------  | ------------------------------------------------------ |
| `clear`      | Clears the console window. |
| `commit`     | Commits and validates the knowledge base. If validation fails, the transaction will not commit. |
| `edit`       | Opens the user's default text editor, specified by the `$EDITOR` environment variable. By default this is set to `vim`. When the editor exits, any query it contains is executed in the Graql shell. This is useful for executing a large chunk of Graql without typing it all in the terminal (e.g. to cut and paste from an example). |
| `exit`       | Exits the REPL |
| `license`    | Prints the license. |
| `load <filename>` | Executes the given file containing a Graql query. |
| `rollback`   | Rolls back the transaction, undoing everything that hasn't been committed. |
| `clean`      | Cleans the entire keyspace, removing everything in the graph. |

{% include tip.html content="Graql shell maintains a history of past commands with the 'up' and 'down' arrows. You can also autocomplete keywords, type and variable names using tab!" %}

{% include links.html %}
