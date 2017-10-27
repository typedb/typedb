---
title: Getting ready
keywords: setup, getting started
last_updated: September 2017
summary: In this lesson you find out what are the prerequisites for the rest of the Academy and you will have the first taste of GRAKN
tags: [getting-started]
sidebar: academy_sidebar
permalink: ./academy/setup.html
folder: overview
toc: false
KB: academy
---

## What you need to know (and where to learn it)

In order to follow the Academy lessons, you will have to have some basic knowledge of the unix shells (Bash or similar). This is out of the scope of this tutorial, but a quick web search will get you all the information you need.

If you do not know where to begin with, try having look at this [tutorial](http://lifehacker.com/5633909/who-needs-a-mouse-learn-to-use-the-command-line-for-almost-anything).

You will also need to have and know how to use a text editor which can save simple, non formatted text files. There are countless alternatives on every platform, just pick your favourite one.


## Setup the Academy Virtual Machine (VM)

To make sure that the GRAKN environment is the same for everyone following this course, we will use a virtual machine (VM from now on) with GRAKN pre-installed and the data we will be using pre-loaded.

To set it up you have to follow some simple steps:

  1. Install Oracle’s VirtualBox if you have not already installed it. It can be downloaded from [here](https://www.virtualbox.org/wiki/Downloads) where you will also find detailed instructions on how to set it up on your favourite OS (it is a fairly simple process).

  1. Download the GRAKN virtual machine following [this link](http://grakn.ai/download-academy) and unzip the downloaded file

  1. Start Virtualbox and from the `File` menu select `Import Appliance`. Open the `GRAKN.ovf` file that you will find in the folder you unzipped in the preceding step. This will import the virtual machine image into VirtualBox

  1. You are ready to go: start the Virtual Machine you just imported and you will be presented a login screen to the virtual machine. Input `grakn` both as a user name and password and you will be in the shell.

![VM Screenshot](/images/academy/1-welcome/VM-screenshot.png)

### Exercise

Once you have started the virtual machine, list the content on the home directory, then move yourself into the folder `grakn/` from here you will be able to start grakn.


## Starting and using GRAKN

With the Virtual Machine up and running, you are ready to start grakn. From the directory `~/grakn/` run the command `./grakn server start` to start Grakn. As soon as you see the Grakn logo, you will be able to connect to the Grakn.

There are several ways to interact with GRAKN. We will mostly be using 3 of them: the Graph visualiser, the Dashboard Console and the Graql shell.


## Visualiser

To access the graph visualiser, with GRAKN running in the virtual machine, simply point your favourite browser (Google Chrome is preferred to use GRAKN) to `localhost:4567` and you will be presented with the following screen.

![Visualiser screenshot](/images/academy/1-welcome/Dashboard.png)

This is the Graph visualiser. In this mode, you can query your GRAKN knowledge base and the results will be presented as a graph. To try it do the following:
In the top right corner click on the `grakn` tab (this is the Keyspace selector); a drop-down menu will appear. Click on `academy` to select the Keyspace where our knowledge base has been loaded
In the box on the top of the page (the Graql editor) write `match $x isa company; get;` and return. The result of the query will be visualised in the main area of the page.

#### GOOD TO KNOW: Keyspaces
In the GRAKN terminology, a keyspace is an isolated storage layer where you can store a knowledge base. This way you can have several knowledge bases in the same running instance of GRAKN without them interacting and steping on each others’ toes.

The Graph visualiser is a good tool to visualise (you guessed it) the graph structure underlying your knowledge base. It is useful to explore the graph, to find connection and to better understand the result of our reasoning engine thanks to the explanation facility (don’t worry if some of these terms sound obscure: they will become clearer during the rest of the course).

When the results to the query are too many, though, too much information is on your screen and the Graph visualiser becomes less useful. For this reason, there is a way to automatically limit the number of results that appear on your screen when you launch a query in the graph visualiser. You can choose anything you want, but 2 is a good number to start experimenting with.

![Visualiser screenshot](/images/academy/1-welcome/Dashboard-settings.png)

#### GOOD TO KNOW: Clearing the graph.
In order to clear both the graph and the GRAQL editor in the dashboard, just SHIFT+Click on the (X) sign next to the editor.


## Dashboard Console
When you have too many results or in general for some type of queries that do not return results in the form of a graph, it is not advisable (or in some case not even possible) to use the graph visualiser. In those cases, you need the Console. To access it click on the "Console" tab on the left of your screen and run again the same query as before to see how the result looks like.

![Visualiser screenshot](/images/academy/1-welcome/Dashboard-Console.png)


## GRAQL Console

For security reasons, the dashboard does not allow you to modify the knowledge base, only to query it. In order to be able to add data or remove things, a lower level access point is needed: the GRAQL shell, which  looks very similar to the Dashboard Console, but requires the command line to be launched.

In order to launch the GRAQL shell, from the command line of the VM, in the folder `grakn`, run `./graql console -k academy`. This will launch the GRAQL shell in the keyspace "academy", that contains the data we are using during this course.

Once again, type `match $x isa company; get;` and hit return to launch the query. As you can see the results looks very similar those in the dashboard console.

To quit the GRAQL shell, just type `exit` to return to the command line.


## Other entry points

The graph visualiser, the console and the GRAQL shell are not the only ways to interact with GRAKN. There are lower level ways to control it programmatically, like the REST endpoints and the Java API, but those are out of the scope of this article. They are treated more in depth in the [documentation](/documentation/api-reference/api-reference.html) that I highly encourage you to consult.


### What you have learned?

If you have made it this far, you should have a working distribution of GRAKN running in a virtual machine, and you should know how to access the main entry points to the system: you should know how to access and query GRAKN from the Graph visualiser, the Dashboard console, and the GRAQL shell.

You are now ready to get your hands dirty and start working with GRAKN.


## What next?

In the [next module](/academy/graql-intro.html) you will start learning about GRAQL, the language of knowledge that is used to control the different components of the software stack. As you will see, it is very readable and easy to learn and use. If you want to have a look around before going on, head to the [documentation portal](/index.html)
