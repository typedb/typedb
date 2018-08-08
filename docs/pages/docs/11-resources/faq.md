---
title: FAQ about Grakn
keywords: troubleshooting
tags: [getting-started]
summary: "Frequently asked questions about Grakn."
sidebar: documentation_sidebar
permalink: /docs/resources/faq
folder: docs
---

{% include note.html content="This page contains some of the questions were are mostly commonly asked, and is updated regularly. There is also a separate [Contributor FAQ](../../contributors/contributor-faq.html) for those collaborating with us on GRAKN.AI. You may find an answer here but, if you do not, please feel free to use our [discussion forums](http://discuss.grakn.ai) to ask us for help or advice." %}

## About GRAKN.AI

### Why did you develop a new query language?

We are often asked why we have developed a new schema and query language rather than use existing standards like [RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework), [OWL](https://en.wikipedia.org/wiki/Web_Ontology_Language) and [SPARQL](https://en.wikipedia.org/wiki/SPARQL).

We have written a [substantial explanation](https://blog.grakn.ai/knowledge-graph-representation-grakn-ai-or-owl-506065bd3f24#.5zjvkta9u) to this question on our blog. In summary, our underlying data model is that of a property graph, so in principle we’re able to import and export from/to RDF if needed. However, our language is designed to strike a different and better balance between expressiveness and complexity than offered by the existing OWL profiles, especially in the context of knowledge graph structures. In consequence, our query language, Graql, is aligned with our schema formalism to enable higher level query capabilities than supported by SPARQL over an RDF data model.

OWL is not well-suited for graph-structures. Because of its formal foundations and computational limitations it is in fact a more natural language for managing tree-shaped data instead. OWL also makes it hard to help validate consistency of data and ensure it is well-structured, and this is what knowledge graph applications require.

### How can I contribute to GRAKN.AI

There are lots of ways you can get involved! Please take a look at our [contributor documentation](../../contributors/index.html). You may be a Java developer and able to help us fix bugs or add new features, but if you're not, there are still loads of projects to get into. We are always keen to see products developed on top of our platform, and have a list of [potential projects](../examples/projects) you could work on using Java, Graql, a JVM-language like Groovy, or even something completely different, like Python, R or Haskell.  And if you're not a programmer, you can still contribute. Our documentation changes regularly and needs review or translation. Please just get in touch using one of our [community channels](https://grakn.ai/community)!

## Bugs and strange behaviour

### Why does Grakn hang when I try to start it?   

I am running `grakn server start` but it hangs on `Starting ...`. Why?

This may be because you have cloned the Grakn repo into a directory which has a space in its name (e.g. `/grakn test`).
You can build our code successfully, but when you run `./grakn server start`, it hangs because the database needs you to have single word pathnames.
Remove the spaces (e.g. `/grakn_test`) and try again.

### Why am I getting ghost vertices?

In a transaction based environment it is possible to have one transaction removing a concept while another concurrently modifies the same concept.
Both transactions may successfully commit in an eventually consistent environment.

The concept is likely to still exist with only the modified properties.
It is possible to safeguard against this by setting the `checkInternalVertexExistence` property to true.
However, this will result in slower transaction as more reads will be necessary.

## Working with  Grakn

### Which OS can I use with Grakn?

You can use Mac OS X or Linux right now. We plan to support Windows at a later date.

### Why is there no logger?

The Grakn libraries do not come bundled with a logger. If you are running GRAKN.AI and do not include on a logger dependency, you will see the following message, printed by slf4j.

```
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
```

So you need to include a logger dependency. In the GRAKN.AI distribution we use [Logback](https://logback.qos.ch/). Take a look at the [`server/logback.xml`](https://github.com/graknlabs/grakn/blob/master/conf/server/logback.xml) used in the Grakn project for an idea how to configure your own.

### How do I load data into Grakn?

There are several ways to load data into Grakn. For small amounts of data (<1000 lines), you an load it directly via the Graql shell. For example, the following loads up the an example file called `family-data.gql`:

```bash
./graql console -f examples/family-data.gql
```

If you have a larger file, you will need to batch load it. The file will be divided in batches that will be committed concurrently. This differs from a regular load, where the whole file is committed in a single chunk when you call commit. See the example below, which loads the Graql file FILENAME.gql, from PATH.

```bash
./graql console -b PATH/FILENAME.gql
```

In order to check the status of the loading, you can open a new terminal window and run the command:

```bash
tail -f logs/grakn.log
```


### I want to load a large amount of data into a knowledge graph - how do I do it?
Graql is single-threaded and doesn't support batch-loading. You may want to use the Java [loader client](../java-library/loader-api), which provides multi-threaded batch loading, or the `-b` flag if you are using the [Graql shell](../get-started/graql-console).

### What are the differences between a batch load and a normal load?

The batch load is faster for larger datasets because it ignores some consistency checks, on the assumption that you have pre-filtered your data. Checks ignored include:

*  When looking up concepts any duplicates which are found are ignored and a random one is returned.
*  When creating a relationship it is possible for an entity to be doubly associated with a role. This is later cleaned up by engine.
*  Concepts with duplicate ids can be inserted.
*  Duplicate relationships can also be inserted.

Ignoring these checks allows data to be processed much faster at the risk of breaking consistency.

### What is post-processing?

The distributed and concurrent nature of the Grakn system means that, sometimes, post processing is required to ensure the data remains consistent.

**Role Player Optimisation**

When allocating entities as role players to multiple relationships for the first time it is possible to create duplicate associations. These associations do not affect the results of any queries or computations. For example, if in a new system we process simultaneously the following three statements in different transactions:    

```graql-test-ignore
1. insert $x has name 'Brad Pitt' isa person; $y has name 'Fury'; (actor: $x, movie: $y) isa acted-in;
2. insert $x has name 'Brad Pitt' isa person; $y has name 'Troy'; (actor: $x, movie: $y) isa acted-in;
3. insert $x has name 'Brad Pitt' isa person; $y has name 'Seven'; (actor: $x, movie: $y) isa acted-in;
```

It is possible for the system to record that `Brad Pitt` is an actor multiple times. The duplications will later be resolved and merged by Grakn engine.

**Merging Resources**

{% include note.html content="This only happens when batch loading." %}

When using a batch load, many safety checks are skipped in favour of speed. One such check is the possible existence of an attribute before creating it. So if the following transactions are executed simultaneously while batch loading:

```graql-test-ignore
1. insert $a has unique-id '1'
2. insert $b has unique-id '1'
3. insert $c has unique-id '1'
```

It would be possible to create multiple resources of the type `unique-id` with the value `1`. These duplicate resources are similarly merged and resolved by Grakn engine.

### Do applications written on top of Grakn have to be in Java?

Currently, there is no official support for languages other than Java, although you can find blog posts that describe our experiments with [Haskell](https://blog.grakn.ai/grakn-ai-and-haskell-c166c7cc1d23), [Python](https://blog.grakn.ai/grakn-pandas-celebrities-5854ad688a4f) and [R](https://blog.grakn.ai/there-r-pandas-in-my-graph-b8b5f40a2f99). We would be very willing to accept proposals from our community and work with contributors to extend these initial offerings, and/or create bindings to other languages.

### How do I visualise a knowledge graph?

Grakn comes with a basic [visualiser](../visualisation-dashboard/visualiser), with a web-interface. We appreciate any feedback you give us about it via the [discussion boards](https://discuss.grakn.ai/t/visualise-my-data/57). You will need to start Grakn, and then use your web browser to visit [localhost:4567](http://localhost:4567/) to visualise a knowledge graph.  Please see the [Get Started Guide](../get-started/setup-guide#test-the-visualiser) for more information about the visualiser.


### How do I clear a knowledge graph?

I want to clear the knowledge graph I've been experimenting with and try something with a new, different schema and dataset. How do I do it?

If you are using the Java API, it's a simple as:

```java-test-ignore
Grakn grakn = new Grakn(Grakn.DEFAULT_URI);
grakn.keyspaces().delete("my-knowledge-base");
```

If you are using the Graql shell and have not committed what you have in the knowledge graph, you can just quit the shell and restart it, and all is clean.

If you've committed, then you must stop Grakn and specifically clean the knowledge graph:

```bash
./grakn server stop
./grakn server clean
```

### How do I run Graql from a bash script?

If you want to run Graql from a bash script, for example, to grep the results, you don't want to have to filter out stuff the license and command prompt. The best way therefor, is to use the -e flag or -f flag, which lets you provide a query to the shell. The -e flag accepts a query, while the -f flag accepts a filename. For example:

```    
./graql console -e "match \$x isa movie;"
```

Notice that you have to escape the dollars to stop the shell interpreting them. You can then pipe the output into a command or a file.
