---
title: Grakn Python Driver
keywords: setup, getting started, download
tags: [getting-started]
summary: "Overview of Grakn Python Driver"
sidebar: documentation_sidebar
permalink: /docs/language-drivers/grakn-python
folder: docs
---

# Grakn Python Client

## Installation

The [Python client](https://github.com/graknlabs/grakn-python) requires Python 3.6.

To install it, simply run:

```bash
$ pip install grakn
```

You will also need access to a Grakn database. Head [here](../get-started/setup-guide.html) to see how to
set up a Grakn database.

## Quickstart

Begin by importing the client:

```python
>>> import grakn
```

Now you can connect to a knowledge base:

```python
>>> client = grakn.Client(uri='http://localhost:4567', keyspace='mykb')
```

You can write to the knowledge base:

```python
>>> client.execute('define person sub entity;')
{}
>>> client.execute('define name sub attribute, datatype string;')
{}
>>> client.execute('define person has name;')
{}
>>> client.execute('insert $bob isa person, has name "Bob";')
[{'bob': {'type': {'label': 'person', '@id': '/kb/mykb/type/person'}, 'id': ...}}]
```

Or read from it:

```python
>>> client.execute('match $bob isa person, has name $name; get $name;')
[{'name': {'type': {'label': 'name', '@id': '/kb/mykb/type/name'}, 'value': 'Bob', 'id': ...}}]
```

You can also configure inference or support for multiple queries:

```python
>>> resp = client.execute('match $x isa person; get; match ($y, $z) isa marriage; get;', infer=False, multi=True)
```
