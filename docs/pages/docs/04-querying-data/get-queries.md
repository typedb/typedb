---
title: Get Queries
keywords: graql, query, get
tags: [graql]
summary: "Graql Get Queries"
sidebar: documentation_sidebar
permalink: /docs/querying-data/get-queries
folder: docs
---

A get query will search the knowledge graph for anything that matches the given [match](./match-clause) part, returning a
result for each match found. To follow along, or experiment further, with the examples given below, please load the
*basic-genealogy.gql* file, which can be found in the *examples* directory of the Grakn installation zip, or on
[Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).


```bash
./grakn server start
./graql console -f <relative-path-to-Grakn>/examples/basic-genealogy.gql
```

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell1" data-toggle="tab">Graql</a></li>
    <li><a href="#java1" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell1">
<pre class="language-graql">
<code>match $x isa person; get;</code></pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre class="language-java">
<code>qb.match(var("x").isa("person")).get();</code></pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

You can also provide as arguments to `get` the variables you wish to see:

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell2" data-toggle="tab">Graql</a></li>
    <li><a href="#java2" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell2">
<pre class="language-graql">
<code>match $x has name $xn; ($x, mother: $y); $y has name $yn; get $xn, $yn;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java2">
<pre class="language-graql">
<code>
qb.match(
    var("x").has("name", var("xn")),
    var().rel("x").rel("mother", "y"),
    var("y").has("name", var("yn"))
).get("xn", "yn");
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->
