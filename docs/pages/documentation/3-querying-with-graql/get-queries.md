---
title: Get Queries
keywords: graql, query, get
last_updated: April 2017
tags: [graql]
summary: "Graql Get Queries"
sidebar: documentation_sidebar
permalink: /documentation/graql/get-queries.html
folder: documentation
---

A get query will search the knowledge base for anything that matches the given [match](matches.html) part, returning a
result for each match found. To follow along, or experiment further, with the examples given below, please load the
*basic-genealogy.gql* file, which can be found in the *examples* directory of the Grakn installation zip, or on
[Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).


```bash
<relative-path-to-Grakn>/bin/grakn.sh start 
<relative-path-to-Grakn>/bin/graql.sh -f <relative-path-to-Grakn>/examples/basic-genealogy.gql
```

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell1" data-toggle="tab">Graql</a></li>
    <li><a href="#java1" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell1">
<pre>match $x isa person; get;</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre>qb.match(var("x").isa("person")).get();</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

You can also provide as arguments to `get` the variables you wish to see:

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell1" data-toggle="tab">Graql</a></li>
    <li><a href="#java1" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell1">
<pre>match $x has name $xn; ($x, mother: $y); $y has name $yn; get $xn, $yn;</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre>
qb.match(
    var("x").has("name", var("xn")),
    var().rel("x").rel("mother", "y"),
    var("y").has("name", var("yn"))
).get("xn", "yn");
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/42" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

{% include links.html %}

