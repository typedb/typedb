---
title: Aggregate Queries
keywords: graql, query, aggregate
last_updated: Tuesday 6, 2016
tags: [graql]
summary: "Graql Aggregate Queries"
sidebar: documentation_sidebar
permalink: /documentation/graql/aggregate-queries.html
folder: documentation
KB: genealogy-plus
---

An aggregate query applies an operation onto a [match](matches.html), to return information about the results (e.g. a count). To follow along, or experiment further, with the examples given below, please load the *basic-genealogy.gql* file, which can be found in the *examples* directory of the Grakn installation zip, or on [Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

```bash
./grakn server start 
./graql console -f <relative-path-to-Grakn>/examples/basic-genealogy.gql
```



## Aggregate Functions

### Ask

Whether the given [match](matches.html) has any results.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell-ask" data-toggle="tab">Graql</a></li>
    <li><a href="#java-ask" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell-ask">
<pre>
match divorce sub relationship; aggregate ask;
match marriage sub relationship; aggregate ask;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java-ask">
<pre>
qb.match(label("divorce").sub("relationship")).aggregate(ask()).execute();
qb.match(label("marriage").sub("relationship")).aggregate(ask()).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### Count

Count the number of results of the match or aggregate result.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell1" data-toggle="tab">Graql</a></li>
    <li><a href="#java1" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell1">
<pre>
match $x isa person; aggregate count;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre>
qb.match(var("x").isa("person")).aggregate(count());
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### Sum

Sum the given attribute variable.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell2" data-toggle="tab">Graql</a></li>
    <li><a href="#java2" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell2">
<pre>
match $x isa person, has age $a; aggregate sum $a;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java2">
<pre>
qb.match(
    var("x").isa("person").has("age", var("a"))
).aggregate(sum("a"));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### Max

Find the maximum of the given attribute variable.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell3" data-toggle="tab">Graql</a></li>
    <li><a href="#java3" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell3">
<pre>
match $x isa person, has age $a; aggregate max $a;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java3">
<pre>
qb.match(
    var("x").isa("person").has("age", var("a"))
).aggregate(max("a"));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### Min

Find the minimum of the given attribute variable.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell4" data-toggle="tab">Graql</a></li>
    <li><a href="#java4" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell4">
<pre>
match $x isa person, has firstname $n; aggregate min $n;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java4">
<pre>
qb.match(
    var("x").isa("person").has("firstname", var("n"))
).aggregate(min("n"));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### Mean

Find the mean (average) of the given attribute variable.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell5" data-toggle="tab">Graql</a></li>
    <li><a href="#java5" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell5">
<pre>
match $x isa person, has age $a; aggregate mean $a;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java5">
<pre>
qb.match(
    var("x").isa("person").has("age", var("a"))
).aggregate(mean("a"));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### Median

Find the median of the given attribute variable.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell6" data-toggle="tab">Graql</a></li>
    <li><a href="#java6" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell6">
<pre>
match $x isa person, has age $a; aggregate median $a;

</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java6">
<pre>
qb.match(
    var("x").isa("person").has("age", var("a"))
).aggregate(median("a"));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### Group

Group the results by the given variable.

The group aggregate can optionally accept a second argument which is another 
aggregate operation, e.g. `count`.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell7" data-toggle="tab">Graql</a></li>
    <li><a href="#java7" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell7">
<pre>
match $x isa person; $y isa person; (parent: $x, child: $y) isa parentship; aggregate group $x;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java7">
<pre>
qb.match(
    var("x").isa("person"),
    var("y").isa("person"),
    var().rel("parent", "x").rel("child", "y").isa("parentship")
).aggregate(group("x"));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### Select

Select and name multiple aggregates.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell8" data-toggle="tab">Graql</a></li>
    <li><a href="#java8" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell8">
<pre>
match $x isa person, has age $a, has gender $g; aggregate (min $a as minAge, max $g as maxGender);
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java8">
<pre>
qb.match(
    var("x").isa("person").has("age", var("a")).has("gender", var("g")),
).aggregate(select(min("a").as("minAge"), max("g").as("maxGender")));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

## When to Use `aggregate` and When to Use `compute`

Aggregate queries are computationally light and run single-threaded on a single machine, but are more flexible than the equivalent [compute query](./compute-queries.html).

For example, you can use an aggregate query to filter results by attribute. The following  aggregate query, allows you to match the number of people of a particular name:

```graql
match $x has identifier contains "Elizabeth"; aggregate count;
```

Compute queries are computationally intensive and run in parallel on a cluster (so are good for big data).

```graql
compute count in person;
```

Can be used to calculate the number of people in the knowledge base very fast, but you can't filter the results to determine the number of people with a certain name.

{% include links.html %}
