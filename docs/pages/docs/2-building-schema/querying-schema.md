---
title: Querying Schema
keywords: graql, query, define
last_updated: August 10, 2016
tags: [graql]
summary: "Querying Grakn Schema using Graql"
sidebar: documentation_sidebar
permalink: /docs/building-schema/querying-schema
folder: docs
KB: genealogy-plus
---

The schema can be queried using the [match clause](../querying-data/match-clause) A match describes a pattern to find in the knowledge base, and the following properties only apply to schema types.

### sub
Match types that are a subclass of the given type.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell7" data-toggle="tab">Graql</a></li>
    <li><a href="#java7" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell7">
<pre class="language-graql">
<code>
match $x sub thing; get; # List all types
match $x sub attribute; get; # List all attribute types
match $x sub entity; get; # List all entity types
match $x sub role; get; # List all role types
match $x sub relationship; get; # List all relationship types
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java7">
<pre class="language-java">
<code>
qb.match(var("x").sub("thing")).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### relates
Match roles to a given relationship.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell8" data-toggle="tab">Graql</a></li>
    <li><a href="#java8" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell8">
<pre class="language-graql">
<code>
match parentship relates $x; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java8">
<pre class="language-java">
<code>
qb.match(label("parentship").relates(var("x"))).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### plays
Match types that play the given role.
<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell9" data-toggle="tab">Graql</a></li>
    <li><a href="#java9" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell9">
<pre class="language-graql">
<code>
match $x plays child; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java9">
<pre class="language-java">
<code>
qb.match(var("x").plays("child")).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### has
Match types that can have the given attribute.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell10" data-toggle="tab">Graql</a></li>
    <li><a href="#java10" data-toggle="tab">Java</a></li>
</ul>

<!--JCS: Why so many duplicates?-->
<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell10">
<pre class="language-graql">
<code>
match $x has firstname; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java10">
<pre class="language-java">
<code>
qb.match(var("x").has("firstname")).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

The above is equivalent to:

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell11" data-toggle="tab">Graql</a></li>
    <li><a href="#java11" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell11">
<pre class="language-graql">
<code>
match $x plays has-firstname-owner; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java11">
<pre class="language-java">
<code>
qb.match(var("x").plays("has-firstname-owner")).get();
</code>
<pre>
match $x plays @has-firstname-owner; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java11">
<pre>
qb.match(var("x").plays("@has-firstname-owner")).get();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### label
Allows you to refer to a specific types by its typename. For example:

```graql
match $x isa $type; $type label 'person'; get;
```

This is equivalent to the following:

```graql
match $x isa person; get;
```

## Predicates

A predicate is a boolean function applied to values. If a concept doesn't have a value, all predicates are considered false.

### Comparators

There are several standard comparators, `=`, `!=`, `>`, `>=`, `<` and `<=`. For
longs and doubles, these sort by value. Strings are ordered lexicographically.
<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell12" data-toggle="tab">Graql</a></li>
    <li><a href="#java12" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell12">
<pre class="language-graql">
<code>
match $x has age > 70; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java12">
<pre class="language-java">
<code>
qb.match(var("x").has("age", gt(70))).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

If a concept doesn't have a value, all predicates are considered false. The query below matches everything where the predicate `>10` is true. So, it will find all concepts with value greater than 10. However, if a concept does not have a value at all, the predicate is considered false, so it won???t appear in the results.

```graql
match $x val >10; get;
```


### Contains
Asks if the given string is a substring.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell13" data-toggle="tab">Graql</a></li>
    <li><a href="#java13" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell13">
<pre class="language-graql">
<code>
match $x has identifier $id; $id val contains "Niesz"; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java13">
<pre class="language-java">
<code>
qb.match(
    var("x").has("identifier", var("id")),
    var("id").val(contains("Niesz"))
).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### Regex
Checks if the value matches a regular expression. This match is across the
entire string, so if you want to match something within a string, you must
surround the expression with `.*`.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell14" data-toggle="tab">Graql</a></li>
    <li><a href="#java14" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell14">
<pre class="language-graql">
<code>
match $x val /.*(Mary|Barbara).*/; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java14">
<pre class="language-java">
<code>
qb.match(var("x").val(regex(".*(Mary|Barbara).*"))).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

## Modifiers

There are a number of modifiers that can be applied to a query:   

* `limit` - Limits the number of results returned from the query.
* `offset` - Offsets the results returned from the query by the given number of results.
* `order` - Orders the results by the given variable's degree. If a type is provided, order by the attribute of that type on that concept. Order is ascending by default.


<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell16" data-toggle="tab">Graql</a></li>
    <li><a href="#java16" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell16">
<pre class="language-graql">
<code>
match $x isa person, has identifier $id; limit 10; offset 5; order by $id asc; get;
match $x isa person, has firstname $y; order by $y asc; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java16">
<pre class="language-java">
<code>
qb.match(var("x").isa("person").has("identifier", var("id")))
    .limit(10)
    .offset(5)
    .orderBy("id", Order.asc)
    .get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

Note that the order in which you specify modifiers can be important. If you make a query and `limit` the results
returned, say to 10 as in the example, then specify the `order by` modifier _after_ the `limit`, you will find that you
get an ordering of 10 arbitrary results (so an ordering of a _sample_ of the results). If instead, you do `order by`,
then `limit` you will get a global ordering.

```graql
match $x isa person, has firstname $y; limit 10; order by $y asc; get;
# Returns 10 arbitrary people, ordered by firstname
match $x isa person, has firstname $y; order by $y asc; limit 10; get;
# Returns the 10 people who come first alphabetically by firstname
```
