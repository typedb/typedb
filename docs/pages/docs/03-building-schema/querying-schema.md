---
title: Querying Schema
keywords: graql, query, define
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
match $x plays @has-firstname-owner; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java11">
<pre class="language-java">
<code>
qb.match(var("x").plays("has-firstname-owner")).get();
</code>
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
