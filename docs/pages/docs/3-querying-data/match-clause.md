---
title: Match Clause
keywords: graql, query, match
tags: [graql]
summary: "Graql Match Clause"
sidebar: documentation_sidebar
permalink: /docs/querying-data/match-clause
folder: docs
KB: genealogy-plus
---

A match describes a pattern to find in the knowledge base. The results of the match can be modified with various
[modifiers](#modifiers). To follow along, or experiment further, with the examples given below, please load the
*basic-genealogy.gql* file, which can be found in the *examples* directory of the Grakn installation zip, or on
[Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

```bash
./grakn server start
./graql console -f <relative-path-to-Grakn>/examples/basic-genealogy.gql
```

## Properties

### isa
Match instances that have the given type. In the example, find all `person` entities.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell1" data-toggle="tab">Graql</a></li>
    <li><a href="#java1" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell1">
<pre class="language-graql"><code>match $x isa person; get;</code></pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre  class="language-java"><code>qb.match(var("x").isa("person")).get();</code></pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### id
Match concepts that have a system id that matches the [predicate](#predicates).  
<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell2" data-toggle="tab">Graql</a></li>
    <li><a href="#java2" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell2">
<pre class="language-graql">
<code>
# Insert one of the system id values that were displayed from the previous query. For example:
match $x id "1216728"; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java2">
<pre class="language-java">
<code>
qb.match(var("x").has("id", "1216728")).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### val

Match all attributes that have a value matching the given [predicate](#predicates).

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell3" data-toggle="tab">Graql</a></li>
    <li><a href="#java3" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell3">
<pre class="language-graql">
<code>
match $x val contains "Bar"; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java3">
<pre class="language-java">
<code>
qb.match(var("x").val(contains("Bar"))).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### has

<!-- TODO: Describe new reified syntax -->

Match things that have the attribute specified. If a [predicate](#predicates) is provided, the attribute must also match that predicate.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell4" data-toggle="tab">Graql</a></li>
    <li><a href="#java4" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell4">
<pre class="language-graql">
<code>
match $x has identifier $y; get;
match $x has identifier contains "Bar"; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java4">
<pre class="language-java">
<code>
qb.match(var("x").has("identifier", var("x"))).get();
qb.match(var("x").has("identifier", contains("Bar"))).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

You can also specify a variable to represent the relationship connecting the thing and the attribute:

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell5" data-toggle="tab">Graql</a></li>
    <li><a href="#java5" data-toggle="tab">Java</a></li>
</ul>

<!-- TODO: Update to final syntax -->
<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell5">
<pre class="language-graql">
<code>
match $x has identifier "Bar" via $r; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java5">
<pre class="language-java">
<code>
qb.match(var("x").has(Label.of("identifier"), var().val("Bar"), var("r"))).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### relationship

Match things that have a relationship with the given variable. If a role is provided, the role player must be playing that role.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell6" data-toggle="tab">Graql</a></li>
    <li><a href="#java6" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell6">
<pre class="language-graql">
<code>
match $x isa person; ($x, $y); get;
match $x isa person; (spouse:$x, $y); get;
match $x isa person; (spouse:$x, $y); $x has identifier $xn; $y has identifier $yn; get;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java6">
<pre class="language-java">
<code>
qb.match(var("x").isa("person"), var().rel("x").rel("y")).get();
qb.match(var("x").isa("person"), var().rel("spouse", "x").rel("y")).get();
qb.match(
  var("x").isa("person"),
  var().rel("spouse", "x").rel("x"),
  var("x").has("identifier", var("xn")),
  var("y").has("identifier", var("yn"))
).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### Variable Patterns

Patterns can be combined into a disjunction ('or') and grouped together with curly braces. Patterns are separated by semicolons, and each pattern is independent of the others. The variable pattern can optionally be bound to a variable or an ID.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell" data-toggle="tab">Graql</a></li>
    <li><a href="#java" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell">
<pre class="language-graql">
<code>match $x isa person, has identifier $y; {$y val contains "Elizabeth";} or {$y val contains "Mary";}; get;</code></pre>
</div>
<div role="tabpanel" class="tab-pane" id="java">
<pre class="language-java">
<code>
qb.match(
    var("x").isa("person").has("identifier", var("y")),
    or(
        var("y").val(contains("Elizabeth")),
        var("y").val(contains("Mary"))
    )
).get();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->
