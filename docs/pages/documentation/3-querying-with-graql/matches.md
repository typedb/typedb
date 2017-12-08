---
title: Matches
keywords: graql, query, match
last_updated: April 2017
tags: [graql]
summary: "Graql Matches"
sidebar: documentation_sidebar
permalink: /documentation/graql/matches.html
folder: documentation
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
<pre>match $x isa person; get;</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre>qb.match(var("x").isa("person")).get();</pre>
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
<pre>
# Insert one of the system id values that were displayed from the previous query. For example:
match $x id "1216728"; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java2">
<pre>
qb.match(var("x").has("id", "1216728")).get();
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
<pre>
match $x val contains "Bar"; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java3">
<pre>
qb.match(var("x").val(contains("Bar"))).get();
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
<pre>
match $x has identifier $y; get;
match $x has identifier contains "Bar"; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java4">
<pre>
qb.match(var("x").has("identifier", var("x"))).get();
qb.match(var("x").has("identifier", contains("Bar"))).get();
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
<pre>
match $x has identifier "Bar" via $r; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java5">
<pre>
qb.match(var("x").has(Label.of("identifier"), var().val("Bar"), var("r"))).get();
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
<pre>
match $x isa person; ($x, $y); get;
match $x isa person; (spouse:$x, $y); get;
match $x isa person; (spouse:$x, $y); $x has identifier $xn; $y has identifier $yn; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java6">
<pre>
qb.match(var("x").isa("person"), var().rel("x").rel("y")).get();
qb.match(var("x").isa("person"), var().rel("spouse", "x").rel("y")).get();
qb.match(
  var("x").isa("person"),
  var().rel("spouse", "x").rel("x"),
  var("x").has("identifier", var("xn")),
  var("y").has("identifier", var("yn"))
).get();
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
<pre>match $x isa person, has identifier $y; {$y val contains "Elizabeth";} or {$y val contains "Mary";}; get;</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java">
<pre>
qb.match(
    var("x").isa("person").has("identifier", var("y")),
    or(
        var("y").val(contains("Elizabeth")),
        var("y").val(contains("Mary"))
    )
).get();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

## Type Properties

The following properties only apply to types.

### sub
Match types that are a subclass of the given type.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell7" data-toggle="tab">Graql</a></li>
    <li><a href="#java7" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell7">
<pre>
match $x sub thing; get; # List all types
match $x sub attribute; get; # List all attribute types
match $x sub entity; get; # List all entity types
match $x sub role; get; # List all role types
match $x sub relationship; get; # List all relationship types
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java7">
<pre>
qb.match(var("x").sub("thing")).get();
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
<pre>
match parentship relates $x; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java8">
<pre>
qb.match(label("parentship").relates(var("x"))).get();
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
<pre>
match $x plays child; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java9">
<pre>
qb.match(var("x").plays("child")).get();
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
<pre>
match $x has firstname; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java10">
<pre>
qb.match(var("x").has("firstname")).get();
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

```
match $x isa $type; $type label 'person'; get;
```

This is equivalent to the following:

```
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
<pre>
match $x has age > 70; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java12">
<pre>
qb.match(var("x").has("age", gt(70))).get();
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
<pre>
match $x has identifier $id; $id val contains "Niesz"; get;

</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java13">
<pre>
qb.match(
    var("x").has("identifier", var("id")),
    var("id").val(contains("Niesz"))
).get();
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
<pre>
match $x val /.*(Mary|Barbara).*/; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java14">
<pre>
qb.match(var("x").val(regex(".*(Mary|Barbara).*"))).get();
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
<pre>
match $x isa person, has identifier $id; limit 10; offset 5; order by $id asc; get;
match $x isa person, has firstname $y; order by $y asc; get;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java16">
<pre>
qb.match(var("x").isa("person").has("identifier", var("id")))
    .limit(10)
    .offset(5)
    .orderBy("id", Order.asc)
    .get();
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

{% include links.html %}

