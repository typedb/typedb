---
title: Delete Queries
keywords: graql, query, delete
last_updated: August 11, 2016
tags: [graql]
summary: "Graql Delete Queries"
sidebar: documentation_sidebar
permalink: /documentation/graql/delete-queries.html
folder: documentation
---

A delete query will delete the specified [variable patterns](#variable-patterns) for every result of the [match query](match-queries.html). If a variable pattern indicates just a variable, then the whole concept will be deleted. If it is more specific (such as indicating the `id` or `isa`) it will only delete the specified properties. 

To follow along, or experiment further, with the examples given below, please load the *basic-genealogy.gql* file, which can be found in the *examples* directory of the Grakn installation zip, or on [Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).


<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell1" data-toggle="tab">Graql</a></li>
    <li><a href="#java1" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell1">
<pre>
match $x isa person; delete $x;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre>
qb.match(var("x").isa("person")).delete("x").execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


## Variable Patterns

A variable pattern in a delete query describes [properties](#properties) to delete on a particular concept. The variable pattern is always bound to a
variable name.

If a variable pattern has no properties, then the concept itself is deleted. Otherwise, only the specified properties are deleted.

## Properties

### relates
Removes the given role from the relationship.
<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell2" data-toggle="tab">Graql</a></li>
    <li><a href="#java2" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell2">
<pre>
match $x label marriage; delete $x relates spouse1;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java2">
<pre>
qb.match(var("x").label("marriage")).delete(var("x").relates("spouse1"));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### plays
Disallows the concept type from playing the given role.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell3" data-toggle="tab">Graql</a></li>
    <li><a href="#java3" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell3">
<pre>
match $x label person; delete $x plays sibling1;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java3">
<pre>
qb.match(var("x").label("person")).delete(var("x").plays("sibling1"));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### has
Deletes the resources of the given type on the concept. If a value is given,
only delete resources matching that value.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell4" data-toggle="tab">Graql</a></li>
    <li><a href="#java4" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell4">
<pre>
match $x has identifier "Mary Guthrie"; delete $x has middlename $y;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java4">
<pre>
qb.match(var("x").has("identifier", "Mary Guthrie")).delete(var("x").has("middlename", var("y")));
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/42" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

{% include links.html %}

