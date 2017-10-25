---
title: Define Queries
keywords: graql, query, define
last_updated: August 10, 2016
tags: [graql]
summary: "Graql Define Queries"
sidebar: documentation_sidebar
permalink: /documentation/graql/define-queries.html
folder: documentation
KB: genealogy-plus
---

The page documents use of the Graql `define` query, which will define a specified [variable pattern](./matches.html#variable-patterns)
describing a schema. To follow along, or experiment further, with the examples given below, please
load the *basic-genealogy.gql* file, which can be found in the *examples* directory of the Grakn installation zip, or on
[Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

{% include note.html content="If you are working in the Graql shell, don't forget to `commit`." %}


## Properties

### id

It is not possible to define a concept with the given id, as this is the job of the system. However, if you attempt to
define by id, you will retrieve a concept if one with that id already exists. The created or retrieved concept can then
be modified with further properties.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell3" data-toggle="tab">Graql</a></li>
    <li><a href="#java3" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell3">
<pre>
<!--test-ignore-->
define id "1376496" plays parent;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java3">
<pre>
<!--test-ignore-->
qb.define(var().id(ConceptId.of("1376496")).plays("parent")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### sub

Set up a hierarchy.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell8" data-toggle="tab">Graql</a></li>
    <li><a href="#java8" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell8">
<pre>
define man sub person;
define woman sub person;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java8">
<pre>
qb.define(label("man").sub("person")).execute();
qb.define(label("woman").sub("person")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### relates
Add a role to a relationship.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell9" data-toggle="tab">Graql</a></li>
    <li><a href="#java9" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell9">
<pre>
define siblings sub relationship, relates sibling, relates sibling;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java9">
<pre>
qb.define(
  label("siblings").sub("relationship")
    .relates("sibling").relates("sibling")
).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### plays
Allow the concept type to play the given role.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell10" data-toggle="tab">Graql</a></li>
    <li><a href="#java10" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell10">
<pre>
define person plays sibling;
define person plays sibling;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java10">
<pre>
qb.define(label("person").plays("sibling")).execute();
qb.define(label("person").plays("sibling")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### has

Allow the concept type to have the given attribute.

This is done by creating a specific relationship relating the concept and attribute.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell11" data-toggle="tab">Graql</a></li>
    <li><a href="#java11" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell11">
<pre>
define person has nickname;
</pre>
</div>

<div role="tabpanel" class="tab-pane" id="java11">
<pre>
qb.define(label("person").has("nickname")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

{% include links.html %}
has