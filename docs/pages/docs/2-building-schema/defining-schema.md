---
title: Defining Schema
keywords: graql, query, define
tags: [graql]
summary: "Defining Grakn Schema using Graql"
sidebar: documentation_sidebar
permalink: /docs/building-schema/defining-schema
folder: docs
KB: genealogy-plus
---

The page documents use of the Graql `define` and `undefine` queries, which will define a specified
[variable pattern](../querying-data/match-clause#variable-patterns) describing a schema. To follow along, or experiment
further, with the examples given below, please load the *basic-genealogy.gql* file, which can be found in the *examples*
directory of the Grakn installation zip, or on
[Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

{% include note.html content="If you are working in the Graql shell, don't forget to `commit`." %}

## Define

[Define queries](../api-references/ddl#define-query) are used to define your schema. Any
[variable patterns](../api-references/dml#patterns) within them are added to the schema:

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell-define" data-toggle="tab">Graql</a></li>
    <li><a href="#java-define" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell-define">
<pre class="language-graql"> <code>
define
person sub entity, has name;
name sub attribute, datatype string;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java-define">
<pre class="language-java"> <code>
qb.define(
    label("person").sub("entity").has("name"),
    label("name").sub("attribute").datatype(STRING)
).execute();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

This example defines an entity `person` and an attribute `name`. `name` is given the datatype `string` and a `person`
can have a name.

## Undefine

[Undefine queries](../api-references/ddl#undefine-query) are used to undefine your schema. Any
[variable patterns](../api-references/dml#patterns) within them are removed from your schema:

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell-undefine-has" data-toggle="tab">Graql</a></li>
    <li><a href="#java-undefine-has" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell-undefine-has">
<pre class="language-graql"> <code>
undefine person has name;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java-undefine-has">
<pre class="language-java"> <code>
qb.undefine(label("person").has("name")).execute();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

This example will stop instances of a `person` from having a `name`. `person` and `name` will both still be in the
schema.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell-undefine-sub" data-toggle="tab">Graql</a></li>
    <li><a href="#java-undefine-sub" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell-undefine-sub">
<pre class="language-graql"> <code>
undefine person sub entity;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java-undefine-sub">
<pre class="language-java"> <code>
qb.undefine(label("person").sub("entity")).execute();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

This example will remove `person` from the schema entirely.

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
<pre class="language-graql"> <code>
<!--test-ignore-->
define id "1376496" plays parent;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java3">
<pre class="language-java"> <code>
<!--test-ignore-->
qb.define(var().id(ConceptId.of("1376496")).plays("parent")).execute();
</code>
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
<pre class="language-graql"> <code>
define
man sub person;
woman sub person;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java8">
<pre class="language-java"> <code>
qb.define(label("man").sub("person")).execute();
qb.define(label("woman").sub("person")).execute();
</code>
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
<pre class="language-graql"> <code>
define siblings sub relationship, relates sibling, relates sibling;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java9">
<pre class="language-java"> <code>
qb.define(
  label("siblings").sub("relationship")
    .relates("sibling").relates("sibling")
).execute();
</code>
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
<pre class="language-graql"> <code>
define person plays sibling;
</code>
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java10">
<pre class="language-java"> <code>
qb.define(label("person").plays("sibling")).execute();
qb.define(label("person").plays("sibling")).execute();
</code>
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
<pre class="language-graql"> <code>
define person has nickname;
</code>
</pre>
</div>

<div role="tabpanel" class="tab-pane" id="java11">
<pre class="language-java"> <code>
qb.define(label("person").has("nickname")).execute();
</code>
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->
