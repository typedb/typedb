---
title: Ask Queries
keywords: graql, query, ask
last_updated: August 10, 2016
tags: [graql]
summary: "Graql Ask Queries"
sidebar: documentation_sidebar
permalink: /documentation/graql/ask-queries.html
folder: documentation
---

An ask query will return whether the given [match query](match-queries.html) has any results. To follow along, or experiment further, with the examples given below, please load the *basic-genealogy.gql* file, which can be found in the *examples* directory of the Grakn installation zip, or on [Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell1" data-toggle="tab">Graql</a></li>
    <li><a href="#java1" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell1">
<pre>
match divorce sub relation; ask;
match marriage sub relation; ask;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre>
qb.match(label("divorce").sub("relation")).ask().execute();
qb.match(label("marriage").sub("relation")).ask().execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/42" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

{% include links.html %}


