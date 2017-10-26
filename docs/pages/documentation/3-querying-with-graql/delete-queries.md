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

A delete query will delete things bound to the specified variables for every result of the [match](matches.html).

To follow along, or experiment further, with the examples given below, please load the *basic-genealogy.gql* file, which can be found in the *examples* directory of the Grakn installation zip, or on [Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).


<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell1" data-toggle="tab">Graql</a></li>
    <li><a href="#java1" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell1">
<pre>
match $x isa person; delete $x;
commit
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre>
qb.match(var("x").isa("person")).delete("x").execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/42" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

{% include links.html %}

