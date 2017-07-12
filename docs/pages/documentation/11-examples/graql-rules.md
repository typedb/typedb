---
title: Reasoning with Graql
keywords: migration
last_updated: February 2017
tags: [reasoning, examples]
summary: "An example to illustrate inference using genealogy data."
sidebar: documentation_sidebar
permalink: /documentation/examples/graql-reasoning.html
folder: documentation
comment_issue_id: 27
---

## Introduction

This is an example of how to use the reasoning facilities of Graql to infer information about family relationships from a simple dataset of genealogy data. The data has been used previously as the basis of a [blog post](https://blog.grakn.ai/family-matters-1bb639396a24#.2e6h72y0m) to illustrate the fundamentals of the Grakn visualiser as well as reasoning and analytics capabilities.

As the blog post explained, the original data was a [document](http://www.lenzenresearch.com/titusnarrlineage.pdf) from [Lenzen Research](http://www.lenzenresearch.com/) that described the family history of Catherine Niesz Titus for three generations of her maternal lineage. Our team gathered together a set of CSV files containing basic information about the family, such as names, dates of birth, death and marriage, who was a parent of who, and who married who.

The full genealogy-graph project can be found on Grakn's [sample-datasets](https://github.com/graknlabs/sample-datasets/tree/master/genealogy-graph) repository on Github. However, we will use a simpler ontology for this example, which is found in *basic-genealogy.gql*, located within the */examples* folder of the Grakn installation. The file can also be found [on Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

In this example, we will explore how to use Grakn to make inferences and find information from the data that is not explicit in the dataset. You can find documentation about writing rules in Graql [here](https://grakn.ai/pages/documentation/graql/graql-rules.html).


## Ontology and Data

On GRAKN.AI, the first step when working with a dataset is to define its ontology in Graql. The ontology is a way to describe the entities and their relations, so the underlying graph can store them as nodes and edges. You can find out more in our guide to the Grakn Knowledge Model. The ontology allows Grakn to perform:

* logical reasoning over the represented knowledge, such as the extraction of implicit information from explicit data (inference)
* discovery of inconsistencies in the data (validation).

The complete ontology for the genealogy-graph demo is in [basic-genealogy.gql](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

The ontology contains a `person` entity, and a number of possible family relationships between `person` entities, such as parent/child, siblings, grandparent/grandchild, which we will discuss shortly. The data for this example is discussed more fully in our [example on CSV migration](./CSV-migration.html), which discusses how the original data is laid out in CSV files and migrated into GRAKN.AI. However, for this example, you do not need any familiarity with CSV migration or the origin of the data we are using.

## Loading the Example

The *basic-genealogy.gql* file contains the ontology, data and rules needed for this example. To load it into a graph, make sure the engine is running and choose a clean keyspace in which to work (in the example below we use the default keyspace, so we are cleaning it before we get started):

```bash
<relative-path-to-Grakn>/bin/grakn.sh clean
<relative-path-to-Grakn>/bin/grakn.sh start
<relative-path-to-Grakn>/bin/graql.sh -f <relative-path-to-Grakn>/examples/basic-genealogy.gql
```

When the terminal prompt returns, the data will have been loaded into the default keyspace, and you can start to look it using the [Grakn visualiser](../grakn-dashboard/visualiser.html), by navigating to [http://localhost:4567](http://localhost:4567) in your browser. Submit a query, such as `match $x isa person` to check that all is well with your graph.

## Inferring Family Relations

By default, inference is switched off, and the only information you can query Grakn about is what was directed loaded from the data. For example, if you submit the following query to the visualiser:

```graql
match (child: $c, parent: $p) isa parentship;
```

You will receive parentship results, but if you clear the query and then submit a new query for gender-specific results, say mother-daughter relationships:

```graql
match (mother: $c, daughter: $p) isa parentship;
```

You will receive no results at all. To find inferred relations between the people in our dataset, you need to activate inference in the Grakn visualiser. Open the Query settings under the cog button, which is on the far right hand side of the horizontal icon menu (at the top of the screen).

You will see the "Activate inference" checkbox. Check it, and Grakn is ready to start building some new information about the family. Try the query again:

```graql
match (mother: $c, daughter: $p) isa parentship;
```

You should see something similar to the screenshot below in your visualiser window.

![Person query](/images/match-isa-mother-daughter.png)

{% include note.html content="You can alternatively make queries in the graql shell. You will need to use `./graql.sh -n` to enable inference when you start the shell." %}

## Inference Rules

Inference is the process of deducing new information from available data. For example, given the following statements:

```
If grass is not an animal.
If vegetarians only eat things which are not animals.
If sheep only eat grass.
```

It is possible to infer the following:

```
Then sheep are vegetarians.
```

The initial statements can be seen as a set of premises. If all the premises are met we can infer a new fact (that sheep are vegatarians). If we hypothetise that sheep are vegetarians then the whole example can be expressed with a particular two-block structure: IF some premises are met, THEN a given hypothesis is true.

This is how reasoning in Graql works. It checks whether the statements in the first block can be verified and, if they can, infers the statement in the second block. The rules are written in Graql, and we call the first set of statements (the IF part or, if you prefer, the antecedent) simply the left hand side (LHS). The second part, not surprisingly, is the right hand side (RHS). Using Graql, both sides of the rule are enclosed in curly braces and preceded by, respectively, the keywords `lhs` and `rhs`.

{% include note.html content="The full documentation for writing rules in Graql is available from [here](https://grakn.ai/pages/documentation/graql/graql-rules.html)." %}


### Example 1: Specific relations between parents and children

As we saw above, it is possible for Grakn to infer the gender-specific roles (`mother`, `father`, `daughter`, `son`) that a `person` entity plays. It does this by applying the following rules:

```graql
$genderizeParentships1 isa inference-rule
lhs
{(parent: $p, child: $c) isa parentship;
$p has gender "male";
$c has gender "male";
}
rhs
{(father: $p, son: $c) isa parentship;};

$genderizeParentships2 isa inference-rule
lhs
{(parent: $p, child: $c) isa parentship;
$p has gender "male";
$c has gender "female";
}
rhs
{(father: $p, daughter: $c) isa parentship;};

$genderizeParentships3 isa inference-rule
lhs
{(parent: $p, child: $c) isa parentship;
$p has gender "female";
$c has gender "male";
}
rhs
{(mother: $p, son: $c) isa parentship;};

$genderizeParentships4 isa inference-rule
lhs
{(parent: $p, child: $c) isa parentship;
$p has gender "female";
$c has gender "female";
}
rhs
{(mother: $p, daughter: $c) isa parentship;};
```

The four rules can be broken down as follows:

* LHS: In the `parentship` relation between child `$c` and parent `$p`, do they both have a `gender` resource that is `male`?
	* RHS: The `parentship` relation is between `father` and `son`
* LHS: In the `parentship` relation between child `$c` and parent `$p`, does `$c` have a `gender` resource that is `female` and `$p` have a `gender` resource that is `male`?
	* RHS: The `parentship` relation is between `father` and `daughter`
* LHS: In the `parentship` relation between child `$c` and parent `$p`, does `$c` have a `gender` resource that is `male` and `$p` have a `gender` resource that is `female`?
	* RHS: The `parentship` relation is between `mother` and `son`
* LHS: In the `parentship` relation between child `$c` and parent `$p`, do both have a `gender` resource that is `female`?
	* RHS: The `parentship` relation is between `mother` and `daughter`

We can use the rule to easily discover the sons who have the same name as their fathers:

```graql
match (father: $p, son: $c) isa parentship; $p has firstname $n; $c has firstname $n;
```

In the genealogy-graph example, there should be two results returned. William and John are names shared between father/son pairs.

### Example 2: A `grandparentship` relation

The *basic-genealogy* file contains a number of rules for setting up family relationships, such as siblings, cousins, in-laws and the following, which sets up a relation called `grandparentship`:

```graql
$parentsOfParentsAreGrandparents isa inference-rule
lhs
{(parent:$p, child: $gc) isa parentship;
(parent: $gp, child: $p) isa parentship;
}
rhs
{(grandparent: $gp, grandchild: $gc) isa grandparentship;};
```

Here, the left hand side rules check two statements:

* does `$p` play the `parent` role in a `parentship` relation, with (`$gc`) playing the `child` role?
* does `$p` also play the `child` role in a `parentship` relation, with (`$gp`) playing the `parent` role?

If so, the right hand side of the rules state that:

* `$gp` and `$gc` are in a `grandparentship` relation, where `$gp` plays the `grandparent` role and `$gc` plays the `grandchild` role.

Some additional rules can add more specifics to the `grandparentship` and assign the entities to the roles `grandson`, `granddaughter`, `grandmother` and `grandfather`:

```graql
$grandParents1 isa inference-rule
lhs
{($p, son: $gc) isa parentship;
(father: $gp, $p) isa parentship;
}
rhs
{(grandfather: $gp, grandson: $gc) isa grandparentship;};

$grandParents2 isa inference-rule
lhs
{($p, daughter: $gc) isa parentship;
(father: $gp, $p) isa parentship;
}
rhs
{(grandfather: $gp, granddaughter: $gc) isa grandparentship;};

$grandParents3 isa inference-rule
lhs
{($p, daughter: $gc) isa parentship;
(mother: $gp, $p) isa parentship;
}
rhs
{(grandmother: $gp, granddaughter: $gc) isa grandparentship;};

$grandParents4 isa inference-rule
lhs
{($p, son: $gc) isa parentship;
(mother: $gp, $p) isa parentship;
}
rhs
{(grandmother: $gp, grandson: $gc) isa grandparentship;};
```

Much as above for the `parentship` relation, the rules can be broken down as follows:

* LHS: There is a `parentship` relation between son `$gc` and parent `$p`, and another `parentship` relation where `$p` is now in the child role and `$gp` is a father. 
	* RHS: The `grandparentship` relation is between `grandfather` and `grandson`
* LHS: There is a `parentship` relation between daughter `$gc` and parent `$p`, and another `parentship` relation where `$p` is now in the child role and `$gp` is a father. 
	* RHS: The `grandparentship` relation is between `grandfather` and `granddaughter`
* LHS: There is a `parentship` relation between daughter `$gc` and parent `$p`, and another `parentship` relation where `$p` is now in the child role and `$gp` is a mother. 
	* RHS: The `grandparentship` relation is between `grandmother` and `granddaughter`
* LHS: There is a `parentship` relation between son `$gc` and parent `$p`, and another `parentship` relation where `$p` is now in the child role and `$gp` is a mother. 
	* RHS: The `grandparentship` relation is between `grandmother` and `grandson`

These rules allow us to find all `grandparentship` relations, and further, it allows us to query, for example, which grandfather/grandson pairs share the same name:

```graql
match (grandfather: $x, grandson: $y) isa grandparentship; $x has firstname $n; $y has firstname $n;
```

In the genealogy-graph example, there should be three results returned. George, Jacob and John are names shared between grandfather/grandson pairs.

![Person query](/images/match-grandfather-grandson-names.png)

### Example 3: A `cousins` relation

Another rule can be used to infer `person` entities who are cousins:

```
$peopleWithSiblingsParentsAreCousins isa inference-rule
lhs
{
(parent: $p, child: $c1) isa parentship;
($p, $p2) isa siblings;
(parent: $p2, child: $c2) isa parentship;
}
rhs
{(cousin1: $c1, cousin2: $c2) isa cousins;};

```

* `$p` is a `parent` and has a `child` `$c1`
* `$p` is a sibling to `$p2`
* `$p2` has a `child` `$c2`
* Then `$c1` and `$c2` are in a `cousins` relationship.

It would also be possible to infer uncle/aunt/nephew/niece relationships, using a similar approach. We will leave that as an exercise for the reader!

## Other Queries 

There are some queries that are currently not possible. For example:

* Who has no children?
* Who has never married?
* Who has the most grandchildren?
* Who has the most brothers?

If you want to infer information from a dataset such as this example, and want to check whether it is possible, please reach out to us via [Slack](https://grakn.ai/slack.html) or our [discussion forum](https://discuss.grakn.ai).


## Where Next?

This example has illustrated the basics of the inference rules used by Graql. Having read it, you may want to further study our documentation about [Graql Rules](../graql/graql-rules.html) and further investigate the [example that imports the genealogy data from CSV format into Grakn](./CSV-migration.html).

We will be using the genealogy data throughout our documentation. For an overview, please see our [Family Matters](https://blog.grakn.ai/family-matters-1bb639396a24#.uelgekrn2) blog post.

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/27" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.

{% include links.html %}
