---
title: The Inference
keywords: setup, getting started, basics
tags: [getting-started, reasoning, graql]
summary: "Introduction to Grakn's inference strategy"
sidebar: documentation_sidebar
permalink: /docs/knowledge-model/inference
folder: docs
---

## Rule and Sub-Type Inference

Inference is a process of extracting implicit information from explicit data. Grakn supports two inference mechanisms:

1. type inference, based on the semantics of the `sub` hierarchies included in the schema
2. rule-based inference involving user-defined IF-THEN rules.

Both mechanisms can be employed when querying the knowledge graph with Graql, thus supporting retrieval of both explicit and implicit information at query time.

### Type Inference
The type inference is based on a simple traversal along the `sub` links. Every instance of a given concept type is automatically classified as an (indirect) instance of all (possibly indirect) supertypes of that type. For example, whenever `customer sub human` is in the schema, every instance of `customer` will be retrieved on the query `match $x isa human`.

Similarly for roles, every instance playing a given role is inferred to also play all its (possibly indirect) super-roles. <!--For example, whenever `inst` plays the role of wife in a relationship of the type `marriage`, the system will infer that `inst` plays also the role of `partner1` in that relationship, given the schema from Figure 2.-->

The type inference is set ON by default when querying Grakn.

### Rule-Based Inference
The rule-based inference exploits a set of user-defined datalog rules and is conducted by means of the  reasoner built natively into Grakn. Every rule is declared as an instance of a built-in Grakn type `rule`.

A rule is an expression of the form `when G1 then G2`, where `G1` and `G2` are a pair of Graql patterns. Whenever the "when" pattern `G1` is found in the data, the "then" pattern `G2` can be assumed to exist. For example:

```graql
define
  location sub entity;

  located-in sub relationship,
    relates located-subject, relates subject-location;

  transitive-location
    when {
      ($x, $y) isa located-in;
      ($y, $z) isa located-in;
    }
    then {
      (located-in:$x, located-x:$z) isa located-in;
    };

```

<br /> <img src="/images/knowledge-model9.png" style="width: 600px;" alt="
An image showing that King's Cross is 'located-in' London and London is 'located-in' the UK - therefore King's Cross is
'located-in' the UK
"/> <br />

<br />

The rule above expresses that, if `$x` has a `located-in` relationship with `$y`, and `$y` has a `located-in` relationship with `$z`, then `$x` has a `located-in` relationship with `$z`. As a concrete example: King's Cross is in London, and London is in the UK, so one can infer that King's Cross is in the UK.

The rule-based inference is currently set ON by default when querying Grakn. It can be deactivated if needed. For more detailed documentation on rules see [Graql Rules](../building-schema/defining-rules).


## Where Next?
Our [Quickstart Tutorial](../get-started/quickstart-tutorial) will show you how to load a schema, rules and data into Grakn using Graql, and to make basic queries.

You can find additional example code and documentation on this portal. We are always adding more and welcome ideas and improvement suggestions. Please get in touch!
