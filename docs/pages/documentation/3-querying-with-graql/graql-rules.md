---
title: Graql Rules
keywords: graql, reasoner
last_updated: March 2017
tags: [graql, reasoning]
summary: "Graql Rules"
sidebar: documentation_sidebar
permalink: /documentation/graql/graql-rules.html
folder: documentation
---

## Introduction

Graql uses machine reasoning to perform inference over data types, relationship types, context disambiguation, implicit relationships and dynamic relationships. This allows you to discover hidden and implicit association between data instances through short and concise statements.

The rule-based reasoning allows automated capture and evolution of patterns within the knowledge base. Graql reasoning is performed at query time and is guaranteed to be complete.

Thanks to the reasoning facility, common patterns in the knowledge base can be defined and associated with existing schema elements.
The association happens by means of rules. This not only allows you to compress and simplify typical queries, but offers the ability to derive new non-trivial information by combining defined patterns.

Provided reasoning is turned on, once a given query is executed, Graql will not only query the knowledge base for exact matches but will also inspect the defined rules to check whether additional information can be found (inferred) by combining the patterns defined in the rules. The completeness property of Graql reasoning guarantees that, for a given content of the knowledge base and the defined rule set, the query result shall contain all possible answers derived by combining database lookups and rule applications.

In this section we shall briefly describe the logics behind the rules as well as how can we define pattern associations by suitably defined rules. You may also want to review our [example of how to work with Graql rules](../examples/graql-reasoning.html).

## Graql Rules

Graql rules assume the following general form:

```
if [rule-body] then [rule-head]
```

People familiar with Prolog/Datalog, may recognise it as similar:

```
[rule-head] :- [rule-body].
```

In logical terms, we restrict the rules to be definite Horn clauses. These can be defined either in terms of a disjunction with at most one unnegated atom or an implication with the consequent consisting of a single atom. Atoms are considered atomic first-order predicates - ones that cannot be decomposed to simpler constructs.

In our system we define both the head and the body of rules as Graql patterns. Consequently, the rules are statements of the form:

```
q1 ∧ q2 ∧ ... ∧ qn → p
```

where qs and the p are atoms that each correspond to a single Graql statement. The "when" of the statement (antecedent) then corresponds to the rule body with the "then" (consequent) corresponding to the rule head.

The implication form of Horn clauses aligns more naturally with Graql semantics as we define the rules in terms of the "when" and "then" which directly correspond to the antecedent and consequent of the implication respectively.

## Graql Rule Syntax
In Graql we refer to the body of the rule as the "when" of the rule (antecedent of the implication) and the head as the "then" of the rule (consequent of the implication). Therefore, in Graql terms, we define rule objects in the following way:

```graql-test-ignore
optional-name sub rule,
when {
    ...;
    ...;
    ...;
},
then {
    ...;
};
```

Each dotted line corresponds to a single Graql variable. The rule name is optional and can be omitted, but it is useful if we want to be able to refer to and identify particular rules in the knowledge base. This way, as inference-rule is a concept, we can attach resources to it:

```graql-test-ignore
myRule sub rule,
when {
    ...;
    ...;
    ...;
},
then {
    ...;
};

$myRule has description 'this is my rule';
```

In Graql the "when" of the rule is required to be a [conjunctive pattern](https://en.wikipedia.org/wiki/Logical_conjunction), whereas the "then" should contain a single pattern. If your use case requires a rule with a disjunction on the "when", please notice that, when using the disjunctive normal form, it can be decomposed into series of conjunctive rules.

A classic reasoning example is the ancestor example. The two Graql rules R1 and R2 stated below define the ancestor relationship, which can be understood as either happening between two generations directly between a parent and a child or between three generations when the first generation hop is expressed via a parentship relationship and the second generation hop is captured by an ancestor relationship.

```graql
define

R1 sub rule,
when {
    (parent: $p, child: $c) isa Parent;
},
then {
    (ancestor: $p, descendant: $c) isa Ancestor;
};

R2 sub rule,
when {
    (parent: $p, child: $c) isa Parent;
    (ancestor: $c, descendant: $d) isa Ancestor;
},
then {
    (ancestor: $p, descendant: $d) isa Ancestor;
};
```

When adding rules such as those defined above with Graql, we simply use an `insert` statement, and load the rules, saved as a *.gql* file, into the knowledge base in a standard manner, much as for a schema.

Defining the above rules in terms of predicates and assuming left-to-right directionality of the roles, we can summarise them in the implication form as:

```
R1: parent(X, Y) → ancestor(X, Y)  
R2: parent(X, Z) ∧ ancestor(Z, Y) → ancestor(X, Y)
```

## Allowed Graql Constructs in Rules
The tables below summarise Graql constructs that are allowed to appear in when
and then of rules.

We define atomic queries as queries that contain at most one potentially rule-resolvable statement.
That means atomic queries contain at most one statement that can potentially appear in the "then" of any rule.

### Queries

| Description        | when | then
| -------------------- |:--|:--|
| atomic queries | ✓ | ✓ |
| conjunctive queries        | ✓ | x |
| disjunctive queries        | x | x |  

### Variable Patterns

| Description        | Pattern Example           | when | then
| -------------------- |:--- |:--|:--|
| `isa` | `$x isa person;` | ✓ | x |
| `id`  | `$x id "264597";` | ✓ | variable needs to be bound within the then  |
| `val` | `$x val contains "Bar";`  | ✓ | indirect only  |
| `has` | `$x has age < 20;` | ✓ | ✓ |
| `relationship` | `(parent: $x, child: $y) isa parentship;` | ✓ | ✓ |
| attribute comparison | `$x val > $y;`  | ✓ | x |
| `!=` | `$x != $y;` | ✓ | x |

### Type Properties

| Description        | Pattern Example   | when | then
| -------------------- |:---|:--|:--|
| `sub`        | `$x sub type;` | ✓| x |
| `plays` | `$x plays parent;` |✓| x |
| `has`        | `$x has firstname;` | ✓ | x |  
| `relates`   | `marriage relates $x;` | ✓ | x |
| `is-abstract` | `$x is-abstract;` | ✓ | x |
| `datatype` | `$x isa attribute, datatype string;` | ✓| x |
| `regex` | `$x isa attribute, regex /hello/;` | ✓ | x |

## Configuration options
Graql provides an option to decide whether reasoning should be performed.
If the reasoning is not turned on, the rules will not be triggered and no knowledge will be inferred.

## Where Next?

There is a complete [example of how to work with Graql rules](../examples/graql-reasoning.html) available, and reasoning is also discussed in our [quick start tutorial](../get-started/quickstart-tutorial.html).

{% include links.html %}
