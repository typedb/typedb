---
title: The Data
keywords: setup, getting started, basics
tags: [getting-started, reasoning, graql]
summary: "Introduction to what data looks like in Grakn"
sidebar: documentation_sidebar
permalink: /docs/knowledge-model/data
folder: docs
---

## Data

The data is expressed by instantiating specific types of entities, relationships, and concrete resources they are associated with, and assigning roles to the instances played for particular relationships. There are three types of data instances:

**Entities**: instances of entity types, for example, `insert $x isa person` creates an instance of the entity type `person`,

**Resources**: instances of attribute types being associated with particular instances, for example, `insert $x isa person, has name "Elisabeth Niesz"` creates an instance of a `person` with the attribute type `name` given the value "Elizabeth Niesz". The unique identifiers for all instances are defined internally within the Grakn system.

**Relations**: instances of relationship types, for example, `insert (employee:$x, employer:$y) isa employment` creates an instance of the relationship type `employment` between `$x`, playing the role of `employee`, and `$y`, playing the role of `employer`.

{% include note.html content="There are no instances of role types in the data layer." %}

```graql-test-ignore
insert
  $x isa person, has name "Elizabeth Niesz";
  $y isa company, has name "Grakn Labs";
  (employee: $x, employer: $y) isa employment;
commit
```

{% include note.html content="`commit` is useful only when you interact with graql console, do not use it when you import data from terminal" %}

<br /> <img src="/images/knowledge-model7.png" style="width: 400px;" alt="
An image showing the same schema as before, containing 'person', 'company', 'name' and 'employment'.
"/> <br />


### Data Validation

To ensure data is correctly structured (i.e. consistent) with respect to the schema, all data instances are validated against the schema constraints. All the explicitly represented schema constraints, together with the inherited ones, form complete schema templates for particular concept types, which guide the validation.

We will consider the structural validation rules that are enforced in a Grakn knowledge base. The following consistency checks are executed upon `commit` depending on what is being committed:

#### Plays Role Validation

This validation rule simply checks if an entity (which is a role player in a relationship) is allowed to play the role it has been allocated to.

The following insertion will fail, because it is attempting to form an `employment` relationship between two `person` entities, rather than a `person` and a `company`:

```graql-test-ignore
insert
  $x isa person, has name "Elizabeth Niesz";
  $y isa person, has name "John Niesz";
  (employee: $x, employer: $y) isa employment;
commit
```


#### Type Validation

This validation rule ensures that abstract types do not have any instances. For example if we declare the type `vehicle` to be abstract, with `car` and `motorbike` to be sub types of `vehicle`, then only cars and motorbikes are allowed to have instances.

#### Role Validation

This rule checks that non abstract roles are part of a relationship. For example if we declare the role `husband` and forget to link it to any relationship, then this check will fail.

#### Relationship Validation

A relationship is valid if:

* all of the role players of the relationship are allowed to play their corresponding roles.
* it has, at minimum, two roles. For example, a `marriage` with only one role `husband` would fail this check.

#### An Example of Validation

Let us say that we want to model a marriage between a man `Bob` and woman `Alice`.
This will be our first attempt:

<!-- This example is meant to fail TODO: Make this only parse, not execute -->
```graql-test-ignore
define
  human is-abstract sub entity;
  human has name;
  name sub attribute datatype string;

  man is-abstract sub human;
  woman sub human;

  marriage sub relationship;
  marriage relates husband;

  woman plays wife;

insert
  $x has name 'Bob' isa man;
  $y has name 'Alice' isa woman;
  (husband: $x, wife: $y) isa marriage;
```

This first attempt was horrible as we ended up failing all the validation rules.
On commit we will see an error similar to this:

```bash
The Type [man] is abstract and cannot have any instances # (1)
```

Then we will see also the following ones:

```bash
A structural validation error has occurred. Please correct the [`3`] errors found.
Role [wife] does not have a relates connection to any Relationship Type. # (2)
The relation [V24656] has a role player playing the role [wife] which it's type [marriage] is not connecting to via a relates connection # (3)
The type [man] of role player [V41176] is not allowed to play Role [husband] # (4)
```

Lets see why:

1. **Type Validation** failed because we accidentally made `man` abstract and we declared `Bob` to be an instance of `man`.
2. **Role Validation** failed because the role `wife` is not connected to any relationship
3. **Relationship Validation** failed because `marriage` only has one role `husband`.
4. **Plays Role Validation** failed because we forgot to say that a `man` can play the role of `husband`.

Let's fix these issues and try again:

```graql
define
  human is-abstract sub entity;
  human has name;
  name sub attribute datatype string;

  man sub human; # Fix (1)
  woman sub human;

  marriage sub relationship;
  marriage relates husband;
  marriage relates wife; # Fix (2) and (3)

  man plays husband; # Fix (4)
  woman plays wife;

insert
  $x has name 'Bob' isa man;
  $y has name 'Alice' isa woman;
  (husband: $x, wife: $y) isa marriage;
```

Now we are correctly modelling the marriage between `Alice` and `Bob`.
