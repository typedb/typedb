---
title: Grakn Knowledge Model
keywords: setup, getting started, basics
last_updated: January 2017
tags: [getting-started, reasoning, graql]
summary: "Introducing the fundamentals of the Grakn knowledge model."
sidebar: documentation_sidebar
permalink: /documentation/the-fundamentals/grakn-knowledge-model.html
folder: documentation
---

In Grakn, a knowledge base is made of two layers: the schema layer and the data layer.

## Schema

In Grakn, the [schema](https://en.wikipedia.org/wiki/Database_schema) is the formal specification of all the relevant concepts and their meaningful associations in a given domain. It allows objects and relationships to be categorised into distinct types, and for generic properties of those types to be expressed. Specifying the schema enables [automated reasoning](https://en.wikipedia.org/wiki/Inference_engine) over the represented knowledge, such as the extraction of implicit information from explicit data ([inference](./grakn-knowledge-model.html#rule-and-sub-type-inference)) or discovery of inconsistencies in the data ([validation](./grakn-knowledge-model.html#data-validation)).  For this reason, the schema must be clearly defined before loading data into the knowledge base.

[Grakn uses its own declarative language, Graql](https://blog.grakn.ai/knowledge-graph-representation-grakn-ai-or-owl-506065bd3f24#.d6mtn9ic2), and Grakn ontologies use four types of concepts for modeling domain knowledge. The categorization of concept types is enforced in the Grakn knowledge model by declaring every concept type as a subtype (i.e. an extension) of exactly one of the four corresponding, built-in concept types:

**`entity`**: Objects or things in the domain. For example, `person`, `man`, `woman`.

**`relationship`**: Relationships between different domain instances. For example, `marriage`, which is typically a relationship between two instances of entity types (`woman` and `man`), playing roles of `wife` and `husband`, respectively.

**`role`**: Roles involved in specific relationships. For example, `wife`, `husband`.

**`attribute`**: Attributes associated with domain instances. For example, `name`. Resources consist of primitive types and values. They are very much like “data properties” in OWL, and have the following properties:

- Datatype - Indicates the datatype of the attribute. For example if the attribute type is age the datatype would be long.
- Regex - Optional. Can be used to constrain string data types to specific regex patterns.
- Unique - A boolean which indicates if the attribute should be unique across the knowledge base.

<br /> <img src="/images/knowledge-model1.png" style="width: 600px;" alt="
Image showing a schema where an entity 'person' has an attribute 'name' and plays the roles 'wife' and 'husband' in
a 'marriage' relationship"
/> <br />

### Building a Schema

In this section, we build up a simple schema to illustrate the concept types in the Grakn knowledge model.

We define two entities, `person` and `company`, each of which have a `name` attribute.

```graql
define
  person sub entity,
  has name;

  company sub entity,
  has name;

  name sub attribute, datatype string;

```

<br /> <img src="/images/knowledge-model2.png" style="width: 400px;" alt="
Image showing a schema where types 'person' and 'company' have an attribute 'name'
"/> <br />

<br />

We subtype the entities:

```graql
define
  person sub entity,
  has name;

  company sub entity,
  has name;

  customer sub person,
  has rating;

  startup sub company,
  has funding;

  name sub attribute, datatype string;
  rating sub attribute, datatype double;
  funding sub attribute, datatype long;
```

<br /> <img src="/images/knowledge-model3.png" style="width: 400px;" alt="
Image showing the above schema, but with additional types 'customer' which subs 'person' and 'startup' subs 'company'
"/> <br />

<br />

We introduce a relationship between a `company` and a `person`:

```graql
define
  person sub entity,
  has name,
  plays employee;

  company sub entity,
  has name,
  plays employer;

  customer sub person,
  has rating;

  startup sub company,
  has funding;

  name sub attribute, datatype string;
  rating sub attribute, datatype double;
  funding sub attribute, datatype long;

  employment sub relationship,
    relates employee, relates employer;
  employee sub role;
  employer sub role;
```

<br /> <img src="/images/knowledge-model4.png" style="width: 400px;" alt="
Image showing the same schema with an 'employment' relationship comprising the roles 'employee' and 'employer'. 'person'
plays 'employee' and 'company' plays 'employer'
"/> <br />

<br />

In the simple example above, we have illustrated the four constructs that relate Grakn concept types to each other:

**`sub`**:  expresses that a concept type is a subtype of (i.e., inherits from) another one.

* For example, `customer sub person`, `startup sub company`.

**`has`**: expresses that a concept type can be associated with a given attribute type.

* For example, `person has name`.

**`plays`**: expresses that instances of a given concept type are allowed to play a specific role.

* For example, `person plays employee`, `company plays employer`.

**`relates`**: expresses that a given relationship type involves a specific role.

* For example, `employment relates employee`, `employment relates employer`.

### Relations

Relationships are inherently non-directional and are defined in terms of roles of entities in the relationship. Relations can have multiple attributes. Here we give the employment relationship a date attribute.

```graql
define
  person sub entity,
  has name,
  plays employee;

  company sub entity,
  has name,
  plays employer;

  name sub attribute, datatype string;
  "date" sub attribute, datatype string;

  employment sub relationship,
    relates employee, relates employer,
    has "date";

  employee sub role;
  employer sub role;
```

<br /> <img src="/images/knowledge-model6.png" style="width: 400px;" alt="
An image showing the schema where the relationship 'employment' now has the attribute 'date'.
"/> <br />
<br />

N-ary relationships are also allowed by Grakn. For example, a three way `employment` relationship that has `employer`, `employee` and `office` roles:

```graql
define
  employment sub relationship,
    relates employee,
    relates employer,
    relates office;

  employee sub role;
  employer sub role;
  office sub role;
```

<br /> <img src="/images/knowledge-model8.png" style="width: 400px;" alt="
An image showing a relationship 'employment' relating three roles: 'employee', 'employer' and 'office'
"/> <br />

### Inheritance

As in object-oriented programming, the inheritance mechanism in Grakn enables subtypes to automatically take on some of the properties of their supertypes. This simplifies the construction of ontologies and helps keep them succinct.

<br />The Grakn knowledge model imposes inheritance of all `has` and `plays` constraints on entity, relationship and attribute types. As a result, the entity type `customer` inherits `has name` and `plays employee` from the `person` supertype, as shown in the diagram below.

Likewise, the `startup` entity type inherits `relates name` and `plays employer` from the `company` supertype.

<br /> <img src="/images/knowledge-model5.png" style="width: 400px;" alt="
An image showing that 'customer' inherits the property of playing 'employee' from its super-type 'person'.
"/> <br />

<br />

Therefore, the `employment` relationship between a `company` and `person` is also, implicitly, between `startup` and `customer`.

{% include note.html content="Concept types can be declared as `is-abstract`, meaning that they cannot have any direct instances. For example, `person sub entity is-abstract` expresses that the only instances of `person` can be those that belong to more specialised subtypes of `person`, e.g., `customer`." %}

### Structural Properties
A well-formed Grakn schema is required to satisfy the following structural properties:

* each concept type can have at most one direct supertype,
* each relationship type must involve at least two distinct role types,
* each relationship type must involve the same number of roles as its direct supertype. In such a case, every role type involved in the subtype relationship must be a (possibly indirect) subtype of exactly one role type involved in the supertype relationship.

## Data

The data is expressed by instantiating specific types of entities, relationships, and concrete resources they are associated with, and assigning roles to the instances played for particular relationships. There are three types of data instances:

**Entities**: instances of entity types, for example, `insert $x isa person` creates an instance of the entity type `person`,

**Resources**: instances of attribute types being associated with particular instances, for example, `insert $x isa person, has name "Elisabeth Niesz"` creates an instance of a `person` with the attribute type `name` given the value "Elizabeth Niesz". The unique identifiers for all instances are defined internally within the Grakn system.

**Relations**: instances of relationship types, for example, `insert (employee:$x, employer:$y) isa employment` creates an instance of the relationship type `employment` between `$x`, playing the role of `employee`, and `$y`, playing the role of `employer`.

{% include note.html content="There are no instances of role types in the data layer." %}

```graql
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

```graql
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

  husband sub role;
  wife sub role;

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
  
  husband sub role;
  wife sub role;
  man plays husband; # Fix (4)
  woman plays wife;

insert
  $x has name 'Bob' isa man;
  $y has name 'Alice' isa woman;
  (husband: $x, wife: $y) isa marriage;
```

Now we are correctly modelling the marriage between `Alice` and `Bob`.

## Rule and Sub-Type Inference

Inference is a process of extracting implicit information from explicit data. Grakn supports two inference mechanisms:

1. type inference, based on the semantics of the `sub` hierarchies included in the schema
2. rule-based inference involving user-defined IF-THEN rules.

Both mechanisms can be employed when querying the knowledge base with Graql, thus supporting retrieval of both explicit and implicit information at query time.

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

  located-subject sub role;
  subject-location sub role;

  transitive-location sub rule,
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

The rule-based inference is currently set OFF by default when querying Grakn, and can be activated as it is needed. For more detailed documentation on rules see [Graql Rules](../graql/graql-rules.html).


## Where Next?
Our [Quickstart Tutorial](../get-started/quickstart-tutorial.html) will show you how to load a schema, rules and data into Grakn using Graql, and to make basic queries.

You can find additional example code and documentation on this portal. We are always adding more and welcome ideas and improvement suggestions. Please get in touch!

{% include links.html %}
