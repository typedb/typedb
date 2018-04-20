---
title: The Model
keywords: setup, getting started, basics
tags: [getting-started, reasoning, graql]
summary: "Introduction to Grakn's knowledge model"
sidebar: documentation_sidebar
permalink: /docs/knowledge-model/model
folder: docs
---

In Grakn, a knowledge graph is made of two layers: the schema layer and the data layer.

## Schema

In Grakn, the [schema](https://en.wikipedia.org/wiki/Database_schema) is the formal specification of all the relevant concepts and their meaningful associations in a given domain. It allows objects and relationships to be categorised into distinct types, and for generic properties of those types to be expressed. Specifying the schema enables [automated reasoning](https://en.wikipedia.org/wiki/Inference_engine) over the represented knowledge, such as the extraction of implicit information from explicit data ([inference](./inference#rule-and-sub-type-inference)) or discovery of inconsistencies in the data ([validation](./data#data-validation)).  For this reason, the schema must be clearly defined before loading data into the knowledge graph.

[Grakn uses its own declarative language, Graql](https://blog.grakn.ai/knowledge-graph-representation-grakn-ai-or-owl-506065bd3f24#.d6mtn9ic2), and Grakn ontologies use four types of concepts for modeling domain knowledge. The categorization of concept types is enforced in the Grakn knowledge model by declaring every concept type as a subtype (i.e. an extension) of exactly one of the four corresponding, built-in concept types:

**`entity`**: Objects or things in the domain. For example, `person`, `man`, `woman`.

**`relationship`**: Relationships between different domain instances. For example, `marriage`, which is typically a relationship between two instances of entity types (`woman` and `man`), playing roles of `wife` and `husband`, respectively.

**`role`**: Roles involved in specific relationships. For example, `wife`, `husband`.

**`attribute`**: Attributes associated with domain instances. For example, `name`. Resources consist of primitive types and values. They are very much like “data properties” in OWL, and have the following properties:

- Datatype - Indicates the datatype of the attribute. For example if the attribute type is age the datatype would be long.
- Regex - Optional. Can be used to constrain string data types to specific regex patterns.

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
  "date" sub attribute, datatype date;

  employment sub relationship,
    relates employee, relates employer,
    has "date";
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
