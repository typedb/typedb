---
title: Grakn Knowledge Model
keywords: setup, getting started, basics
last_updated: January 2017
tags: [getting-started, reasoning, graql]
summary: "Introducing the fundamentals of the Grakn knowledge model."
sidebar: documentation_sidebar
permalink: /documentation/the-fundamentals/grakn-knowledge-model.html
folder: documentation
comment_issue_id: 17
---

In Grakn, a graph is made of two layers: the ontology layer and the data layer. 

## Ontology

In Grakn, the [ontology](https://en.wikipedia.org/wiki/Ontology_(information_science)) is the formal specification of all the relevant concepts and their meaningful associations in a given domain. It allows objects and relationships to be categorised into distinct types, and for generic properties of those types to be expressed. Specifying the ontology enables [automated reasoning](https://en.wikipedia.org/wiki/Inference_engine) over the represented knowledge, such as the extraction of implicit information from explicit data ([inference](./grakn-knowledge-model.html#rule-and-sub-type-inference)) or discovery of inconsistencies in the data ([validation](./grakn-knowledge-model.html#data-validation)).  For this reason, the ontology must be clearly defined before loading data into the graph. 

[Grakn uses its our own declarative ontology language, Graql](https://blog.grakn.ai/knowledge-graph-representation-grakn-ai-or-owl-506065bd3f24#.d6mtn9ic2), and Grakn ontologies use four types of concepts for modeling domain knowledge. The categorization of concept types is enforced in the Grakn knowledge model by declaring every concept type as a subtype (i.e. an extension) of exactly one of the four corresponding, built-in concept types:

**`entity`**: Objects or things in the domain. For example, `person`, `man`, `woman`.    

**`relationship`**: Relationships between different domain instances. For example, `marriage`, which is typically a relationship between two instances of entity types (`woman` and `man`), playing roles of `wife` and `husband`, respectively.

**`role`**: Roles involved in specific relationships. For example, `wife`, `husband`.     

**`resource`**: Attributes associated with domain instances. For example, `name`. Resources consist of primitive types and values. They are very much like “data properties” in OWL, and have the following properties:

- Datatype - Indicates the datatype of the resource. For example if the resource type is age the datatype would be long.
- Regex - Optional. Can be used to constrain string data types to specific regex patterns.
- Unique - A boolean which indicates if the resource should be unique across the graph.   

<br /> <img src="/images/knowledge-model1.png" style="width: 600px;"/> <br />

### Building an Ontology

In this section, we build up a simple ontology to illustrate the concept types in the Grakn knowledge model. 

We define two entities, `person` and `company`, each of which have a `name` resource.

```graql
insert
  person sub entity,
  has name;
  
  company sub entity,
  has name;
  
  name sub resource, datatype string;
  
```

<br /> <img src="/images/knowledge-model2.png" style="width: 400px;"/> <br />

<br />

We subtype the entities:

```graql
insert
  person sub entity,
  has name;
    
  company sub entity,
  has name;
  
  customer sub person,
  has rating;

  startup sub company,
  has funding;
  
  name sub resource, datatype string;
  rating sub resource, datatype double;
  funding sub resource, datatype long;
```

<br /> <img src="/images/knowledge-model3.png" style="width: 400px;"/> <br />

<br />

We introduce a relationship between a `company` and a `person`:

```graql
insert
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
  
  name sub resource, datatype string;
  rating sub resource, datatype double;
  funding sub resource, datatype long;
  
  employment sub relationship,
    relates employee, relates employer;
  employee sub role;
  employer sub role;
``` 

<br /> <img src="/images/knowledge-model4.png" style="width: 400px;"/> <br />

<br /> 

In the simple example above, we have illustrated the four constructs that relate Grakn concept types to each other:

**`sub`**:  expresses that a concept type is a subtype of (i.e., inherits from) another one. 

* For example, `customer sub person`, `startup sub company`.    

**`has`**: expresses that a concept type can be associated with a given resource type. 

* For example, `person has name`.    

**`plays`**: expresses that instances of a given concept type are allowed to play a specific role. 

* For example, `person plays employee`, `company plays employer`.    

**`relates`**: expresses that a given relationship type involves a specific role.

* For example, `employment relates employee`, `employment relates employer`.

### Relations

Relationships are inherently non-directional and are defined in terms of roles of entities in the relationship. Relations can have multiple attributes. Here we give the employment relationship a date resource.

```graql
insert
  person sub entity,
  has name,
  plays employee;
    
  company sub entity,
  has name,
  plays employer;  
  
  name sub resource, datatype string;
  "date" sub resource, datatype string;
  
  employment sub relationship,
    relates employee, relates employer,
    has "date";
    	
  employee sub role;
  employer sub role;
```

<br /> <img src="/images/knowledge-model6.png" style="width: 400px;"/> <br />

<br />

N-ary relationships are also allowed by Grakn. For example, a three way `employment` relationship that has `employer`, `employee` and `office` roles:

```graql
insert
  employment sub relationship,
    relates employee,
    relates employer,
    relates office;
    	
  employee sub role;
  employer sub role; 
  office sub role;
```

<br /> <img src="/images/knowledge-model8.png" style="width: 400px;"/> <br />

### Inheritance

As in object-oriented programming, the inheritance mechanism in Grakn enables subtypes to automatically take on some of the properties of their supertypes. This simplifies the construction of ontologies and helps keep them succinct. 

<br />The Grakn knowledge model imposes inheritance of all `has` and `plays` constraints on entity, relationship and resource types. As a result, the entity type `customer` inherits `has name` and `plays employee` from the `person` supertype, as shown in the diagram below. 

Likewise, the `startup` entity type inherits `relates name` and `plays employer` from the `company` supertype.

<br /> <img src="/images/knowledge-model5.png" style="width: 400px;"/> <br />

<br />

Therefore, the `employment` relationship between a `company` and `person` is also, implicitly, between `startup` and `customer`.

{% include note.html content="Concept types can be declared as `is-abstract`, meaning that they cannot have any direct instances. For example, `person sub entity is-abstract` expresses that the only instances of `person` can be those that belong to more specialised subtypes of `person`, e.g., `customer`." %}

### Structural Properties
A well-formed Grakn ontology is required to satisfy the following structural properties:

* each concept type can have at most one direct supertype,
* each relationship type must involve at least two distinct role types, 
* each relationship type must involve the same number of roles as its direct supertype. In such a case, every role type involved in the subtype relationship must be a (possibly indirect) subtype of exactly one role type involved in the supertype relationship.

## Data

The data is expressed by instantiating specific types of entities, relationships, and concrete resources they are associated with, and assigning roles to the instances played for particular relationships. There are three types of data instances:

**Entities**: instances of entity types, for example, `insert $x isa person` creates an instance of the entity type `person`,

**Resources**: instances of resource types being associated with particular instances, for example, `insert $x isa person, has name "Elisabeth Niesz"` creates an instance of a `person` with the resource type `name` given the value "Elizabeth Niesz". The unique identifiers for all instances are defined internally within the Grakn system.

**Relations**: instances of relationship types, for example, `insert (employee:$x, employer:$y) isa employment` creates an instance of the relationship type `employment` between `$x`, playing the role of `employee`, and `$y`, playing the role of `employer`.

{% include note.html content="There are no instances of role types in the data layer." %}

```graql
insert
  $x isa person, has name "Elizabeth Niesz";
  $y isa company, has name "Grakn Labs";
  (employee: $x, employer: $y) isa employment;
```

<br /> <img src="/images/knowledge-model7.png" style="width: 400px;"/> <br />


### Data Validation

To ensure data is correctly structured (i.e. consistent) with respect to the ontology, all data instances are validated against the ontology constraints. All the explicitly represented ontology constraints, together with the inherited ones, form complete schema templates for particular concept types, which guide the validation. 

We will consider the structural validation rules that are enforced in a Grakn graph. The following consistency checks are executed upon `commit` depending on what is being committed:

#### Plays Role Validation 

This validation rule simply checks if an entity (which is a role player in a relationship) is allowed to play the role it has been allocated to. 

The following insertion will fail, because it is attempting to form an `employment` relationship between two `person` entities, rather than a `person` and a `company`:

```graql
insert
  $x isa person, has name "Elizabeth Niesz";
  $y isa person, has name "John Niesz";
  (employee: $x, employer: $y) isa employment;
```


#### Type Validation

This validation rule ensures that abstract types do not have any instances. For example if we declare the type `vehicle` to be abstract, with `car` and `motorbike` to be sub types of `vehicle`, then only cars and motorbikes are allowed to have instances. 

#### Role Validation

This rule checks that non abstract roles are part of a relationship. For example if we declare the role `husband` and forget to link it to any relationship, then this check will fail.

#### Relation Validation

A relationship is valid if: 

* all of the role players of the relationship are allowed to play their corresponding roles. 
* it has, at minimum, two roles. For example, a `marriage` with only one role `husband` would fail this check.

#### An Example of Validation

Let us say that we want to model a marriage between a man `Bob` and woman `Alice`.
This will be our first attempt:

<!-- This example is meant to fail TODO: Make this only parse, not execute -->
```graql-test-ignore
insert
  human is-abstract sub entity;
  human has name;
  name sub resource datatype string;
  
  man is-abstract sub human;
  woman sub human;
  
  marriage sub relationship;
  marriage relates husband;

  husband sub role;
  wife sub role;
  
  woman plays wife;
    
  $x has name 'Bob' isa man;
  $y has name 'Alice' isa woman;
  (husband: $x, wife: $y) isa marriage;
```
        
This first attempt was horrible as we ended up failing all the validation rules.         
On commit we will see an error similar to this:

```bash
A structural validation error has occurred. Please correct the [`5`] errors found.
RoleType ['wife'] does not have exactly one relates connection to any RelationType.
The abstract Type ['man'] should not have any instances
Relation Type ['marriage'] does not have two or more roles
The relationship ['RELATION-marriage-2b58b138-2c33-478c-8e8c-e7b357a20941'] has an invalid structure. This is either due to having more role players than roles or the Relation Type ['marriage'] not having the correct relates connection to one of the provided roles. The provided roles('2'): ['husband,wife,']The provided role players('2'): ['husband,wife,']
The type ['man'] of role player ['ENTITY-man-2482cb91-1f12-40ea-b659-49d07d06ddf1'] is not allowed to play RoleType ['husband']
```
    
Lets see why:

1. **Role Validation** failed because the role `wife` is not connected to any relationship
2. **Relation Validation** failed because `marriage` only has one role `husband`.
3. **Type Validation** failed because we accidentally made `man` abstract and we declared `Bob` to be an instance of `man`.
4. **Plays Role Validation** failed because we forgot to say that a `man` can play the role of `husband`.
5. **Relation Validation** failed because `Alice` is playing the role of a `wife` which is not part of a `marriage` and `Bob` is playing the role of a `husband`, which as a man he is not allowed to do.

Let's fix these issues and try again:

```graql
insert
  human is-abstract sub entity;
  human has name;
  name sub resource datatype string;
  
  man sub human; # Fix (3)
  woman sub human;
  
  marriage sub relationship;
  marriage relates husband;
  marriage relates wife; # Fix (1) and (2) and part of (5)
  
  husband sub role;
  wife sub role;
  man plays husband; # Fix (4)
  woman plays wife;  

  $x has name 'Bob' isa man;
  $y has name 'Alice' isa woman;
  (husband: $x, wife: $y) isa marriage;
```

Now we are correctly modelling the marriage between `Alice` and `Bob`.

## Rule and Sub-Type Inference

Inference is a process of extracting implicit information from explicit data. Grakn supports two inference mechanisms:

1. type inference, based on the semantics of the `sub` hierarchies included in the ontology
2. rule-based inference involving user-defined IF-THEN rules.

Both mechanisms can be employed when querying the knowledge graph with Graql, thus supporting retrieval of both explicit and implicit information at query time.      

### Type Inference
The type inference is based on a simple graph traversal along the `sub` edges. Every instance of a given concept type is automatically classified as an (indirect) instance of all (possibly indirect) supertypes of that type. For example, whenever `customer sub human` is in the ontology, every instance of `customer` will be retrieved on the query `match $x isa human`.

Similarly for roles, every instance playing a given role is inferred to also play all its (possibly indirect) super-roles. <!--For example, whenever `inst` plays the role of wife in a relationship of the type `marriage`, the system will infer that `inst` plays also the role of `partner1` in that relationship, given the ontology from Figure 2.-->

The type inference is set ON by default when querying Grakn.  

### Rule-Based Inference
The rule-based inference exploits a set of user-defined datalog rules and is conducted by means of the  reasoner built natively into Grakn. Every rule is declared as an instance of a built-in Grakn type `inference-rule`.

A rule is an expression of the form `when G1 then G2`, where `G1` and `G2` are a pair of Graql patterns. Whenever the "when" pattern `G1` is found in the data, the "then" pattern `G2` can be assumed to exist and optionally materialised (inserted). For example:

```graql
insert
  location sub entity;
  
  located-in sub relationship,
    relates located-subject, relates subject-location;
    	
  located-subject sub role;
  subject-location sub role;

  $transitive-location isa inference-rule,
    when {
      ($x, $y) isa located-in;
      ($y, $z) isa located-in;
    }
    then {
      ($x, $z) isa located-in;
    };

```

<br /> <img src="/images/knowledge-model9.png" style="width: 600px;"/> <br />

<br />

The rule above expresses that, if `$x` has a `located-in` relationship with `$y`, and `$y` has a `located-in` relationship with `$z`, then `$x` has a `located-in` relationship with `$z`. As a concrete example: King's Cross is in London, and London is in the UK, so one can infer that King's Cross is in the UK.

The rule-based inference is currently set OFF by default when querying Grakn, and can be activated as it is needed. For more detailed documentation on rules see [Graql Rules](../graql/graql-rules.html).


## Where Next?
Our [Quickstart Tutorial](../get-started/quickstart-tutorial.html) will show you how to load an ontology, rules and data into Grakn using Graql, and to make basic queries.

You can find additional example code and documentation on this portal. We are always adding more and welcome ideas and improvement suggestions. Please get in touch!

{% include links.html %}

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/17" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.
