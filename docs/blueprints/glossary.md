---
status: incomplete
---

# Glossary

## Type system

### Type

Any type in the type system.

### Schema type

A type containg data inserted into the database. Cases:

* Entity type,
* Relation type,
* Attribute type,
* Trait type.

### Value type

A type containing a pre-defined set of values. Cases:

* Primitive value type: `bool`, `string`, ...
* Structured value type / struct value type / struct: user-defined

### Data instance / instance

An element in an entity, relation, or attribute type. Special cases:

* Entity: An element in an entity type.
* Relation: An element in a relation type.
* Attribute: An element in attribute type.
* Object: An element in an entity or relation type.
* Roleplayer: Object that is cast into the element of a role type.
* Owner: Object that is cast into the element of an ownership type.

### Data value / value

An element in a value type.

### Attribute instance value / attribute value

An attribute cast into an element of its value type.

### Data element / element

Any element in any type (i.e. value or instance).

### Concept

A element or a type.

### Concept row

Mapping of variables to concepts

### Stream

An ordered concept row.

### Answer set

The set of concept rows that satisfy a pattern in the minimal way.

### Answer

An element in the answer set of a pattern.


## TypeQL syntax

### Schema query

Query pertaining to schema manipulation

### Data pipeline

Query pertaining to data manipulation or retrieval

### Stage

#### Data manipulation stage

* `match`, `insert`, `delete`, `define`, `undefine`,

#### Stream modifier stage

* `select`, `sort`, ...

### Functions

Callable `match-return` query.

* scalar function: returns a concept 1-tuple. signature: `-> A`
* tuple function: returns a concept n-tuple, n > 1. signature `-> A, B, C`
* stream function: returns a stream of concept tuples. signature `-> { A, ... }`
* (scalar stream function: returns a stream of concept 1-tuples. `signature -> { A }`)
* (tuple stream function: returns a stream of concept n-tuples, n > 1. signature `-> { A, B, C}`)

### Statement

A syntactic unit (cannot be subdivided). Variations:

* Simple statement: statement not containing `,`
* Combined statement: statement combined with `,`

### Pattern

Context for variables (this "context" describes properties of the variables), used to form a data retrieval query.

### Stream reduction / reduction

* `list`, `sum`, `count` ...

### Clause

part of a query pipeline

### Scope

`{ pattern }`

### Suffix

`[]` and `?`.
