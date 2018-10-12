---
title: Grakn Python Client
keywords: setup, getting started, download, driver
tags: [getting-started]
sidebar: documentation_sidebar
permalink: /docs/language-drivers/client-python
folder: docs
symlink: true
---

# Grakn Python Client

A Python client for [Grakn](https://grakn.ai)

Works with Grakn >=1.3.0 up to this driver version, and Python >= 3.6

# Installation

```
pip3 install grakn
```

To obtain the Grakn database itself, head [here](https://grakn.ai/pages/documentation/get-started/setup-guide.html) for the setup guide.

# Quickstart

## Grakn client, sessions, and transactions

In the interpreter or in your source, import `grakn`:

```
import grakn
```

You can then instantiate a client, open a session, and create transactions.     
_NOTE_: Grakn's default gRPC port is 48555 for versions >=1.3. Port 4567 (the old default REST endpoint) is deprecated for clients.

```
client = grakn.Grakn(uri="localhost:48555")
session = client.session(keyspace="mykeyspace")
tx = session.transaction(grakn.TxType.WRITE)
```

Alternatively, you can also use `with` statements, which automatically closes sessions and transactions:
```
client = grakn.Grakn(uri="localhost:48555")
with client.session(keyspace="mykeyspace") as session:
    with session.transaction(grakn.TxType.READ) as tx:
        ...
```

Credentials can be passed into the initial constructor as a dictionary, if you are a KGMS user:
```
client = grakn.Grakn(uri='localhost:48555', credentials={'username': 'xxxx', 'password': 'yyyy'})
```

If a transaction fails (throws exception), it automatically gets closed.
Also note that closing a transaction means that _all concept objects retrieved from that transaction are no longer queryable via internal getters/setters_.

For example
```
# obtain a tx
tx = session.transaction(grakn.TxType.READ)
person_type = tx.get_schema_concept("person") # creates a local Concept object with the `tx` bound internally
print("Label of person type: {0}".format(person_type.label()) # uses interal `tx` to retrieve label from server
tx.close()
# the following will raise an exception, internally bound transaction is closed
print("Label of person type: {0}".format(person_type.label())
```

## Basic retrievals and insertions 

You can execute Graql queries and iterate through the answers as follows:
```
# Perform a query that returns an iterator of ConceptMap answers
answer_iterator = tx.query("match $x isa person; limit 10; get;") 
# Request first response
a_concept_map_answer = next(answer_iterator) 
# Get the dictionary of variables : concepts, retrieve variable 'x'
person = a_concept_map_answer.map()['x']      

# we can also iterate using a `for` loop
some_people = []
for concept_map in answer_iterator:           
    # Get 'x' again, without going through .map()
    some_people.append(concept_map.get('x'))    
    break 

# skip the iteration and .get('x') and extract all the concepts in one go
remaining_people = answer_iterator.collect_concepts() 

# explicit close if not using `with` statements
tx.close()
```

_NOTE_: queries will return almost immediately -- this is because Grakn lazily evaluates the request on the server when
the local iterator is consumed, not when the request is created. Each time `next(iter)` is called, the client executes a fast RPC request 
to the server to obtain the next concrete result.

You might also want to make some insertions using `.query()`
```
# Perform insert query that returns an iterator of ConceptMap of inserted concepts
insert_iterator = tx.query("insert $x isa person, has birth-date 2018-08-06;") 
concepts = insert_iterator.collect_concepts()
print("Inserted a person with ID: {0}".format(concepts[0].id))
# Don't forget to commit() to persist changes
tx.commit()
```

Or you can use the methods available on Concept objects, known as the Concept API:
```
person_type = tx.get_schema_concept("person") # retrieve the "person" schema type 
person = person_type.create()                 # instantiate a person
birth_date_type = tx.get_schema_concept("birth-date") " retrieve the "birth-date" schema type
date = datetime.datetime(year=2018, month=8, day=6) # requires `import datetime`
birth_date = birth_date_type.create(date)     # instantiate a date with a python datetime object
person.has(birth_date)                        # attach the birth_date concept to the person concept 
tx.commit()                                   # write changes to Grakn 
```


# API reference


First create a new Grakn object/client with

```
// URI must be a string containing host address and gRPC port of a running Grakn instance, e.g. "localhost:48555"
client = grakn.Grakn(URI)
```

on the Grakn object the following methods are available:

**Grakn**

| Method                                   | Return type       | Description                                          |
| ---------------------------------------- | ----------------- | ---------------------------------------------------- |
| `session(String keyspace)`               | *Session*         | Return a new Session bound to the specified keyspace |
| `keyspaces().delete(String keyspace)`      | *None*            | Deletes the specified keyspace                       |
| `keyspaces().retrieve()`                   | List of *String*  | Retrieves all available keyspaces                    |



on the Session the following methods are available:

**Session**

| Method                            | Return type   | Description                                                                           |
| --------------------------------- | ------------- | ------------------------------------------------------------------------------------- |
| `transaction(grakn.TxType)`       | *Transaction* | Return a new Transaction bound to the keyspace of this session                        |
| `close()`                         | *None*        | This must be used to correctly terminate session and close communication with server. |


Once obtained a `Transaction` you will be able to:

 **Transaction**

| Method                                                       | Return type               | Description                                                                                                                                                                                    |
| ------------------------------------------------------------ | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `query(String graql_query, infer=True)`                      | Iterator of *Answer*      | Executes a Graql query on the session keyspace. It's possible to specify whether to enable inference, which is ON by default.                                                                  |
| `commit()`                                                   | *None*                    | Commit current Transaction, persisting changes in the graph. After committing, the transaction will be closed and you will need to get a new one from the session                              |
| `close()`                                                    | *None*                    | Closes current Transaction without committing. This makes the transaction unusable.                                                                                                            |
| `get_concept(String concept_id)`                             | *Concept* or *None*       | Retrieves a Concept by ConceptId                                                                                                                                                               |
| `get_schema_concept(String label)`                           | *SchemaConcept* or *None* | Retrieves a SchemaConcept by label                                                                                                                                                             |
| `get_attributes_by_value(attribute_value, grakn.DataType)`   | Iterator of *Attribute*   | Get all Attributes holding the value provided, if any exist                                                                                                                                    |
| `put_entity_type(String label)`                              | *EntityType*              | Create a new EntityType with super-type entity, or return a pre-existing EntityType with the specified label                                                                                   |
| `put_relationship_type(String label)`                        | *RelationshipType*        | Create a new RelationshipType with super-type relation, or return a pre-existing RelationshipType with the specified label                                                                     |
| `put_attribute_type(String label, grakn.DataType)`           | *AttributeType*           | Create a new AttributeType with super-type attribute, or return a pre-existing AttributeType with the specified label and DataType                                                             |
| `put_role(String label)`                                     | *Role*                    | Create a Role, or return a pre-existing Role, with the specified label.                                                                                                                        |
| `put_rule(String label, String when, String then)`           | *Rule*                    | Create a Rule, or return a pre-existing Rule, with the specified label                                                                                                                         |

**Iterator**

Some of the following Concept methods return a python iterator, which can be converted into a list using standard constructs such as `list(iterator)`. The iterator will either return *Answer* or *Concept* objects.

| Method                    | Return type                 | Description                                                                                                                                                                                                                                                               |
| ------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `collect_concepts()`      | List of *Concept*           | Consumes the iterator and return list of Concepts. **This helper is useful on Iterator that return ConceptMap answer types**. It is useful when one wants to work directly on Concepts without the need to traverse the result map or access the explanation. |

_NOTE_: these iterators represent a lazy evaluation of a query or method on the Grakn server, and will be created very quickly. The actual work
is performed when the iterator is consumed, creating an RPC to the server to obtain the next concrete `Answer` or `Concept`.


**Answer**

This object represents a query answer and it is contained in the Iterator returned by `transaction.query()` method.     
There are **different types of Answer**, based on the type of query executed a different type of Answer will be returned:   

| Query Type                           | Answer Type       |
|--------------------------------------|-------------------|
| `define`                               | ConceptMap        |
| `undefine`                             | ConceptMap        |
| `get`                                  | ConceptMap        |
| `insert`                               | ConceptMap        |
| `delete`                               | ConceptMap        |
| `aggregate count/min/max/sum/mean/std` |  Value            |
| `aggregate group`                      | AnswerGroup       |
| `compute count/min/max/sum/mean/std`   | Value             |
| `compute path`                         | ConceptList       |
| `compute cluster`                      | ConceptSet        |
| `compute centrality`                  | ConceptSetMeasure |

**ConceptMap**

| Method          | Return type              | Description                                                                                     |
| --------------- | ------------------------ | ----------------------------------------------------------------------------------------------- |
| `map()`         | Dict of *str* to *Concept* | Returns result dictionary in which every variable name (key) is linked to a Concept.          |
| `explanation()` | *Explanation* or *null*    | Returns an Explanation object if the current Answer contains inferred Concepts, null otherwise. |

**Value**

| Method          | Return type              | Description                                                                                     |
| --------------- | ------------------------ | ----------------------------------------------------------------------------------------------- |
| `number()`      | int or float             | Returns numeric value of the Answer.                                                            |
| `explanation()` | *Explanation* or *null*  | Returns an Explanation object if the current Answer contains inferred Concepts, null otherwise. |

**ConceptList**

| Method          | Return type              | Description                                                                                     |
| --------------- | ------------------------ | ----------------------------------------------------------------------------------------------- |
| `list()`        | Array of *String*        | Returns list of Concept IDs.                                                                    |
| `explanation()` | *Explanation* or *null*  | Returns an Explanation object if the current Answer contains inferred Concepts, null otherwise. |

**ConceptSet**

| Method          | Return type              | Description                                                                                     |
| --------------- | ------------------------ | ----------------------------------------------------------------------------------------------- |
| `set()`         | Set of *String*          | Returns a set containing Concept IDs.                                                           |
| `explanation()` | *Explanation* or *null*  | Returns an Explanation object if the current Answer contains inferred Concepts, null otherwise. |

**ConceptSetMeasure**

| Method          | Return type              | Description                                                                                     |
| --------------- | ------------------------ | ----------------------------------------------------------------------------------------------- |
| `measurement()` | int or float             | Returns numeric value that is associated to the set of Concepts contained in the current Answer.|
| `set()`         | Set of *String*          | Returns a set containing Concept IDs.                                                           |
| `explanation()` | *Explanation* or *null*  | Returns an Explanation object if the current Answer contains inferred Concepts, null otherwise. |

**AnswerGroup**

| Method          | Return type              | Description                                                                                     |
| --------------- | ------------------------ | ----------------------------------------------------------------------------------------------- |
| `owner()`       | *Concept*                | Returns the Concepts which is the group owner.                                                  |
| `answers()`     | List of *Answer*         | Returns list of Answers that belongs to this group.                                             |
| `explanation()` | *Explanation* or *null*  | Returns an Explanation object if the current Answer contains inferred Concepts, null otherwise. |

**Explanation**

| Method           | Return type       | Description                                                                                 |
| ---------------- | ----------------- | ------------------------------------------------------------------------------------------- |
| `query_pattern()`| String            | Returns a query pattern that describes how the owning answer was retrieved.                 |
| `answers()`      | List of *Answer*  | Set of deducted/factual answers that allowed us to determine that the owning answer is true |


**Concepts hierarchy**

Grakn is composed of different types of Concepts, that have a specific hierarchy

```
                                                Concept
                                                /       \
                                              /           \
                                            /               \
                                          /                   \
                                  SchemaConcept                    Thing
                                  /     |    \                    /   |  \
                                /       |     \                 /     |    \
                              /         |      \              /       |      \
                            Type      Rule    Role     Entity    Attribute   Relationship
                        /     |   \
                      /       |     \
                    /         |       \
            EntityType  AttributeType  RelationshipType
```
---

All Concepts are bound to the transaction that has been used to retrieve them.
If for any reason a trasaction gets closed all the concepts bound to it won't be able to
communicate with the database anymore, so all the methods won't work and the user will have to re-query
for the needed concepts.

All setters return the same concept object (`self`) to facilitate chaining setters (e.g. `person.has(name).has(age)`).

---

**Concept**

These fields are accessible in every `Concept`:

| Field                   | Type        | Description                                      |
| ----------------------  | ----------- | ------------------------------------------------ |
| `id`                    | *String*      | Delete concept                                   |

These methods are available on every type of `Concept`

| Method                  | Return type | Description                                      |
| ----------------------  | ----------- | ------------------------------------------------ |
| `delete()`              | *None*      | Delete concept                                   |
| `is_schema_concept()`   | *Boolean*   | Check whether this Concept is a SchemaConcept    |
| `is_type() `            | *Boolean*   | Check whether this Concept is a Type             |
| `is_thing() `           | *Boolean*   | Check whether this Concept is a Thing            |
| `is_attribute_type()`   | *Boolean*   | Check whether this Concept is an AttributeType   |
| `is_entity_type() `     | *Boolean*   | Check whether this Concept is an EntityType      |
| `is_relationship_type()`| *Boolean*   | Check whether this Concept is a RelationshipType |
| `is_role()`             | *Boolean*   | Check whether this Concept is a Role             |
| `is_rule()`             | *Boolean*   | Check whether this Concept is a Rule             |
| `is_attribute()`        | *Boolean*   | Check whether this Concept is an Attribute       |
| `is_entity()`           | *Boolean*   | Check whether this Concept is a Entity           |
| `is_relationship()`     | *Boolean*   | Check whether this Concept is a Relationship     |

  **Schema concept**

A `SchemaConcept` concept has all the `Concept` methods plus the following:

| Method                      | Return type                 | Description                                                                                                                                                          |
| --------------------------- | --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `label()`                   | *String*                    | Get label of this SchemaConcept                                                                                                                                      |
| `label(String value)`       | *None*                      | Set label of this SchemaConcept                                                                                                                                      |
| `is_implicit()`             | *Boolean*                   | Returns `True` when the SchemaConcept is implicit, i.e. when it's been created by Grakn and not explicitly by the user, `False` when explicitly created by the user. |
| `sup(Type)`                 | *None*                      | Set direct super SchemaConcept of this SchemaConcept                                                                                                                 |
| `sup()`                     | *SchemaConcept*  or *None*  | Get direct super SchemaConcept of this SchemaConcept                                                                                                                 |
| `subs()`                    | Iterator of *SchemaConcept* | Get all indirect subs of this SchemaConcept.                                                                                                                         |
| `sups()`                    | Iterator of *SchemaConcept* | Get all indirect sups of this SchemaConcept.                                                                                                                         |

  **Thing**

 A `Thing` concept has all the `Concept` methods plus the following:

 | Method                               | Return type                | Description                                                                                                                                                                       |
 | ------------------------------------ | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
 | `is_inferred()`                      | *Boolean*                  | Returns `True` if this Thing is inferred by Reasoner, `False` otherwise                                                                                                           |
 | `type()`                             | *Type*                     | Returns a Type which is the type of this Thing. This Thing is an instance of that type.                                                                                           |
 | `relationships(*Role)`               | Iterator of *Relationship* | Returns Relationships which this Thing takes part in, which may **optionally** be narrowed to a particular set according to the Roles you are interested in                       |
 | `attributes(*AttributeType)`         | Iterator of *Attribute*    | Returns Attributes attached to this Thing, which may **optionally** be narrowed to a particular set according to the AttributeTypes you are interested in                         |
 | `roles()`                            | Iterator of *Role*         | Returns the Roles that this Thing is currently playing                                                                                                                            |
 | `keys(*AttributeType)`               | Iterator of *Attribute*    | Returns a collection of Attribute attached to this Thing as a key, which may **optionally** be narrowed to a particular set according to the AttributeTypes you are interested in |
 | `has(Attribute)`                     | *None*                     | Attaches the provided Attribute to this Thing                                                                                                                                     |
 | `unhas(Attribute)`                   | *None*                     | Removes the provided Attribute from this Thing                                                                                                                                    |

  **Attribute**

  An `Attribute` concept has all the `Thing` methods plus the following:


 | Method             | Return type         | Description                                               |
 | ------------------ | ------------------- | --------------------------------------------------------- |
 | `value()`    | *String*            | Get value of this Attribute                               |
 | `owners()`   | Iterator of *Thing* | Returns the set of all Things that possess this Attribute |

  **Relationship**

A `Relationship` concept has all the `Thing` methods plus the following:


| Method                        | Return type               | Description                                                                                              |
| ----------------------------- | ------------------------- | -------------------------------------------------------------------------------------------------------- |
| `role_players_map()`          | Dict[*Role*, set[*Thing*]]| Returns a dictionary that links all the Roles of this Relationship to all the Things that are playing each Role |
| `role_players(*Role)`         | Iterator of *Thing*       | Returns a list of every Thing involved in this Relationship, optionally filtered by Roles played         |
| `assign(Role, Thing)`         | *None*                    | Expands this Relationship to include a new role player (Thing) which is playing a specific Role          |
| `unassign(Role, Thing)`       | *None*                    | Removes the Thing which is playing a Role in this Relationship.                                          |  |

    NB: There are no specific methods for `Entity` concept.

  **Type**

A `Type` concept has all the `SchemaConcept` methods plus the following:


| Method                       | Return type                 | Description                                                                                                                    |
| ---------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `is_abstract(Boolean)`       | *None*                      | Sets the Type to be abstract - which prevents it from having any instances                                                     |
| `is_abstract()`              | *Boolean*                   | Returns `True` if the type is set to abstract, `False` otherwise                                                               |
| `playing()`                  | Iterator of *Role*          | Returns all the Roles which instances of this Type can indirectly play                                                         |
| `plays(Role)`                | *None*                      | Add a new Role to the ones that the instances of this Type are allowed to play                                                 |
| `attributes()`               | Iterator of *AttributeType* | The AttributeTypes which this Type is linked with.                                                                             |
| `instances()`                | Iterator of *Thing*         | Get all indirect instances of this Type                                                                                        |
| `keys()`                     | Iterator of *AttributeType* | The AttributeTypes which this Type is linked with as a key                                                                     |
| `key(AttributeType)`         | *None*                      | Creates an implicit RelationshipType which allows this Type and a AttributeType to be linked in a strictly one-to-one mapping. |
| `has(AttributeType)`         | *None*                      | Add a new AttributeType which the instances of this Type are allowed to have attached to themselves                            |
| `unplay(Role)`               | *None*                      | Delete a Role from the ones that the instances of this Type are allowed to play                                                |
| `unhas(AttributeType)`       | *None*                      | Delete AttributeType from the ones that the instances of this Type are allowed to have attached to themselves                  |
| `unkey(AttributeType)`       | *None*                      | Delete AttributeType from available keys                                                                                       |

  **AttributeType**

  An `AttributeType` concept has all the `Type` methods plus the following:


  | Method                      | Return type           | Description                                                                                                                                 |
  | --------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
  | `create(value)`             | *Attribute*           | Create new Attribute of this type with the provided value. The value provided must conform to the DataType specified for this AttributeType |
  | `attribute(value)`          | *Attribute* or *None* | Retrieve the Attribute with the provided value if it exists                                                                                 |
  | `data_type()`               | *Enum of Grakn.DataType*   | Get the data type to which instances of the AttributeType must have                                                                         |
  | `regex()`                   | *String* or *None*    | Retrieve the regular expression to which instances of this AttributeType must conform to, or `None` if no regular expression is set         |
  | `regex(String regex)`       | *None*                | Set the regular expression that instances of the AttributeType must conform to                                                              |

  **RelationshipType**

  A `RelationshipType` concept has all the `Type` methods plus the following:

 | Method                 | Return type        | Description                                                                          |
 | ---------------------- | ------------------ | ------------------------------------------------------------------------------------ |
 | `create()`             | *Relationship*     | Creates and returns a new Relationship instance, whose direct type will be this type |
 | `roles()`              | Iterator of *Role* | Returns a list of the RoleTypes which make up this RelationshipType                  |
 | `relates(Role)`        | *None*             | Sets a new Role for this RelationshipType                                            |
 | `unrelate(Role)`       | *None*             | Delete a Role from this RelationshipType                                             |

  **EntityType**

  An `EntityType` concept has all the `Type` methods plus the following:

  | Method           | Return type | Description                                                                    |
  | ---------------- | ----------- | ------------------------------------------------------------------------------ |
  | `create()`       | *Entity*    | Creates and returns a new Entity instance, whose direct type will be this type |


  **Role**

  A `Role` concept has all the `SchemaConcept` methods plus the following:

| Method                  | Return type                    | Description                                                 |
| ----------------------- | ------------------------------ | ----------------------------------------------------------- |
| `relationships()`       | Iterator of *RelationshipType* | Returns the RelationshipTypes that this Role takes part in. |
| `players()`             | Iterator of *Type*             | Returns a collection of the Types that can play this Role   |

  **Rule**

A `Rule` concept has all the `SchemaConcept` methods plus the following:

| Method            | Return type | Description                                                                                                |
| ----------------- | ----------- | ---------------------------------------------------------------------------------------------------------- |
| `get_when()`      | *String*    | Retrieves the when part of this Rule. When this query is satisfied the "then" part of the rule is executed |
| `get_then()`      | *String*    | Retrieves the then part of this Rule. This query is executed when the "when" part of the rule is satisfied |
