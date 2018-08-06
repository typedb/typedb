# Grakn Python Client

A Python client for [Grakn](https://grakn.ai) 

Requires Grakn 1.3.0 and Python >= 3.6

# Installation

```
pip3 install grakn 
```

To obtain the Grakn database itself, head [here](https://grakn.ai/pages/documentation/get-started/setup-guide.html) for the setup guide. 

# Quickstart

In the interpreter or in your source, import `grakn`:

```
import grakn
```

You can then instantiate a client, open a session, and create transactions:
```
client = grakn.Grakn(uri="localhost:48555")
session = client.session(keyspace="mykeyspace")
tx = session.transaction(grakn.TxType.WRITE)
```

Alternatively, you can also use `with` statements as follows:
```
client = grakn.Grakn(uri="localhost:48555")
with client.session(keyspace="mykeyspace") as session:
    with session.transaction(grakn.TxType.READ):
        ...
```
to automatically close sessions and transactions.

Credentials can be passed into the initial constructor as a dictionary:
```
client = grakn.Grakn(uri='localhost:48555', credentials={'username': 'xxxx', 'password': 'yyyy'})
```

You can execute Graql queries and iterate through the answers as follows:
```
answer_iterator = tx.query("match $x isa person; limit 10; get;")
an_answer = next(answer)
person = an_answer.get('x')
tx.close()
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
| `keyspace.delete(String keyspace)`       | *None*            | Deletes the specified keyspace                       |
| `keyspace.retrieve()`                    | List of *String*  | Retrieves all available keyspaces                    |



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
| `collect_concepts()`      | List of *Concept*           | Consumes the iterator and return list of Concepts. **This helper is useful on Iterator returned by transaction.query() method**. It is useful when one wants to work directly on Concepts without the need to traverse the result map or access the explanation. |


**Answer**

This object represents a query answer and it is contained in the Iterator returned by `transaction.query()` method, the following methods are available:

| Method          | Return type                           | Description                                                                                     |
| --------------- | --------------------------------------| ----------------------------------------------------------------------------------------------- |
| `get(var=None)` | Dict[str, *Concept*] or *Concept*     | Returns result dictionary mapping variables (type `str`) to a *Concept*, or directly return a *Concept* if `var` is in the dict.|
| `explanation()` | *Explanation* or *None*               | Returns an Explanation object if the current Answer contains inferred Concepts, None otherwise. |

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
  | `data_type()`               | *String*              | Get the data type to which instances of the AttributeType must have                                                                         |
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


