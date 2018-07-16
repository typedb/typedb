# Grakn Node.js Client

A Node.js client for [Grakn](https://grakn.ai)

Requires Grakn 1.3.0

# Installation

To install the Grakn client, simply run:

```
npm install grakn
```

You will also need access to a Grakn database. Head [here](https://grakn.ai/pages/documentation/get-started/setup-guide.html) to get started with Grakn.

# Quickstart

Begin by importing Grakn:

```
const Grakn = require('grakn');
```

Now you can create a new session and open a new Grakn transaction:

```
const grakn = new Grakn('localhost:48555');
const session = grakn.session('keyspace');
const tx = await session.transaction(Grakn.txType.WRITE);
```

Execute Graql query (this example works inside an `async` function):

```
const resultIterator = await tx.query("match $x isa person; limit 10; get;");
const resultMap = await resultIterator.next();
const person = resultMap.get('x');
tx.close();
```

# API Reference

First create a new Grakn object with 

```
// URI must a string containing host address and gRPC port of a running Grakn instance, e.g. "localhost:48555"
const grakn = new Grakn(URI);
```

on the Grakn object the following methods are available:

**Grakn**

| Method                                   | Return type       | Description                                               |
| ---------------------------------------- | ----------------- | --------------------------------------------------------- |
| `session(String keyspace)`               | *GraknSession*    | Return a new GraknSession bound to the specified keyspace |
| async `keyspace.delete(String keyspace)` | *void*            | Deletes the specified keyspace                            |
| async `keyspace.retrieve()`              | Array of *String* | Retrieves all available keyspaces                         |



on the GraknSession the following methods are available:

**GraknSession**

| Method                            | Return type | Description                                                |
| --------------------------------- | ----------- | ---------------------------------------------------------- |
| async `transaction(Grakn.txType)` | *GraknTx*   | Return a new GraknTx bound to the keyspace of this session |
 

Once obtained a `GraknTx` you will be able to:

 **GraknTx**  
 
| Method                                                       | Return type                          | Description                                                                                                                                                   |
| ------------------------------------------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| async `query(String graqlQuery)`                             | Iterator of Map<*String*, *Concept*> | Executes a Graql query on the session keyspace. It returns a list of Maps, in each map the key represents the Graql variable specified in the query           |
| async `commit()`                                             | *void*                               | Commit current GraknTx, persisting changes in the graph. After committing, the transaction will be closed and you will need to get a new one from the session |
| async `close()`                                              | *void*                               | Closes current GraknTx without committing. This makes the transaction unusable.                                                                               |
| async `getConcept(String conceptId)`                         | *Concept* or *null*                  | Retrieves a Concept by ConceptId                                                                                                                              |
| async `getSchemaConcept(String label)`                       | *SchemaConcept* or *null*            | Retrieves a SchemaConcept by label                                                                                                                            |
| async `getAttributesByValue(attributeValue, Grakn.dataType)` | Iterator of *Attribute*              | Get all Attributes holding the value provided, if any exists                                                                                                  |
| async `putEntityType(String label)`                          | *EntityType*                         | Create a new EntityType with super-type entity, or return a pre-existing EntityType with the specified label                                                  |
| async `putRelationshipType(String label)`                    | *RelationshipType*                   | Create a new RelationshipType with super-type relation, or return a pre-existing RelationshipType with the specified label                                    |
| async `putAttributeType(String label, Grakn.dataType)`       | *AttributeType*                      | Create a new AttributeType with super-type attribute, or return a pre-existing AttributeType with the specified label and DataType                            |
| async `putRole(String label)`                                | *Role*                               | Create a Role, or return a pre-existing Role, with the specified label.                                                                                       |
| async `putRule(String label, String when, String then)`      | *Rule*                               | Create a Rule, or return a pre-existing Rule, with the specified label                                                                                        |

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
**Iterator**

Some of the following Concept methods return an Iterator,
on every iterator the following methods are available:

| Method            | Return type                  | Description                                                                |
| ----------------- | ---------------------------- | -------------------------------------------------------------------------- |
| async `next()`    | *Iterator element* or *null* | Retrieves next element or returns null when no more elements are available |
| async `collect()` | Array of *Iterator element*  | Consumes the iterator and collect all the elements into an array           |

**Concept** 

These methods are available on every type of `Concept` 

| Method                 | Return type | Description                                      |
| ---------------------- | ----------- | ------------------------------------------------ |
| async `delete()`       | *void*      | Delete concept                                   |
| `isSchemaConcept()`    | *Boolean*   | Check whether this Concept is a SchemaConcept    |
| `isType() `            | *Boolean*   | Check whether this Concept is a Type             |
| `isThing() `           | *Boolean*   | Check whether this Concept is a Thing            |
| `isAttributeType()`    | *Boolean*   | Check whether this Concept is an AttributeType   |
| `isEntityType() `      | *Boolean*   | Check whether this Concept is an EntityType      |
| `isRelationshipType()` | *Boolean*   | Check whether this Concept is a RelationshipType |
| `isRole()`             | *Boolean*   | Check whether this Concept is a Role             |
| `isRule()`             | *Boolean*   | Check whether this Concept is a Rule             |
| `isAttribute()`        | *Boolean*   | Check whether this Concept is an Attribute       |
| `isEntity()`           | *Boolean*   | Check whether this Concept is a Entity           |
| `isRelationship()`     | *Boolean*   | Check whether this Concept is a Relationship     |
  
  **Schema concept**  

A `SchemaConcept` concept has all the `Concept` methods plus the following:

| Method                      | Return type                 | Description                                                                                                                                                          |
| --------------------------- | --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| async `label()`             | *String*                    | Get label of this SchemaConcept                                                                                                                                      |
| async `label(String value)` | *void*                      | Set label of this SchemaConcept                                                                                                                                      |
| async `isImplicit()`        | *Boolean*                   | Returns `true` when the SchemaConcept is implicit, i.e. when it's been created by Grakn and not explicitly by the user, `false` when explicitly created by the user. |
| async `sup(Type)`           | *void*                      | Set direct super SchemaConcept of this SchemaConcept                                                                                                                 |
| async `sup()`               | *SchemaConcept*  or *null*  | Get direct super SchemaConcept of this SchemaConcept                                                                                                                 |
| async `subs()`              | Iterator of *SchemaConcept* | Get all indirect subs of this SchemaConcept.                                                                                                                         |
| async `sups()`              | Iterator of *SchemaConcept* | Get all indirect sups of this SchemaConcept.                                                                                                                         |
   
  **Thing** 

 A `Thing` concept has all the `Concept` methods plus the following: 

 | Method                               | Return type                | Description                                                                                                                                                                       |
 | ------------------------------------ | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
 | async `isInferred()`                 | *Boolean*                  | Returns `true` if this Thing is inferred by Reasoner, `false` otherwise                                                                                                           |
 | async `type()`                       | *Type*                     | Returns a Type which is the type of this Thing. This Thing is an instance of that type.                                                                                           |
 | async `relationships(...Role)`       | Iterator of *Relationship* | Returns Relationships which this Thing takes part in, which may **optionally** be narrowed to a particular set according to the Roles you are interested in                       |
 | async `attributes(...AttributeType)` | Iterator of *Attribute*    | Returns Attributes attached to this Thing, which may **optionally** be narrowed to a particular set according to the AttributeTypes you are interested in                         |
 | async `plays()`                      | Iterator of *Role*         | Returns the Roles that this Thing is currently playing                                                                                                                            |
 | async `keys(...Attributetype)`       | Iterator of *Attribute*    | Returns a collection of Attribute attached to this Thing as a key, which may **optionally** be narrowed to a particular set according to the AttributeTypes you are interested in |
 | async `has(Attribute)`               | *void*                     | Attaches the provided Attribute to this Thing                                                                                                                                     |
 | async `unhas(Attribute)`             | *void*                     | Removes the provided Attribute from this Thing                                                                                                                                    |
   
  **Attribute** 

  An `Attribute` concept has all the `Thing` methods plus the following:

  
 | Method             | Return type         | Description                                               |
 | ------------------ | ------------------- | --------------------------------------------------------- |
 | async `dataType()` | *String*            | Returns DataType of this Attribute                        |
 | async `value()`    | *String*            | Get value of this Attribute                               |
 | async `owners()`   | Iterator of *Thing* | Returns the set of all Things that possess this Attribute |
   
  **Relationship**  

A `Relationship` concept has all the `Thing` methods plus the following:

  
| Method                        | Return type               | Description                                                                                              |
| ----------------------------- | ------------------------- | -------------------------------------------------------------------------------------------------------- |
| async `rolePlayersMap()`      | Map<*Role*, Set<*Thing*>> | Returns a Map that links all the Roles of this Relationship to all the Things that are playing each Role |
| async `rolePlayers(...Role)`  | Iterator of *Thing*       | Returns a list of every Thing involved in this Relationship, optionally filtered by Roles played         |
| async `assign(Role, Thing)`   | *void*                    | Expands this Relationship to include a new role player (Thing) which is playing a specific Role          |
| async `unassign(Role, Thing)` | *void*                    | Removes the Thing which is playing a Role in this Relationship.                                          |  |

    NB: There are no specific methods for `Entity` concept.

  **Type**  

A `Type` concept has all the `SchemaConcept` methods plus the following:

  
| Method                       | Return type                 | Description                                                                                                                    |
| ---------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| async `isAbstract(Boolean)`  | *void*                      | Sets the Type to be abstract - which prevents it from having any instances                                                     |
| async `isAbstract()`         | *Boolean*                   | Returns `true` if the type is set to abstract, `false` otherwise                                                               |
| async `playing()`            | Iterator of *Role*          | Returns all the Roles which instances of this Type can indirectly play                                                         |
| async `plays(Role)`          | *void*                      | Add a new Role to the ones that the instances of this Type are allowed to play                                                 |
| async `attributes()`         | Iterator of *AttributeType* | The AttributeTypes which this Type is linked with.                                                                             |
| async `instances()`          | Iterator of *Thing*         | Get all indirect instances of this Type                                                                                        |
| async `keys()`               | Iterator of *AttributeType* | The AttributeTypes which this Type is linked with as a key                                                                     |
| async `key(AttributeType)`   | *void*                      | Creates an implicit RelationshipType which allows this Type and a AttributeType to be linked in a strictly one-to-one mapping. |
| async `has(AttributeType)`   | *void*                      | Add a new AttributeType which the instances of this Type are allowed to have attached to themselves                            |
| async `unplay(Role)`         | *void*                      | Delete a Role from the ones that the instances of this Type are allowed to play                                                |
| async `unhas(AttributeType)` | *void*                      | Delete AttributeType from the ones that the instances of this Type are allowed to have attached to themselves                  |
| async `unkey(AttributeType)` | *void*                      | Delete AttributeType from available keys                                                                                       |
  
  **AttributeType**

  An `AttributeType` concept has all the `Type` methods plus the following:

  
  | Method                      | Return type           | Description                                                                                                                                              |
  | --------------------------- | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | async `create(value)`       | *Attribute*           | Returns new or existing Attribute of this type with the provided value. The value provided must conform to the DataType specified for this AttributeType |
  | async `attribute(value)`    | *Attribute* or *null* | Returns the Attribute with the provided value or null if no such Attribute exists                                                                        |
  | async `dataType()`          | *String*              | Get the data type to which instances of the AttributeType must have                                                                                      |
  | async `regex()`             | *String* or *null*    | Retrieve the regular expression to which instances of this AttributeType must conform to, or `null` if no regular expression is set                      |
  | async `regex(String regex)` | *void*                | Set the regular expression that instances of the AttributeType must conform to                                                                           |
   
  **RelationshipType**  

  A `RelationshipType` concept has all the `Type` methods plus the following:
 
 | Method                 | Return type        | Description                                                                          |
 | ---------------------- | ------------------ | ------------------------------------------------------------------------------------ |
 | async `create()`       | *Relationship*     | Creates and returns a new Relationship instance, whose direct type will be this type |
 | async `roles()`        | Iterator of *Role* | Returns a list of the RoleTypes which make up this RelationshipType                  |
 | async `relates(Role)`  | *void*             | Sets a new Role for this RelationshipType                                            |
 | async `unrelate(Role)` | *void*             | Delete a Role from this RelationshipType                                             |
  
  **EntityType**  

  An `EntityType` concept has all the `Type` methods plus the following:

  | Method           | Return type | Description                                                                    |
  | ---------------- | ----------- | ------------------------------------------------------------------------------ |
  | async `create()` | *Entity*    | Creates and returns a new Entity instance, whose direct type will be this type |


  **Role**  

  A `Role` concept has all the `SchemaConcept` methods plus the following:
  
| Method                  | Return type                    | Description                                                 |
| ----------------------- | ------------------------------ | ----------------------------------------------------------- |
| async `relationships()` | Iterator of *RelationshipType* | Returns the RelationshipTypes that this Role takes part in. |
| async `players()`       | Iterator of *Type*             | Returns a collection of the Types that can play this Role   |
  
  **Rule**  

A `Rule` concept has all the `SchemaConcept` methods plus the following:  
  
| Method            | Return type | Description                                                                                                |
| ----------------- | ----------- | ---------------------------------------------------------------------------------------------------------- |
| async `getWhen()` | *String*    | Retrieves the when part of this Rule. When this query is satisfied the "then" part of the rule is executed |
| async `getThen()` | *String*    | Retrieves the then part of this Rule. This query is executed when the "when" part of the rule is satisfied |
