/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.QueryBuilder;

import java.util.Collection;
import java.util.Map;

/**
 * A GraknGraph instance which connects to a specific graph keyspace.
 * Through this object, transactions are automatically opened, which enable the graph to be queried and mutated.
 */

public interface GraknGraph extends AutoCloseable{
    //------------------------------------- Concept Construction ----------------------------------

    /**
     * Create a new Entity Type, or return a pre-existing Entity Type, with the specified name.
     *
     * @param name A unique name for the Entity Type
     * @return A new or existing Entity Type with the provided name
     */
    EntityType putEntityType(String name);

    /**
     * Create a new Entity Type, or return a pre-existing Entity Type, with the specified name.
     *
     * @param name A unique name for the Entity Type
     * @return A new or existing Entity Type with the provided name
     */
    EntityType putEntityType(TypeName name);

    /**
     * Create a Resource Type, or return a pre-existing Resource Type, with the specified name.
     *
     * @param name A unique name for the Resource Type
     * @param dataType The data type of the resource type.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing Resource Type with the provided name.
     */
    <V> ResourceType<V> putResourceType(String name, ResourceType.DataType<V> dataType);

    /**
     * Create a Resource Type, or return a pre-existing Resource Type, with the specified name.
     *
     * @param name A unique name for the Resource Type
     * @param dataType The data type of the resource type.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing Resource Type with the provided name.
     */
    <V> ResourceType<V> putResourceType(TypeName name, ResourceType.DataType<V> dataType);

    /**
     * Create a unique Resource Type, or return a pre-existing Resource Type, with the specified name.
     * The Resource Type is guaranteed to be unique, in that its instances can be connected to one entity.
     *
     * @param name A unique name for the Resource Type
     * @param dataType The data type of the resource type.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing Resource Type with the provided name.
     */
    <V> ResourceType <V> putResourceTypeUnique(String name, ResourceType.DataType<V> dataType);
    /**
     * Create a unique Resource Type, or return a pre-existing Resource Type, with the specified name.
     * The Resource Type is guaranteed to be unique, in that its instances can be connected to one entity.
     *
     * @param name A unique name for the Resource Type
     * @param dataType The data type of the resource type.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing Resource Type with the provided name.
     */
    <V> ResourceType <V> putResourceTypeUnique(TypeName name, ResourceType.DataType<V> dataType);

    /**
     * Create a Rule Type, or return a pre-existing Rule Type, with the specified name.
     *
     * @param name A unique name for the Rule Type
     * @return new or existing Rule Type with the provided Id.
     */
    RuleType putRuleType(String name);

    /**
     * Create a Rule Type, or return a pre-existing Rule Type, with the specified name.
     *
     * @param name A unique name for the Rule Type
     * @return new or existing Rule Type with the provided Id.
     */
    RuleType putRuleType(TypeName name);

    /**
     * Create a Relation Type, or return a pre-existing Relation Type, with the specified name.
     *
     * @param name A unique name for the Relation Type
     * @return A new or existing Relation Type with the provided Id.
     */
    RelationType putRelationType(String name);

    /**
     * Create a Relation Type, or return a pre-existing Relation Type, with the specified name.
     *
     * @param name A unique name for the Relation Type
     * @return A new or existing Relation Type with the provided Id.
     */
    RelationType putRelationType(TypeName name);

    /**
     * Create a Role Type, or return a pre-existing Role Type, with the specified name.
     *
     * @param name A unique name for the Role Type
     * @return new or existing Role Type with the provided Id.
     */
    RoleType putRoleType(String name);

    /**
     * Create a Role Type, or return a pre-existing Role Type, with the specified name.
     *
     * @param name A unique name for the Role Type
     * @return new or existing Role Type with the provided Id.
     */
    RoleType putRoleType(TypeName name);

    //------------------------------------- Concept Lookup ----------------------------------
    /**
     * Get the Concept with identifier provided, if it exists.
     *
     * @param id A unique identifier for the Concept in the graph.
     * @return The Concept with the provided id or null if no such Concept exists.
     */
    <T extends Concept> T getConcept(ConceptId id);

    /**
     * Get the Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Type in the graph.
     * @return The Type with the provided name or null if no such Type exists.
     */
    <T extends Type> T getType(TypeName name);

    /**
     * Get the Resources holding the value provided, if they exist.
     *
     * @param value A value which a Resource in the graph may be holding.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resources holding the provided value or an empty collection if no such Resource exists.
     */
    <V> Collection<Resource<V>> getResourcesByValue(V value);

    /**
     * Get the Entity Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Entity Type in the graph.
     * @return The Entity Type  with the provided name or null if no such Entity Type exists.
     */
    EntityType getEntityType(String name);

    /**
     * Get the Relation Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Relation Type in the graph.
     * @return The Relation Type with the provided name or null if no such Relation Type exists.
     */
    RelationType getRelationType(String name);

    /**
     * Get the Resource Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Resource Type in the graph.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resource Type with the provided name or null if no such Resource Type exists.
     */
    <V> ResourceType<V> getResourceType(String name);

    /**
     * Get the Role Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Role Type in the graph.
     * @return The Role Type  with the provided name or null if no such Role Type exists.
     */
    RoleType getRoleType(String name);

    /**
     * Get the Rule Type with the name provided, if it exists.
     *
     * @param name A unique name which identifies the Rule Type in the graph.
     * @return The Rule Type with the provided name or null if no such Rule Type exists.
     */
    RuleType getRuleType(String name);

    /**
     * Get a collection of Relations that match the specified Relation Type and role map, if it exists.
     * Caller specifies a Relation Type and a role map, which lists the Instances or Resources in the relationship, and the roles each play.
     *
     * @param relationType The Relation Type which we wish to find a Relation instance of.
     * @param roleMap A role map specifying the rolePlayers (Instances or Resources) in the relationship and the roles (Role Types) they play.
     * @return A collection of Relations which meet the above requirements or an empty collection is no relationship exists fulfilling the above requirements.
     */
    Relation getRelation(RelationType relationType, Map<RoleType, Instance> roleMap);

    //------------------------------------- Utilities ----------------------------------

    /**
     * Returns access to the low-level details of the graph via GraknAdmin
     * @see GraknAdmin
     *
     * @return The admin interface which allows you to access more low level details of the graph.
     */
    GraknAdmin admin();

    /**
     * Utility function to specify whether implicit and system-generated types should be returned.
     * @param flag Specifies if implicit and system-generated types should be returned.
     */
    void showImplicitConcepts(boolean flag);

    /**
     * Utility function to specify whether implicit concepts should be exposed.
     *
     * @return true if implicit concepts are exposed.
     */
    boolean implicitConceptsVisible();

    /**
     * Closes and clears the current graph.
     */
    void clear();

    /**
     * Utility function to get the name of the keyspace where the graph is persisted.
     *
     * @return The name of the keyspace where the graph is persisted
     */
    String getKeyspace();

    /**
     * Utility function to determine whether the graph has been closed.
     *
     * @return True if the graph has been closed
     */
    boolean isClosed();

    /**
     * Returns a QueryBuilder
     *
     * @return returns a query builder to allow for the creation of graql queries
     * @see QueryBuilder
     */
    QueryBuilder graql();

    /**
     * Validates and attempts to commit the graph.
     * An exception is thrown if validation fails or if the graph cannot be persisted due to an underlying database issue.
     *
     * @throws GraknValidationException is thrown when a structural validation fails.
     */
    void commit() throws GraknValidationException;

    /**
     * Resets the current transaction without committing.
     *
     */
    void rollback();

    /**
     * Closes the current transaction. If no transactions remain open the graph connection is closed permanently and
     * the {@link GraknGraphFactory} must be used to get a new connection.
     */
    void close();

    /**
     * Opens the graph. This must be called before a new thread can use the graph.
     */
    void open();
}
