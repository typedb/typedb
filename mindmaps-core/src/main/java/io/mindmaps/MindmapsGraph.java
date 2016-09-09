/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package io.mindmaps;

import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.RuleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.MindmapsValidationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;
import java.util.Map;

/**
 * A thread bound mindmaps transaction
 */
public interface MindmapsGraph extends AutoCloseable{
    //------------------------------------- Concept Construction ----------------------------------

    /**
     *
     * @param id A unique Id for the Entity Type
     * @return A new or existing Entity Type with the provided Id.
     */
    EntityType putEntityType(String id);

    /**
     *
     * @param id A unique Id for the Resource Type
     * @param type The data type of the resource type.
     *             Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing Resource Type with the provided Id.
     */
    <V> ResourceType <V> putResourceType(String id, ResourceType.DataType<V> type);

    /**
     *
     * @param id A unique Id for the Rule Type
     * @return new or existing Rule Type with the provided Id.
     */
    RuleType putRuleType(String id);

    /**
     *
     * @param id A unique Id for the Relation Type
     * @return A new or existing Relation Type with the provided Id.
     */
    RelationType putRelationType(String id);

    /**
     *
     * @param id A unique Id for the Role Type
     * @return new or existing Role Type with the provided Id.
     */
    RoleType putRoleType(String id);

    /**
     *
     * @param id A unique id for the Entity
     * @param type The type of this Entity
     * @return A new or existing Entity with the provided Id.
     */
    Entity putEntity(String id, EntityType type);

    /**
     *
     * @param type The type of this Entity
     * @return A new entity.
     */
    Entity addEntity(EntityType type);

    /**
     *
     * @param <V> The data type of both the ResourceType and the Resource.
     *           Supported types include: String, Long, Double, Boolean.
     * @param value A value for the Resource which is unique to it's type
     * @param type The resource type of this resource.
     * @return new or existing Resource with the provided Id.
     */
    <V> Resource <V> putResource(V value, ResourceType<V> type);

    /**
     *
     * @param id A unique Id for the Rule
     * @param lhs A string representing the left hand side GraQL query.
     * @param rhs A string representing the right hand side GraQL query.
     * @param type The rule type of this Rule
     * @return new or existing Rule with the provided Id.
     */
    Rule putRule(String id, String lhs, String rhs, RuleType type);

    /**
     * @param lhs A string representing the left hand side GraQL query.
     * @param rhs A string representing the right hand side GraQL query.
     * @param type The rule type of this Rule
     * @return a new Rule
     */
    Rule addRule(String lhs, String rhs, RuleType type);

    /**
     *
     * @param id A unique Id for the Relation
     * @param type The relation type of this Relation
     * @return A new empty relation which can be fully customised
     */
    Relation putRelation(String id, RelationType type);

    /**
     *
     * @param type The relation type of this Relation
     * @return A new empty relation which can be fully customised
     */
    Relation addRelation(RelationType type);
    //------------------------------------- Concept Lookup ----------------------------------
    /**
     *
     * @param id A unique Id which identifies the Concept in the graph.
     * @return The Concept with the provided Id or null if no such Concept exists.
     */
    Concept getConcept(String id);

    /**
     *
     * @param id A unique Id which identifies the Type in the graph.
     * @return The Type with the provided Id or null if no such Type exists.
     */
    Type getType(String id);

    /**
     *
     * @param id A unique Id which identifies the Instance in the graph.
     * @return The Instance with the provided Id or null if no such Instance exists.
     */
    Instance getInstance(String id);

    /**
     *
     * @param id A unique Id which identifies the Entity in the graph.
     * @return The Entity with the provided Id or null if no such Entity exists.
     */
    Entity getEntity(String id);

    /**
     *
     * @param id A unique Id which identifies the Resource in the graph.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resource with the provided Id or null if no such Resource exists.
     */
    <V> Resource<V> getResource(String id);

    /**
     *
     * @param value A value which a Resource in the graph may be holding.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resources holding the provided value or an empty collection if no such Resource exists.
     */
    <V> Collection<Resource<V>> getResourcesByValue(V value);

    /**
     *
     * @param id A unique Id which identifies the Rule in the graph.
     * @return The Rule with the provided Id or null if no such Rule exists.
     */
    Rule getRule(String id);

    /**
     *
     * @param id A unique Id which identifies the Entity Type in the graph.
     * @return The Entity Type  with the provided Id or null if no such Entity Type  exists.
     */
    EntityType getEntityType(String id);

    /**
     *
     * @param id A unique Id which identifies the Relation Type in the graph.
     * @return The Relation Type with the provided Id or null if no such Relation Type exists.
     */
    RelationType getRelationType(String id);

    /**
     *
     * @param id A unique Id which identifies the Resource Type in the graph.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resource Type with the provided Id or null if no such Resource Type exists.
     */
    <V> ResourceType<V> getResourceType(String id);

    /**
     *
     * @param id A unique Id which identifies the Role Type in the graph.
     * @return The Role Type  with the provided Id or null if no such Role Type exists.
     */
    RoleType getRoleType(String id);

    /**
     *
     * @param id A unique Id which identifies the Rule Type in the graph.
     * @return The Rule Type with the provided Id or null if no such Rule Type exists.
     */
    RuleType getRuleType(String id);

    //------------------------------------- Relationship Handling ----------------------------------
    /**
     *
     * @return The meta type -> type. The root of all Types.
     */
    Type getMetaType();

    /**
     *
     * @return The meta relation type -> relation-type. The root of all Relation Types.
     */
    Type getMetaRelationType();

    /**
     *
     * @return The meta role type -> role-type. The root of all the Role Types.
     */
    Type getMetaRoleType();

    /**
     *
     * @return The meta resource type -> resource-type. The root of all the Resource Types.
     */
    Type getMetaResourceType();

    /**
     *
     * @return The meta entity type -> entity-type. The root of all the Entity Types.
     */
    Type getMetaEntityType();

    /**
     *
     * @return The meta rule type -> rule-type. The root of all the Rule Types.
     */
    Type getMetaRuleType();

    /**
     *
     * @return The meta rule -> inference-rule.
     */
    RuleType getMetaRuleInference();

    /**
     *
     * @return The meta rule -> constraint-rule.
     */
    RuleType getMetaRuleConstraint();

    /**
     *
     * @param relationType The Relation Type which we wish to find a Relation instance of.
     * @param roleMap A role map specifying the rolePlayers (Instances or Resources) in the relationship and the roles (Role Types) they play.
     * @return A collection of Relations which meet the above requirements or an empty collection is no relationship exists fulfilling the above requirements.
     */
    Relation getRelation(RelationType relationType, Map<RoleType, Instance> roleMap);

    /**
     *
     * @param id The id of the relation object you are looking for
     * @return The relation object.
     */
    Relation getRelation(String id);
    //------------------------------------- Utilities ----------------------------------
    /**
     * Closes and clears the current graph.
     */
    void clear();

    /**
     *
     * @return The name of the keyspace where the graph is persisted
     */
    String getKeyspace();

    /**
     *
     * @return A read only tinkerpop traversal for manually traversing the graph
     */
    GraphTraversalSource getTinkerTraversal();

    /**
     * Validates and attempts to commit the graph. An exception is thrown if validation fails or if the graph cannot be persisted due to an underlying database issue.
     * @throws MindmapsValidationException is thrown when a structural validation fails.
     */
    void commit() throws MindmapsValidationException;

    /**
     * Resets the current transaction without commiting.
     */
    void rollback();

    /**
     * Closes the current graph, rendering it unusable.
     */
    void close();
}
