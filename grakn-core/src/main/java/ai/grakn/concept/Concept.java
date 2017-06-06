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

package ai.grakn.concept;

import ai.grakn.exception.GraphOperationException;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.CheckReturnValue;


/**
 * <p>
 *     The base concept implementation.
 * </p>
 *
 * <p>
 *     A concept which can represent anything in the graph which wraps a tinkerpop {@link Vertex}.
 *     This class forms the basis of assuring the graph follows the Grakn object model.
 *     It provides methods to retrieve information about the Concept, and determine if it is a {@link Type}
 *     ({@link EntityType}, {@link RoleType}, {@link RelationType}, {@link RuleType} or {@link ResourceType})
 *     or an {@link Instance} ({@link Entity}, {@link Relation} , {@link Resource}, {@link Rule}).
 * </p>
 *
 * @author fppt
 *
 */
public interface Concept extends Comparable<Concept>{
    //------------------------------------- Accessors ----------------------------------
    /**
     * Get the unique ID associated with the Concept.
     *
     * @return A value the concept's unique id.
     */
    @CheckReturnValue
    ConceptId getId();

    //------------------------------------- Other ---------------------------------

    /**
     * Return as a Type if the Concept is a Type.
     *
     * @return A Type if the concept is a Type
     */
    @CheckReturnValue
    Type asType();

    /**
     * Return as an Instance if the Concept is an Instance.
     *
     * @return An Instance if the concept is an Instance
     */
    @CheckReturnValue
    Instance asInstance();

    /**
     * Return as an EntityType if the Concept is an Entity Type.
     *
     * @return A Entity Type if the concept is an Entity Type
     */
    @CheckReturnValue
    EntityType asEntityType();

    /**
     * Return as a RoleType if the Concept is a Role Type.
     *
     * @return A Role Type if the concept is a Role Type
     */
    @CheckReturnValue
    RoleType asRoleType();

    /**
     * Return as a Relation Type if the concept is a Relation Type.
     *
     * @return A Relation Type if the concept is a Relation Type
     */
    @CheckReturnValue
    RelationType asRelationType();

    /**
     * Return as a Resource Type if the Concept is a Resource Type.
     *
     * @return A Resource Type if the concept is a Resource Type
     */
    @CheckReturnValue
    <D> ResourceType<D> asResourceType();

    /**
     * Return as a Rule Type if the Concept is a Rule Type.
     *
     * @return A Rule Type if the concept is a Rule Type
     */
    @CheckReturnValue
    RuleType asRuleType();

    /**
     * Return as an Entity, if the Concept is an Entity Instance.
     * @return An Entity if the concept is an Instance
     */
    @CheckReturnValue
    Entity asEntity();

    /**
     * Return as a Relation if the Concept is a Relation Instance.
     *
     * @return A Relation if the concept is a Relation
     */
    @CheckReturnValue
    Relation asRelation();

    /**
     * Return as a Resource if the Concept is a Resource Instance.
     *
     * @return A Resource if the concept is a Resource
     */
    @CheckReturnValue
    <D> Resource<D> asResource();

    /**
     * Return as a Rule if the Concept is a Rule Instance.
     *
     * @return A Rule if the concept is a Rule
     */
    @CheckReturnValue
    Rule asRule();

    /**
     * Determine if the Concept is a Type.
     *
     * @return true if the concept is a Type
     */
    @CheckReturnValue
    boolean isType();

    /**
     * Determine if the Concept is an Instance.
     *
     * @return true if the concept is an Instance
     */
    @CheckReturnValue
    boolean isInstance();

    /**
     * Determine if the Concept is an Entity Type.
     *
     * @return true if the concept is an Entity Type
     */
    @CheckReturnValue
    boolean isEntityType();

    /**
     * Determine if the Concept is a Role Type.
     *
     * @return true if the concept is a Role Type
     */
    @CheckReturnValue
    boolean isRoleType();

    /**
     * Determine if the Concept is a Relation Type.
     *
     * @return true if the concept is a Relation Type
     */
    @CheckReturnValue
    boolean isRelationType();

    /**
     * Determine if the Concept is a Resource Type.
     *
     * @return true if the concept is a Resource Type
     */
    @CheckReturnValue
    boolean isResourceType();

    /**
     * Determine if the Concept is a Rule Type.
     *
     * @return true if the concept is a Rule Type
     */
    @CheckReturnValue
    boolean isRuleType();

    /**
     * Determine if the Concept is an Entity.
     *
     * @return true if the concept is a Entity
     */
    @CheckReturnValue
    boolean isEntity();

    /**
     * Determine if the Concept is a Relation.
     *
     * @return true if the concept is a Relation
     */
    @CheckReturnValue
    boolean isRelation();

    /**
     * Determine if the Concept is a Resource.
     *
     * @return true if the concept is a Resource
     */
    @CheckReturnValue
    boolean isResource();

    /**
     * Determine if the Concept is a Rule.
     *
     * @return true if the concept is a Rule
     */
    @CheckReturnValue
    boolean isRule();

    /**
     * Delete the Concept.
     *
     * @throws GraphOperationException Throws an exception if this is a type with incoming concepts.
     */
    void delete() throws GraphOperationException;
}