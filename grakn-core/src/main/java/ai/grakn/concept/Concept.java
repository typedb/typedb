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
 *     or an {@link Thing} ({@link Entity}, {@link Relation} , {@link Resource}, {@link Rule}).
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
     * Return as a {@link OntologyElement} if the {@link Concept} is a {@link OntologyElement}.
     *
     * @return A {@link OntologyElement} if the {@link Concept} is a {@link OntologyElement}
     */
    @CheckReturnValue
    OntologyElement asOntologyElement();

    /**
     * Return as a {@link Type} if the {@link Concept} is a {@link Type}.
     *
     * @return A {@link Type} if the {@link Concept} is a {@link Type}
     */
    @CheckReturnValue
    Type asType();

    /**
     * Return as an {@link Thing} if the {@link Concept} is an {@link Thing}.
     *
     * @return An {@link Thing} if the {@link Concept} is an {@link Thing}
     */
    @CheckReturnValue
    Thing asInstance();

    /**
     * Return as an {@link EntityType} if the {@link Concept} is an {@link EntityType}.
     *
     * @return A {@link EntityType} if the {@link Concept} is an {@link EntityType}
     */
    @CheckReturnValue
    EntityType asEntityType();

    /**
     * Return as a {@link RoleType} if the {@link Concept} is a {@link RoleType}.
     *
     * @return A {@link RoleType} if the {@link Concept} is a {@link RoleType}
     */
    @CheckReturnValue
    RoleType asRoleType();

    /**
     * Return as a {@link RelationType} if the {@link Concept} is a {@link RelationType}.
     *
     * @return A {@link RelationType} if the {@link Concept} is a {@link RelationType}
     */
    @CheckReturnValue
    RelationType asRelationType();

    /**
     * Return as a {@link RelationType} if the {@link Concept} is a {@link RelationType}
     *
     * @return A {@link RelationType} if the {@link Concept} is a {@link RelationType}
     */
    @CheckReturnValue
    <D> ResourceType<D> asResourceType();

    /**
     * Return as a {@link RuleType} if the {@link Concept} is a {@link RuleType}.
     *
     * @return A {@link RuleType} if the {@link Concept} is a {@link RuleType}
     */
    @CheckReturnValue
    RuleType asRuleType();

    /**
     * Return as an {@link Entity}, if the {@link Concept} is an {@link Entity} {@link Thing}.
     * @return An {@link Entity} if the {@link Concept} is a {@link Thing}
     */
    @CheckReturnValue
    Entity asEntity();

    /**
     * Return as a {@link Relation} if the {@link Concept} is a {@link Relation} {@link Thing}.
     *
     * @return A {@link Relation}  if the {@link Concept} is a {@link Relation}
     */
    @CheckReturnValue
    Relation asRelation();

    /**
     * Return as a {@link Resource}  if the {@link Concept} is a {@link Resource} {@link Thing}.
     *
     * @return A {@link Resource} if the {@link Concept} is a {@link Resource}
     */
    @CheckReturnValue
    <D> Resource<D> asResource();

    /**
     * Return as a {@link Rule} if the {@link Concept} is a {@link Rule} {@link Thing}.
     *
     * @return A {@link Rule} if the {@link Concept} is a {@link Rule}
     */
    @CheckReturnValue
    Rule asRule();

    /**
     * Determine if the {@link Concept} is a {@link OntologyElement}
     *
     * @return true if the{@link Concept} concept is a {@link OntologyElement}
     */
    @CheckReturnValue
    boolean isOntologyElement();

    /**
     * Determine if the {@link Concept} is a {@link Type}.
     *
     * @return true if the{@link Concept} concept is a {@link Type}
     */
    @CheckReturnValue
    boolean isType();

    /**
     * Determine if the {@link Concept} is an {@link Thing}.
     *
     * @return true if the {@link Concept} is an {@link Thing}
     */
    @CheckReturnValue
    boolean isInstance();

    /**
     * Determine if the {@link Concept} is an {@link EntityType}.
     *
     * @return true if the {@link Concept} is an {@link EntityType}.
     */
    @CheckReturnValue
    boolean isEntityType();

    /**
     * Determine if the {@link Concept} is a {@link RoleType}.
     *
     * @return true if the {@link Concept} is a {@link RoleType}
     */
    @CheckReturnValue
    boolean isRoleType();

    /**
     * Determine if the {@link Concept} is a {@link RelationType}.
     *
     * @return true if the {@link Concept} is a {@link RelationType}
     */
    @CheckReturnValue
    boolean isRelationType();

    /**
     * Determine if the {@link Concept} is a {@link ResourceType}.
     *
     * @return true if the{@link Concept} concept is a {@link ResourceType}
     */
    @CheckReturnValue
    boolean isResourceType();

    /**
     * Determine if the {@link Concept} is a {@link RuleType}.
     *
     * @return true if the {@link Concept} is a {@link RuleType}
     */
    @CheckReturnValue
    boolean isRuleType();

    /**
     * Determine if the {@link Concept} is an {@link Entity}.
     *
     * @return true if the {@link Concept} is a {@link Entity}
     */
    @CheckReturnValue
    boolean isEntity();

    /**
     * Determine if the {@link Concept} is a {@link Relation}.
     *
     * @return true if the {@link Concept} is a {@link Relation}
     */
    @CheckReturnValue
    boolean isRelation();

    /**
     * Determine if the {@link Concept} is a {@link Resource}.
     *
     * @return true if the {@link Concept} is a {@link Resource}
     */
    @CheckReturnValue
    boolean isResource();

    /**
     * Determine if the {@link Concept} is a {@link Rule}.
     *
     * @return true if the {@link Concept} is a {@link Rule}
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