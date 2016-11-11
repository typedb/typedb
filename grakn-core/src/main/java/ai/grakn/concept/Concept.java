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

import ai.grakn.exception.ConceptException;

/**
 * A Concept which represents anything in the graph.
 */
public interface Concept extends Comparable<Concept>{
    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return A string representing the concept's unique id.
     */
    String getId();

    /**
     *
     * @return A Type which is the type of this concept. This concept is an instance of that type.
     */
    Type type();

    //------------------------------------- Other ---------------------------------

    /**
     *
     * @return A Type if the concept is a Type
     */
    Type asType();

    /**
     *
     * @return An Instance if the concept is an Instance
     */
    Instance asInstance();

    /**
     *
     * @return A Entity Type if the concept is a Entity Type
     */
    EntityType asEntityType();

    /**
     *
     * @return A Role Type if the concept is a Role Type
     */
    RoleType asRoleType();

    /**
     *
     * @return A Relation Type if the concept is a Relation Type
     */
    RelationType asRelationType();

    /**
     *
     * @return A Resource Type if the concept is a Resource Type
     */
    <D> ResourceType<D> asResourceType();

    /**
     *
     * @return A Rule Type if the concept is a Rule Type
     */
    RuleType asRuleType();

    /**
     *
     * @return An Entity if the concept is an Instance
     */
    Entity asEntity();

    /**
     *
     * @return A Relation if the concept is a Relation
     */
    Relation asRelation();

    /**
     *
     * @return A Resource if the concept is a Resource
     */
    <D> Resource<D> asResource();

    /**
     *
     * @return A Rule if the concept is a Rule
     */
    Rule asRule();

    /**
     *
     * @return true if the concept is a Type
     */
    boolean isType();

    /**
     *
     * @return true if the concept is an Instance
     */
    boolean isInstance();

    /**
     *
     * @return true if the concept is a Entity Type
     */
    boolean isEntityType();

    /**
     *
     * @return true if the concept is a Role Type
     */
    boolean isRoleType();

    /**
     *
     * @return true if the concept is a Relation Type
     */
    boolean isRelationType();

    /**
     *
     * @return true if the concept is a Resource Type
     */
    boolean isResourceType();

    /**
     *
     * @return true if the concept is a Rule Type
     */
    boolean isRuleType();

    /**
     *
     * @return true if the concept is a Entity
     */
    boolean isEntity();

    /**
     *
     * @return true if the concept is a Relation
     */
    boolean isRelation();

    /**
     *
     * @return true if the concept is a Resource
     */
    boolean isResource();

    /**
     *
     * @return true if the concept is a Rule
     */
    boolean isRule();

    /**
     * Deletes the concept.
     * @throws ConceptException Throws an exception if the node has any edges attached to it.
     */
    void delete() throws ConceptException;
}