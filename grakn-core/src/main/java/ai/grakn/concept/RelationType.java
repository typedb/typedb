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

import java.util.Collection;

/**
 * <p>
 *     An ontological element which categorises how {@link Instance}s may relate to each other.
 * </p>
 *
 * <p>
 *     A relation type defines how {@link Type} may relate to one another.
 *     They are used to model and categorise n-ary relationships.
 * </p>
 *
 * @author fppt
 *
 */
public interface RelationType extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Creates and returns a new {@link Relation} instance, whose direct type will be this type.
     * @see Relation
     *
     * @return a new empty relation.
     *
     * @throws ConceptException if this is a meta type
     */
    Relation addRelation();

    /**
     * Sets the supertype of the RelationType to be the RelationType specified.
     *
     * @param type The supertype of this RelationType
     * @return  The RelationType itself.
     */
    RelationType superType(RelationType type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this relation type
     * @return The RelationType itself
     */
    RelationType subType(RelationType type);

    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    RelationType scope(Instance scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    RelationType deleteScope(Instance scope);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    RelationType key(ResourceType resourceType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    RelationType resource(ResourceType resourceType);

    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves a list of the RoleTypes that make up this RelationType.
     * @see RoleType
     *
     * @return A list of the RoleTypes which make up this RelationType.
     */
    Collection<RoleType> relates();

    //------------------------------------- Edge Handling ----------------------------------

    /**
     * Sets a new RoleType for this RelationType.
     * @see RoleType
     *
     * @param roleType A new role which is part of this relationship.
     * @return The RelationType itself.
     */
    RelationType relates(RoleType roleType);

    //------------------------------------- Other ----------------------------------

    /**
     * Delete a RoleType from this RelationType
     * @see RoleType
     *
     * @param roleType The RoleType to delete from the RelationType.
     * @return The RelationType itself.
     */
    RelationType deleteRelates(RoleType roleType);

    //---- Inherited Methods
    /**
     * Sets the RelationType to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the concept is to be abstract (true) or not (false).
     * @return The RelationType itself.
     */
    RelationType setAbstract(Boolean isAbstract);

    /**
     * Returns the direct supertype of this RelationType.
     * @return The direct supertype of this RelationType
     */
    RelationType superType();

    /**
     * Returns a collection of subtypes of this RelationType.
     *
     * @return All the sub types of this RelationType
     */
    Collection<RelationType> subTypes();

    /**
     * Sets the RoleType which instances of this RelationType may play.
     *
     * @param roleType The RoleType which the instances of this Type are allowed to play.
     * @return  The RelationType itself.
     */
    RelationType plays(RoleType roleType);

    /**
     * Removes the RoleType to prevent instances of this RelationType from playing it.
     *
     * @param roleType The RoleType which the instances of this Type should no longer be allowed to play.
     * @return The RelationType itself.
     */
    RelationType deletePlays(RoleType roleType);

    /**
     * Retrieve all the Relation instances of this RelationType
     * @see Relation
     *
     * @return All the Relation instances of this relation type
     */
    Collection<Relation> instances();

    /**
     *
     * @return a deep copy of this concept.
     */
    RelationType copy();
}
