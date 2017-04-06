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

import java.util.Collection;

/**
 * <p>
 *     An ontological element which defines a role which can be played in a relation type.
 * </p>
 *
 * <p>
 *     This ontological element defines the roles which make up a {@link RelationType}.
 *     It behaves similarly to {@link Type} when relating to other types.
 *     It has some additional functionality:
 *     1. It cannot play a role to itself.
 *     2. It is special in that it is unique to relation types.
 * </p>
 *
 * @author fppt
 *
 */
public interface RoleType extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Sets the RoleType to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the RoleType is to be abstract (true) or not (false).
     *
     * @return The RoleType itself
     */
    RoleType setAbstract(Boolean isAbstract);

    /**
     * Sets the supertype of this RoleType.
     *
     * @param type The supertype of this RoleType
     * @return The RoleType itself
     */
    RoleType superType(RoleType type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this role type
     * @return The RoleType itself
     */
    RoleType subType(RoleType type);

    /**
     * Sets the RoleType which instances of this Type may play.
     *
     * @param roleType The RoleType which the instances of this Type are allowed to play.
     * @return The RoleType itself
     */
    RoleType plays(RoleType roleType);

    /**
     * Removes the RoleType to prevent instances from playing it
     *
     * @param roleType The RoleType which the instances of this Type should no longer be allowed to play.
     * @return The RoleType itself
     */
    RoleType deletePlays(RoleType roleType);


    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    RoleType scope(Instance scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    RoleType deleteScope(Instance scope);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    RoleType key(ResourceType resourceType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    RoleType resource(ResourceType resourceType);

    //------------------------------------- Accessors ----------------------------------
    /**
     * Returns the supertype of this RoleType.
     *
     * @return The supertype of this RoleType
     */
    RoleType superType();

    /**
     * Returns the subtypes of this RoleType.
     *
     * @return The sub types of this RoleType
     */
    Collection<RoleType> subTypes();

    /**
     * Returns the RelationTypes that this RoleType takes part in.
     * @see RelationType
     *
     * @return The RelationTypes which this role takes part in.
     */
    Collection<RelationType> relationTypes();

    /**
     * Returns a collection of the Types that can play this RoleType.
     * @see Type
     *
     * @return A list of all the Types which can play this role.
     */
    Collection<Type> playedByTypes();

    /**
     *
     * @return a deep copy of this concept.
     */
    RoleType copy();
}

