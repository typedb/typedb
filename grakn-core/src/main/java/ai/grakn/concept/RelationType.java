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

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * <p>
 *     An ontological element which categorises how {@link Thing}s may relate to each other.
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
     * Changes the {@link Label} of this {@link Concept} to a new one.
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    RelationType setLabel(Label label);

    /**
     * Creates and returns a new {@link Relation} instance, whose direct type will be this type.
     * @see Relation
     *
     * @return a new empty relation.
     *
     * @throws GraphOperationException if this is a meta type
     */
    Relation addRelation();

    /**
     * Sets the supertype of the RelationType to be the RelationType specified.
     *
     * @param type The supertype of this RelationType
     * @return  The RelationType itself.
     */
    RelationType sup(RelationType type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this relation type
     * @return The RelationType itself
     */
    RelationType sub(RelationType type);

    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    @Override
    RelationType scope(Thing scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    @Override
    RelationType deleteScope(Thing scope);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RelationType key(ResourceType resourceType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RelationType resource(ResourceType resourceType);

    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves a list of the RoleTypes that make up this RelationType.
     * @see Role
     *
     * @return A list of the RoleTypes which make up this RelationType.
     */
    @CheckReturnValue
    Collection<Role> relates();

    //------------------------------------- Edge Handling ----------------------------------

    /**
     * Sets a new Role for this RelationType.
     * @see Role
     *
     * @param role A new role which is part of this relationship.
     * @return The RelationType itself.
     */
    RelationType relates(Role role);

    //------------------------------------- Other ----------------------------------

    /**
     * Delete a Role from this RelationType
     * @see Role
     *
     * @param role The Role to delete from the RelationType.
     * @return The RelationType itself.
     */
    RelationType deleteRelates(Role role);

    //---- Inherited Methods
    /**
     * Sets the RelationType to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the concept is to be abstract (true) or not (false).
     * @return The RelationType itself.
     */
    @Override
    RelationType setAbstract(Boolean isAbstract);

    /**
     * Returns the direct supertype of this RelationType.
     * @return The direct supertype of this RelationType
     */
    @Override
    @Nonnull
    RelationType sup();

    /**
     * Returns a collection of subtypes of this RelationType.
     *
     * @return All the sub types of this RelationType
     */
    @Override
    Stream<RelationType> subs();

    /**
     * Sets the Role which instances of this RelationType may play.
     *
     * @param role The Role which the instances of this Type are allowed to play.
     * @return  The RelationType itself.
     */
    @Override
    RelationType plays(Role role);

    /**
     * Removes the Role to prevent instances of this RelationType from playing it.
     *
     * @param role The Role which the instances of this Type should no longer be allowed to play.
     * @return The RelationType itself.
     */
    @Override
    RelationType deletePlays(Role role);

    /**
     * Retrieve all the Relation instances of this RelationType
     * @see Relation
     *
     * @return All the Relation instances of this relation type
     */
    @Override
    Collection<Relation> instances();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default RelationType asRelationType(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRelationType(){
        return true;
    }
}
