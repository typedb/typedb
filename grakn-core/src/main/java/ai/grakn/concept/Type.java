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
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * <p>
 *     A Type represents any ontological element in the graph.
 * </p>
 *
 * <p>
 *     Types are used to model the behaviour of {@link Thing} and how they relate to each other.
 *     They also aid in categorising {@link Thing} to different types.
 * </p>
 *
 * @see EntityType
 * @see Role
 * @see RelationshipType
 * @see ResourceType
 * @see RuleType
 *
 * @author fppt
 *
 */
public interface Type extends SchemaConcept {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Changes the {@link Label} of this {@link Concept} to a new one.
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    Type setLabel(Label label);

    /**
     * Sets the Entity Type to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the concept is to be abstract (true) or not (false).
     * @return The concept itself
     *
     * @throws GraphOperationException if this is a meta-type
     */
    Type setAbstract(Boolean isAbstract) throws GraphOperationException;

    /**
     *
     * @param role The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     *
     * @throws GraphOperationException if this is a meta-type
     */
    Type plays(Role role) throws GraphOperationException;

    /**
     * Creates a {@link RelationshipType} which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     *
     * @throws GraphOperationException if this is a meta-type
     */
    Type key(ResourceType resourceType) throws GraphOperationException;

    /**
     * Creates a {@link RelationshipType} which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     *
     * @throws GraphOperationException if this is a meta-type
     */
     Type resource(ResourceType resourceType) throws GraphOperationException;

    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    Type scope(Thing scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    Type deleteScope(Thing scope);

    //------------------------------------- Accessors ---------------------------------

    /**
     *
     * @return A list of Role Types which instances of this Type can indirectly play.
     */
    Stream<Role> plays();

    /**
     *
     * @return The resource types which this type is linked with.
     */
    @CheckReturnValue
    Stream<ResourceType> resources();

    /**
     *
     * @return The resource types which this type is linked with as a key.
     */
    @CheckReturnValue
    Stream<ResourceType> keys();

    /**
     *
     * @return The direct super of this Type
     */
    @CheckReturnValue
    @Nullable
    Type sup();

    /**
     * Get all indirect sub-types of this type.
     *
     * The indirect sub-types are the type itself and all indirect sub-types of direct sub-types.
     *
     * @return All the indirect sub-types of this Type
     */
    @CheckReturnValue
    Stream<? extends Type> subs();

    /**
     * Get all indirect instances of this type.
     *
     * The indirect instances are the direct instances and all indirect instances of direct sub-types.
     *
     * @return All the indirect instances of this type.
     */
    @CheckReturnValue
    Stream<? extends Thing> instances();

    /**
     * Return if the type is set to abstract.
     *
     * By default, types are not abstract.
     *
     * @return returns true if the type is set to be abstract.
     */
    @CheckReturnValue
    Boolean isAbstract();

    /**
     * Retrieve a list of the Instances that scope this Type.
     *
     * @return A list of the Instances that scope this Type.
     */
    @CheckReturnValue
    Stream<Thing> scopes();

    //------------------------------------- Other ----------------------------------
    /**
     *
     * @param role The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Type itself.
     */
    Type deletePlays(Role role);

    @Deprecated
    @CheckReturnValue
    @Override
    default Type asType(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isType(){
        return true;
    }
}
