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
 *     Ontology element used to represent categories.
 * </p>
 *
 * <p>
 *     An ontological element which represents categories instances can fall within.
 *     Any instance of a Entity Type is called an {@link Entity}.
 * </p>
 *
 * @author fppt
 *
 */
public interface EntityType extends Type{
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Sets the EntityType to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the EntityType is to be abstract (true) or not (false).
     *
     * @return The EntityType itself
     */
    EntityType setAbstract(Boolean isAbstract);

    /**
     * Sets the direct supertype of the EntityType to be the EntityType specified.
     *
     * @param type The supertype of this EntityType
     * @return The EntityType itself
     *
     * @throws ConceptException if this is a meta type
     * @throws ConceptException if the given supertype is already an indirect subtype of this type
     */
    EntityType superType(EntityType type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this entity type
     * @return The EntityType itself
     *
     * @throws ConceptException if the sub type is a meta type
     * @throws ConceptException if the given subtype is already an indirect supertype of this type
     */
    EntityType subType(EntityType type);

    /**
     * Sets the RoleType which instances of this EntityType may play.
     *
     * @param roleType The Role Type which the instances of this EntityType are allowed to play.
     * @return The EntityType itself
     */
    EntityType plays(RoleType roleType);

    /**
     * Removes the RoleType to prevent instances of this EntityType from playing it.
     *
     * @param roleType The Role Type which the instances of this EntityType should no longer be allowed to play.
     * @return The EntityType itself
     */
    EntityType deletePlays(RoleType roleType);

    /**
     * Creates and returns a new Entity instance, whose direct type will be this type.
     * @see Entity
     *
     * @return a new empty entity.
     *
     * @throws ConceptException if this is a meta type
     */
    Entity addEntity();

    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    EntityType scope(Instance scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    EntityType deleteScope(Instance scope);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    EntityType key(ResourceType resourceType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    EntityType resource(ResourceType resourceType);

    //------------------------------------- Accessors ----------------------------------
    /**
     * Returns the supertype of this EntityType.
     *
     * @return The supertype of this EntityType
     */
    EntityType superType();

    /**
     * Returns a collection of subtypes of this EntityType.
     *
     * @return All the sub classes of this EntityType
     */
    Collection<EntityType> subTypes();

    /**
     * Returns a collection of all Entity instances for this EntityType.
     *
     * @see Entity
     *
     * @return All the instances of this EntityType.
     */
    Collection<Entity> instances();

    /**
     *
     * @return a deep copy of this concept.
     */
    EntityType copy();
}
