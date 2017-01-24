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
     * Sets the supertype of the EntityType to be the EntityType specified.
     *
     * @param type The supertype of this EntityType
     * @return The EntityType itself
     */
    EntityType superType(EntityType type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this entity type
     * @return The EntityType itself
     */
    EntityType subType(EntityType type);

    /**
     * Sets the RoleType which instances of this EntityType may play.
     *
     * @param roleType The Role Type which the instances of this EntityType are allowed to play.
     * @return The EntityType itself
     */
    EntityType playsRole(RoleType roleType);

    /**
     * Removes the RoleType to prevent instances of this EntityType from playing it.
     *
     * @param roleType The Role Type which the instances of this EntityType should no longer be allowed to play.
     * @return The EntityType itself
     */
    EntityType deletePlaysRole(RoleType roleType);

    /**
     * Creates and returns a new Entity instance.
     * @see Entity
     *
     * @return a new empty entity.
     */
    Entity addEntity();

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
}
