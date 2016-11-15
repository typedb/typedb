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
 * An ontological element which represents the categories instances can fall within.
 */
public interface EntityType extends Type{
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Entity Type itself
     */
    EntityType setAbstract(Boolean isAbstract);

    /**
     *
     * @param type The super of this Entity Type
     * @return The Entity Type itself
     */
    EntityType superType(EntityType type);

    /**
     *
     * @param roleType The Role Type which the instances of this Entity Type are allowed to play.
     * @return The Entity Type itself
     */
    EntityType playsRole(RoleType roleType);

    /**
     *
     * @param roleType The Role Type which the instances of this Entity Type should no longer be allowed to play.
     * @return The Entity Type itself
     */
    EntityType deletePlaysRole(RoleType roleType);

    /**
     *
     * @return a new empty entity.
     */
    Entity addEntity();

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return The super of this Entity Type
     */
    EntityType superType();

    /**
     *
     * @return All the sub classes of this Entity Type
     */
    Collection<EntityType> subTypes();

    /**
     *
     * @return All the instances of this Entity Type.
     */
    Collection<Entity> instances();
}
