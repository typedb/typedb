/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.model;

import java.util.Collection;

/**
 * An ontological element which defines a role which can be played in a relation type.
 */
public interface RoleType extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param id The new unique id of the Role Type.
     * @return The Role Type itself
     */
    RoleType setId(String id);

    /**
     *
     * @param subject The new unique subject of the Role Type.
     * @return The Role Type itself
     */
    RoleType setSubject(String subject);

    /**
     *
     * @param value The String value to store in the Role Type
     * @return The Role Type itself
     */
    RoleType setValue(String value);

    /**
     *
     * @param isAbstract  Specifies if the Role Type is abstract (true) or not (false).
     *                    If the Role Type is abstract it is not allowed to have any instances.
     * @return The Role Type itself
     */
    RoleType setAbstract(Boolean isAbstract);

    /**
     *
     * @param type The super type of this Role Type
     * @return The Role Type itself
     */
    RoleType superType(RoleType type);

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Role Type itself
     */
    RoleType playsRole(RoleType roleType);

    /**
     *
     * @param roleType The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Role Type itself
     */
    RoleType deletePlaysRole(RoleType roleType);

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return The super type of this Role Type
     */
    RoleType superType();

    /**
     *
     * @return The sub types of this Role Type
     */
    Collection<RoleType> subTypes();

    /**
     *
     * @return The Relation Type which this role takes part in.
     */
    RelationType relationType();

    /**
     *
     * @return A list of all the Concept Types which can play this role.
     */
    Collection<Type> playedByTypes();
}

