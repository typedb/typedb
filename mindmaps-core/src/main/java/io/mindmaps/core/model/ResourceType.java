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

import io.mindmaps.core.implementation.Data;

import java.util.Collection;

/**
 * A Resource Type which can hold different values.
 * @param <D> The data tyoe of this resource type.
 */
public interface ResourceType<D> extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param id The new unique id of the Resource Type.
     * @return The Resource Type itself.
     */
    ResourceType<D> setId(String id);

    /**
     *
     * @param subject The new unique subject of the Resource Type.
     * @return The Resource Type itself.
     */
    ResourceType<D> setSubject(String subject);

    /**
     *
     * @param value The String value to store in the Resource Type
     * @return The Resource Type itself.
     */
    ResourceType<D> setValue(String value);

    /**
     *
     * @param isAbstract  Specifies if the Resource Type is abstract (true) or not (false).
     *                    If the Resource Type is abstract it is not allowed to have any instances.
     * @return The Resource Type itself.
     */
    ResourceType<D> setAbstract(Boolean isAbstract);

    /**
     *
     * @param type The super type of this Resource Type.
     * @return The Resource Type itself.
     */
    ResourceType<D> superType(ResourceType<D> type);

    /**
     *
     * @param roleType The Role Type which the instances of this Resource Type are allowed to play.
     * @return The Resource Type itself.
     */
    ResourceType<D> playsRole(RoleType roleType);

    /**
     *
     * @param roleType The Role Type which the instances of this Resource Type should no longer be allowed to play.
     * @return The Resource Type itself.
     */
    ResourceType<D> deletePlaysRole(RoleType roleType);

    /**
     * @param regex The regular expression which instances of this resource must conform to.
     * @return The Resource Type itself.
     */
    ResourceType<D> setRegex(String regex);

    /**
     * @param isUnique Indicates if the resource should be Unique to the Instance or not.
     * @return The Resource Type itself.
     */
    ResourceType<D> setUnique(boolean isUnique);

    //------------------------------------- Accessors ---------------------------------
    /**
     *
     * @return The super of this Resource Type
     */
    ResourceType<D> superType();

    /**
     *
     * @return The sub types of this Resource Type
     */
    Collection<ResourceType<D>> subTypes();

    /**
     *
     * @return The resource instances of this Resource Type
     */
    Collection<Resource<D>> instances();

    /**
     * @return The data type which instances of this resource must conform to.
     */
    Data<D> getDataType();

    /**
     * @return The regular expression which instances of this resource must conform to.
     */
    String getRegex();

    /**
     * @return Indicates if the resource is Unique to the Instance or not.
     */
    boolean isUnique();
}
