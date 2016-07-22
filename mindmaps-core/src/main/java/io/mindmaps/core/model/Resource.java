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
 * A concept which represents a resource.
 * @param <D> The data type of this resource. Supported Types include: String, Long, Double, and Boolean
 */
public interface Resource<D> extends Instance{
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param id The new unique id of the instance.
     * @return The Resource itself
     */
    Resource<D> setId(String id);

    /**
     *
     * @param subject The new unique subject of the instance.
     * @return The Resource itself
     */
    Resource<D> setSubject(String subject);

    /**
     *
     * @param value The value to store on the resource
     * @return The Resource itself
     */
    Resource<D> setValue(D value);

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return The Resource itself
     */
    D getValue();

    /**
     *
     * @return the type of this resource
     */
    ResourceType<D> type();

    /**
     *
     * @return The data type of this Resource's type.
     */
    Data<D> dataType();

    /**
     * @return The list of all Instances which posses this resource
     */
    Collection<Instance> ownerInstances();

}
