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
 * An instance of Entity Type which represents some data in the graph.
 */
public interface Entity extends Instance{
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param id The new unique id of the instance.
     * @return The Entity itself
     */
    Entity setId(String id);

    /**
     *
     * @param subject The new unique subject of the instance.
     * @return The Entity itself
     */
    Entity setSubject(String subject);

    /**
     *
     * @param value The String value to store on this Entity
     * @return The Entity itself
     */
    Entity setValue(String value);

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return The String value stored on this Entity
     */
    String getValue();

    /**
     *
     * @return The Entity Type of this Entity
     */
    EntityType type();

    /**
     *
     * @return A collection of resources attached to this Instance.
     */
    Collection<Resource<?>> resources();
}
