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
 * This represents an instance of a Type. It represents data in the graph.
 */
public interface Instance extends Concept{
    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @param id The new unique id of the instance.
     * @return The instance itself.
     */
    Instance setId(String id);

    /**
     *
     * @param subject The new unique subject of the instance.
     * @return The instance itself.
     */
    Instance setSubject(String subject);

    /**
     *
     * @param roleTypes An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    Collection<Relation> relations(RoleType... roleTypes);

    /**
     *
     * @return A set of all the Role Types which this instance plays.
     */
    Collection<RoleType> playsRoles();
}
