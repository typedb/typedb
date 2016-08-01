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

public interface RelationType extends Type {
    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return A list of the Role Types which make up this Relation Type.
     */
    Collection<RoleType> hasRoles();

    //------------------------------------- Edge Handling ----------------------------------

    /**
     *
     * @param roleType A new role which is part of this relationship.
     * @return The Relation Type itself.
     */
    RelationType hasRole(RoleType roleType);

    //------------------------------------- Other ----------------------------------

    /**
     *
     * @param roleType The role type to delete from this relationship.
     * @return The Relation Type itself.
     */
    RelationType deleteHasRole(RoleType roleType);

    //---- Inherited Methods
    RelationType setId(String id);
    RelationType setSubject(String subject);
    RelationType setValue(String value);
    RelationType setAbstract(Boolean isAbstract);
    RelationType superType();
    RelationType superType(RelationType type);
    Collection<RelationType> subTypes();
    RelationType playsRole(RoleType roleType);
    RelationType deletePlaysRole(RoleType roleType);
    Collection<Relation> instances();
}
