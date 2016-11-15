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
 * A Relation Type is an ontological element used to concept how entity types relate to one another.
 */
public interface RelationType extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @return a new empty relation.
     */
    Relation addRelation();

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
    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Relation Type itself.
     */
    RelationType setAbstract(Boolean isAbstract);

    /**
     *
     * @return The super type of this Relation Type
     */
    RelationType superType();

    /**
     *
     * @param type The super type of this Relation Type
     * @return  The Relation Type itself.
     */
    RelationType superType(RelationType type);

    /**
     *
     * @return All the sub types of this Relation Type
     */
    Collection<RelationType> subTypes();

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return  The Relation Type itself.
     */
    RelationType playsRole(RoleType roleType);

    /**
     *
     * @param roleType The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Relation Type itself.
     */
    RelationType deletePlaysRole(RoleType roleType);

    /**
     *
     * @return All the Relation instances of this relation type
     */
    Collection<Relation> instances();
}
