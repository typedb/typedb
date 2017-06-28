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

import javax.annotation.CheckReturnValue;
import java.util.Collection;

/**
 * <p>
 *     An ontological element which defines a role which can be played in a relation type.
 * </p>
 *
 * <p>
 *     This ontological element defines the roles which make up a {@link RelationType}.
 *     It behaves similarly to {@link Type} when relating to other types.
 *     It has some additional functionality:
 *     1. It cannot play a role to itself.
 *     2. It is special in that it is unique to relation types.
 * </p>
 *
 * @author fppt
 *
 */
public interface Role extends OntologyConcept {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Sets the supertype of this Role.
     *
     * @param type The supertype of this Role
     * @return The Role itself
     */
    Role superType(Role type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this role type
     * @return The Role itself
     */
    Role subType(Role type);

    //------------------------------------- Accessors ----------------------------------
    /**
     * Returns the supertype of this Role.
     *
     * @return The supertype of this Role
     */
    @Override
    Role superType();

    /**
     * Returns the subtypes of this Role.
     *
     * @return The sub types of this Role
     */
    @Override
    Collection<Role> subTypes();

    /**
     * Returns the RelationTypes that this Role takes part in.
     * @see RelationType
     *
     * @return The RelationTypes which this role takes part in.
     */
    @CheckReturnValue
    Collection<RelationType> relationTypes();

    /**
     * Returns a collection of the Types that can play this Role.
     * @see Type
     *
     * @return A list of all the Types which can play this role.
     */
    @CheckReturnValue
    Collection<Type> playedByTypes();
}

