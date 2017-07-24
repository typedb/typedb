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
import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * <p>
 *     An {@link OntologyConcept} which defines a role which can be played in a {@link RelationType}
 * </p>
 *
 * <p>
 *     This ontological element defines the {@link Role} which make up a {@link RelationType}.
 *     It behaves similarly to {@link OntologyConcept} when relating to other types.
 * </p>
 *
 * @author fppt
 *
 */
public interface Role extends OntologyConcept {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Changes the {@link Label} of this {@link Concept} to a new one.
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    Role setLabel(Label label);

    /**
     * Sets the super of this Role.
     *
     * @param type The super of this Role
     * @return The Role itself
     */
    Role sup(Role type);

    /**
     * Adds another sub to this type
     *
     * @param type The sub type of this role type
     * @return The Role itself
     */
    Role sub(Role type);

    //------------------------------------- Accessors ----------------------------------
    /**
     * Returns the super of this Role.
     *
     * @return The super of this Role
     */
    @Override
    @Nonnull
    Role sup();

    /**
     * Returns the sub of this Role.
     *
     * @return The sub of this Role
     */
    @Override
    Collection<Role> subs();

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

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Role asRole(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRole(){
        return true;
    }
}

