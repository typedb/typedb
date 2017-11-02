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
import java.util.stream.Stream;

/**
 * <p>
 *     An {@link SchemaConcept} which defines a role which can be played in a {@link RelationshipType}
 * </p>
 *
 * <p>
 *     This ontological element defines the {@link Role} which make up a {@link RelationshipType}.
 *     It behaves similarly to {@link SchemaConcept} when relating to other types.
 * </p>
 *
 * @author fppt
 *
 */
public interface Role extends SchemaConcept {
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
     * @return All the super-types of this this {@link Role}
     */
    @Override
    Stream<Role> sups();

    /**
     * Returns the sub of this Role.
     *
     * @return The sub of this Role
     */
    @Override
    Stream<Role> subs();

    /**
     * Returns the {@link RelationshipType}s that this {@link Role} takes part in.
     * @see RelationshipType
     *
     * @return The {@link RelationshipType} which this {@link Role} takes part in.
     */
    @CheckReturnValue
    Stream<RelationshipType> relationshipTypes();

    /**
     * Returns a collection of the {@link Type}s that can play this {@link Role}.
     * @see Type
     *
     * @return A list of all the {@link Type}s which can play this {@link Role}.
     */
    @CheckReturnValue
    Stream<Type> playedByTypes();

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

