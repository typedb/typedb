/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.kb.concept.api;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * An SchemaConcept which defines a role which can be played in a RelationType
 * This ontological element defines the Role which make up a RelationType.
 * It behaves similarly to SchemaConcept when relating to other types.
 */
public interface Role extends SchemaConcept {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Changes the Label of this Concept to a new one.
     *
     * @param label The new Label.
     * @return The Concept itself
     */
    Role label(Label label);

    /**
     * Sets the super of this Role.
     *
     * @param type The super of this Role
     * @return The Role itself
     */
    Role sup(Role type);

    //------------------------------------- Accessors ----------------------------------

    /**
     * Returns the super of this Role.
     *
     * @return The super of this Role
     */
    @Nullable
    @Override
    Role sup();

    /**
     * @return All the super-types of this this Role
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
     * Returns the RelationTypes that this Role takes part in.
     *
     * @return The RelationType which this Role takes part in.
     * see RelationType
     */
    @CheckReturnValue
    Stream<RelationType> relations();

    /**
     * Returns a collection of the Types that can play this Role.
     *
     * @return A list of all the Types which can play this Role.
     * see Type
     */
    @CheckReturnValue
    Stream<Type> players();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Role asRole() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRole() {
        return true;
    }
}

