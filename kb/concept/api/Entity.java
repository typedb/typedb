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

/**
 * An instance of Entity Type EntityType
 * This represents an entity in the graph.
 * Entities are objects which are defined by their Attribute and their links to
 * other entities via Relation
 */
public interface Entity extends Thing {
    //------------------------------------- Accessors ----------------------------------

    /**
     * @return The EntityType of this Entity
     * see EntityType
     */
    @Override
    EntityType type();

    /**
     * Creates a relation from this instance to the provided Attribute.
     *
     * @param attribute The Attribute to which a relation is created
     * @return The instance itself
     */
    @Override
    Entity has(Attribute attribute);

    /**
     * Removes the provided Attribute from this Entity
     *
     * @param attribute the Attribute to be removed
     * @return The Entity itself
     */
    @Override
    Entity unhas(Attribute attribute);

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Entity asEntity() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isEntity() {
        return true;
    }
}
