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
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Encapsulates relations between Thing
 * A relation which is an instance of a RelationType defines how instances may relate to one another.
 * It represents how different entities relate to one another.
 * Relation are used to model n-ary relations between instances.
 */
public interface Relation extends Thing {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Creates a relation from this instance to the provided Attribute.
     *
     * @param attribute The Attribute to which a relation is created
     * @return The instance itself
     */
    @Override
    Relation has(Attribute attribute);

    //------------------------------------- Accessors ----------------------------------

    /**
     * Retrieve the associated RelationType for this Relation.
     *
     * @return The associated RelationType for this Relation.
     * see RelationType
     */
    @Override
    RelationType type();

    /**
     * Retrieve a list of all Instances involved in the Relation, and the Role they play.
     *
     * @return A list of all the role types and the instances playing them in this Relation.
     * see Role
     */
    @CheckReturnValue
    Map<Role, Set<Thing>> rolePlayersMap();

    /**
     * Retrieves a list of every Thing involved in the Relation, filtered by Role played.
     *
     * @param roles used to filter the returned instances only to ones that play any of the role types.
     *              If blank, returns all role players.
     * @return a list of every Thing involved in the Relation.
     */
    @CheckReturnValue
    Stream<Thing> rolePlayers(Role... roles);

    /**
     * Expands this Relation to include a new role player which is playing a specific role.
     *
     * @param role   The Role Type of the new role player.
     * @param player The new role player.
     * @return The Relation itself.
     */
    Relation assign(Role role, Thing player);

    /**
     * Removes the provided Attribute from this Relation
     *
     * @param attribute the Attribute to be removed
     * @return The Relation itself
     */
    @Override
    Relation unhas(Attribute attribute);

    /**
     * Removes the Thing which is playing a Role in this Relation.
     * If the Thing is not playing any Role in this Relation nothing happens.
     *
     * @param role   The Role being played by the Thing
     * @param player The Thing playing the Role in this Relation
     */
    void unassign(Role role, Thing player);

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Relation asRelation() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRelation() {
        return true;
    }
}
