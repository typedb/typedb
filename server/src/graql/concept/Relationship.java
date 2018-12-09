/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.concept;

import javax.annotation.CheckReturnValue;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Encapsulates relationships between {@link Thing}
 * A relation which is an instance of a {@link RelationshipType} defines how instances may relate to one another.
 * It represents how different entities relate to one another.
 * {@link Relationship} are used to model n-ary relationships between instances.
 */
public interface Relationship extends Thing {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Creates a relation from this instance to the provided {@link Attribute}.
     *
     * @param attribute The {@link Attribute} to which a relationship is created
     * @return The instance itself
     */
    @Override
    Relationship has(Attribute attribute);

    //------------------------------------- Accessors ----------------------------------

    /**
     * Retrieve the associated {@link RelationshipType} for this {@link Relationship}.
     *
     * @return The associated {@link RelationshipType} for this {@link Relationship}.
     * @see RelationshipType
     */
    @Override
    RelationshipType type();

    /**
     * Retrieve a list of all Instances involved in the {@link Relationship}, and the {@link Role} they play.
     *
     * @return A list of all the role types and the instances playing them in this {@link Relationship}.
     * @see Role
     */
    @CheckReturnValue
    Map<Role, Set<Thing>> rolePlayersMap();

    /**
     * Retrieves a list of every {@link Thing} involved in the {@link Relationship}, filtered by {@link Role} played.
     *
     * @param roles used to filter the returned instances only to ones that play any of the role types.
     *              If blank, returns all role players.
     * @return a list of every {@link Thing} involved in the {@link Relationship}.
     */
    @CheckReturnValue
    Stream<Thing> rolePlayers(Role... roles);

    /**
     * Expands this {@link Relationship} to include a new role player which is playing a specific role.
     *
     * @param role   The Role Type of the new role player.
     * @param player The new role player.
     * @return The {@link Relationship} itself.
     */
    Relationship assign(Role role, Thing player);

    /**
     * Removes the provided {@link Attribute} from this {@link Relationship}
     *
     * @param attribute the {@link Attribute} to be removed
     * @return The {@link Relationship} itself
     */
    @Override
    Relationship unhas(Attribute attribute);

    /**
     * Removes the {@link Thing} which is playing a {@link Role} in this {@link Relationship}.
     * If the {@link Thing} is not playing any {@link Role} in this {@link Relationship} nothing happens.
     *
     * @param role   The {@link Role} being played by the {@link Thing}
     * @param player The {@link Thing} playing the {@link Role} in this {@link Relationship}
     */
    void unassign(Role role, Thing player);

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Relationship asRelationship() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRelationship() {
        return true;
    }
}
