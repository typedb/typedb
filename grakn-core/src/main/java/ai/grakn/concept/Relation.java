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


import ai.grakn.exception.ConceptException;

import java.util.Collection;
import java.util.Map;

/**
 * <p>
 *     Encapsulates relationships between {@link Instance}
 * </p>
 *
 * <p>
 *     A relation which is an instance of a {@link RelationType} defines how instances may relate to one another.
 *     It represents how different entities relate to one another.
 *     Relations are used to model n-ary relationships between instances.
 * </p>
 *
 * @author fppt
 *
 */
public interface Relation extends Instance {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Sets the Instance that can scope this Relation to a specific domain.
     * For example, if a relation is only valid for a particular language you can scope
     * the relation to an Instance representing that language.
     *
     * @param instance A new instance which scopes this Relation
     * @return The Relation itself
     */
    Relation scope(Instance instance);

    //------------------------------------- Accessors ----------------------------------

    /**
     * Retrieve the associated Relation Type for this Relation.
     * @see RelationType
     *
     * @return The associated Relation Type for this Relation.
     */
    RelationType type();

    /**
     * Retrieve a list of all Instances involved in the Relation, and the Role Types they play.
     * @see RoleType
     *
     * @return A list of all the Instances and their Role Types.
     */
    Map<RoleType, Instance> rolePlayers();

    /**
     * Retrieve a list of the Instances that scope this Relation.
     *
     * @return A list of the Instances that scope this Relation.
     */
    Collection<Instance> scopes();

    /**
     * Expands this Relation to include a new role player which is playing a specific role.
     *
     * @param roleType The Role Type of the new role player.
     * @param instance The new role player.
     * @return The Relation itself.
     */
    Relation putRolePlayer(RoleType roleType, Instance instance);

    //------------------------------------- Other ----------------------------------

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Relation.
     * @return The Relation itself
     */
    Relation deleteScope(Instance scope) throws ConceptException;
}
