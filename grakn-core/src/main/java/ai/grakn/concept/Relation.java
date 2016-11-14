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
 * A relation represents and instance of a relation type which concept how different entities relate to one another.
 */
public interface Relation extends Instance {
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param instance A new instance which can scope this Relation
     * @return The Relation itself
     */
    Relation scope(Instance instance);

    //------------------------------------- Accessors ----------------------------------

    /**
     *
     * @return The Relation type of this Relation.
     */
    RelationType type();

    /**
     *
     * @return A list of all the Instances involved in the relationships and the Role Types which they play.
     */
    Map<RoleType, Instance> rolePlayers();

    /**
     *
     * @return A list of the Instances which scope this Relation
     */
    Collection<Instance> scopes();

    /**
     * Expands this Relation to include a new role player which is playing a specific role.
     * @param roleType The role of the new role player.
     * @param instance The new role player.
     * @return The Relation itself
     */
    Relation putRolePlayer(RoleType roleType, Instance instance);

    //------------------------------------- Other ----------------------------------

    /**
     * @param scope A concept which is currently scoping this concept.
     * @return The Relation itself
     */
    Relation deleteScope(Instance scope) throws ConceptException;
}
