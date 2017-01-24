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

package ai.grakn.graph;


import ai.grakn.exception.GraknValidationException;

import java.util.Set;

/**
 * <p>
 *     The Engine Specific Graph Interface
 * </p>
 *
 * <p>
 *     Provides common methods for advanced inteaction with the graph.
 *     This is used internally by Engine
 * </p>
 *
 * @author fppt
 *
 */
public interface EngineGraknGraph extends BaseGraknGraph {

    /**
     * Commits the transaction without submitting any commit logs through the REST API
     *
     * @throws GraknValidationException is thrown when a structural validation fails.
     */
    void commitTx() throws GraknValidationException;

    /**
     * Merges duplicate castings if one is found.
     * @param castingId The id of the casting to check for duplicates
     * @return true if some castings were merged
     */
    boolean fixDuplicateCasting(Object castingId);

    /**
     *
     * @param resourceIds The resourceIDs which possible contain duplicates.
     * @return True if a commit is required.
     */
    boolean fixDuplicateResources(Set<Object> resourceIds);
}
