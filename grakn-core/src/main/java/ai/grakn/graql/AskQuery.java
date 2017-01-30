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

package ai.grakn.graql;

import ai.grakn.graql.admin.AskQueryAdmin;
import ai.grakn.GraknGraph;

/**
 * A query that will return whether a match query can be found in the graph.
 * <p>
 * An {@code AskQuery} is created from a {@code MatchQuery}, which describes what patterns it should find.
 *
 * @author Felix Chapman
 */
public interface AskQuery extends Query<Boolean> {

    /**
     * @param graph the graph to execute the query on
     * @return a new AskQuery with the graph set
     */
    @Override
    AskQuery withGraph(GraknGraph graph);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    AskQueryAdmin admin();
}
