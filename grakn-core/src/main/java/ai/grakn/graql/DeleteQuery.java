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

import ai.grakn.GraknGraph;
import ai.grakn.graql.admin.DeleteQueryAdmin;

/**
 * A query for deleting concepts from a match query.
 * <p>
 * A {@code DeleteQuery} is built from a {@code MatchQuery} and will perform a delete operation for every result of
 * the @{code MatchQuery}.
 * <p>
 * The delete operation to perform is based on what {@code Var} objects are provided to it. If only variable names
 * are provided, then the delete query will delete the concept bound to each given variable name. If property flags
 * are provided, e.g. {@code var("x").has("name")} then only those properties are deleted.
 *
 * @author Felix Chapman
 */
public interface DeleteQuery extends Query<Void> {

    /**
     * @param graph the graph to execute the query on
     * @return a new DeleteQuery with the graph set
     */
    DeleteQuery withGraph(GraknGraph graph);

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    DeleteQueryAdmin admin();
}
