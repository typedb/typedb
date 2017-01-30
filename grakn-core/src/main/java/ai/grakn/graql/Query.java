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

import java.util.stream.Stream;

/**
 * A Graql query of any kind. May read and write to the graph.
 *
 * @param <T> The result type after executing the query
 *
 * @author Felix Chapman
 */
public interface Query<T> {

    /**
     * @param graph the graph to execute the query on
     * @return a new query with the graph set
     */
    Query<T> withGraph(GraknGraph graph);

    /**
     * Execute the query against the graph (potentially writing to the graph) and return a result
     * @return the result of the query
     */
    T execute();

    /**
     * Execute the query and return a human-readable stream of results
     */
    Stream<String> resultsString(Printer printer);

    /**
     * Whether this query will modify the graph
     */
    boolean isReadOnly();
}
