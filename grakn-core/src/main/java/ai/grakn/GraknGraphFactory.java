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

package ai.grakn;


/**
 * <p>
 *     Builds a Grakn Graph factory
 * </p>
 *
 * <p>
 *     This class facilitates the construction of Grakn Graphs by determining which factory should be built.
 *     The graphs produced by a factory are singletons bound to a specific keyspace.
 *     To create graphs bound to a different keyspace you must create another factory
 *     using {@link Grakn#factory(String, String)}
 *
 * </p>
 *
 * @author fppt
 */
public interface GraknGraphFactory {
    /**
     * Get a new or existing graph.
     *
     * @return A new or existing Grakn graph
     * @see GraknGraph
     */
    GraknGraph getGraph();

    /**
     * Get a new or existing graph with batch loading enabled.
     *
     * @return A new or existing Grakn graph with batch loading enabled
     * @see GraknGraph
     */
    GraknGraph getGraphBatchLoading();

    /**
     * Get a new or existing GraknComputer.
     *
     * @return A new or existing Grakn graph computer
     * @see GraknComputer
     */
    GraknComputer getGraphComputer();
}
