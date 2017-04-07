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
import ai.grakn.concept.TypeLabel;

import java.util.Collection;

/**
 * A query that triggers an analytics OLAP computation on a graph.
 *
 * @param <T> the type of result this query will return
 *
 * @author Jason Liu
 */
public interface ComputeQuery<T> extends Query<T> {

    /**
     * @param graph the graph to execute the compute query on
     * @return a ComputeQuery with the graph set
     */
    @Override
    ComputeQuery<T> withGraph(GraknGraph graph);

    /**
     * @param subTypelabels an array of types to include in the subgraph
     * @return a ComputeQuery with the subTypelabels set
     */
    ComputeQuery<T> in(String... subTypelabels);

    /**
     * @param subTypeLabels a collection of types to include in the subgraph
     * @return a ComputeQuery with the subTypeLabels set
     */
    ComputeQuery<T> in(Collection<TypeLabel> subTypeLabels);

    /**
     * kill the compute query, terminate the job
     */
    void kill();
}
