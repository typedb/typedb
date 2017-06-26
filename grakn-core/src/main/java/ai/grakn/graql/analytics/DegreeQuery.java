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

package ai.grakn.graql.analytics;

import ai.grakn.GraknGraph;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.ComputeQuery;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Compute the number of relations that each instance takes part in.
 *
 * @author Jason Liu
 */
public interface DegreeQuery extends ComputeQuery<Map<Long, Set<String>>> {

    /**
     * @param subTypeLabels an array of types to include in the subgraph
     * @return a DegreeQuery with the subTypeLabels set
     */
    @Override
    DegreeQuery in(String... subTypeLabels);

    /**
     * @param subTypeLabels a collection of types to include in the subgraph
     * @return a DegreeQuery with the subTypeLabels set
     */
    @Override
    DegreeQuery in(Collection<TypeLabel> subTypeLabels);

    /**
     * @param ofTypeLabels an array of types in the subgraph to compute degree of. By default the degrees of all the
     *                    types in the graph will be computed
     * @return a DegreeQuery with the subTypeLabels set
     */
    DegreeQuery of(String... ofTypeLabels);

    /**
     * @param ofTypeLabels a collection of types in the subgraph to compute degree of. By default the degrees of all the
     *                    types in the graph will be computed
     * @return a DegreeQuery with the subTypeLabels set
     */
    DegreeQuery of(Collection<TypeLabel> ofTypeLabels);

    /**
     * @param graph the graph to execute the query on
     * @return a DegreeQuery with the graph set
     */
    @Override
    DegreeQuery withGraph(GraknGraph graph);
}
