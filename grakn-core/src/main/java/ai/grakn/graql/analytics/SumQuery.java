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
import java.util.Optional;

/**
 * Compute the sum of the selected resource-type.
 *
 * @author Jason Liu
 */
public interface SumQuery extends ComputeQuery<Optional<Number>> {

    /**
     * @param resourceTypeLabels an array of types of resources to execute the query on
     * @return a SumQuery with the subTypeLabels set
     */
    SumQuery of(String... resourceTypeLabels);

    /**
     * @param resourceTypeLabels a collection of types of resources to execute the query on
     * @return a SumQuery with the subTypeLabels set
     */
    SumQuery of(Collection<TypeLabel> resourceTypeLabels);

    /**
     * @param subTypeLabels an array of types to include in the subgraph
     * @return a SumQuery with the subTypeLabels set
     */
    @Override
    SumQuery in(String... subTypeLabels);

    /**
     * @param subTypeLabels a collection of types to include in the subgraph
     * @return a SumQuery with the subTypeLabels set
     */
    @Override
    SumQuery in(Collection<TypeLabel> subTypeLabels);

    /**
     * Execute the query.
     *
     * @return the sum if the given resource types have instances, otherwise an empty Optional instance
     */
    @Override
    Optional<Number> execute();

    /**
     * @param graph the graph to execute the query on
     * @return a SumQuery with the graph set
     */
    @Override
    SumQuery withGraph(GraknGraph graph);
}
