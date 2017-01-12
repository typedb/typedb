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
import ai.grakn.concept.TypeName;
import ai.grakn.graql.ComputeQuery;

import java.util.Collection;
import java.util.Optional;

/**
 * Compute the minimum value of the selected resource-type.
 */
public interface MinQuery extends ComputeQuery<Optional<Number>> {

    /**
     * @param resourceTypeNames an array of types of resources to execute the query on
     * @return a MinQuery with the subTypeNames set
     */
    MinQuery of(TypeName... resourceTypeNames);

    /**
     * @param resourceTypeNames a collection of types of resources to execute the query on
     * @return a MinQuery with the subTypeNames set
     */
    MinQuery of(Collection<TypeName> resourceTypeNames);

    /**
     * @param subTypeNames an array of types to include in the subgraph
     * @return a MinQuery with the subTypeNames set
     */
    @Override
    MinQuery in(TypeName... subTypeNames);

    /**
     * @param subTypeNames a collection of types to include in the subgraph
     * @return a MinQuery with the subTypeNames set
     */
    @Override
    MinQuery in(Collection<TypeName> subTypeNames);

    /**
     * Execute the query.
     *
     * @return the min value if the given resource types have instances, otherwise an empty Optional instance
     */
    @Override
    Optional<Number> execute();

    /**
     * @param graph the graph to execute the query on
     * @return a MinQuery with the graph set
     */
    @Override
    MinQuery withGraph(GraknGraph graph);
}
