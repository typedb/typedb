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
import ai.grakn.graql.ComputeQuery;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface DegreeQuery<T> extends ComputeQuery<T> {

    /**
     * Persist the result in the graph after executing the query.
     *
     * @return a DegreeQuery with persist flag set
     */
    DegreeQuery<String> persist();

    /**
     * @param subTypeNames an array of types to include in the subgraph
     * @return a DegreeQuery with the subTypeNames set
     */
    @Override
    DegreeQuery<T> in(String... subTypeNames);

    /**
     * @param subTypeNames a collection of types to include in the subgraph
     * @return a DegreeQuery with the subTypeNames set
     */
    @Override
    DegreeQuery<T> in(Collection<String> subTypeNames);

    /**
     * @param graph the graph to execute the query on
     * @return a DegreeQuery with the graph set
     */
    @Override
    DegreeQuery<T> withGraph(GraknGraph graph);
}
