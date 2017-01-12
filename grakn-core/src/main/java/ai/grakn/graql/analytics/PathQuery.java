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
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.ComputeQuery;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Compute the shortest path between two instances.
 */
public interface PathQuery extends ComputeQuery<Optional<List<Concept>>> {

    /**
     * @param sourceId the id of the source instance
     * @return a PathQuery with the source instance set
     */
    PathQuery from(ConceptId sourceId);

    /**
     * @param destinationId the id of the destination instance
     * @return a PathQuery with the the destination instance set
     */
    PathQuery to(ConceptId destinationId);

    /**
     * @param subTypeNames an array of types to include in the subgraph
     * @return a PathQuery with the subTypeNames set
     */
    @Override
    PathQuery in(TypeName... subTypeNames);

    /**
     * @param subTypeNames a collection of types to include in the subgraph
     * @return a PathQuery with the subTypeNames set
     */
    @Override
    PathQuery in(Collection<TypeName> subTypeNames);

    /**
     * Execute the query.
     *
     * @return the list of instances along the path if a path exists, otherwise an empty Optional instance
     */
    @Override
    Optional<List<Concept>> execute();

    /**
     * @param graph the graph to execute the query on
     * @return a PathQuery with the graph set
     */
    @Override
    PathQuery withGraph(GraknGraph graph);
}
