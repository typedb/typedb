/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Compute the shortest path between two instances.
 *
 * @author Jason Liu
 */
public interface PathQuery extends ComputeQuery<Optional<List<Concept>>> {

    /**
     * @param sourceId the id of the source instance
     * @return a PathQuery with the source instance set
     */
    PathQuery from(ConceptId sourceId);

    /**
     * Get the id of the source instance
     */
    ConceptId from();

    /**
     * @param destinationId the id of the destination instance
     * @return a PathQuery with the the destination instance set
     */
    PathQuery to(ConceptId destinationId);

    /**
     * Get the id of the destination instance
     */
    ConceptId to();

    /**
     * @param subTypeLabels an array of types to include in the subgraph
     * @return a PathQuery with the subTypeLabels set
     */
    @Override
    PathQuery in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the subgraph
     * @return a PathQuery with the subLabels set
     */
    @Override
    PathQuery in(Collection<? extends Label> subLabels);

    /**
     * Execute the query.
     *
     * @return the list of shortest paths
     */
    @Override
    Optional<List<Concept>> execute();

    /**
     * @param tx the transaction to execute the query on
     * @return a PathQuery with the transaction set
     */
    @Override
    PathQuery withTx(GraknTx tx);

    /**
     * Allow attributes and their relationships to be included.
     */
    @Override
    PathQuery includeAttribute();
}
