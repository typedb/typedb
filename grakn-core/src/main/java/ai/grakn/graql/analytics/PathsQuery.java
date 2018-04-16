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

/*-
 * #%L
 * grakn-core
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;

import java.util.Collection;
import java.util.List;

/**
 * Compute all the shortest path(s) between two instances.
 *
 * @author Jason Liu
 */
public interface PathsQuery extends ComputeQuery<List<List<Concept>>> {

    /**
     * @param sourceId the id of the source instance
     * @return a PathQuery with the source instance set
     */
    PathsQuery from(ConceptId sourceId);

    /**
     * Get the id of the source instance
     */
    ConceptId from();

    /**
     * @param destinationId the id of the destination instance
     * @return a PathQuery with the the destination instance set
     */
    PathsQuery to(ConceptId destinationId);

    /**
     * Get the id of the destination instance
     */
    ConceptId to();

    /**
     * @param subTypeLabels an array of types to include in the subgraph
     * @return a PathQuery with the subTypeLabels set
     */
    @Override
    PathsQuery in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the subgraph
     * @return a PathQuery with the subLabels set
     */
    @Override
    PathsQuery in(Collection<? extends Label> subLabels);

    /**
     * Execute the query.
     *
     * @return the list of shortest paths
     */
    @Override
    List<List<Concept>> execute();

    /**
     * @param tx the transaction to execute the query on
     * @return a PathQuery with the transaction set
     */
    @Override
    PathsQuery withTx(GraknTx tx);

    /**
     * Allow attributes and their relationships to be included.
     */
    @Override
    PathsQuery includeAttribute();
}
