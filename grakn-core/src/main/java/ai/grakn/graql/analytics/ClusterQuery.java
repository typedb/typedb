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

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Compute the connected components in the sub graph.
 *
 * @param <T> the type of result this query will return
 * @author Jason Liu
 */
public interface ClusterQuery<T> extends ComputeQuery<T> {

    /**
     * Return the instances in each cluster after executing the query. By default, the size of each cluster is
     * returned after executing the query.
     *
     * @return a ClusterQuery with members flag set
     */
    ClusterQuery<Map<String, Set<String>>> members();

    /**
     * Return only the cluster containing the given concept after executing the query.
     *
     * @param conceptId The id of the give concept
     * @return a ClusterQuery
     */
    ClusterQuery<T> clusterSize(ConceptId conceptId);

    /**
     * @param clusterSize the size of the clusters returned and/or persisted
     * @return a ClusterQuery with cluster set
     */
    ClusterQuery<T> clusterSize(long clusterSize);

    /**
     * @param subTypeLabels an array of types to include in the sub graph
     * @return a ClusterQuery with the subTypeLabels set
     */
    @Override
    ClusterQuery<T> in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the sub graph
     * @return a ClusterQuery with the subLabels set
     */
    @Override
    ClusterQuery<T> in(Collection<Label> subLabels);

    /**
     * @param tx the transaction to execute the query on
     * @return a ClusterQuery with the transaction set
     */
    @Override
    ClusterQuery<T> withTx(GraknTx tx);

    /**
     * Allow attributes and their relationships to be included.
     */
    @Override
    ClusterQuery<T> includeAttribute();
}
