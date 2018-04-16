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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Compute the connected components in the sub graph.
 * <p>
 * https://en.wikipedia.org/wiki/Connected_component_(graph_theory)
 * </p>
 *
 * @param <T> the type of result this query will return
 * @author Jason Liu
 */
public interface ConnectedComponentQuery<T> extends ComputeQuery<T> {

    /**
     * Return the instances in each cluster after executing the query. By default, the size of each cluster is
     * returned after executing the query.
     *
     * @return a ConnectedComponentQuery with members flag set
     */
    ConnectedComponentQuery<Map<String, Set<String>>> membersOn();

    /**
     * Return the size of each cluster after executing the query.
     *
     * @return a ConnectedComponentQuery with members flag set
     */
    ConnectedComponentQuery<Map<String, Long>> membersOff();

    /**
     * Get whether this query will return the members of each cluster.
     */
    boolean isMembersSet();

    /**
     * Get the source ID, if one is set with {@link #of(ConceptId)}.
     */
    Optional<ConceptId> sourceId();

    /**
     * Return only the cluster containing the given concept after executing the query.
     *
     * @param conceptId The id of the given concept. conceptId is ignored if it's null.
     * @return a ConnectedComponentQuery
     */
    ConnectedComponentQuery<T> of(ConceptId conceptId);

    /**
     * @param clusterSize the size of the clusters returned
     * @return a ConnectedComponentQuery with cluster set
     */
    ConnectedComponentQuery<T> clusterSize(long clusterSize);

    /**
     * @param subTypeLabels an array of types to include in the sub graph
     * @return a ConnectedComponentQuery with the subTypeLabels set
     */
    @Override
    ConnectedComponentQuery<T> in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the sub graph
     * @return a ConnectedComponentQuery with the subLabels set
     */
    @Override
    ConnectedComponentQuery<T> in(Collection<? extends Label> subLabels);

    /**
     * @param tx the transaction to execute the query on
     * @return a ConnectedComponentQuery with the transaction set
     */
    @Override
    ConnectedComponentQuery<T> withTx(GraknTx tx);

    /**
     * Allow attributes and their relationships to be included.
     */
    @Override
    ConnectedComponentQuery<T> includeAttribute();

    /**
     * Get the size of the clusters returned
     */
    @Nullable
    Long clusterSize();
}
