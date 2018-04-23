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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.analytics;

import ai.grakn.API;
import ai.grakn.GraknTx;
import ai.grakn.concept.Label;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Compute centrality of concepts using k-core (coreness).
 * <p>
 * https://en.wikipedia.org/wiki/Degeneracy_(graph_theory)#k-Cores
 * </p>
 * Note that instances of relationship types will not be included in the result.
 *
 * @author Jason Liu
 */
public interface CorenessQuery extends ComputeQuery<Map<Long, Set<String>>> {

    /**
     * @param k set the min value of coreness in k-core. Be default, k = 2.
     * @return a CorenessQuery with min value of coreness set
     */
    CorenessQuery minK(long k);

    /**
     * Get the min value of coreness in k-core.
     */
    long minK();

    /**
     * @param ofTypeLabels an array of types in the subgraph to compute coreness of. By default the coreness of all
     *                     entities and attributes are computed.
     * @return a CorenessQuery with the subTypeLabels set
     */
    CorenessQuery of(String... ofTypeLabels);

    /**
     * @param ofLabels a collection of types in the subgraph to compute coreness of. By default the coreness of all
     *                 entities and attributes are computed.
     * @return a CorenessQuery with the subTypeLabels set
     */
     @API
     CorenessQuery of(Collection<Label> ofLabels);

    /**
     * Get the collection of types to execute the query on
     */
    Collection<? extends Label> ofTypes();

    /**
     * @param subTypeLabels an array of types to include in the subgraph.
     *                      By default CorenessQuery includes all entities, relationships and attributes.
     * @return a CorenessQuery with the subTypeLabels set
     */
    @Override
    CorenessQuery in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the subgraph.
     *                  By default CorenessQuery includes all entities, relationships and attributes.
     * @return a CorenessQuery with the inTypes set
     */
    @Override
    CorenessQuery in(Collection<? extends Label> subLabels);

    /**
     * @param tx the transaction to execute the query on
     * @return a CorenessQuery with the transaction set
     */
    @Override
    CorenessQuery withTx(GraknTx tx);
}
