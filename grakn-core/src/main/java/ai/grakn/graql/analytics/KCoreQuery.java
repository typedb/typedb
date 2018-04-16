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
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Compute interlinked core areas using k-core.
 * <p>
 * https://en.wikipedia.org/wiki/Degeneracy_(graph_theory)#k-Cores
 * </p>
 * Note that instances of relationship types will not be included in the result.
 *
 * @author Jason Liu
 */
public interface KCoreQuery extends ComputeQuery<Map<String, Set<String>>> {

    /**
     * @param k set the value of k in k-core. k can not be smaller than 2.
     * @return a KCoreQuery with k value set
     */
    KCoreQuery kValue(long k);

    /**
     * Get the value of k in k-core.
     */
    long kValue();

    /**
     * @param subTypeLabels an array of types to include in the subgraph.
     *                      By default KCoreQuery uses instances of all the types.
     * @return a KCoreQuery with the subTypeLabels set
     */
    @Override
    KCoreQuery in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the subgraph.
     *                  By default KCoreQuery uses instances of all the types.
     * @return a KCoreQuery with the subLabels set
     */
    @Override
    KCoreQuery in(Collection<? extends Label> subLabels);

    /**
     * @param tx the transaction to execute the query on
     * @return a KCoreQuery with the transaction set
     */
    @Override
    KCoreQuery withTx(GraknTx tx);

    /**
     * Allow attributes and their relationships to be included.
     */
    @Override
    KCoreQuery includeAttribute();
}
