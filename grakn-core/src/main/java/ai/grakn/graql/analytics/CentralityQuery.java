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
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Compute centrality of concepts.
 * <p>
 * https://en.wikipedia.org/wiki/Centrality
 * </p>
 *
 * @author Jason Liu
 */
public interface CentralityQuery extends ComputeQuery<Map<Long, Set<String>>> {

    /**
     * The centrality measures supported.
     */
    enum CentralityMeasure {

        DEGREE("degree"),
        K_CORE("k-core");
        private final String measure;

        CentralityMeasure(String measure) {
            this.measure = measure;
        }

        @CheckReturnValue
        public String getMeasure() {
            return measure;
        }
    }

    /**
     * @param measure the specific centrality measure.
     * @return a CentralityQuery with the specific measure set
     */
    CentralityQuery using(CentralityMeasure measure);

    /**
     * @param ofTypeLabels an array of types in the subgraph to compute centrality of.
     * @return a CentralityQuery with the subTypeLabels set
     */
    CentralityQuery of(String... ofTypeLabels);

    /**
     * @param ofLabels a collection of types in the subgraph to compute centrality of. By default the centrality of all
     *                 entities and attributes are computed.
     * @return a CentralityQuery with the subTypeLabels set
     */
    CentralityQuery of(Collection<Label> ofLabels);

    /**
     * @param subTypeLabels an array of types to include in the subgraph.
     *                      By default CentralityQuery includes all entities, relationships and attributes.
     * @return a CentralityQuery with the subTypeLabels set
     */
    @Override
    CentralityQuery in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the subgraph.
     *                  By default CentralityQuery includes all entities, relationships and attributes.
     * @return a CentralityQuery with the subLabels set
     */
    @Override
    CentralityQuery in(Collection<Label> subLabels);

    /**
     * @param tx the transaction to execute the query on
     * @return a CentralityQuery with the transaction set
     */
    @Override
    CentralityQuery withTx(GraknTx tx);
}
