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

import javax.annotation.CheckReturnValue;

/**
 * Starting point for creating centrality queries.
 * <p>
 * https://en.wikipedia.org/wiki/Centrality
 * </p>
 * Currently, degree and k-core can be used to compute centrality.
 *
 * @author Jason Liu
 */
public interface CentralityQueryBuilder {

    /**
     * @param tx the {@link GraknTx} to execute the compute query on
     * @return a compute query builder with the graph set
     */
    @API
    @CheckReturnValue
    CentralityQueryBuilder withTx(GraknTx tx);

    /**
     * Compute centrality using k-core.
     *
     * @return a Coreness Query
     */
    @CheckReturnValue
    CorenessQuery usingKCore();

    /**
     * Compute centrality using degree.
     *
     * @return a Degree Query
     */
    @CheckReturnValue
    DegreeQuery usingDegree();
}
