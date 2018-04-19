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

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.graql.StatisticsQuery;

import java.util.Collection;
import java.util.Optional;

/**
 * Compute the maximum value of the selected resource-type.
 *
 * @author Jason Liu
 */
public interface MaxQuery extends StatisticsQuery<Optional<Number>> {

    /**
     * @param resourceTypeLabels an array of types of resources to execute the query on
     * @return a MaxQuery with the subTypeLabels set
     */
    @Override
    MaxQuery of(String... resourceTypeLabels);

    /**
     * @param resourceLabels a collection of types of resources to execute the query on
     * @return a MaxQuery with the subTypeLabels set
     */
    @Override
    MaxQuery of(Collection<Label> resourceLabels);

    /**
     * @param subTypeLabels an array of types to include in the subgraph
     * @return a MaxQuery with the subTypeLabels set
     */
    @Override
    MaxQuery in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the subgraph
     * @return a MaxQuery with the subLabels set
     */
    @Override
    MaxQuery in(Collection<? extends Label> subLabels);

    /**
     * Execute the query.
     *
     * @return the max value if the given resource types have instances, otherwise an empty Optional instance
     */
    @Override
    Optional<Number> execute();

    /**
     * @param tx the transaction to execute the query on
     * @return a MaxQuery with the transaction set
     */
    @Override
    MaxQuery withTx(GraknTx tx);
}
