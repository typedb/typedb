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
import ai.grakn.graql.StatisticsQuery;

import java.util.Collection;
import java.util.Optional;

/**
 * Compute the mean of the selected resource-type.
 *
 * @author Jason Liu
 */
public interface MeanQuery extends StatisticsQuery<Optional<Double>> {

    /**
     * @param resourceTypeLabels an array of types of resources to execute the query on
     * @return a MeanQuery with the subTypeLabels set
     */
    MeanQuery of(String... resourceTypeLabels);

    /**
     * @param resourceLabels a collection of types of resources to execute the query on
     * @return a MeanQuery with the subTypeLabels set
     */
    MeanQuery of(Collection<Label> resourceLabels);

    /**
     * @param subTypeLabels an array of types to include in the subgraph
     * @return a MeanQuery with the subTypeLabels set
     */
    @Override
    MeanQuery in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the subgraph
     * @return a MeanQuery with the subLabels set
     */
    @Override
    MeanQuery in(Collection<? extends Label> subLabels);

    /**
     * Execute the query.
     *
     * @return the mean value if the given resource types have instances, otherwise an empty Optional instance
     */
    @Override
    Optional<Double> execute();

    /**
     * @param tx the transaction to execute the query on
     * @return a MeanQuery with the transaction set
     */
    @Override
    MeanQuery withTx(GraknTx tx);
}
