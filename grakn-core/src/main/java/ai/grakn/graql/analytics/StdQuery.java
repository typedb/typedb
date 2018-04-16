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
import ai.grakn.graql.StatisticsQuery;

import java.util.Collection;
import java.util.Optional;

/**
 * Compute the standard deviation of the selected resource-type.
 *
 * @author Jason Liu
 */
public interface StdQuery extends StatisticsQuery<Optional<Double>> {

    /**
     * @param resourceTypeLabels an array of types of resources to execute the query on
     * @return a StdQuery with the subTypeLabels set
     */
    StdQuery of(String... resourceTypeLabels);

    /**
     * @param resourceLabels a collection of types of resources to execute the query on
     * @return a StdQuery with the subTypeLabels set
     */
    StdQuery of(Collection<Label> resourceLabels);

    /**
     * @param subTypeLabels an array of types to include in the subgraph
     * @return a StdQuery with the subTypeLabels set
     */
    @Override
    StdQuery in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the subgraph
     * @return a StdQuery with the subLabels set
     */
    @Override
    StdQuery in(Collection<? extends Label> subLabels);

    /**
     * Execute the query.
     *
     * @return the standard deviation if the given resource types have instances, otherwise an empty Optional instance
     */
    @Override
    Optional<Double> execute();

    /**
     * @param tx the transaction to execute the query on
     * @return a StdQuery with the transaction set
     */
    @Override
    StdQuery withTx(GraknTx tx);
}
