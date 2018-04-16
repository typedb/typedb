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

/**
 * Count the number of instances in the subgraph.
 *
 * @author Jason Liu
 */

public interface CountQuery extends ComputeQuery<Long> {

    /**
     * @param subTypeLabels an array of types to include in the subgraph
     * @return a CountQuery with the subTypeLabels set
     */
    @Override
    CountQuery in(String... subTypeLabels);

    /**
     * @param subLabels a collection of types to include in the subgraph
     * @return a CountQuery with the subLabels set
     */
    @Override
    CountQuery in(Collection<? extends Label> subLabels);

    /**
     * Execute the query.
     *
     * @return the number of instances in the graph
     */
    @Override
    Long execute();

    /**
     * @param tx the transaction to execute the query on
     * @return a CountQuery with the transaction set
     */
    @Override
    CountQuery withTx(GraknTx tx);
}
