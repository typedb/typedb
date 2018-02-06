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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknComputer;
import ai.grakn.GraknTx;
import ai.grakn.graql.analytics.MeanQuery;
import ai.grakn.graql.internal.analytics.MeanMapReduce;

import java.util.Map;
import java.util.Optional;

class MeanQueryImpl extends AbstractStatisticsQuery<Optional<Double>, MeanQuery> implements MeanQuery {

    MeanQueryImpl(Optional<GraknTx> tx) {
        super(tx);
    }

    @Override
    protected final Optional<Double> innerExecute(GraknTx tx, GraknComputer computer) {
        Optional<Map<String, Double>> result = execWithMapReduce(tx, computer, MeanMapReduce::new);

        return result.map(meanPair ->
                meanPair.get(MeanMapReduce.SUM) / meanPair.get(MeanMapReduce.COUNT)
        );
    }

    @Override
    String getName() {
        return "mean";
    }
}
