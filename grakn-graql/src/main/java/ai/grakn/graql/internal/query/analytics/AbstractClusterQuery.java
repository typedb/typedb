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

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknTx;
import ai.grakn.graql.analytics.ComputeQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static ai.grakn.util.GraqlSyntax.COMMA_SPACE;
import static ai.grakn.util.GraqlSyntax.Compute.CLUSTER;
import static java.util.stream.Collectors.joining;

abstract class AbstractClusterQuery<T, V extends ComputeQuery<T>>
        extends AbstractComputeQuery<T, V> {
    AbstractClusterQuery(Optional<GraknTx> tx) {
        super(tx);
    }

    abstract String algorithmString();

    abstract String argumentsString();

    @Override
    final String methodString() {
        return CLUSTER;
    }

    @Override
    String conditionsString() {
        List<String> conditionsList = new ArrayList<>();

        if (!inTypes().isEmpty()) conditionsList.add(inTypesString());
        conditionsList.add(algorithmString());
        if (!argumentsString().isEmpty()) conditionsList.add(argumentsString());

        return conditionsList.stream().collect(joining(COMMA_SPACE));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AbstractClusterQuery that = (AbstractClusterQuery) o;

        return algorithmString() == that.algorithmString();
    }
}
