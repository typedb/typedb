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

import ai.grakn.API;
import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.graql.analytics.StatisticsQuery;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static ai.grakn.util.CommonUtil.toImmutableSet;
import static ai.grakn.util.GraqlSyntax.COMMA_SPACE;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.OF;
import static ai.grakn.util.GraqlSyntax.SPACE;
import static java.util.stream.Collectors.joining;

abstract class AbstractStatisticsQuery<T, V extends StatisticsQuery<T>>
        extends AbstractComputeQuery<T, V> implements StatisticsQuery<T> {

    private ImmutableSet<Label> ofTypes = ImmutableSet.of();

    private static final boolean INCLUDE_ATTRIBUTE = true;

    AbstractStatisticsQuery(Optional<GraknTx> tx) {
        super(tx, INCLUDE_ATTRIBUTE);
    }

    @API
    public final V of(String... statisticsResourceTypeLabels) {
        return of(Arrays.stream(statisticsResourceTypeLabels).map(Label::of).collect(toImmutableSet()));
    }

    @API
    public final V of(Collection<Label> statisticsResourceLabels) {
        this.ofTypes = ImmutableSet.copyOf(statisticsResourceLabels);
        return (V) this;
    }

    public final Collection<? extends Label> ofTypes() {
        return ofTypes;
    }

    @Override
    String conditionsString() {
        List<String> conditionsList = new ArrayList<>();

        if (!ofTypes().isEmpty()) conditionsList.add(ofTypesString());
        if (!inTypes().isEmpty()) conditionsList.add(inTypesString());

        return conditionsList.stream().collect(joining(COMMA_SPACE));
    }

    private String ofTypesString() {
        if (!ofTypes.isEmpty()) return OF + SPACE + typesString(ofTypes);

        return "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AbstractStatisticsQuery<?, ?> that = (AbstractStatisticsQuery<?, ?>) o;

        return ofTypes.equals(that.ofTypes);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + ofTypes.hashCode();
        result = 31 * result + inTypes.hashCode();
        return result;
    }
}
