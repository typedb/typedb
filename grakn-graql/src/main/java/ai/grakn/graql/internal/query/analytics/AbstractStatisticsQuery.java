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

import ai.grakn.API;
import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQueryOf;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;

abstract class AbstractStatisticsQuery<T, V extends ComputeQueryOf<T>>
        extends AbstractComputeQuery<T, V> implements ComputeQueryOf<T> {

    private ImmutableSet<Label> statisticsResourceLabels = ImmutableSet.of();

    AbstractStatisticsQuery(Optional<GraknTx> tx) {
        super(tx);
    }

    @API
    public V of(String... statisticsResourceTypeLabels) {
        return of(Arrays.stream(statisticsResourceTypeLabels).map(Label::of).collect(toImmutableSet()));
    }

    @API
    public V of(Collection<Label> statisticsResourceLabels) {
        this.statisticsResourceLabels = ImmutableSet.copyOf(statisticsResourceLabels);
        return (V) this;
    }

    public final Collection<? extends Label> ofLabels() {
        return statisticsResourceLabels;
    }

    @Override
    public boolean isStatisticsQuery() {
        return true;
    }

    @Override
    final String graqlString() {
        return getName() + resourcesString() + subtypeString();
    }

    abstract String getName();

    private String resourcesString() {
        return " of " + statisticsResourceLabels.stream()
                .map(StringConverter::typeLabelToString).collect(joining(", "));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AbstractStatisticsQuery<?, ?> that = (AbstractStatisticsQuery<?, ?>) o;

        return statisticsResourceLabels.equals(that.statisticsResourceLabels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + statisticsResourceLabels.hashCode();
        return result;
    }
}
