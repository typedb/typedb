/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.graql.Aggregate;
import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Aggregate that finds mean of a match query.
 */
class MeanAggregate extends AbstractAggregate<Map<VarName, Concept>, Optional<Double>> {

    private final VarName varName;
    private final CountAggregate countAggregate;
    private final Aggregate<Map<VarName, Concept>, Number> sumAggregate;

    MeanAggregate(VarName varName) {
        this.varName = varName;
        countAggregate = new CountAggregate();
        sumAggregate = Aggregates.sum(varName);
    }

    @Override
    public Optional<Double> apply(Stream<? extends Map<VarName, Concept>> stream) {
        List<? extends Map<VarName, Concept>> list = stream.collect(toList());

        long count = countAggregate.apply(list.stream());

        if (count == 0) {
            return Optional.empty();
        } else {
            Number sum = sumAggregate.apply(list.stream());
            return Optional.of(sum.doubleValue() / count);
        }
    }

    @Override
    public String toString() {
        return "mean " + varName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MeanAggregate that = (MeanAggregate) o;

        return varName.equals(that.varName);
    }

    @Override
    public int hashCode() {
        return varName.hashCode();
    }
}
