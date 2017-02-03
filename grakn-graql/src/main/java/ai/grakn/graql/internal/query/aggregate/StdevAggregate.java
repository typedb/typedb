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
 *
 */

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Stream;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static java.util.stream.Collectors.toList;

/**
 * Aggregate that finds standard deviation of a match query.
 */
// TODO: Rename this to `std` to match analytics
class StdevAggregate extends AbstractAggregate<Map<VarName, Concept>, Optional<Double>> {

    private final VarName varName;

    StdevAggregate(VarName varName) {
        this.varName = varName;
    }

    @Override
    public Optional<Double> apply(Stream<? extends Map<VarName, Concept>> stream) {
        List<Double> numbers = stream
                .map(result -> result.get(varName).<Number>asResource().getValue().doubleValue())
                .collect(toList());

        OptionalDouble optAverage = numbers.stream().mapToDouble(x -> x).average();

        if (!optAverage.isPresent()) {
            return Optional.empty();
        }

        double average = optAverage.getAsDouble();

        OptionalDouble variance = numbers.stream().mapToDouble(x -> pow(x - average, 2)).average();

        if (!variance.isPresent()) {
            return Optional.empty();
        } else {
            return Optional.of(sqrt(variance.getAsDouble()));
        }
    }

    @Override
    public String toString() {
        return "stdev " + varName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StdevAggregate that = (StdevAggregate) o;

        return varName.equals(that.varName);
    }

    @Override
    public int hashCode() {
        return varName.hashCode();
    }
}
