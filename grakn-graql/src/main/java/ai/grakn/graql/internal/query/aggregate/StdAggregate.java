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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.Math.sqrt;

/**
 * Aggregate that finds the unbiased sample standard deviation of a match query.
 */
class StdAggregate extends AbstractAggregate<Map<VarName, Concept>, Optional<Double>> {

    private final VarName varName;

    StdAggregate(VarName varName) {
        this.varName = varName;
    }

    @Override
    public Optional<Double> apply(Stream<? extends Map<VarName, Concept>> stream) {
        Stream<Double> numStream = stream.map(result -> result.get(varName).<Number>asResource().getValue().doubleValue());

        Iterable<Double> data = numStream::iterator;

        // Online algorithm to calculate unbiased sample standard deviation
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
        long n = 0;
        double mean = 0d;
        double M2 = 0d;

        for (double x : data) {
            n += 1;
            double delta = x - mean;
            mean += delta / (double) n;
            double delta2 = x - mean;
            M2 += delta*delta2;
        }

        if (n < 2) {
            return Optional.empty();
        } else {
            return Optional.of(sqrt(M2 / (double) (n - 1)));
        }
    }

    @Override
    public String toString() {
        return "std " + varName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StdAggregate that = (StdAggregate) o;

        return varName.equals(that.varName);
    }

    @Override
    public int hashCode() {
        return varName.hashCode();
    }
}
