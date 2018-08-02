/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.graql.Aggregate;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.answer.Value;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.Math.sqrt;

/**
 * Aggregate that finds the unbiased sample standard deviation of a {@link Match}.
 */
class StdAggregate implements Aggregate<Value> {

    private final Var varName;

    StdAggregate(Var varName) {
        this.varName = varName;
    }

    @Override
    public List<Value> apply(Stream<? extends ConceptMap> stream) {
        Stream<Double> numStream = stream.map(result -> result.get(varName).<Number>asAttribute().value().doubleValue());

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
            return Collections.emptyList();
        } else {
            return Collections.singletonList(new Value(sqrt(M2 / (double) (n - 1))));
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
