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

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;

import java.util.stream.Stream;

/**
 * Aggregate that sums results of a {@link Match}.
 */
class SumAggregate extends AbstractAggregate<Number> {

    private final Var varName;

    SumAggregate(Var varName) {
        this.varName = varName;
    }

    @Override
    public Number apply(Stream<? extends ConceptMap> stream) {
        // initial value is set to null so that we can return null if there is no Answers to consume
        return stream.map(result -> (Number) result.get(varName).asAttribute().value()).reduce(null, this::add);
    }

    private Number add(Number x, Number y) {
        // if this method is called, then there is at least one number to apply SumAggregate to, thus we set x back to 0
        if (x == null) x = 0;

        // This method is necessary because Number doesn't support '+' because java!
        if (x instanceof Long || y instanceof Long) {
            return x.longValue() + y.longValue();
        } else {
            return x.doubleValue() + y.doubleValue();
        }
    }

    @Override
    public String toString() {
        return "sum " + varName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SumAggregate that = (SumAggregate) o;

        return varName.equals(that.varName);
    }

    @Override
    public int hashCode() {
        return varName.hashCode();
    }
}
