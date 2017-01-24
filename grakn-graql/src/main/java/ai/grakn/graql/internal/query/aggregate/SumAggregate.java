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

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Aggregate that sums results of a match query.
 */
class SumAggregate extends AbstractAggregate<Map<VarName, Concept>, Number> {

    private final VarName varName;

    SumAggregate(VarName varName) {
        this.varName = varName;
    }

    @Override
    public Number apply(Stream<? extends Map<VarName, Concept>> stream) {
        return stream.map(result -> (Number) result.get(varName).asResource().getValue()).reduce(0, this::add);
    }

    private Number add(Number x, Number y) {
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
